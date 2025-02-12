import argparse
from io import BytesIO
import math
import os
import sys
from matplotlib import pyplot as plt
import numpy as np
import torch
import faiss
import faiss.contrib.torch_utils
from tqdm import tqdm
from sklearn.mixture import GaussianMixture
from datetime import datetime

base_dir = "./"
sys.path.insert(0, base_dir)
sys.path.insert(0, os.getcwd())

from data_loader.kandinsky_dataset_loader import KandinskyDatasetLoader
from utility.minio import cmd

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--minio-access-key', type=str, help='Minio access key')
    parser.add_argument('--minio-secret-key', type=str, help='Minio secret key')
    parser.add_argument('--minio-addr', type=str, help='Minio address')
    parser.add_argument('--dataset', type=str, help='Name of the dataset', default="environmental")
    parser.add_argument('--target-avg-points', type=int, help='Target average of datapoints per sphere', 
                        default=5)
    parser.add_argument('--percentile', type=int, help='Percentile', default=75)
    parser.add_argument('--std', type=float, help='Standard deviation', default=1)
    parser.add_argument('--n-spheres', type=int, help='Number of spheres', default=100000)
    parser.add_argument('--num-bins', type=int, help='Number of score bins', default=8)
    parser.add_argument('--bin-size', type=int, help='Range of each bin', default=1)

    return parser.parse_args()

class SphericalGaussianGenerator:
    def __init__(self,
                 minio_client,
                 dataset):
        
        self.dataloader= KandinskyDatasetLoader(minio_client=minio_client,
                                                dataset=dataset)
        
        self.minio_client= minio_client
        self.dataset= dataset

    def load_dataset(self):

        # load data from mongodb
        self.feature_vectors, self.scores= self.dataloader.load_clip_vector_data()
        self.feature_vectors= np.array(self.feature_vectors, dtype='float32')

    def generate_spheres(self, n_spheres, target_avg_points, num_bins, bin_size, percentile, std, discard_threshold=None):
     
        # Calculate max and min vectors
        max_vector = np.max(self.feature_vectors, axis=0)
        min_vector = np.min(self.feature_vectors, axis=0)

        bins=[]
        for i in range(num_bins-1):
            max_score= int((i+1-(num_bins/2)) * bin_size)
            bins.append(max_score)
        
        bins.append(np.inf)

        print("generating the initial spheres-------------")
        # Generate random values between 0 and 1, then scale and shift them into the [min, max] range for each feature
        sphere_centers = np.random.rand(n_spheres, len(max_vector)) * (max_vector - min_vector) + min_vector
        # Convert sphere_centers to float32
        sphere_centers = sphere_centers.astype('float32')

        d = self.feature_vectors.shape[1]
        # remove
        nlist = 50  # how many cells
        quantizer = faiss.IndexFlatL2(d)
        cpu_index = faiss.IndexIVFFlat(quantizer, d, nlist)

        cpu_index = faiss.IndexFlatL2(d)
        
        if torch.cuda.is_available():
            res = faiss.StandardGpuResources()
            index = faiss.index_cpu_to_gpu(res, 0, cpu_index)
        
        index.train(self.feature_vectors)
        index.add(self.feature_vectors)
        
        print("Searching for k nearest neighbors for each sphere center-------------")
        # Search for the k nearest neighbors of each sphere center in the dataset
        distances, indices = index.search(sphere_centers, target_avg_points)

        # The radius of each sphere is the distance to the k-th nearest neighbor
        radii = distances[:, -1]
        
        # Determine which spheres to keep based on the discard threshold
        if discard_threshold is not None:
            valid_mask = radii < discard_threshold
            valid_centers = sphere_centers[valid_mask]
            valid_distances = distances[valid_mask]
            indices = indices[valid_mask]
        else:
            valid_distances = distances
            valid_centers = sphere_centers

        valid_distances = valid_distances ** 0.5
        
        print("Processing sphere data-------------")
        # Prepare to collect sphere data and statistics
        sphere_data = []
        total_covered_points = set()

        # Assuming 'scores' contains the scores for all points and 'bins' defines the score bins
        for center, distance_vector, sphere_indices in tqdm(zip(valid_centers, valid_distances, indices), total=len(valid_centers)):
            # Extract indices of points within the sphere
            point_indices = sphere_indices
            
            d = np.percentile(distance_vector, percentile)
            variance = (d / std) ** 2
            sigma = d / std
            fall_off = 2 * np.sqrt(2 * np.log(2)) * sigma

            # Calculate score distribution for the sphere
            score_distribution = np.zeros(len(bins))
            sum_weights = .0
            sphere_scores=[]
            for i, idx in enumerate(point_indices, 0):
                score = self.scores[idx]
                sphere_scores.append(score)
                weight = gaussian_pdf(distance_vector[i], variance)
                sum_weights += weight

                for i, bin_edge in enumerate(bins):
                    if score < bin_edge:
                        score_distribution[i] += weight
                        break

            # Normalize the score distribution by the number of points in the sphere
            if len(point_indices) > 0:
                score_distribution = score_distribution / sum_weights
            
            # Update sphere data and covered points
            sphere_data.append({
                'center': center, 
                'gaussian_sphere_variance': variance,
                'gaussian_sphere_sigma': sigma,
                'gaussian_sphere_fall_off': fall_off,
                'mean_sigma_score': np.mean(sphere_scores), 
                'variance': np.var(sphere_scores),
                'points': point_indices, 
                "score_distribution": score_distribution
            })
            total_covered_points.update(point_indices)
        
        # Calculate statistics
        points_per_sphere = [len(sphere['points']) for sphere in sphere_data]
        avg_points_per_sphere = np.mean(points_per_sphere) if points_per_sphere else 0

        print(f"total datapoints: {len(total_covered_points)}")
        print(f"average points per sphere: {avg_points_per_sphere}")
        
        self.plot(sphere_data, points_per_sphere, n_spheres, self.scores, percentile, std, target_avg_points, num_bins, bin_size)

        return sphere_data, avg_points_per_sphere, len(total_covered_points)


    def load_sphere_dataset(self, n_spheres, target_avg_points, num_bins=8, bin_size=1, percentile=75, std=1, output_type="score_distribution", input_type="guassian_sphere_variance"):
        # generating spheres
        sphere_data, avg_points_per_sphere, total_covered_points= self.generate_spheres(n_spheres=n_spheres,
                                                       target_avg_points=target_avg_points,
                                                       num_bins=num_bins,
                                                       bin_size=bin_size,
                                                       percentile=percentile,
                                                       std=std)
        
        inputs=[]
        outputs=[]
        for sphere in sphere_data:
            # get input vectors
            
            inputs.append(np.concatenate([sphere['center'], [sphere[input_type]]]))
            # get score distribution
            outputs.append(sphere[output_type])

        return inputs, outputs 

    def plot(self, sphere_data, points_per_sphere, n_spheres, scores, percentile, std, target_avg_points, num_bins, bin_size):
        fig, axs = plt.subplots(1, 3, figsize=(24, 8))  # Adjust for three subplots
        
        # Calculate mean scores as before
        mean_scores = [np.mean([scores[j] for j in sphere_data[i]['points']]) if sphere_data[i] else 0 for i in range(n_spheres)]
        sphere_variance= [data['variance'] for data in sphere_data]
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        # info text about the model
        plt.figtext(0.02, 0.7, "Date = {}"
                                "\n Number of Spheres = {}"
                                "\n Number of Points = {}"
                                "\n Percentile = {}"
                                "\n std = {}"
                                "\n target average points = {}"
                                "\n number of bins = {}"
                                "\n size of bin = {}".format(current_time,
                                                n_spheres,
                                                len(scores),
                                                percentile,
                                                std,
                                                target_avg_points,
                                                num_bins,
                                                bin_size))

        # Histogram of Points per Sphere
        axs[0].hist(points_per_sphere, color='skyblue', bins=np.arange(min(points_per_sphere)-1, max(points_per_sphere) + 1, 1))
        axs[0].set_xlabel('Number of Points')
        axs[0].set_ylabel('Frequency')
        axs[0].set_title('Distribution of Points per Sphere')
        
        # Histogram of Mean Scores
        axs[1].hist(mean_scores, color='lightgreen', bins=20)  # Adjust bins as needed
        axs[1].set_xlabel('Mean Score')
        axs[1].set_ylabel('Frequency')
        axs[1].set_title('Distribution of Mean Scores')

        # Histogram of Sphere Variance
        axs[2].hist(sphere_variance, color='lightcoral', bins=20)  # Adjust bins as needed
        axs[2].set_xlabel('Sphere Variance')
        axs[2].set_ylabel('Frequency')
        axs[2].set_title('Distribution of Sphere variance')
        
        # Adjust spacing between subplots
        plt.subplots_adjust(hspace=0.7, wspace=0.3, left=0.3)
        

        # Save the figure to a file
        buf = BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)

        # Upload the graph report
        # Ensure cmd.upload_data(...) is appropriately defined to handle your MinIO upload.
        
        cmd.upload_data(self.minio_client, 'datasets', f"environmental/output/sphere_dataset/{current_time}_graphs_percentile_{percentile}%_std_{std}.png", buf)  

        # Clear the current figure to prevent overlap with future plots
        plt.clf()

def gaussian_pdf(distance, variance):
    denom = (2*np.pi*variance)**.5
    num = np.exp(-float(distance)**2/(2*variance))
    return num/denom

def main():
    args= parse_args()

    # get minio client
    minio_client = cmd.get_minio_client(minio_access_key=args.minio_access_key,
                                        minio_secret_key=args.minio_secret_key,
                                        minio_ip_addr=args.minio_addr)

    generator= SphericalGaussianGenerator(minio_client=minio_client,
                                    dataset=args.dataset)
    
    inputs, outputs = generator.load_sphere_dataset(num_bins= args.num_bins,
                                                    bin_size= args.bin_size,
                                                    n_spheres=args.n_spheres,
                                                    target_avg_points= args.target_avg_points,
                                                    percentile=args.percentile,
                                                    std = args.std)
    
    
if __name__ == "__main__":
    main()


