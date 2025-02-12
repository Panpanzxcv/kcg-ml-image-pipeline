import argparse
from io import BytesIO
import math
import os
import sys
from matplotlib import pyplot as plt
import numpy as np
import faiss
from tqdm import tqdm

base_dir = "./"
sys.path.insert(0, base_dir)
sys.path.insert(0, os.getcwd())

from data_loader.kandinsky_dataset_loader import KandinskyDatasetLoader
from utility.minio import cmd

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--minio-access-key', type=str, help='Minio access key')
    parser.add_argument('--minio-secret-key', type=str, help='Minio secret key')
    parser.add_argument('--dataset', type=str, help='Name of the dataset', default="environmental")
    parser.add_argument('--target-avg-points', type=int, help='Target average of datapoints per sphere', 
                        default=5)
    parser.add_argument('--n-spheres', type=int, help='Number of spheres', default=100000)
    parser.add_argument('--num-bins', type=int, help='Number of score bins', default=8)
    parser.add_argument('--bin-size', type=int, help='Range of each bin', default=1)

    return parser.parse_args()

class UniformSphereGenerator:
    def __init__(self,
                 minio_client,
                 dataset):
        
        self.dataloader= KandinskyDatasetLoader(minio_client=minio_client,
                                                dataset=dataset)
        self.scores=[]
        self.feature_vectors=[]
        self.max_vector=None
        self.min_vector=None
        
        self.minio_client= minio_client
        self.dataset= dataset

    def load_data(self):
        # load data from mongodb
        feature_vectors, scores= self.dataloader.load_clip_vector_data()
        # set features vectors and scores
        self.feature_vectors= np.array(feature_vectors, dtype='float32')
        self.scores = scores
        # set min and max vectors
        self.max_vector = np.max(feature_vectors, axis=0)
        self.min_vector = np.min(feature_vectors, axis=0)

    def generate_spheres(self, n_spheres, target_avg_points, output_type="score_distribution", num_bins=8, bin_size=1, discard_threshold=None):
        # checking if data has been loaded
        if len(self.feature_vectors)==0:
            raise Exception("You must load datapoints first before generating spheres.")
        
        print("generating the initial spheres-------------")
        # Generate random values between 0 and 1, then scale and shift them into the [min, max] range for each feature
        sphere_centers = np.random.rand(n_spheres, len(self.max_vector)) * (self.max_vector - self.min_vector) + self.min_vector
        # Convert sphere_centers to float32
        sphere_centers = sphere_centers.astype('float32')

        res = faiss.StandardGpuResources()

        d = self.feature_vectors.shape[1]
        # build a flat (CPU) index
        index_flat = faiss.IndexFlatL2(d)
        # make it into a gpu index
        index = faiss.index_cpu_to_gpu(res, 0, index_flat)

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
            valid_radii = radii[valid_mask]
            indices = indices[valid_mask]
        else:
            valid_centers = sphere_centers
            valid_radii = radii

        print("Processing sphere data-------------")
        # Prepare to collect sphere data and statistics
        sphere_data = []
        total_covered_points = set()
        inputs=[]
        targets=[]
        
        # prepare array for score bins
        if output_type =="score_distribution":
            bins=[]
            for i in range(num_bins-1):
                max_score= int((i+1-(num_bins/2)) * bin_size)
                bins.append(max_score)
            
            bins.append(np.inf)

        # Assuming 'scores' contains the scores for all points and 'bins' defines the score bins
        for center, radius, sphere_indices in zip(valid_centers, valid_radii, indices):
            # Extract indices and scores of points within the sphere
            point_indices = sphere_indices
            sphere_scores=[self.scores[idx] for idx in point_indices]

            if output_type=="score_distribution":            
                # Calculate score distribution for the sphere
                score_distribution = np.zeros(len(bins))
                for score in sphere_scores:
                    for i, bin_edge in enumerate(bins):
                        if score < bin_edge:
                            score_distribution[i] += 1
                            break

                # Normalize the score distribution by the number of points in the sphere
                target = score_distribution / len(point_indices)
            elif output_type=="mean_sigma_score":
                target= np.mean(sphere_scores)
            elif output_type=="variance":
                target= np.var(sphere_scores)
            
            # Update inputs and targets
            inputs.append(np.append(center, math.sqrt(radius)))
            targets.append(target)
            total_covered_points.update(point_indices)

        print(f"total datapoints: {len(total_covered_points)}")
        
        return inputs, targets

    def plot(self, sphere_data, points_per_sphere, n_spheres, scores):
        fig, axs = plt.subplots(1, 3, figsize=(24, 8))  # Adjust for three subplots
        
        # Calculate mean scores as before
        mean_scores = [np.mean([scores[j] for j in sphere_data[i]['points']]) if sphere_data[i] else 0 for i in range(n_spheres)]
        sphere_radii= [data['radius'] for data in sphere_data]
        
        # Histogram of Points per Sphere
        axs[0].hist(points_per_sphere, color='skyblue', bins=np.arange(min(points_per_sphere), max(points_per_sphere) + 1, 1))
        axs[0].set_xlabel('Number of Points')
        axs[0].set_ylabel('Frequency')
        axs[0].set_title('Distribution of Points per Sphere')
        
        # Histogram of Mean Scores
        axs[1].hist(mean_scores, color='lightgreen', bins=20)  # Adjust bins as needed
        axs[1].set_xlabel('Mean Score')
        axs[1].set_ylabel('Frequency')
        axs[1].set_title('Distribution of Mean Scores')

        # Histogram of Sphere Radii
        axs[2].hist(sphere_radii, color='lightcoral', bins=20)  # Adjust bins as needed
        axs[2].set_xlabel('Sphere Radii')
        axs[2].set_ylabel('Frequency')
        axs[2].set_title('Distribution of Sphere Radii')
        
        plt.tight_layout()

        # Save the figure to a file
        buf = BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)

        # Upload the graph report
        # Ensure cmd.upload_data(...) is appropriately defined to handle your MinIO upload.
        cmd.upload_data(self.minio_client, 'datasets', "environmental/output/sphere_dataset/graphs.png", buf)  

        # Clear the current figure to prevent overlap with future plots
        plt.clf()

def main():
    args= parse_args()

    # get minio client
    minio_client = cmd.get_minio_client(minio_access_key=args.minio_access_key,
                                        minio_secret_key=args.minio_secret_key)

    generator= UniformSphereGenerator(minio_client=minio_client,
                                    dataset=args.dataset)
    
    inputs, outputs = generator.generate_spheres(num_bins= args.num_bins,
                                                    bin_size= args.bin_size,
                                                    n_spheres=args.n_spheres,
                                                    target_avg_points= args.target_avg_points)
    
    
if __name__ == "__main__":
    main()


