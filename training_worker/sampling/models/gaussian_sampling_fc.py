from datetime import datetime
from io import BytesIO
import os
import sys
import tempfile
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import numpy as np
import torch
import time
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset
from torch.utils.data import random_split
from torch.utils.data.dataloader import DataLoader
from sklearn.metrics import confusion_matrix
import seaborn as sb
from sklearn.metrics import classification_report
from enum import Enum

base_directory = "./"
sys.path.insert(0, base_directory)
from training_worker.sampling.scripts.spherical_gaussian_sampling_dataset import SphericalGaussianGenerator
from utility.minio import cmd

class DatasetLoader(Dataset):
    def __init__(self, features, labels):
        """
        Initialize the dataset with features and labels.
        :param features: A NumPy array of the input features.
        :param labels: A NumPy array of the corresponding labels.
        """
        # Convert the data to torch.FloatTensor as it is the standard data type for floats in PyTorch
        self.features = torch.FloatTensor(np.array(features))
        self.labels = torch.FloatTensor(np.array(labels))

    def __len__(self):
        """
        Return the total number of samples in the dataset.
        """
        return len(self.features)

    def __getitem__(self, idx):
        """
        Retrieve the features and label of the sample at the given index.
        :param idx: The index of the sample.
        """
        sample_features = self.features[idx]
        sample_label = self.labels[idx]
        return sample_features, sample_label


class SamplingFCNetwork(nn.Module):
    def __init__(self, minio_client, input_size=1281, hidden_sizes=[512, 256], input_type="gaussian_sphere_variance" , output_size=8, 
                 bin_size=1, output_type="score_distribution", dataset="environmental"):
        super(SamplingFCNetwork, self).__init__()
        # set device
        if torch.cuda.is_available():
            device = 'cuda'
        else:
            device = 'cpu'
        self._device = torch.device(device)

        # Define the multi-layered model architecture
        layers = [nn.Linear(input_size, hidden_sizes[0])]
        for i in range(len(hidden_sizes)-1):
            layers.append(nn.ReLU())
            layers.append(nn.Linear(hidden_sizes[i], hidden_sizes[i+1]))
        
        # Adjusting the last layer to use LogSoftmax for KLDivLoss compatibility
        layers += [nn.Linear(hidden_sizes[-1], output_size), nn.LogSoftmax(dim=1)]

        # Combine all layers into a sequential model
        self.model = nn.Sequential(*layers).to(self._device)
        self.input_size= input_size
        self.output_size= output_size
        self.bin_size= bin_size
        self.minio_client= minio_client
        self.input_type= input_type
        self.output_type= output_type
        self.dataset=dataset
        self.date = datetime.now().strftime("%Y_%m_%d")
        self.local_path, self.minio_path=self.get_model_path()
        self.class_labels= self.get_class_labels()
        self.metadata = None
        
        self.dataloader = SphericalGaussianGenerator(minio_client, dataset)

    def set_config(self, sampling_parameter= None):
        self.sampling_parameter = sampling_parameter

    def get_model_path(self):
        local_path=f"output/{self.output_type}_fc_{self.input_type}.pth"
        minio_path=f"{self.dataset}/models/sampling/{self.date}_{self.output_type}_fc_{self.input_type}.pth"

        return local_path, minio_path

    def get_class_labels(self):
        output_size= self.output_size
        bin_size= self.bin_size

        class_labels=[]
        for i in range(0, output_size):
            # calculate min and max for bin
            min_score_value= int((i-(output_size/2)) * bin_size)
            max_score_value= int(min_score_value + bin_size)
            # get label str values
            if i==0:
                class_label= f"<{max_score_value}"
            elif i == output_size-1:
                class_label= f">{min_score_value}"
            else:
                class_label= f"[{min_score_value},{max_score_value}]"

            class_labels.append(class_label)

        return class_labels 

    def train(self, n_spheres, target_avg_points, learning_rate=0.001, validation_split=0.2, num_epochs=100, batch_size=256, is_per_epoch=False):

        # load the dataset depends on sampling type
        self.dataloader.load_dataset()

        criterion = nn.KLDivLoss(reduction='batchmean')  # Using KLDivLoss
        optimizer = optim.Adam(self.parameters(), lr=learning_rate)  # Define the optimizer

        # save loss for each epoch and features
        train_loss=[]
        val_loss=[]

        best_state = {
            "val_loss": float('inf'), # Initialize best validation loss as infinity
            "train_loss": float('inf') # Initialize best training loss as infinity
        }

        start = time.time()
        best_model_state = self.model
        # Training and Validation Loop
        for epoch in range(num_epochs):
            self.model.eval()
            total_val_loss = 0
            total_val_samples = 0
            
            if epoch == 0 or is_per_epoch:
                train_dataset, val_dataset, \
                    train_loader, val_loader, \
                        train_size, val_size = self.get_data_for_training(n_spheres, target_avg_points, validation_split, batch_size)

            with torch.no_grad():
                for inputs, targets in val_loader:
                    inputs=inputs.to(self._device)
                    targets=targets.to(self._device)

                    outputs = self.model(inputs)
                    loss = criterion(outputs, targets)

                    total_val_loss += loss.item() * inputs.size(0)
                    total_val_samples += inputs.size(0)
                    
            self.model.train()
            total_train_loss = 0
            total_train_samples = 0
            
            for inputs, targets in train_loader:
                inputs=inputs.to(self._device)
                targets=targets.to(self._device)

                optimizer.zero_grad()
                outputs = self.model(inputs)
                loss = criterion(outputs, targets)
                
                loss.backward()
                optimizer.step()

                total_train_loss += loss.item() * inputs.size(0)
                total_train_samples += inputs.size(0)

            avg_train_loss = total_train_loss / total_train_samples
            avg_val_loss = total_val_loss / total_val_samples
            train_loss.append(avg_train_loss)
            val_loss.append(avg_val_loss)

            # Update best model if current epoch's validation loss is the best
            if val_loss[-1] < best_state["val_loss"]:
                best_state = {
                    "model": self.model,
                    "epoch": epoch,
                    "train_dataset": train_dataset,
                    "val_dataset": val_dataset,
                    "train_size": train_size,
                    "val_size": val_size,
                    "train_loss": train_loss[-1],
                    "val_loss": val_loss[-1],
                }
            print(f'Epoch {epoch+1}/{num_epochs}, Train Loss: {avg_train_loss}, Val Loss: {avg_val_loss}')
        
        self.model= best_state["model"]
        
        end = time.time()
        training_time= end - start

        start = time.time()
        # Classifying all validation datapoints
        val_preds, val_true, val_residuals = self.classify(best_state["val_dataset"], batch_size)
        _, _, train_residuals = self.classify(best_state["train_dataset"], batch_size)

        end = time.time()
        inference_speed=(best_state["train_size"] + best_state["val_size"])/(end - start)
        print(f'Time taken for inference of {best_state["train_size"] + best_state["val_size"]} data points is: {end - start:.2f} seconds')

        # val_residuals = np.array(true_val_avg_scores) - np.array(pred_val_avg_scores)
        # train_residuals = np.array(true_train_avg_scores) - np.array(pred_train_avg_scores)
        
        self.save_graph_report(train_loss, val_loss,
                               best_state["train_loss"], best_state["val_loss"], 
                               val_residuals, train_residuals,
                               train_size, val_size, best_state["epoch"])
        
        # self.save_confusion_matrix(val_true, val_preds)
        
        self.save_model_report(num_training=train_size,
                              num_validation=val_size,
                              training_time=training_time,
                              y_pred=val_preds, 
                              y_true=val_true,
                              train_loss=best_state["train_loss"], 
                              val_loss=best_state["val_loss"], 
                              inference_speed= inference_speed,
                              learning_rate=learning_rate, best_model_epoch=best_state["epoch"])
        
        self.save_metadata(inputs, target_avg_points, learning_rate, num_epochs, batch_size)
        
        return best_state["val_loss"]
        

    def get_data_for_training(self, n_spheres, target_avg_points, validation_split, batch_size):
        
        # load the dataset depends on sampling type
        inputs, outputs = self.dataloader.load_sphere_dataset(n_spheres,target_avg_points, self.output_size, self.bin_size, self.sampling_parameter["percentile"], self.sampling_parameter["std"], self.output_type, self.input_type)
        
        dataset= DatasetLoader(features=inputs, labels=outputs)
        # Split dataset into training and validation
        val_size = int(len(dataset) * validation_split)
        train_size = len(dataset) - val_size
        train_dataset, val_dataset = random_split(dataset, [train_size, val_size])

        # Create data loaders
        train_loader = DataLoader(dataset=train_dataset, batch_size=batch_size, shuffle=True, drop_last=True)
        val_loader = DataLoader(dataset=val_dataset, batch_size=batch_size, shuffle=True, drop_last=True)

        return train_dataset, val_dataset, train_loader, val_loader, val_size, train_size
    

    def save_model_report(self,num_training,
                              num_validation,
                              training_time,
                              y_true,
                              y_pred, 
                              train_loss, 
                              val_loss, 
                              inference_speed,
                              learning_rate,
                              best_model_epoch):
        input_type="[input_clip_vector[1280], {}(float)]".format(self.input_type)

        report_text = (
            "================ Model Report ==================\n"
            f"Number of training datapoints: {num_training} \n"
            f"Number of validation datapoints: {num_validation} \n"
            f"Total training Time: {training_time:.2f} seconds\n"
            "Loss Function: L1 \n"
            f"Epoch of best model: {best_model_epoch} \n"
            f"Learning Rate: {learning_rate} \n"
            f"Training Loss: {train_loss} \n"
            f"Validation Loss: {val_loss} \n"
            f"Inference Speed: {inference_speed:.2f} predictions per second\n\n"
            "================ Input and output ==================\n"
            f"Input: {input_type} \n"
            f"Input Size: {self.input_size} \n" 
            f"Output: {self.output_type} \n\n"
        )

        # Add Sampling Method Parameter
        report_text += (
            f"================ Sampling Policy  ==================\n"
            f"type: {self.input_type}"
        )
        if self.sampling_parameter is not None:
            for key, value in zip(self.sampling_parameter.keys(), self.sampling_parameter.values()):
                report_text += (
                    f"{key}: {value}\n"
                )
        else:
            report_text += "No Sampling Parameter"

        # Define the local file path for the report
        local_report_path = 'output/model_report.txt'

        # Save the report to a local file
        with open(local_report_path, 'w') as report_file:
            report_file.write(report_text)

        # Read the contents of the local file
        with open(local_report_path, 'rb') as file:
            content = file.read()

        # Upload the local file to MinIO
        buffer = BytesIO(content)
        buffer.seek(0)

        cmd.upload_data(self.minio_client, 'datasets', self.minio_path.replace('.pth', '.txt'), buffer)

        # Remove the temporary file
        os.remove(local_report_path)

    def save_graph_report(self, train_loss_per_round, val_loss_per_round,
                          best_train_loss, best_val_loss, 
                          val_residuals, train_residuals,
                          training_size, validation_size, best_model_epoch):
        fig, axs = plt.subplots(3, 1, figsize=(12, 10))
        
        #info text about the model
        fig_report_text = (
            "Date = {}\n"
            "Dataset = {}\n"
            "Model type = {}\n"
            "Input type = {}\n"
            "Input shape = {}\n"
            "Output type= {}\n\n"
            ""
            "Training size = {}\n"
            "Validation size = {}\n"
            "Training loss = {:.4f}\n"
            "Validation loss = {:.4f}\n"
            "Epoch of Best model = {}\n"
            "\n".format(self.date,
                                            self.dataset,
                                            'Fc_Network',
                                            self.input_type,
                                            self.input_size,
                                            self.output_type,
                                            training_size,
                                            validation_size,
                                            best_train_loss,
                                            best_val_loss,
                                            best_model_epoch
        ))

        fig_report_text += (
            "Sampling Policy: {}\n".format(self.input_type)
        )
        if self.sampling_parameter is not None:
            for key, value in zip(self.sampling_parameter.keys(), self.sampling_parameter.values()):
                fig_report_text += (
                    f"{key}: {value}\n"
                )
        else:
            fig_report_text += "No Sampling Parameter"

        plt.figtext(0.02, 0.7, fig_report_text)
        
        # Plot validation and training Rmse vs. Rounds
        axs[0].plot(range(1, len(train_loss_per_round) + 1), train_loss_per_round,'b', label='Training loss')
        axs[0].plot(range(1, len(val_loss_per_round) + 1), val_loss_per_round,'r', label='Validation loss')
        axs[0].set_title('KL loss per Round')
        axs[0].set_ylabel('Loss')
        axs[0].set_xlabel('Epochs')
        axs[0].legend(['Training loss', 'Validation loss'])

        # plot histogram of training residuals
        axs[1].hist(train_residuals, bins=30, color='blue', alpha=0.7)
        axs[1].set_xlabel('Residuals')
        axs[1].set_ylabel('Frequency')
        axs[1].set_title('Training Residual Histogram')

        # plot histogram of validation residuals
        axs[2].hist(val_residuals, bins=30, color='blue', alpha=0.7)
        axs[2].set_xlabel('Residuals')
        axs[2].set_ylabel('Frequency')
        axs[2].set_title('Validation Residual Histogram')

        # Adjust spacing between subplots
        plt.subplots_adjust(hspace=0.7, wspace=0.3, left=0.3)

        plt.savefig(self.local_path.replace('.pth', '.png'))

        # Save the figure to a file
        buf = BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)

        # upload the graph report
        cmd.upload_data(self.minio_client, 'datasets', self.minio_path.replace('.pth', '.png'), buf)  

        # Clear the current figure
        plt.clf()


    def predict(self, data, batch_size=64):
        # Convert the features array into a PyTorch Tensor
        features_tensor = torch.Tensor(np.array(data)).to(self._device)

        # Ensure the model is in evaluation mode
        self.model.eval()

        # List to hold all predictions
        predictions = []

        # Perform prediction in batches
        with torch.no_grad():
            for i in range(0, len(features_tensor), batch_size):
                batch = features_tensor[i:i + batch_size]  # Extract a batch
                outputs = self.model(batch)  # Get predictions for this batch
                predictions.append(torch.exp(outputs))

        # Concatenate all predictions and convert to a NumPy array
        predictions = torch.cat(predictions, dim=0).cpu().numpy()

        return predictions         


    def classify(self, dataset, batch_size=64):
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=False)
        self.model.eval()  # Set the model to evaluation mode
        predictions = []
        true_values = []
        with torch.no_grad():
            for inputs, targets in loader:
                inputs= inputs.to(self._device)
                outputs = self.model(inputs)
                true_values.append(targets)
                predictions.append(torch.exp(outputs))
        
        # Concatenate all predictions and convert to a NumPy array
        predictions = torch.cat(predictions, dim=0).cpu().numpy()
        true_values = torch.cat(true_values, dim=0).cpu().numpy()

        pred_labels=[]
        true_labels=[]

        residuals=[]
        for pred_probs, true_probs in zip(predictions, true_values):
            pred_label= np.argmax(pred_probs)
            true_label= np.argmax(true_probs)
            pred_labels.append(self.class_labels[pred_label])
            true_labels.append(self.class_labels[true_label])
            
            residual= np.mean(np.abs(pred_probs - true_probs))
            residuals.append(residual)

        return pred_labels, true_labels, residuals


    def load_model(self):
        # get model file data from MinIO
        prefix= f"{self.dataset}/models/sampling/"
        suffix= f"_{self.output_type}_fc_{self.input_type}.pth"
        model_files=cmd.get_list_of_objects_with_prefix(self.minio_client, 'datasets', prefix)
        most_recent_model = None

        for model_file in model_files:
            if model_file.endswith(suffix):
                most_recent_model = model_file

        if most_recent_model:
            model_file_data =cmd.get_file_from_minio(self.minio_client, 'datasets', most_recent_model)
        else:
            print("No .pth files found in the list.")
            return None
        
        print(most_recent_model)

        # Create a temporary file and write the downloaded content into it
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            for data in model_file_data.stream(amt=8192):
                temp_file.write(data)

        # Load the model from the downloaded bytes
        self.metadata = torch.load(temp_file.name)
        self.feature_max_value = self.metadata["feature_max_value"]
        self.feature_min_value = self.metadata["feature_min_value"]

        self.model.load_state_dict(self.metadata["model_state"])
        # Remove the temporary file
        os.remove(temp_file.name)


    def save_metadata(self, inputs, points_per_sphere, learning_rate, num_epochs, training_batch_size):
        feature_input_vector = [input[-1] for input in inputs]

        self.feature_min_value = min(feature_input_vector)
        self.feature_max_value = max(feature_input_vector)

        self.metadata = {
            'points_per_sphere': points_per_sphere,
            'num_epochs': num_epochs,
            'learning_rate': learning_rate,
            'training_batch_size': training_batch_size,
            'model_state': self.model.state_dict(),
            'feature_min_value': self.feature_min_value,
            'feature_max_value': self.feature_max_value
        }


    def save_model(self):
        if self.metadata is None:
            raise Exception("you have to train the model before saving.")
        
        # Save the model locally
        torch.save(self.metadata, self.local_path)

        # Read the contents of the saved model file
        with open(self.local_path, 'rb') as model_file:
            model_bytes = model_file.read()

        # Upload the model to MinIO
        cmd.upload_data(self.minio_client, 'datasets', self.minio_path, BytesIO(model_bytes))
        print(f'Model saved to {self.minio_path}')