import torch
import torch.nn as nn
import torch.optim as optim
from datetime import datetime
import os
from os.path import basename
import sys
import hashlib
import time
import json
from io import BytesIO
from safetensors.torch import save as safetensors_save
from safetensors.torch import load as safetensors_load

base_directory = os.getcwd()
sys.path.insert(0, base_directory)

from utility.minio import cmd
from data_loader.tagged_data_loader import TaggedDatasetLoader

class LinearRegression:
    def __init__(self, device=None):
        self.model_type = 'linear-regression'
        self.model_file_path = None
        self.model_hash = ''
        self.date = datetime.now().strftime("%Y-%m-%d")

        self.model = None
        self.tag_string = None
        self.input_size = None
        self.output_size = None
        self.epochs = None
        self.learning_rate = None
        self.loss_func_name = None
        self.loss_func = None
        self.normalize_feature_vectors = None

        self.training_loss = 0.0
        self.validation_loss = 0.0

        if not device and torch.cuda.is_available():
            device = 'cuda'
        else:
            device = 'cpu'

        self._device = torch.device(device)

    def set_config(self,
                   tag_string,
                   input_size,
                   output_size=1,
                   epochs=100,
                   learning_rate=0.001,
                   loss_func_name="mse",
                   normalize_feature_vectors=False):
        self.model = nn.Sequential(
            nn.Linear(input_size, output_size),
            nn.Identity()
        )

        self.tag_string = tag_string
        self.input_size = input_size
        self.output_size = output_size
        self.epochs = epochs
        self.learning_rate = learning_rate
        self.loss_func_name = loss_func_name
        self.normalize_feature_vectors = normalize_feature_vectors

        self.loss_func = get_loss_func(loss_func_name)

    def _hash_model(self):
        """
        Hashes the current state of the model, and stores the hash in the
        instance of the classifier.
        """
        model_str = str(self.model.state_dict())
        self.model_hash = hashlib.sha256(model_str.encode()).hexdigest()

    def to_safetensors(self):
        # get model hash
        self._hash_model()
        model_hash = self.model_hash

        metadata = {
            'model-type': self.model_type,
            'model-file-path': self.model_file_path,
            'model-hash': model_hash,
            'date': self.date,
            'tag-string': self.tag_string,
            'input-size': "{}".format(self.input_size),
            'output-size': "{}".format(self.output_size),
            'epochs': "{}".format(self.epochs),
            'learning-rate': "{}".format(self.learning_rate),
            'loss-func': self.loss_func_name,
            'normalize-feature-vectors': "{}".format(self.normalize_feature_vectors),
            'training-loss': "{}".format(self.training_loss),
            'validation-loss': "{}".format(self.validation_loss),
        }

        model = self.model.state_dict()

        return model, metadata

    def save_model(self, minio_client, datasets_bucket, model_output_path):
        self.model_file_path = model_output_path

        # Preparing the model to be saved
        model, metadata = self.to_safetensors()

        # Saving the model to minio
        buffer = BytesIO()
        safetensors_buffer = safetensors_save(tensors=model,
                                              metadata=metadata)
        buffer.write(safetensors_buffer)
        buffer.seek(0)

        # upload the model
        cmd.upload_data(minio_client, datasets_bucket, model_output_path, buffer)

    def load_safetensors(self, model_buffer):
        data = model_buffer.read()
        safetensors_data = safetensors_load(data)

        # load metadata
        n_header = data[:8]
        n = int.from_bytes(n_header, "little")
        metadata_bytes = data[8: 8 + n]
        header = json.loads(metadata_bytes)
        model = header.get("__metadata__", {})

        # Restoring model metadata
        self.model_type = model['model-type']
        self.model_file_path = model['model-file-path']
        self.model_hash = model['model-hash']
        self.date = model['date']
        self.tag_string = model['tag-string']
        self.input_size = model['input-size']
        self.output_size = model['output-size']
        self.epochs = model['epochs']
        self.learning_rate = model['learning-rate']
        self.loss_func_name = model['loss-func']
        self.normalize_feature_vectors = model['normalize-feature-vectors']
        self.training_loss = model['training-loss']
        self.validation_loss = model['validation-loss']
        print(self.input_size, self.output_size, "-----")
        # Loading state dictionary
        self.model = nn.Sequential(
            nn.Linear(int(self.input_size), int(self.output_size)),
            nn.Identity()
        )
        # self.model.parameters()
        self.model.load_state_dict(safetensors_data)

        # load loss func
        self.loss_func = get_loss_func(self.loss_func_name)

    def train(self, tag_loader: TaggedDatasetLoader):
        training_loss_per_epoch = []
        validation_loss_per_epoch = []
        optimizer = optim.SGD(self.model.parameters(), lr=self.learning_rate)

        for epoch in range(self.epochs):
            # get new negative features per epoch
            training_inputs, training_targets = tag_loader.get_shuffled_positive_and_negative_training()
            validation_inputs, validation_targets = tag_loader.get_shuffled_positive_and_negative_validation()
            if self.normalize_feature_vectors:
                training_inputs = normalize_feature_vector(training_inputs)
                training_targets = normalize_feature_vector(training_targets)
                validation_inputs = normalize_feature_vector(validation_inputs)
                validation_targets = normalize_feature_vector(validation_targets)

            optimizer.zero_grad()
            training_outputs = self.model(training_inputs)
            loss = self.loss_func(training_outputs, training_targets)
            loss.backward()
            optimizer.step()

            # Validation step
            with torch.no_grad():
                validation_outputs = self.model(validation_inputs)
                validation_loss = self.loss_func(validation_outputs, validation_targets)

            if epoch % 10 == 0:
                print(
                    f"Epoch {epoch}/{self.epochs} | Loss: {loss.item():.4f} | Validation Loss: {validation_loss.item():.4f}")
            training_loss_per_epoch.append(loss.item())
            validation_loss_per_epoch.append(validation_loss.item())

        # Calculating and storing performance metrics
        training_outputs = self.model(training_inputs)
        validation_outputs = self.model(validation_inputs)

        # Storing loss
        self.training_loss = self.loss_func(training_outputs, training_targets)
        self.validation_loss = self.loss_func(validation_outputs, validation_targets)

        return training_outputs, validation_outputs, training_loss_per_epoch, validation_loss_per_epoch

    def predict(self, inputs, normalize_feature_vectors=False):
        with torch.no_grad():
            # inputs = torch.tensor(inputs)
            if normalize_feature_vectors:
                inputs = normalize_feature_vector(inputs)

            outputs = self.model(inputs).squeeze()

            return outputs
        
    def classify(self, input, normalize_feature_vectors=False):
        if normalize_feature_vectors:
            input = normalize_feature_vector(input)
        input = input.to(self._device)
        return self.model(input).squeeze()
        
    def classify_pooled_embeddings(self, positive_embedding_array, negative_embedding_array):
        # Average pooling
        embedding_array = torch.cat((positive_embedding_array, negative_embedding_array), dim=-1)
        avg_pool = torch.nn.AvgPool2d(kernel_size=(77, 1))

        embedding_array = avg_pool(embedding_array)
        embedding_array = embedding_array.squeeze().unsqueeze(0)

        return self.classify(embedding_array)
    
    def predict_positive_or_negative_only_pooled(self, embedding_array):
        # Average pooling

        avg_pool = torch.nn.AvgPool2d(kernel_size=(77, 1))

        embedding_array = avg_pool(embedding_array)
        embedding_array = embedding_array.squeeze().unsqueeze(0)

        return self.classify(embedding_array)

    def load_model(self, minio_client, model_dataset, tag_name, model_type, scoring_model, not_include, device=None):
        input_path = f"{model_dataset}/models/classifiers/{tag_name}/"
        file_suffix = ".safetensors"

        # Use the MinIO client's list_objects method directly with recursive=True
        model_files = [obj.object_name for obj in minio_client.list_objects('datasets', prefix=input_path, recursive=True) if obj.object_name.endswith(file_suffix) and model_type in obj.object_name and scoring_model in obj.object_name and not_include not in obj.object_name ]
        
        if not model_files:
            print(f"No .safetensors models found for tag: {tag_name}")
            return None

        # Assuming there's only one model per tag or choosing the first one
        model_files.sort(reverse=True)
        model_file = model_files[0]
        print(f"Loading model: {model_file}")

        model_data = minio_client.get_object('datasets', model_file)
        
        linear_model = LinearRegression(device=self._device)
        
        # Create a BytesIO object from the model data
        byte_buffer = BytesIO(model_data.data)
        linear_model.load_safetensors(byte_buffer)

        print(f"Model loaded for tag: {tag_name}")
        
        return linear_model, basename(model_file)


def normalize_feature_vector(feature_vector):
    return feature_vector / torch.linalg.norm(feature_vector, dim=1, keepdim=True)


def get_loss_func(loss_func_name="mse"):
    if loss_func_name == "mse":
        loss_func = nn.MSELoss()
    elif loss_func_name == "bce":
        loss_func = nn.BCELoss()

    return loss_func
