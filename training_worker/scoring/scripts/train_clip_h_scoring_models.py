import argparse
import os
import sys
import torch
import msgpack

base_dir = "./"
sys.path.insert(0, base_dir)
sys.path.insert(0, os.getcwd())

from training_worker.scoring.models.scoring_fc import ScoringFCNetwork
# from training_worker.scoring.models.scoring_xgboost import ScoringXgboostModel
from training_worker.scoring.models.scoring_treeconnect import ScoringTreeConnectNetwork
from utility.http import request
from utility.minio import cmd

DATA_MINIO_DIRECTORY="data/latent-generator"
API_URL = "http://192.168.3.1:8111"

def parse_list(option_string):
    return [int(item) for item in option_string.split(',')]

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--minio-access-key', type=str, help='Minio access key')
    parser.add_argument('--minio-secret-key', type=str, help='Minio secret key')
    parser.add_argument('--dataset', type=str, help='Name of the dataset', default="environmental")
    parser.add_argument('--model-type', type=str, help='Model type, fc, xgboost and treeconnect', default="fc")
    parser.add_argument('--hidden-layers', type=parse_list, help='List of hidden layer sizes, separated by commas', default=[512, 256])
    parser.add_argument('--output-type', type=str, help='output type for the model', default="sigma_score")
    parser.add_argument('--output-size', type=int, help='size of output', default=1)
    parser.add_argument('--kandinsky-batch-size', type=int, default=5)
    parser.add_argument('--training-batch-size', type=int, default=64)
    parser.add_argument('--epochs', type=int, default=10)
    parser.add_argument('--learning-rate', type=float, default=0.001)
    parser.add_argument('--num-samples', type=int, default=10000)

    return parser.parse_args()

class ABRankingFcTrainingPipeline:
    def __init__(self,
                    minio_access_key,
                    minio_secret_key,
                    dataset,
                    model_type,
                    hidden_layers,
                    output_type,
                    output_size,
                    kandinsky_batch_size=5,
                    training_batch_size=64,
                    num_samples=10000,
                    learning_rate=0.001,
                    epochs=10):
        
        # get minio client
        self.minio_client = cmd.get_minio_client(minio_access_key=minio_access_key,
                                            minio_secret_key=minio_secret_key)
        
        # get device
        if torch.cuda.is_available():
            device = 'cuda'
        else:
            device = 'cpu'
        self.device = torch.device(device)
        
        self.dataset= dataset
        self.training_batch_size= training_batch_size
        self.kandinsky_batch_size= kandinsky_batch_size
        self.num_samples= num_samples
        self.learning_rate= learning_rate
        self.epochs= epochs
        self.model_type= model_type
        self.output_type= output_type
        self.output_size= output_size
        self.hidden_layers = hidden_layers

    def train(self):
        inputs=[]
        outputs=[]

        # get self training data
        self_training_path = DATA_MINIO_DIRECTORY + f"/self_training/"
        self_training_files = self.minio_client.list_objects('datasets', prefix=self_training_path, recursive=True)
        self_training_files = [file.object_name for file in self_training_files]

        for file in self_training_files:
            print(file)

            # get data
            data = self.minio_client.get_object('datasets', file)
            # Read the content of the msgpack file
            content = data.read()

            # Deserialize the content using msgpack
            self_training_data = msgpack.loads(content)
            
            # append the self training data to list of data
            self_training_inputs, self_training_outputs= self.load_self_training_data(self_training_data, self.output_type)
            inputs.extend(self_training_inputs)
            outputs.extend(self_training_outputs)
        
        # training and saving the model
        if self.model_type == "fc" or self.model_type == "all":
            print(f"training an fc model for the {self.dataset} dataset")
            model= ScoringFCNetwork(minio_client=self.minio_client, 
                                    dataset=self.dataset, 
                                    output_type=self.output_type,
                                    output_size= self.output_size,
                                    hidden_sizes= self.hidden_layers)
            loss=model.train(inputs, outputs, num_epochs= self.epochs, batch_size=self.training_batch_size, learning_rate=self.learning_rate)
            model.save_model()
        
        if self.model_type == "treeconnect" or self.model_type == "all":
            print(f"training a treeconnect model for the {self.dataset} dataset")
            model= ScoringTreeConnectNetwork(minio_client=self.minio_client, 
                                             dataset=self.dataset, 
                                             output_type=self.output_type,
                                             output_size= self.output_size,
                                             hidden_sizes= self.hidden_layers)
            loss=model.train(inputs, outputs, num_epochs= self.epochs, batch_size=self.training_batch_size, learning_rate=self.learning_rate)
            model.save_model()
        
        # if self.model_type=="xgboost" or self.model_type == "all":
        #     print(f"training an xgboost model for the {self.dataset} dataset")
        #     model= ScoringXgboostModel(minio_client=self.minio_client, 
        #                                 dataset=self.dataset, 
        #                                 input_type=self.input_type, 
        #                                 output_type=self.output_type,
        #                                 input_size= self.input_size,
        #                                 output_size= self.output_size)
            
        #     loss=model.train(inputs, outputs)
        #     model.save_model()
    
    def load_self_training_data(self, data, output_type):
        inputs=[]
        outputs=[]

        for datapoint in data:
            input_clip = datapoint["input_clip"][0]

            if output_type == "sigma_score":
                output_result= datapoint["output_clip_score"]

            inputs.append(input_clip)
            outputs.append(output_result)
        
        return inputs, outputs

def main():
    args = parse_args()
    global DATA_MINIO_DIRECTORY

    if args.dataset != "all":
        training_pipeline=ABRankingFcTrainingPipeline(minio_access_key=args.minio_access_key,
                                    minio_secret_key=args.minio_secret_key,
                                    dataset= args.dataset,
                                    model_type= args.model_type,
                                    hidden_layers= args.hidden_layers,
                                    output_type= args.output_type,
                                    output_size= args.output_size,
                                    kandinsky_batch_size=args.kandinsky_batch_size,
                                    training_batch_size=args.training_batch_size,
                                    num_samples= args.num_samples,
                                    epochs= args.epochs,
                                    learning_rate= args.learning_rate)
        
        DATA_MINIO_DIRECTORY= f"{args.dataset}/data/latent-generator"
        
        # do self training
        training_pipeline.train()
    
    else:
        # if all, train models for all existing datasets
        # get dataset name list
        dataset_names = request.http_get_dataset_names()
        print("dataset names=", dataset_names)
        for dataset in dataset_names:
            DATA_MINIO_DIRECTORY= f"{dataset}/data/latent-generator"
            
            try:
                # initialize training pipeline
                training_pipeline=ABRankingFcTrainingPipeline(minio_access_key=args.minio_access_key,
                                    minio_secret_key=args.minio_secret_key,
                                    dataset= dataset,
                                    model_type=args.model_type,
                                    hidden_layers= args.hidden_layers,
                                    output_type= args.output_type,
                                    output_size= args.output_size,
                                    kandinsky_batch_size=args.kandinsky_batch_size,
                                    training_batch_size=args.training_batch_size,
                                    num_samples= args.num_samples,
                                    epochs= args.epochs,
                                    learning_rate= args.learning_rate)
                
                # Train the model
                training_pipeline.train()

            except Exception as e:
                print("Error training model for {}: {}".format(dataset, e))

if __name__ == "__main__":
    main()

            