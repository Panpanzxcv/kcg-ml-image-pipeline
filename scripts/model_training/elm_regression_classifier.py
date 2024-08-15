import os
import sys
import argparse

base_directory = os.getcwd()
sys.path.insert(0, base_directory)

from training_worker.classifiers.scripts.elm_regression import train_classifier
from utility.http.request import http_get_tag_list

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Train elm regression classifier model")

    parser.add_argument('--minio-ip-addr', type=str, help='Minio ip address')
    parser.add_argument('--minio-access-key', type=str, help='Minio access key')
    parser.add_argument('--minio-secret-key', type=str, help='Minio secret key')
    parser.add_argument('--input-type', type=str, default="embedding")
    parser.add_argument('--image-type', type=str, default="all") # Options ["all", "512"]
    parser.add_argument('--tag-name', type=str)
    parser.add_argument('--hidden-layer-neuron-count', type=int, default=3000)
    parser.add_argument('--pooling-strategy', type=int, default=0)
    parser.add_argument('--train-percent', type=float, default=0.9)

    return parser.parse_args()


if __name__=="__main__":

    args = parse_arguments()
    
    image_types = {
        "all": "all_resolutions",
        "512": "512*512_resolutions"
    }

    tags = http_get_tag_list()
    
    if args.tag_name != "all":
        try:
            tag_id = None
            for tag in tags:
                if args.tag_name == tag["tag_string"]:
                    tag_id = tag["tag_id"]

            if tag_id is None:
                raise Exception(f"There is no tag called {args.tag_name}")

            print("Training model for tag: {}".format(args.tag_name))
            train_classifier(minio_ip_addr=args.minio_ip_addr,
                             minio_access_key=args.minio_access_key,
                             minio_secret_key=args.minio_secret_key,
                             input_type=args.input_type,
                             image_type=image_types[args.image_type],
                             tag_name=args.tag_name,
                             tag_id=tag_id,
                             hidden_layer_neuron_count=args.hidden_layer_neuron_count,
                             pooling_strategy=args.pooling_strategy,
                             train_percent=args.train_percent)
        except Exception as e:
            print("Error training model for tag {}: {}".format(args.tag_name, e))
    else:
        # train for all
        tag_string_list = []
        tag_id_list = []
        for tag in tags:
            tag_string_list.append(tag["tag_string"])
            tag_id_list.append(tag["tag_id"])

        print("tags found = ", tag_string_list)
        for tag_id, tag_name in zip(tag_id_list,tag_string_list):
            try:
                print("Training model for tag: {}".format(tag_name))
                train_classifier(minio_ip_addr=args.minio_ip_addr,
                                 minio_access_key=args.minio_access_key,
                                 minio_secret_key=args.minio_secret_key,
                                 input_type=args.input_type,
                                 image_type=image_types[args.image_type],
                                 tag_name=tag_name,
                                 tag_id=tag_id,
                                 hidden_layer_neuron_count=args.hidden_layer_neuron_count,
                                 pooling_strategy=args.pooling_strategy,
                                 train_percent=args.train_percent)
            except Exception as e:
                print("Error training model for tag {}: {}".format(tag_name, e))
            print("==============================================================================")
