import sys
import os
from datetime import datetime
from pytz import timezone
from io import BytesIO
import torch
from io import StringIO
from torchinfo import summary

base_directory = os.getcwd()
sys.path.insert(0, base_directory)

from data_loader.tagged_data_loader import TaggedDatasetLoader
from utility.minio import cmd
from utility.http import request
from training_worker.ab_ranking.model import constants
from training_worker.classifiers.models import linear_regression
from training_worker.classifiers.models.reports.linear_train_graph_report import get_graph_report
from training_worker.classifiers.models.reports.linear_train_txt_report import get_train_txt_report
from training_worker.classifiers.models.reports.get_model_card import get_model_card_buf

def torchinfo_summary(model, data):
    # Capturing torchinfo output
    string_buffer = StringIO()
    sys.stdout = string_buffer

    # Running torchinfo
    summary(model, input_size=data[0].size(), col_names=("input_size", "output_size", "num_params"))

    # Get the captured output as a string
    summary_string = string_buffer.getvalue()

    # Restore the standard output
    sys.stdout = sys.__stdout__

    return summary_string

def train_classifier(minio_ip_addr=None,
                     minio_access_key=None,
                     minio_secret_key=None,
                     input_type="embedding",
                     tag_name=None,
                     tag_id=None,
                     pooling_strategy=constants.AVERAGE_POOLING,
                     train_percent=0.9,
                     epochs=100,
                     learning_rate=0.001,
                     loss_func_name="mse",
                     normalize_feature_vectors=False
                    ):
    date_now = datetime.now(tz=timezone("Asia/Hong_Kong")).strftime('%Y-%m-%d')
    print("Current datetime: {}".format(datetime.now(tz=timezone("Asia/Hong_Kong"))))
    bucket_name = "datasets"
    network_type = "linear-regression"
    output_type = "score"

    # check input type
    if input_type not in constants.ALLOWED_INPUT_TYPES:
        raise Exception("input type is not supported: {}".format(input_type))

    input_shape = 2 * 768
    if input_type in [constants.EMBEDDING_POSITIVE, constants.EMBEDDING_NEGATIVE, constants.CLIP]:
        input_shape = 768
    if input_type in [constants.KANDINSKY_CLIP]:
        input_shape = 1280

    # load data
    tag_loader = TaggedDatasetLoader(minio_ip_addr=minio_ip_addr,
                                     minio_access_key=minio_access_key,
                                     minio_secret_key=minio_secret_key,
                                     tag_name=tag_name,
                                     input_type=input_type,
                                     pooling_strategy=pooling_strategy,
                                     train_percent=train_percent)
    tag_loader.load_dataset()

    # get dataset name
    dataset_name = tag_loader.dataset_name
    output_path = "{}/models/classifiers/{}".format(dataset_name, tag_name)

    # mix training positive and negative
    training_features, training_targets = tag_loader.get_shuffled_positive_and_negative_training()

    # mix validation positive and negative
    validation_features, validation_targets = tag_loader.get_shuffled_positive_and_negative_validation()

    # get final filename
    sequence = 0
    filename = "{}-{:02}-{}-{}-{}-{}".format(date_now, sequence, tag_name, output_type, network_type, input_type)

    # if exist, increment sequence
    while True:
        filename = "{}-{:02}-{}-{}-{}-{}".format(date_now, sequence, tag_name, output_type, network_type, input_type)
        exists = cmd.is_object_exists(tag_loader.minio_client, bucket_name,
                                      os.path.join(output_path, filename + ".safetensors"))
        if not exists:
            break

        sequence += 1

    # train model
    classifier_model = linear_regression.LinearRegression()
    classifier_model.set_config(tag_string=tag_name,
                                input_size=input_shape,
                                output_size=1,
                                epochs=epochs,
                                learning_rate=learning_rate,
                                loss_func_name=loss_func_name,
                                normalize_feature_vectors=normalize_feature_vectors)

    (train_pred,
     validation_pred,
     training_loss_per_epoch,
     validation_loss_per_epoch) = classifier_model.train(tag_loader=tag_loader)

    # save model
    # Upload model to minio
    model_name = "{}.safetensors".format(filename)
    model_output_path = os.path.join(output_path, model_name)
    classifier_model.save_model(tag_loader.minio_client, bucket_name, model_output_path)

    # Generate report
    data_for_summary = training_features
    nn_summary = torchinfo_summary(classifier_model.model, data_for_summary)
    report_str = get_train_txt_report(model_class=classifier_model,
                                      validation_predictions=validation_pred,
                                      training_percent=train_percent,
                                      positive_dataset_total_size=tag_loader.positive_dataset_total_size,
                                      negative_dataset_total_size=tag_loader.negative_dataset_total_size,
                                      training_positive_size=tag_loader.training_positive_size,
                                      validation_positive_size=tag_loader.validation_positive_size,
                                      training_negative_size=tag_loader.training_negative_size,
                                      validation_negative_size=tag_loader.validation_negative_size,
                                      nn_summary=nn_summary)

    report_name = "{}.txt".format(filename)
    report_output_path = os.path.join(output_path, report_name)
    report_buffer = BytesIO(report_str.encode(encoding='UTF-8'))
    cmd.upload_data(tag_loader.minio_client, bucket_name, report_output_path, report_buffer)

    # save graph report
    # upload the graph report
    graph_buffer = get_graph_report(model_class=classifier_model,
                                    train_predictions=train_pred.detach().cpu().numpy(),
                                    training_targets=training_targets.detach().cpu().numpy(),
                                    validation_predictions=validation_pred.detach().cpu().numpy(),
                                    validation_targets=validation_targets.detach().cpu().numpy(),
                                    date=date_now,
                                    dataset_name=dataset_name,
                                    network_type=network_type,
                                    input_type=input_type,
                                    input_shape=input_shape,
                                    output_type=output_type,
                                    tag_string=tag_name,
                                    positive_dataset_total_size=tag_loader.positive_dataset_total_size,
                                    negative_dataset_total_size=tag_loader.negative_dataset_total_size,
                                    training_positive_size=tag_loader.training_positive_size,
                                    validation_positive_size=tag_loader.validation_positive_size,
                                    training_negative_size=tag_loader.training_negative_size,
                                    validation_negative_size=tag_loader.validation_negative_size,
                                    pooling_strategy=pooling_strategy,
                                    training_loss_per_epoch=training_loss_per_epoch,
                                    validation_loss_per_epoch=validation_loss_per_epoch
                                    )
    graph_name = "{}.png".format(filename)
    graph_output_path = os.path.join(output_path, graph_name)
    cmd.upload_data(tag_loader.minio_client, bucket_name, graph_output_path, graph_buffer)

    # get model card and upload
    classifier_name="{}-{}-{}-{}".format(tag_name, output_type, network_type, input_type)
    model_card_name = "{}.json".format(filename)
    model_card_name_output_path = os.path.join(output_path, model_card_name)
    model_card_buf, model_card = get_model_card_buf(classifier_name= classifier_name,
                                                    tag_id= tag_id,
                                                    latest_model= filename,
                                                    model_path= model_output_path,
                                                    creation_time=date_now)
    cmd.upload_data(tag_loader.minio_client, bucket_name, model_card_name_output_path, model_card_buf)

    # save model to mongodb if its input type is clip-h
    if(input_type==constants.KANDINSKY_CLIP):
        request.http_add_classifier_model(model_card)