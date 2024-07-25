from io import BytesIO
import json


def get_model_card_buf(model,
                       number_of_training_points,
                       number_of_validation_points,
                       graph_report_path,
                       input_type,
                       output_type):
    model_card = {
        "model_creation_date": model.date,
        "model_type": model.model_type,
        "model_path": model.file_path,
        "model_file_hash": model.model_hash,
        "input_type": input_type,
        "output_type": output_type,
        "number_of_training_points": "{}".format(number_of_training_points),
        "number_of_validation_points": "{}".format(number_of_validation_points),
        "training_loss": "{}".format(model.training_loss.item()),
        "validation_loss": "{}".format(model.validation_loss.item()),
        "graph_report": graph_report_path,
    }

    buf = BytesIO()
    buf.write(json.dumps(model_card, indent=4).encode())
    buf.seek(0)

    return buf, json.dumps(model_card, indent=4)


def get_xgboost_model_card_buf(date,
                               model_type,
                               model_path,
                               model_hash,
                               input_type,
                               output_type,
                               number_of_training_points,
                               number_of_validation_points,
                               training_loss,
                               validation_loss,
                               graph_report_path,
                               ):
    model_card = {
        "model_creation_date": date,
        "model_type": model_type,
        "model_path": model_path,
        "model_file_hash": model_hash,
        "input_type": input_type,
        "output_type": output_type,
        "number_of_training_points": "{}".format(number_of_training_points),
        "number_of_validation_points": "{}".format(number_of_validation_points),
        "training_loss": "{}".format(training_loss),
        "validation_loss": "{}".format(validation_loss),
        "graph_report": graph_report_path,
    }

    buf = BytesIO()
    buf.write(json.dumps(model_card, indent=4).encode())
    buf.seek(0)

    return buf, json.dumps(model_card, indent=4)

def get_ranking_model_data(model_name,
                       model_type,
                       rank_id,
                       model_path,
                       latest_model_creation_time,
                       creation_time):
    model_card = {
                "ranking_model_id": None,
                "model_name": model_name,
                "model_type": model_type,
                "rank_id": rank_id,
                "latest_model_creation_time": latest_model_creation_time,
                "model_path": model_path,
                "creation_time": creation_time
            }

    buf = BytesIO()
    buf.write(json.dumps(model_card, indent=4).encode())
    buf.seek(0)

    return json.dumps(model_card, indent=4)
