from fastapi import Request
import sys

from orchestration.api.utils.datetime_utils import get_current_datetime_str
from orchestration.api.utils.uuid64 import Uuid64
sys.path.insert(0, './')

from orchestration.api.mongo_schema.clustering_schemas import ClusterModel


def find_cluster_model_by_model_id(request: Request, model_id: int):
    try:
        data = request.app.cluster_model_collection.find_one({"model_id": model_id}, {'_id': False})
        return dict(data) if data is not None else None
    except Exception as e:
        raise Exception(f"Error while finding cluster model with model_id {model_id} in database: {e}")

def find_cluster_model(request: Request, model_name: str, cluster_level: int):
    try:
        data = request.app.cluster_model_collection.find_one(
            {"model_name": model_name, "cluster_level": cluster_level}, 
            {'_id': False}
        )
        return dict(data) if data is not None else None
    except Exception as e:
        raise Exception(f"Error while finding cluster model with model name:{model_name} \
            and cluster level:{cluster_level} in database: {e}")

def add_cluster_model(request: Request, cluster_model: ClusterModel):
    try:
        cluster_model.model_id = Uuid64.create_new_uuid().to_mongo_value()
        cluster_model.creation_date = get_current_datetime_str()
        request.app.cluster_model_collection.insert_one(cluster_model.to_dict())
        return cluster_model.to_dict()
    except Exception as e:
        raise Exception(f"Error while inserting cluster model {cluster_model.to_dict()} in database: {e}")
    
def update_cluster_model(request: Request, model: ClusterModel):
    try:
        request.app.cluster_model_collection.update_one({"model_id": model.model_id}, {"$set": model.to_dict()})
        return model.to_dict()
    except Exception as e:
        raise Exception(f"Error while updating cluster model {model.to_dict()} in database: {e}")
    
def delete_cluster_model_by_model_id(request: Request, model_id: int):
    try:
        result = request.app.cluster_model_collection.delete_one({"model_id": model_id})
        return result.deleted_count
    except Exception as e:
        raise Exception(f"Error while deleting cluster model with model id {model_id    } in database: {e}")