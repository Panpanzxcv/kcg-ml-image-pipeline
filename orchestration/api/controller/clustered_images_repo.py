from fastapi import Request
from typing import List
import sys
sys.path.insert(0, './')

from orchestration.api.mongo_schema.clustering_schemas import ClusteredImageMetadata

def find_clustered_image_by_image_uuid(request: Request, image_uuid: int):
    try:
        data = request.app.clustered_images_collection.find_one({"image_uuid": image_uuid}, {'_id': False})
        return dict(data) if data is not None else None
    except Exception as e:
        raise Exception(f"Error while finding clustered Image with image uuid {image_uuid} in database: {e}")

def find_clustered_images_by_pipeline(request: Request, aggregate_pipeline: List[dict]):
    try:
        data = request.app.clustered_images_collection.aggregate(aggregate_pipeline)
        return list(data) if data is not None else None
    except Exception as e:
        raise Exception(f"Error while finding clustered Images with aggregate_pipeline {aggregate_pipeline} in database: {e}")

def add_clustered_image(request: Request, clustered_image: ClusteredImageMetadata):
    try:
        request.app.clustered_images_collection.insert_one(clustered_image.to_dict())
        return clustered_image.to_dict()
    except Exception as e:
        raise Exception(f"Error while inserting clustered image data {clustered_image.to_dict()} in database: {e}")
    
def update_clustered_image(request: Request, clustered_image: ClusteredImageMetadata):
    try:
        request.app.clustered_images_collection.update_one({"id": clustered_image.image_uuid}, {"$set": clustered_image.to_dict()})
        return clustered_image.to_dict()
    except Exception as e:
        raise Exception(f"Error while updating clustered image {clustered_image.to_dict()} in database: {e}")
    
def delete_clustered_image_by_image_uuid(request: Request, image_uuid: int):
    try:
        result = request.app.clustered_images_collection.delete_one({"image_uuid": image_uuid})
        return result.deleted_count
    except Exception as e:
        raise Exception(f"Error while deleting clustered image data with image_uuid {image_uuid} in database: {e}")

def delete_all_images(request: Request):
    try:
        result = request.app.clustered_images_collection.delete_many({})
        return result.deleted_count
    except Exception as e:
        raise Exception(f"Error while deleting all clustered images in database: {e}")