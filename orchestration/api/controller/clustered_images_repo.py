from fastapi import Request

import sys
sys.path.insert(0, './')

from orchestration.api.mongo_schema.clustering_schemas import ClusteredImageMetadata

def find_clustered_image_by_image_uuid(request: Request, image_uuid: int):
    try:
        data = request.app.clustered_images_collection.find_one({"image_uuid": image_uuid}, {'_id': False})
        return dict(data) if data is not None else None
    except Exception as e:
        raise Exception(f"Error while finding clustered Image with image uuid {image_uuid} in database: {e}")
    
def add_clustered_image(request: Request, image_data: ClusteredImageMetadata):
    try:
        request.app.clustered_images_collection.insert_one(image_data.to_dict())
        return image_data.to_dict()
    except Exception as e:
        raise Exception(f"Error while inserting clustered image data {image_data.to_dict()} in database: {e}")
    
def update_clustered_image(request: Request, image_data: ClusteredImageMetadata):
    try:
        request.app.clustered_images_collection.update_one({"id": image_data.image_uuid}, {"$set": image_data.to_dict()})
        return image_data.to_dict()
    except Exception as e:
        raise Exception(f"Error while updating clustered image {image_data.to_dict()} in database: {e}")
    
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