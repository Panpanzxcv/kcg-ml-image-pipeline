from pydantic import BaseModel, Field, constr, validator
from typing import List, Union, Optional
import re
from datetime import datetime
from orchestration.api.api_utils import uuid64_number_to_string
from orchestration.api.mongo_schemas import ImageMetadata
from orchestration.api.utils.uuid64 import Uuid64

class AllImagesResponse(BaseModel):
    uuid: str
    index: int
    bucket_id: int
    dataset_id: int
    image_hash: str
    image_path: str
    date: int            


class ListAllImagesResponse(BaseModel):
    images: List[AllImagesResponse]

class ListAllImagePathsResponse(BaseModel):
    image_paths: List[str]

class AllImagesHelpers():
    @staticmethod
    def clean_image_for_api_response(data: dict):
        data.pop('_id', None)

        if "uuid" in data:
            if isinstance(data['uuid'], int):
                uuid64 = Uuid64.from_mongo_value(data['uuid'])
                data['uuid'] = uuid64.to_formatted_str()
            if isinstance(data['uuid'], Uuid64):
                data['uuid'] = data['uuid'].to_formatted_str()

    @staticmethod
    def clean_image_list_for_api_response(data_list: List[dict]):
         for image_data in data_list:
            AllImagesHelpers.clean_image_for_api_response(image_data)
