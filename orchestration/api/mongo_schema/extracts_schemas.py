from typing import List
import uuid

from orchestration.api.api_utils import uuid64_number_to_string
from orchestration.api.utils.uuid64 import Uuid64

class ExtractsHelpers():
    @staticmethod
    def clean_extract_for_api_response(data: dict):
        data.pop('_id', None)
        if "uuid" in data:
            if isinstance(data['uuid'], uuid.UUID):
                    data['uuid'] = str(data['uuid'])

        if "image_uuid" in data:
            if isinstance(data['image_uuid'], int):
                uuid64 = Uuid64.from_mongo_value(data['image_uuid'])
                data['image_uuid'] = uuid64.to_formatted_str()
            if isinstance(data['image_uuid'], Uuid64):
                data['image_uuid'] = data['image_uuid'].to_formatted_str()

    @staticmethod
    def clean_extract_list_for_api_response(data_list: List[dict]):
         for image_data in data_list:
            ExtractsHelpers.clean_extract_for_api_response(image_data)
