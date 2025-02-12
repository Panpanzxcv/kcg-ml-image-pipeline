from starlette.responses import Response
import json, typing
import time
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from enum import Enum
import time
from fastapi import Request
from typing import TypeVar, Generic, List, Any, Dict, Optional
from pydantic import BaseModel
from orchestration.api.mongo_schema.tag_schemas import TagDefinition, TagCategory, ImageTag
from orchestration.api.mongo_schema.pseudo_tag_schemas import ImagePseudoTag
from orchestration.api.mongo_schemas import VideoMetaData
from datetime import datetime
from minio import Minio
from dateutil import parser
from datetime import datetime
import os
from typing import List, Union
from urllib.parse import urlparse, parse_qs
import random
from minio.error import S3Error



class IrrelevantResponse(BaseModel):
    uuid: str
    file_hash: str
    rank_model_id: int
    image_source: str

class IrrelevantResponseV1(BaseModel):
    uuid: str
    file_hash: str
    rank_model_id: int
    image_source: str

class GenerationCounts(BaseModel):
    character: int
    environmental: int
    external_images: int
    icons: int
    mech: int
    propaganda_poster: int
    ranks: int
    test_generations: int
    variants: int
    waifu: int

class ListGenerationsCountPerDayResponse(BaseModel):
    results: Dict[str, GenerationCounts]


class JobStatsResponse(BaseModel):
    total: int
    pending_count: int
    progress_count: int
    completed_count: int
    failed_count: int    

class DeletedCount(BaseModel):
    deleted_count: int    

class BoolIrrelevantResponse(BaseModel):
    irrelevant: bool    

class ListIrrelevantResponse(BaseModel):
    images: List[IrrelevantResponseV1]    

class DoneResponse(BaseModel):
    Done: bool

class DatasetResponse(BaseModel):
    datasets: List[str]

class SeqSeqIdResponseData(BaseModel):
    dataset_name: str
    subfolder_count: int
    file_count: int

class SeqIdResponse(BaseModel):
    sequential_ids : List[SeqSeqIdResponseData]
    
class SeqIdDatasetResponse(BaseModel):
    dataset: str
    sequential_id: int

class SetRateResponse(BaseModel):
    dataset: str
    last_update: datetime
    dataset_rate: float
    relevance_model: str
    ranking_model: str

class ResponseRelevanceModel(BaseModel):
    last_update: datetime
    relevance_model: str

class SetHourlyResponse(BaseModel):
    dataset: str
    last_update: datetime
    hourly_limit: int
    relevance_model: str
    ranking_model: str

class HourlyResponse(BaseModel):
    hourly_limit: str

class RateResponse(BaseModel):
    dataset_rate: str

class FilePathResponse(BaseModel):
    file_path: str

class ListFilePathResponse(BaseModel):
    file_paths: List[FilePathResponse]

class DatasetConfig(BaseModel):
    dataset_name: str
    dataset_rate: Union[str, None] = None
    relevance_model: Union[str, None] = None
    ranking_model: Union[str, None] = None
    hourly_limit: Union[int, None] = None
    top_k: Union[int, None] = None
    generation_policy: Union[str, None] = None
    relevance_threshold: Union[int, None] = None

class ResponseDatasetConfig(BaseModel):
    dataset_name: Optional[str]
    last_update: Optional[datetime]
    dataset_rate: Optional[str]
    relevance_model: Optional[str]
    ranking_model: Optional[str]
    hourly_limit: Optional[int]
    top_k: Optional[int]
    generation_policy: Optional[str]
    relevance_threshold: Optional[int]    

class RankinModelResponse(BaseModel):
    last_update: datetime
    ranking_model: str

class ListDatasetConfig(BaseModel):
    configs: List[ResponseDatasetConfig]

class SingleModelResponse(BaseModel):
    model_name: str
    model_architecture: str
    model_creation_date: str
    model_type: str
    model_path: str
    model_file_hash: str
    input_type: str
    output_type: str
    number_of_training_points: str
    number_of_validation_points: str
    training_loss: str
    validation_loss: str
    graph_report: str

class JsonContentResponse(BaseModel):
    json_content: dict

class ModelResponse(BaseModel):
    models: List[SingleModelResponse]

class TagDefinitionV1(BaseModel):
    file_hash: str
    tag_id: int
    tag_string: str
    tag_category_id: int
    tag_description: str 
    tag_vector_index: int
    deprecated: bool 
    user_who_created: str 
    creation_time: str

class TagListForImagesV1(BaseModel):
    images: List[TagDefinitionV1]


class TagListForImages(BaseModel):
    tags: List[TagDefinition]

class TagDefinitionV2(BaseModel):
    tag_id: int
    tag_string: str
    tag_type: int
    tag_category_id: int
    tag_description: str 
    tag_vector_index: int
    deprecated: bool 
    deprecated_tag_category: bool
    user_who_created: str 
    creation_time: str

class TagListForImagesV2(BaseModel):
    tags: List[TagDefinitionV2]

class ModelTypeResponse(BaseModel):
    model_types: List[str]
    
class ModelsAndScoresResponse(BaseModel):
    models: List[str]
    scores: List[str]

class ListImageTag(BaseModel):
     images: List[ImageTag]

class RankCountResponse(BaseModel):
    image_hash: str
    count: int

class CountResponse(BaseModel):
    count: int

class RechableResponse(BaseModel):
    reachable: bool

class ResponsePolicies(BaseModel):
    generation_policies: List[str]

class VectorIndexUpdateRequest(BaseModel):
    vector_index: int

class WasPresentResponse(BaseModel):
    wasPresent: bool

class TagsCategoryListResponse(BaseModel):
    tag_categories: List[TagCategory]

class TagsListResponse(BaseModel):
    tags: List[TagDefinition]

class TagCountResponse(BaseModel):
    tag_id: int
    count: dict

class ModelIdResponse(BaseModel):
    model_id: int

class UrlResponse(BaseModel):
    url: str

class TagIdResponse(BaseModel):
    tag_id: int

class PseudoTagIdResponse(BaseModel):
    pseudo_tag_id: int

class GetClipPhraseResponse(BaseModel):
    phrase : str
    clip_vector: List[List[float]]

class GetKandinskyClipResponse(BaseModel):
    clip_vector: List[List[float]]

class ImageData(BaseModel):
    image_path: str
    image_hash: str
    score: float

class TagResponse(BaseModel):
    tag_id: int
    tag_string: str 
    tag_type: int
    tag_category_id: int
    tag_description: str  
    tag_vector_index: int
    deprecated: bool = False
    deprecated_tag_category: bool = False
    user_who_created: str
    creation_time: str

class AddJob(BaseModel):
    uuid: str
    creation_time: str

def validate_date_format(date_str: Optional[str]):
    try:
        if date_str is not None:
            # Attempt to parse the date string using dateutil.parser
            parsed_date = parser.parse(date_str)
            # If parsing succeeds, return the original date string
            return date_str
        else:
            return None
    except ValueError:
        # If parsing fails, return None
        return None


class PrettyJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=4,
            separators=(", ", ": "),
        ).encode("utf-8")


class ErrorCode(Enum):
    SUCCESS = 0
    OTHER_ERROR = 1
    ELEMENT_NOT_FOUND = 2
    INVALID_PARAMS = 3


T = TypeVar('T')
class StandardSuccessResponse(BaseModel, Generic[T]):
    url: str
    duration: int
    response: T


class StandardErrorResponse(BaseModel):
    url: str
    duration: int
    errorCode: int
    errorString: str


class ApiResponseHandler:
    def __init__(self, request: Request):
        self.url = str(request.url)
        self.start_time = time.time()

    def _elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @staticmethod
    def listErrors(errors: List[int]) -> dict:
        repsonse = {}
        for err in errors:
            repsonse[err] = {"model": StandardErrorResponse}
        return repsonse

    def create_success_response(self, response_data: dict, http_status_code: int, headers: dict = {"Cache-Control": "no-store"}):
        # Validate the provided HTTP status code
        if not 200 <= http_status_code < 300:
            raise ValueError("Invalid HTTP status code for a success response. Must be between 200 and 299.")

        response_content = {
            "url": self.url,
            "duration": self._elapsed_time(),
            "response": response_data
        }
        return PrettyJSONResponse(status_code=http_status_code, content=response_content, headers=headers)
    
    def create_success_delete_response(self, reachable: bool):
        return PrettyJSONResponse(
            status_code=200,
            content={
                "url": self.url,
                "duration": self._elapsed_time(),
                "response": {"reachable": reachable}
            },
            headers={"Cache-Control": "no-store"}
        )

    def create_error_response(self, error_code: ErrorCode, error_string: str, http_status_code: int):
        return PrettyJSONResponse(
            status_code=http_status_code,
            content={
                "url": self.url,
                "duration": self._elapsed_time(),
                "errorCode": error_code.value,
                "errorString": error_string
            }
        )

     

class BaseStandardResponseV1(BaseModel):
    request_error_string: str = ""
    request_error_code: int = 0
    request_url: str
    request_dictionary: dict 
    request_method: str
    request_complete_time: float
    request_time_start: datetime 
    request_time_finished: datetime
    request_response_code: int 

class StandardSuccessResponseV1(BaseStandardResponseV1, Generic[T]):
    response: T 


class StandardErrorResponseV1(BaseStandardResponseV1):
    pass

     
class ApiResponseHandlerV1:
    def __init__(self, request: Request, body_data: Optional[Dict[str, Any]] = None, _created_with_helper=False):
        self.request = request
        self.url = str(request.url)
        self.start_time = datetime.now() 
        self.query_params = dict(request.query_params)

        # At some point this must be used to throw errors if the instance is not created using a helper method.
        if _created_with_helper is False:
            pass

        # Parse the URL to extract and store the path
        parsed_url = urlparse(self.url)
        self.url_path = parsed_url.path  # Store the path part of the URL

        self.request_data = {
            "body": body_data or {},  # Set from the provided body data
            "query": dict(request.query_params)  # Extracted from request
        }

    @staticmethod
    async def createInstance(request: Request):
        body = await request.body()
        body_dictionary = {}
        if (len(body) > 0):
            body_string = body.decode('utf-8')
            body_dictionary = json.loads(body_string)

        instance = ApiResponseHandlerV1(request, body_dictionary, _created_with_helper=True)
        return instance
    
    # In middlewares, this must be called instead of "createInstance", as "createInstance" may hang trying to get the request body.
    @staticmethod
    def createInstanceWithBody(request: Request, body_data: Dict[str, Any]):
        instance = ApiResponseHandlerV1(request, body_data, _created_with_helper=True)
        return instance

    
    def _elapsed_time(self) -> float:
        return datetime.now() - self.start_time
    
    @staticmethod
    def listErrors(errors: List[int]) -> dict:
        repsonse = {}
        for err in errors:
            repsonse[err] = {"model": StandardErrorResponseV1}
        return repsonse

    def _create_metadata_and_process_headers(
        self,
        http_status_code: int,
        headers: dict,
    ):
        if headers.get("Cache-Control") is None:
            headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0"

        return {
            "request_error_string": '',
            "request_error_code": 0,
            "request_url": self.url_path,
            "request_dictionary": self.request_data,
            "request_method": self.request.method,
            "request_complete_time": str(self._elapsed_time()),
            "request_time_start": self.start_time.isoformat(),
            "request_time_finished": datetime.now().isoformat(),
            "request_response_code": http_status_code
        }

    def create_success_response_v1(
        self,
        response_data: dict,
        http_status_code: int, 
        headers: dict = {},
    ):
        # Validate the provided HTTP status code
        if not 200 <= http_status_code < 300:
            raise ValueError("Invalid HTTP status code for a success response. Must be between 200 and 299.")
        
        response_content = self._create_metadata_and_process_headers(http_status_code, headers)
        response_content["response"] = response_data

        return PrettyJSONResponse(status_code=http_status_code, content=response_content, headers=headers)


    def create_success_delete_response_v1(
        self, 
        wasPresent: bool, 
        http_status_code: int = 200,
        headers: dict = {}
    ):
        response_content = self._create_metadata_and_process_headers(http_status_code, headers)
        response_content["response"] = {"wasPresent": wasPresent}

        return PrettyJSONResponse(status_code=http_status_code, content=response_content, headers=headers)

    def create_error_response_v1(
        self,
        error_code: ErrorCode,
        error_string: str,
        http_status_code: int,
        headers: dict = {},
    ):
            # Validate the provided HTTP status code
            if not 400 <= http_status_code < 599:
                raise ValueError("Invalid HTTP status code for a success response. Must be between 400 and 599.")
            
            response_content = self._create_metadata_and_process_headers(http_status_code, headers)
            response_content["request_error_string"] = error_string
            response_content["request_error_code"] = error_code.value
            
            return PrettyJSONResponse(status_code=http_status_code, content=response_content, headers=headers)

            

def find_or_create_next_folder_and_index(client: Minio, bucket: str, base_folder: str) -> (str, int):
    """
    Finds the next folder for storing an image, creating a new one if the last is full,
    and determines the next image index.
    """
    try:
        objects = client.list_objects(bucket, prefix=base_folder+"/", recursive=True)
        folder_counts = {}
        latest_index = -1  # Start before the first possible index
        
        for obj in objects:
            folder, filename = os.path.split(obj.object_name)
            folder_counts[folder] = folder_counts.get(folder, 0) + 1
            
            # Attempt to parse the filename as an index
            try:
                index = int(os.path.splitext(filename)[0])
                latest_index = max(latest_index, index)
            except ValueError:
                pass  # Filename isn't a simple integer index

        if folder_counts:
            sorted_folders = sorted(folder_counts.items(), key=lambda x: x[0])
            last_folder, count = sorted_folders[-1]
            if count < 1000:
                return last_folder, latest_index + 1
            else:
                folder_number = int(last_folder.split('/')[-1]) + 1
                new_folder = f"{base_folder}/{folder_number:04d}"
                return new_folder, 0  # Start indexing at 0 for a new folder
        else:
            # No folders exist yet, start with the first one
            return f"{base_folder}/0001", 0
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}")
    

class CountLastHour(BaseModel):
    jobs_count: dict

def get_id(bucket: str, dataset: str) -> str:
    return '{}_{}'.format(bucket, dataset)

def get_file_extension(file_format: str) -> str:
    file_format = file_format.lower()
    if file_format in ["jpg", "jpeg"]:
        file_format = "jpg"

    return file_format


def get_minio_file_path(seq_id, bucket_name, dataset_name, format, sample_size = 1000):

    folder_id = (seq_id // sample_size) + 1
    file_id = (seq_id % sample_size)
    
    folder_name = f"{folder_id:04d}"
    file_name = f"{file_id:06d}"
    format = get_file_extension(format)
    
    path = f'{bucket_name}/{dataset_name}/{folder_name}/{file_name}'

    return f'{path}.{format}'
    
def get_next_external_dataset_seq_id(request: Request, bucket:str, dataset: str):
    counter = \
        request.app.external_dataset_sequential_id.find_one({"bucket": bucket, 
                                                            "dataset": dataset})
    if counter is None:
        request.app.external_dataset_sequential_id.insert_one({"bucket": bucket,
                                                   "dataset": dataset,
                                                   "count": 0})
    counter_seq = counter["count"] if counter else 0 
    counter_seq += 1
    
    return counter_seq

def update_external_dataset_seq_id(request: Request, bucket:str, dataset: str, seq_id = 0):

    try:
        ret = request.app.external_dataset_sequential_id.update_one(
            {"bucket": bucket, "dataset": dataset},
            {"$set": {"count": seq_id}})
    except Exception as e:
        raise Exception("Updating of external image sequential id failed: {}".format(e))
    
def get_video_short_hash_from_url(url: str) -> str:    
    # Parse the URL using urlparse
    parsed_url = urlparse(url=url)

    # Extract the query parameters using parse_qs
    query_params = parse_qs(qs=parsed_url.query)
    # Get the value of the 'v' parameter
    video_short_hash = query_params.get('v', [""])[0]

    if not video_short_hash:
        raise ValueError("The video short hash is empty.")
    
    return video_short_hash

def get_ingress_video_path(bucket:str, video_metadata: VideoMetaData) -> str:
    fname = '{}_{}p{}fps'\
        .format(get_video_short_hash_from_url(video_metadata.source_url),
                video_metadata.video_resolution.split('x')[1],
                video_metadata.video_frame_rate)
    path = f'{bucket}/{video_metadata.dataset}/{fname}'
    
    return f'{path}.{video_metadata.file_type}'
    
    
def build_date_query(date_from: Optional[Union[str, datetime]] = None, 
                     date_to: Optional[Union[str, datetime]] = None,  
                     key: str = "creation_time") -> dict:
    
    date_range_query = {}
    if date_from:
        date_range_query["$gte"] = \
            date_from.strftime('%Y-%m-%d') if isinstance(date_from, datetime) else date_from
    if date_to:
        date_range_query["$lte"] = \
            date_to.strftime('%Y-%m-%d') if isinstance(date_to, datetime) else date_to
    
    return {key: date_range_query} if date_range_query else {}

def old_date_for_migrations_to_unix_int32(dt_str):
    if 'T' not in dt_str and ' ' not in dt_str:
        dt_str += "T00:00:00.000"

    formats = ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"time data '{dt_str}' does not match any known format")
    
    unix_time = int(time.mktime(dt.timetuple()))
    return unix_time & 0xFFFFFFFF


def api_date_to_unix_int32(date_str: str):
    """
    Converts a date string in the format 'YYYY-MM-DDTHH:MM:SS' to a Unix timestamp.
    
    Parameters:
    - date_str (str): The date string to be converted. Must be in the format 'YYYY-MM-DDTHH:MM:SS'.
    
    Returns:
    - int: The Unix timestamp representation of the date.
    """
    print(f"Input date string: {date_str}")
    
    try:
        # Parse the date string to a datetime object
        parsed_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        print(f"Parsed datetime object: {parsed_date}")
        
        # Convert datetime object to Unix timestamp and cast to int32
        unix_timestamp = int(parsed_date.timestamp())
        print(f"Unix timestamp: {unix_timestamp}")
        
        return unix_timestamp
    except ValueError as e:
        # Print error message if date parsing fails
        print(f"Error parsing date string: {e}")
        return None
    


def determine_bucket_id(file_path):
    if "extracts" in file_path:
        return 1
    elif "external" in file_path:
        return 2
    else:
        raise ValueError(f"Unknown bucket ID for file_path: {file_path}")


def generate_uuid(task_creation_time):
    formats = ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            dt = datetime.strptime(task_creation_time, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"time data '{task_creation_time}' does not match any known format")
    
    unix_time = int(time.mktime(dt.timetuple()))
    unix_time_32bit = unix_time & 0xFFFFFFFF
    random_32bit = random.randint(0, 0xFFFFFFFF)
    uuid = (random_32bit & 0xFFFFFFFF) | (unix_time_32bit << 32)
    return uuid

def datetime_to_unix_int32(dt_str):
    formats = ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"time data '{dt_str}' does not match any known format")
    
    unix_time = int(dt.timestamp())
    return unix_time & 0xFFFFFFFF

def insert_into_all_images(image_data, dataset_id, all_images_collection):
    try:
        # Determine the bucket ID based on the file_path
        bucket_id = determine_bucket_id(image_data.get("file_path"))

        # Generate UUID and Unix timestamp
        task_creation_time = image_data.get("upload_date", str(datetime.now()))
        image_uuid = generate_uuid(task_creation_time)
        date_int32 = datetime_to_unix_int32(task_creation_time)

        # Create the document to be inserted
        new_document = {
            "uuid": image_uuid,
            "index": -1,  # Not used but included as per requirement
            "bucket_id": bucket_id,
            "dataset_id": dataset_id,
            "image_hash": image_data.get("image_hash"),
            "image_path": image_data.get("file_path"),
            "date": date_int32,
        }

        all_images_collection.insert_one(new_document)
        print(f"Inserted new document into all-images collection: {new_document}")

        return image_uuid

    except Exception as e:
        print(f"Error inserting into all-images collection: {e}")


def insert_into_all_images_for_completed(image_data, dataset_id, all_images_collection):
    try:
        # Determine the bucket ID based on the output file path
        file_path = image_data.get("task_output_file_dict", {}).get("output_file_path")
        if not file_path:
            print("No file path found in task_output_file_dict")
            return
        
        bucket_id = 0

        # Generate UUID and Unix timestamp
        task_creation_time = image_data.get("task_creation_time", str(datetime.now()))
        image_uuid = generate_uuid(task_creation_time)
        date_int32 = datetime_to_unix_int32(task_creation_time)

        # Create the document to be inserted
        new_document = {
            "uuid": image_uuid,
            "index": -1,  # Not used but included as per requirement
            "bucket_id": bucket_id,
            "dataset_id": dataset_id,
            "image_hash": image_data.get("task_output_file_dict", {}).get("output_file_hash"),
            "image_path": file_path,
            "date": date_int32,
        }

        all_images_collection.insert_one(new_document)
        print(f"Inserted new document into all-images collection: {new_document}")

        return image_uuid  # Return the generated UUID

    except Exception as e:
        print(f"Error inserting into all-images collection: {e}")
    


def check_image_usage(request, image_hash):
    """
    Check if the image is used in a selection datapoint, image pair ranking, or has a tag assigned.
    """
    # Check if the image is used in ranking datapoints
    datapoint_usage = request.app.ranking_datapoints_collection.find_one({
        "$or": [
            {"image_1_metadata.file_hash": image_hash},
            {"image_2_metadata.file_hash": image_hash}
        ]
    })

    if datapoint_usage:
        return False, "Image is used in a selection datapoint."

    # Check if the image is used in image pair ranking
    pair_ranking_usage = request.app.image_pair_ranking_collection.find_one({
        "$or": [
            {"image_1_metadata.file_hash": image_hash},
            {"image_2_metadata.file_hash": image_hash}
        ]
    })

    if pair_ranking_usage:
        return False, "Image is used in an image pair ranking."

    # Check if the image has a tag assigned
    tag_assigned = request.app.image_tags_collection.find_one({
        "image_hash": image_hash
    })

    if tag_assigned:
        return False, "Image has a tag assigned."

    return True, None


def remove_from_additional_collections(request, image_hash, bucket_id, image_source):
    """
    Remove documents associated with the given image_hash or file_hash
    from additional collections, with additional checks for bucket number 
    and image source.
    """
    collections_to_remove = [
        request.app.all_image_collection,
        request.app.image_rank_scores_collection,
        request.app.image_classifier_scores_collection,
        request.app.image_rank_use_count_collection,
        request.app.irrelevant_images_collection,  # This uses "file_hash" instead of "image_hash"
        request.app.image_hashes_collection,
    ]

    # Conditional collections based on image_source
    if image_source == "generated_image":
        collections_to_remove.extend([
            request.app.image_sigma_scores_collection,
            request.app.image_residuals_collection,
            request.app.image_percentiles_collection,
            request.app.image_residual_percentiles_collection,
        ])

    for collection in collections_to_remove:
        query = {}

        # Handle special cases
        if collection == request.app.all_image_collection:
            query = {"image_hash": image_hash, "bucket_id": bucket_id}
            print(f"Removing documents with image_hash: {image_hash} and bucket_id: {bucket_id} from {collection.name}")
        
        elif collection in [request.app.image_rank_scores_collection, 
                            request.app.image_classifier_scores_collection, 
                            request.app.irrelevant_images_collection]:
            if collection == request.app.irrelevant_images_collection:
                query = {"file_hash": image_hash, "image_source": image_source}
            else:
                query = {"image_hash": image_hash, "image_source": image_source}
            print(f"Removing documents with {query} from {collection.name}")
        
        else:
            query = {"image_hash": image_hash}
            print(f"Removing documents with image_hash: {image_hash} from {collection.name}")
        
        # Execute deletion
        result = collection.delete_many(query)
        print(f"Deleted {result.deleted_count} documents from {collection.name}")






def delete_files_from_minio(minio_client, bucket_name, object_name):
    """
    Delete files from MinIO. This function will attempt to delete the main file
    and associated files. If any file is missing, it will silently continue. 
    If there is any other error, it will raise an exception.
    """
    files_to_delete = [
        object_name,
        f"{object_name.rsplit('.', 1)[0]}_clip_kandinsky.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_clip.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_data.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_embedding.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_vae_latent.msgpack",
    ]

    for file in files_to_delete:
        try:
            minio_client.remove_object(bucket_name, file)
            print(f"Successfully removed {file} from {bucket_name}")
        except S3Error as e:
            if e.code == 'NoSuchKey':
                # Silently continue if the file does not exist
                print(f"File {file} does not exist in bucket {bucket_name}, skipping.")
                continue
            else:
                # Raise the exception if it's any other error
                raise e
        except Exception as e:
            # Raise the exception for any other general errors
            raise e



def uuid64_number_to_string(uuid_number):
    hex_string = uuid_number.to_bytes(8, 'big').hex()
    return hex_string[0:4] + '-' + hex_string[4:8] + '-' + hex_string[8:12] + '-' + hex_string[12:16]



def get_bucket_id(image_source):
    """
    Translate image_source to the corresponding bucket_id.
    """
    if image_source == 'generated_image':
        return 0
    elif image_source == 'extract_image':
        return 1
    elif image_source == 'external_image':
        return 2
    else:
        return None

def get_bucket_id_for_image_source(image_source: str) -> int:
    if image_source == 'generated_image':
        return 0
    elif image_source == 'extract_image':
        return 1
    elif image_source == 'external_image':
        return 2
    else:
        raise ValueError("Invalid image_source value")
