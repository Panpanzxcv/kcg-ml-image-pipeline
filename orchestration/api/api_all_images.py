from fastapi import Request, HTTPException, APIRouter, Response, Query, status, File, UploadFile
from datetime import datetime, timedelta
from typing import Optional
import pymongo
from utility.minio import cmd
from utility.path import separate_bucket_and_file_path
from .mongo_schemas import Task, ImageMetadata, UUIDImageMetadata, ListTask
from .api_utils import PrettyJSONResponse, StandardSuccessResponseV1, ApiResponseHandlerV1, WasPresentResponse, ErrorCode, api_date_to_unix_int32, build_date_query, validate_date_format
from .api_ranking import get_image_rank_use_count
import os
from .api_utils import find_or_create_next_folder_and_index
from orchestration.api.mongo_schema.all_images_schemas import AllImagesHelpers, AllImagesResponse, ListAllImagePathsResponse, ListAllImagesResponse
import io
from typing import List
from PIL import Image
import time

router = APIRouter()

@router.get("/all-images/list-images",
            description="list images according dataset_id and bucket_id",
            tags=["all-images"],
            response_model=StandardSuccessResponseV1[ListAllImagesResponse],
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def list_all_images(
    request: Request,
    bucket_ids: Optional[List[int]] = Query(None, description="Bucket IDs"),
    dataset_ids: Optional[List[int]] = Query(None, description="Dataset IDs"),
    limit: int = Query(20, description="Limit on the number of results returned"),
    offset: int = Query(0, description="Offset for the results to be returned"),
    order: str = Query("desc", description="Order in which the data should be returned. 'asc' for oldest first, 'desc' for newest first"),
    start_date: Optional[str] = Query(None, description="Start date for filtering results, Must be in the format 'YYYY-MM-DDTHH:MM:SS "),
    end_date: Optional[str] = Query(None, description="End date for filtering results, Must be in the format 'YYYY-MM-DDTHH:MM:SS"),
    time_interval: Optional[int] = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        query = {}

        # Add the OR conditions for buckets and datasets
        if bucket_ids or dataset_ids:
            query_conditions = []
            if bucket_ids:
                query_conditions.append({"bucket_id": {"$in": bucket_ids}})
            if dataset_ids:
                query_conditions.append({"dataset_id": {"$in": dataset_ids}})
            if query_conditions:
                query = {"$or": query_conditions}

        print(f"Initial query conditions: {query}")

        # Add date filters to the query
        date_query = {}
        if start_date:
            start_date_unix = api_date_to_unix_int32(start_date)
            if start_date_unix is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.OTHER_ERROR,
                    error_string="Invalid start_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=422
                )
            date_query['$gte'] = start_date_unix
        if end_date:
            end_date_unix = api_date_to_unix_int32(end_date)
            if end_date_unix is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.OTHER_ERROR,
                    error_string="Invalid end_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=422
                )
            date_query['$lte'] = end_date_unix
                

        print(f"Date query after adding start_date and end_date: {date_query}")

        # Calculate the time threshold based on the current time and the specified interval
        if time_interval is not None:
            current_time = datetime.utcnow()
            if time_unit == "minutes":
                threshold_time = current_time - timedelta(minutes=time_interval)
            elif time_unit == "hours":
                threshold_time = current_time - timedelta(hours=time_interval)
            else:
                raise HTTPException(status_code=400, detail="Invalid time unit. Use 'minutes' or 'hours'.")
            date_query['$gte'] = int(threshold_time.timestamp())

        print(f"Date query after adding time interval: {date_query}")

        if date_query:
            query['date'] = date_query

        print(f"Final query: {query}")

        # Decide the sort order based on the 'order' parameter
        sort_order = -1 if order == "desc" else 1

        # Query the collection with pagination and sorting
        cursor = request.app.all_image_collection.find(query).sort('date', sort_order).skip(offset).limit(limit)
        images = list(cursor)

        print(f"Number of images found: {len(images)}")

        AllImagesHelpers.clean_image_list_for_api_response(images)

        return response_handler.create_success_response_v1(
            response_data={"images": images},
            http_status_code=200
        )

    except Exception as e:
        print(f"Exception: {e}")
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )
    

@router.get("/all-images/get-image-by-hash", 
            description="Retrieve an image from all-images collection by its hash",
            tags=["all-images"],  
            response_model=StandardSuccessResponseV1[AllImagesResponse],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_image_by_hash(request: Request, image_hash: str):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        # Find the image in the all-images collection by its hash
        image_data = request.app.all_image_collection.find_one({"image_hash": image_hash})
        
        if image_data is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND, 
                error_string="Image with this hash does not exist in the all-images collection",
                http_status_code=404
            )
        
        AllImagesHelpers.clean_image_for_api_response(image_data)
        # Return the found image data
        return api_response_handler.create_success_response_v1(
            response_data=image_data,
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )
        


@router.get("/all-images/get-random-image", 
            tags = ["all-images"], 
            description= "Get random images by image type and date range",
            response_model=StandardSuccessResponseV1[ListAllImagePathsResponse],  
            responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
def get_random_images_by_date_range_and_image_type(
    request: Request,
    image_type: str = Query('all_resolutions', description="Resolution of the images to be returned. Options: 'all_resolutions', '512*512_resolutions'"),
    size: int = 10,
    start_date: str = None,
    end_date: str = None,
):
    response_handler = ApiResponseHandlerV1(request)
    
    try:
        # Add date filters to the query
        date_query = {}
        if start_date:
            start_date_unix = api_date_to_unix_int32(start_date)
            if start_date_unix is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.OTHER_ERROR,
                    error_string="Invalid start_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=422
                )
            date_query['$gte'] = start_date_unix
        if end_date:
            end_date_unix = api_date_to_unix_int32(end_date)
            if end_date_unix is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.OTHER_ERROR,
                    error_string="Invalid end_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=422
                )
            date_query['$lte'] = end_date_unix
        if start_date or end_date:
            query = {"date": date_query}
        else:
            query = {}
        if image_type == 'all_resolutions':
            pass
        elif image_type == '512*512_resolutions':
            query["bucket_id"] = {"$in": [0, 1]} # Get images from extract and datasets bucket
        else:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS, 
                error_string="Invalid resolution. Options: 'all_resolutions', '512*512_resolutions", 
                http_status_code=400,
            )
            
        pipeline = [
            {"$match": query},
            {"$sample": {"size": size}},
            {"$project": {"_id": 0, "image_path": 1}}
        ]
        
        images = list(request.app.all_image_collection.aggregate(pipeline))
        image_paths = [image['image_path'] for image in images]
        return response_handler.create_success_response_v1(
                                                            response_data={"image_paths": image_paths}, 
                                                            http_status_code=200,
                                                            )
    
    except Exception as e:
        # Log the exception details here, if necessary
        print(e)
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, error_string="Internal Server Error", http_status_code=500
        )