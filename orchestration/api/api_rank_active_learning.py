import uuid
from fastapi import Request, HTTPException, APIRouter, Response, Query, status
from datetime import datetime, timedelta
import pymongo 
from utility.minio import cmd
from orchestration.api.mongo_schema.active_learning_schemas import RankSelection, ListResponseRankSelection, ResponseRankSelection, FlaggedResponse, JsonMinioResponse, RankSelectionV1
from .api_utils import ApiResponseHandlerV1, ErrorCode, StandardSuccessResponseV1, StandardErrorResponseV1, WasPresentResponse, CountResponse, IrrelevantResponse, ListIrrelevantResponse, BoolIrrelevantResponse, ListGenerationsCountPerDayResponse, IrrelevantResponseV1
from orchestration.api.mongo_schema.active_learning_schemas import  RankActiveLearningPair, ListRankActiveLearningPair, ResponseImageInfo, ResponseImageInfoV1, ListScoreImageTask, ListRankActiveLearningPairWithScore, ResponseRankSelectionV1
from .mongo_schemas import FlaggedDataUpdate
import os
from datetime import datetime, timezone
from typing import List
from io import BytesIO
from bson import ObjectId
from typing import Optional
from pymongo import ReturnDocument
import json
from collections import OrderedDict
import io
import random
import time

router = APIRouter()
 
generated_image = "generated_image"

@router.post("/rank-active-learning-queue/add-image-pair",
             description="Adds a new image pair to the rank active ranking queue. If there is already a pair with the same images, rank and policy, no new entry is added to the queue.",
             status_code=200,
             response_model=StandardSuccessResponseV1[RankActiveLearningPair],
             tags=["Rank Active Learning"],  
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
def add_image_pair(request: Request, job_uuid_1: str = Query(...), job_uuid_2: str = Query(...), rank_active_learning_policy_id: int = Query(...), rank_model_id: int = Query(...), metadata: str = Query(None), generation_string: str = Query(None) ):
    api_response_handler = ApiResponseHandlerV1(request)

    # Check if an entry with the same parameters already exists
    existing_pair = request.app.rank_active_learning_pairs_collection.find_one({
        "rank_model_id": rank_model_id,
        "rank_active_learning_policy_id": rank_active_learning_policy_id,
        "images_data.job_uuid_1": job_uuid_1,
        "images_data.job_uuid_2": job_uuid_2
    })

    if existing_pair:
        existing_pair.pop('_id', None)  # Remove MongoDB ObjectId from the response
        return api_response_handler.create_success_response_v1(
            response_data=existing_pair,
            http_status_code=200
        )

    def extract_job_details(job_uuid, suffix):

        job = request.app.completed_jobs_collection.find_one({"uuid": job_uuid})

        output_file_path = job["task_output_file_dict"]["output_file_path"]
        task_creation_time = job["task_creation_time"]
        path_parts = output_file_path.split('/')
        if len(path_parts) < 4:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Invalid output file path format",
                http_status_code=422
            )

        return {
            f"job_uuid_{suffix}": job_uuid,
            f"file_name_{suffix}": path_parts[-1],
            f"image_path_{suffix}": output_file_path,
            f"image_hash_{suffix}": job["task_output_file_dict"]["output_file_hash"],
            f"job_creation_time_{suffix}": task_creation_time,
        }
    
    job_1 = request.app.completed_jobs_collection.find_one({"uuid": job_uuid_1})
    if not job_1:
        return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Job {job_uuid_1} not found",
                http_status_code=404
            )
    else:
        job_details_1 = extract_job_details(job_uuid_1, "1")

    job_2 = request.app.completed_jobs_collection.find_one({"uuid": job_uuid_2})
    if not job_2:
        return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Job {job_uuid_2} not found",
                http_status_code=404
            )
    else:
        job_details_2 = extract_job_details(job_uuid_2, "2")


    rank = request.app.rank_collection.find_one(
        {"rank_model_id": rank_model_id}
    )

    if not rank:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Rank with ID {rank} not found",
            http_status_code=404
        )


    policy = request.app.rank_active_learning_policies_collection.find_one(
        {"rank_active_learning_policy_id": rank_active_learning_policy_id}
    )
    if not policy:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Active learning policy with ID {rank_active_learning_policy_id} not found",
            http_status_code=404
        )
    
    policy_string = policy.get("rank_active_learning_policy", "")

    combined_job_details = {
        "rank_model_id": rank_model_id,
        "rank_active_learning_policy_id": rank_active_learning_policy_id,
        "metadata": metadata,
        "generation_string": generation_string,
        "creation_date": datetime.utcnow().isoformat(),  # UTC time
        "images_data": [job_details_1, job_details_2]
    }

    creation_date_1 = datetime.fromisoformat(job_details_1["job_creation_time_1"]).strftime("%Y-%m-%d")
    creation_date_2 = datetime.fromisoformat(job_details_2["job_creation_time_2"]).strftime("%Y-%m-%d")

    json_data = json.dumps([combined_job_details], indent=4).encode('utf-8')  # Note the list brackets around combined_job_details
    data = BytesIO(json_data)

    # Define the path for the JSON file
    base_file_name_1 = job_details_1['file_name_1'].split('.')[0]
    base_file_name_2 = job_details_2['file_name_2'].split('.')[0]
    json_file_name = f"{policy_string}_{creation_date_1}_{base_file_name_1}_and_{creation_date_2}_{base_file_name_2}.json"
    full_path = f"ranks/{combined_job_details['rank_model_id']}/active_learning_queue/{json_file_name}"

    cmd.upload_data(request.app.minio_client, "datasets", full_path, data)


    mongo_combined_job_details = {"file_name": json_file_name, **combined_job_details}
    request.app.rank_active_learning_pairs_collection.insert_one(mongo_combined_job_details)

    mongo_combined_job_details.pop('_id', None)


    return api_response_handler.create_success_response_v1(
            response_data=mongo_combined_job_details,
            http_status_code=200
        )



@router.get("/rank-active-learning-queue/list-image-pairs",
            description="Lists all the rank active learning image pairs",
            response_model=StandardSuccessResponseV1[ListRankActiveLearningPair],
            status_code=200,
            tags=["Rank Active Learning"],  
            responses=ApiResponseHandlerV1.listErrors([400, 422]))
def list_image_pairs(request: Request):
    api_response_handler = ApiResponseHandlerV1(request)
    
    # check if exist
    items = list(request.app.rank_active_learning_pairs_collection.find({}))
    
    score_data = []
    for item in items:
        # remove the auto generated '_id' field
        item.pop('_id', None)
        score_data.append(item)
    
    # Return a standardized success response with the score data
    return api_response_handler.create_success_response_v1(
        response_data={"pairs": score_data},
        http_status_code=200
    )

@router.delete("/rank-active-learning-queue/delete-image-pair",
               description="Deletes an image pair from the rank active learning queue and removes the file from MinIO.",
               response_model=StandardSuccessResponseV1[WasPresentResponse],
               status_code=200,
               tags=["Rank Active Learning"],
               responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def delete_image_rank_data_point(request: Request, file_name: str = Query(..., description="The file name of the data point to delete")):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        # Find the document to get the full path for deletion in MinIO
        document = request.app.rank_active_learning_pairs_collection.find_one({"file_name": file_name})
        
        if not document:
            return api_response_handler.create_success_delete_response_v1(False)
        
        # Construct the full path of the file in MinIO
        full_path = f"ranks/{document['rank_model_id']}/active_learning_queue/{file_name}"
        
        # Delete the file from MinIO
        try:
            request.app.minio_client.remove_object("datasets", full_path)
            print(f"Deleted file from MinIO: {full_path}")
        except Exception as minio_error:
            print(f"Error deleting file from MinIO: {str(minio_error)}")
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.OTHER_ERROR,
                error_string=f"Failed to delete file from MinIO: {str(minio_error)}",
                http_status_code=500
            )

        # Delete the document from MongoDB
        delete_result = request.app.rank_active_learning_pairs_collection.delete_one({"file_name": file_name})
        # Return a standard response with wasPresent set to true if there was a deletion
        return api_response_handler.create_success_delete_response_v1(delete_result.deleted_count != 0)
    
    except Exception as e:
        print(f"Error during API execution: {str(e)}")
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )

    


@router.get("/rank-active-learning-queue/count-image-pairs", 
            response_model=StandardSuccessResponseV1[CountResponse],
            status_code=200,
            tags=["Rank Active Learning"],
            description="Counts how many image pairs in the rank active learning queue for specified policy and model.",
            responses=ApiResponseHandlerV1.listErrors([500]))
async def count_queue_pairs(request: Request, 
                            policy_id: int = Query(None, description="Filter by the rank active learning policy ID"), 
                            rank_model_id: int = Query(None, description="Filter by the rank model ID")):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Build the query based on the provided parameters
        query = {}
        if policy_id is not None:
            query['rank_active_learning_policy_id'] = policy_id
        if rank_model_id is not None:
            query['rank_model_id'] = rank_model_id

        # Count documents in the rank_active_learning_pairs_collection with the constructed query
        count = request.app.rank_active_learning_pairs_collection.count_documents(query)

        return api_response_handler.create_success_response_v1(
            response_data={"count": count},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        ) 



@router.get("/rank-active-learning-queue/get-random-image-pair", 
            description="Gets random image pairs from the rank active learning queue",
            response_model=StandardSuccessResponseV1[ListRankActiveLearningPair],
            status_code=200,
            tags=["Rank Active Learning"],  
            responses=ApiResponseHandlerV1.listErrors([400, 422]))
async def random_queue_pair(request: Request, rank_model_id : Optional[int] = None, size: int = 1, rank_active_learning_policy_id: Optional[int] = None):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        # Define the aggregation pipeline
        pipeline = []

        match_filter = {}
        if rank_model_id is not None:
            match_filter["rank_model_id"] = rank_model_id
        if rank_active_learning_policy_id is not None:
            match_filter["rank_active_learning_policy_id"] = rank_active_learning_policy_id

        if match_filter:
            pipeline.append({"$match": match_filter})

        # Add the random sampling stage to the pipeline
        pipeline.append({"$sample": {"size": size}})

        # Use MongoDB's aggregation framework to randomly select documents
        random_pairs_cursor = request.app.rank_active_learning_pairs_collection.aggregate(pipeline)

        # Convert the cursor to a list of dictionaries
        random_pairs = []
        for pair in random_pairs_cursor:
            pair['_id'] = str(pair['_id'])  # Convert _id ObjectId to string
            random_pairs.append(pair)

        return api_response_handler.create_success_response_v1(
            response_data={"pairs": random_pairs},
            http_status_code=200
        )

    except Exception as e:
        # Handle exceptions that may occur during database operation
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )

@router.get("/rank-active-learning-queue/get-random-image-pair-v1", 
            description="Gets random image pairs from the rank active learning queue, It returns the classifier score of each image as a number or null (if no score is found for the image",
            response_model=StandardSuccessResponseV1[ListRankActiveLearningPairWithScore],
            status_code=200,
            tags=["Rank Active Learning"],  
            responses=ApiResponseHandlerV1.listErrors([400, 422]))
async def random_queue_pair(request: Request, rank_model_id: Optional[int] = None, size: int = 1, rank_active_learning_policy_id: Optional[int] = None):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        # Define the aggregation pipeline
        pipeline = []

        match_filter = {}
        if rank_model_id is not None:
            match_filter["rank_model_id"] = rank_model_id
        if rank_active_learning_policy_id is not None:
            match_filter["rank_active_learning_policy_id"] = rank_active_learning_policy_id

        if match_filter:
            pipeline.append({"$match": match_filter})

        # Add the random sampling stage to the pipeline
        pipeline.append({"$sample": {"size": size}})

        # Use MongoDB's aggregation framework to randomly select documents
        random_pairs_cursor = request.app.rank_active_learning_pairs_collection.aggregate(pipeline)

        # Convert the cursor to a list of dictionaries
        random_pairs = []
        for pair in random_pairs_cursor:
            pair['_id'] = str(pair['_id'])  # Convert _id ObjectId to string
            random_pairs.append(pair)

        # Fetch classifier_id from rank_model_id
        classifier_id = None
        if rank_model_id is not None:
            rank = request.app.rank_collection.find_one({'rank_model_id': rank_model_id})
            if rank:
                classifier_id = rank.get("classifier_id")

        if classifier_id is not None:
            for pair in random_pairs:
                images_data = pair['images_data']
                if len(images_data) == 2:
                    job_uuid_1 = images_data[0].get('job_uuid_1')
                    job_uuid_2 = images_data[1].get('job_uuid_2')
                    score_1 = request.app.image_classifier_scores_collection.find_one(
                        {'classifier_id': classifier_id, 'job_uuid': job_uuid_1}, {'score': 1, '_id': 0}
                    )
                    score_2 = request.app.image_classifier_scores_collection.find_one(
                        {'classifier_id': classifier_id, 'job_uuid': job_uuid_2}, {'score': 1, '_id': 0}
                    )
                    pair['score_1'] = score_1['score'] if score_1 else None
                    pair['score_2'] = score_2['score'] if score_2 else None
                else:
                    pair['score_1'] = None
                    pair['score_2'] = None
        else:
            for pair in random_pairs:
                pair['score_1'] = None
                pair['score_2'] = None

        return api_response_handler.create_success_response_v1(
            response_data={"pairs": random_pairs},
            http_status_code=200
        )

    except Exception as e:
        # Handle exceptions that may occur during database operation
        print(f"Error during API execution: {str(e)}")
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )




@router.post("/rank-training/add-ranking-data-point", 
             status_code=201,
             tags=['rank-training'],
             response_model=StandardSuccessResponseV1[ResponseRankSelection],
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def add_datapoints(request: Request, selection: RankSelection, image_source: str = Query(..., description="Image source to filter by", regex="^(generated_image|external_image|extract_image)$")):
    api_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        rank = request.app.rank_collection.find_one(
            {"rank_model_id": selection.rank_model_id}
        )

        if not rank:
            return api_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Rank with ID {selection.rank_model_id} not found",
                http_status_code=404
            )

        policy = None
        if selection.rank_active_learning_policy_id:
            policy = request.app.rank_active_learning_policies_collection.find_one(
                {"rank_active_learning_policy_id": selection.rank_active_learning_policy_id}
            )

        # Extract policy details only if policy is not None
        rank_active_learning_policy = policy.get("rank_active_learning_policy", None) if policy else None

        current_time = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
        file_name = f"{current_time}-{selection.username}.json"
        dataset = selection.image_1_metadata.file_path.split('/')[1]
        rank_model_string = rank.get("rank_model_string", None)

        # Convert selection to dict and add image_source to image metadata
        dict_data = selection.to_dict()
        dict_data['image_1_metadata']['image_source'] = image_source
        dict_data['image_2_metadata']['image_source'] = image_source

        # Prepare ordered data for MongoDB insertion
        mongo_data = OrderedDict([
            ("_id", ObjectId()),  # Generate new ObjectId
            ("file_name", file_name),
            *dict_data.items(),  # Unpack the rest of dict_data
            ("datetime", current_time)
        ])

        # Insert the ordered data into MongoDB
        request.app.ranking_datapoints_collection.insert_one(mongo_data)

        formatted_rank_model_id = f"{selection.rank_model_id:05d}"
        # Prepare data for MinIO upload (excluding the '_id' field)
        minio_data = mongo_data.copy()
        minio_data.pop("_id")
        minio_data.pop("file_name")
        path = f"ranks/{formatted_rank_model_id}/data/ranking/aggregate"
        full_path = os.path.join(path, file_name)
        json_data = json.dumps(minio_data, indent=4).encode('utf-8')
        data = BytesIO(json_data)

        # Upload data to MinIO
        try:
            cmd.upload_data(request.app.minio_client, "datasets", full_path, data)
            print(f"Uploaded successfully to MinIO: {full_path}")
        except Exception as e:
            print(f"Error uploading to MinIO: {str(e)}")
            return api_handler.create_error_response_v1(
                error_code=ErrorCode.OTHER_ERROR,
                error_string=f"Failed to upload file to MinIO: {str(e)}",
                http_status_code=500
            )

        mongo_data.pop("_id")
        # Return a success response
        return api_handler.create_success_response_v1(
            response_data=mongo_data,
            http_status_code=201
        )

    except Exception as e:
        return api_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )
    

@router.post("/rank-training/add-ranking-data-point-v1", 
             status_code=201,
             tags=['rank-training'],
             response_model=StandardSuccessResponseV1[ResponseRankSelectionV1],
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def add_datapoints_v1(request: Request, selection: RankSelectionV1):
    api_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        valid_image_sources = {"generated_image", "extract_image", "external_image"}
        
        if selection.image_1_metadata.image_source not in valid_image_sources:
            return api_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="Invalid image_source for image_1_metadata. Must be one of 'generated_image', 'extract_image', 'external_image'.",
                http_status_code=422
            )
        
        if selection.image_2_metadata.image_source not in valid_image_sources:
            return api_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="Invalid image_source for image_2_metadata. Must be one of 'generated_image', 'extract_image', 'external_image'.",
                http_status_code=422
            )
        
        rank = request.app.rank_collection.find_one(
            {"rank_model_id": selection.rank_model_id}
        )

        if not rank:
            return api_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Rank with ID {selection.rank_model_id} not found",
                http_status_code=404
            )

        policy = None
        if selection.rank_active_learning_policy_id:
            policy = request.app.rank_active_learning_policies_collection.find_one(
                {"rank_active_learning_policy_id": selection.rank_active_learning_policy_id}
            )

        current_time = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
        file_name = f"{current_time}-{selection.username}.json"

        dict_data = selection.to_dict()

        # Prepare ordered data for MongoDB insertion
        mongo_data = OrderedDict([
            ("_id", ObjectId()),  # Generate new ObjectId
            ("file_name", file_name),
            *dict_data.items(),  # Unpack the rest of dict_data
            ("datetime", current_time)
        ])

        # Insert the ordered data into MongoDB
        request.app.ranking_datapoints_collection.insert_one(mongo_data)

        formatted_rank_model_id = f"{selection.rank_model_id:05d}"
        # Prepare data for MinIO upload (excluding the '_id' field)
        minio_data = mongo_data.copy()
        minio_data.pop("_id")
        minio_data.pop("file_name")
        path = f"ranks/{formatted_rank_model_id}/data/ranking/aggregate"
        full_path = os.path.join(path, file_name)
        json_data = json.dumps(minio_data, indent=4).encode('utf-8')
        data = BytesIO(json_data)

        # Upload data to MinIO
        try:
            cmd.upload_data(request.app.minio_client, "datasets", full_path, data)
            print(f"Uploaded successfully to MinIO: {full_path}")
        except Exception as e:
            print(f"Error uploading to MinIO: {str(e)}")
            return api_handler.create_error_response_v1(
                error_code=ErrorCode.OTHER_ERROR,
                error_string=f"Failed to upload file to MinIO: {str(e)}",
                http_status_code=500
            )

        mongo_data.pop("_id")
        # Return a success response
        return api_handler.create_success_response_v1(
            response_data=mongo_data,
            http_status_code=201
        )

    except Exception as e:
        return api_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )


@router.get("/rank-training/list-ranking-datapoints",
            description="Lists all the ranking datapoints",
            response_model=StandardSuccessResponseV1[ListResponseRankSelection],
            status_code=200,
            tags=["rank-training"],  
            responses=ApiResponseHandlerV1.listErrors([400, 422]))
def list_ranking_datapoints(request: Request):
    api_response_handler = ApiResponseHandlerV1(request)
    
    # check if exist
    items = list(request.app.ranking_datapoints_collection.find({}))
    
    score_data = []
    for item in items:
        # remove the auto generated '_id' field
        item.pop('_id', None)
        
        # Ensure all fields are present, set default values if not
        item.setdefault('flagged', None)
        item.setdefault('flagged_by_user', None)
        item.setdefault('flagged_time', None)
        
        score_data.append(item)
    
    # Return a standardized success response with the score data
    return api_response_handler.create_success_response_v1(
        response_data={"datapoints": score_data},
        http_status_code=200
    )

@router.get("/rank-training/sort-ranking-data-by-date", 
            description="changed with /rank-training/sort-ranking-data-by-date-v1",
            tags=["deprecated3"],
            response_model=StandardSuccessResponseV1[ListResponseRankSelection],  
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def sort_ranking_data_by_date_v2(
    request: Request,
    start_date: Optional[str] = Query(None, description="Start date (inclusive) in YYYY-MM-DD format"),
    rank_model_id: Optional[int] = Query(None, description="Rank model ID to filter by"),
    end_date: Optional[str] = Query(None, description="End date (inclusive) in YYYY-MM-DD format"),
    skip: int = Query(0, alias="offset"),
    limit: int = Query(10, alias="limit"),
    order: str = Query("desc", regex="^(desc|asc)$")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        query_filter = {}
        date_filter = {}

        if rank_model_id is not None:
            query_filter["rank_model_id"] = rank_model_id

        if start_date:
            date_filter["$gte"] = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            date_filter["$lte"] = datetime.strptime(end_date, "%Y-%m-%d")

        if date_filter:
            query_filter["datetime"] = date_filter

        sort_order = pymongo.DESCENDING if order == "desc" else pymongo.ASCENDING
        cursor = request.app.ranking_datapoints_collection.find(query_filter).sort(
            "datetime", sort_order  
        ).skip(skip).limit(limit)

        ranking_data = []
        for doc in cursor:
            doc.pop('_id', None)  # Correctly remove '_id' field from each document
            
            # Ensure all fields are present, set default values if not
            doc.setdefault('flagged', None)
            doc.setdefault('flagged_by_user', None)
            doc.setdefault('flagged_time', None)
            
            ranking_data.append(doc)

        return response_handler.create_success_response_v1(
            response_data={"datapoints": ranking_data}, 
            http_status_code=200
        )
    except Exception as e:
        print("Error during API execution:", str(e))
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Internal Server Error: {str(e)}",
            http_status_code=500
        )

  

@router.get("/rank-training/sort-ranking-data-by-date-v1", 
            description="Sort rank data by date",
            tags=["rank-training"],
            response_model=StandardSuccessResponseV1[ListResponseRankSelection],  
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def sort_ranking_data_by_date_v3(
    request: Request,
    start_date: Optional[str] = Query(None, description="Start date (inclusive) in YYYY-MM-DD format"),
    rank_model_id: Optional[int] = Query(None, description="Rank model ID to filter by"),
    end_date: Optional[str] = Query(None, description="End date (inclusive) in YYYY-MM-DD format"),
    skip: int = Query(0, alias="offset"),
    limit: int = Query(10, alias="limit"),
    order: str = Query("desc", regex="^(desc|asc)$"),
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        query_filter = {}
        date_filter = {}

        if rank_model_id is not None:
            query_filter["rank_model_id"] = rank_model_id

        if start_date:
            date_filter["$gte"] = start_date + "-00-00-00"
        if end_date:
            date_filter["$lte"] = end_date + "-23-59-59"

        if date_filter:
            query_filter["datetime"] = date_filter

        sort_order = pymongo.DESCENDING if order == "desc" else pymongo.ASCENDING
        cursor = request.app.ranking_datapoints_collection.find(query_filter).sort(
            "datetime", sort_order  
        ).skip(skip).limit(limit)

        ranking_data = []
        for doc in cursor:
            doc.pop('_id', None)  # Correctly remove '_id' field from each document
            
            # Ensure all fields are present, set default values if not
            doc.setdefault('flagged', None)
            doc.setdefault('flagged_by_user', None)
            doc.setdefault('flagged_time', None)
            
            ranking_data.append(doc)

        return response_handler.create_success_response_v1(
            response_data={"datapoints": ranking_data}, 
            http_status_code=200
        )
    except Exception as e:
        print("Error during API execution:", str(e))
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Internal Server Error: {str(e)}",
            http_status_code=500
        )



@router.get("/rank-training/count-ranking-data-points", 
            description="Count ranking data points based on specific rank models or policies",
            tags=["rank-training"],
            response_model=StandardSuccessResponseV1[CountResponse],  
            responses=ApiResponseHandlerV1.listErrors([500]))
async def count_ranking_data(request: Request, 
                             policy_id: int = Query(None, description="Filter by the rank active learning policy ID"), 
                             rank_model_id: int = Query(None, description="Filter by the rank model ID")):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Build the query based on the provided parameters
        query = {}
        if policy_id is not None:
            query['rank_active_learning_policy_id'] = policy_id
        if rank_model_id is not None:
            query['rank_model_id'] = rank_model_id

        # Get the count of documents in the ranking_datapoints_collection based on the constructed query
        count = request.app.ranking_datapoints_collection.count_documents(query)

        # Return the count with a success response
        return response_handler.create_success_response_v1(
            response_data={"count": count}, 
            http_status_code=200,
        )
    except Exception as e:
        # Handle exceptions and return an error response
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )

@router.put("/rank-training/update-ranking-datapoint", 
            tags=['rank-training'], 
            response_model=StandardSuccessResponseV1[FlaggedResponse],
            responses=ApiResponseHandlerV1.listErrors([404, 422]))
async def update_ranking_datapoint(request: Request, rank_model_id: int, filename: str, update_data: FlaggedDataUpdate):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    formatted_rank_model_id = f"{rank_model_id:05d}"

    # Construct the object name based on the dataset
    object_name = f"ranks/{formatted_rank_model_id}/data/ranking/aggregate/{filename}"

    flagged_time = datetime.now().isoformat()

    # Fetch the content of the specified JSON file from MinIO
    try:
        data = cmd.get_file_from_minio(request.app.minio_client, "datasets", object_name)
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Error fetching file {filename}: {str(e)}",
            http_status_code=404,
        )

    if data is None:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"File {filename} not found.",
            http_status_code=404,
        )

    file_content = ""
    for chunk in data.stream(32 * 1024):
        file_content += chunk.decode('utf-8')

    try:
        # Load the existing content and update it
        content_dict = json.loads(file_content)
        content_dict["flagged"] = update_data.flagged
        content_dict["flagged_by_user"] = update_data.flagged_by_user
        content_dict["flagged_time"] = flagged_time

        # Save the modified file back to MinIO
        updated_content = json.dumps(content_dict, indent=2)
        updated_data = io.BytesIO(updated_content.encode('utf-8'))
        request.app.minio_client.put_object("datasets", object_name, updated_data, len(updated_content))
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Failed to update file {filename}: {str(e)}",
            http_status_code=500,
        )

    # Update the document in MongoDB
    query = {"file_name": filename}
    update = {"$set": {
        "flagged": update_data.flagged,
        "flagged_by_user": update_data.flagged_by_user,
        "flagged_time": datetime.now().isoformat()
    }}
    updated_document = request.app.ranking_datapoints_collection.find_one_and_update(
        query, update, return_document=ReturnDocument.AFTER
    )

    if updated_document is None:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Document with filename {filename} not found in MongoDB.",
            http_status_code=404,
        )
    
    if '_id' in updated_document:
        updated_document['_id'] = str(updated_document['_id'])

    return response_handler.create_success_response_v1(
        response_data=updated_document,
        http_status_code=200,
    )        


@router.get("/rank-training/read-ranking-datapoint", 
            tags=['rank-training'], 
            description="read ranking datapoints",
            response_model=StandardSuccessResponseV1[JsonMinioResponse], 
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def read_ranking_datapoints(request: Request, rank_model_id: int, filename: str = Query(..., description="Filename of the JSON to read")):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        formatted_rank_model_id = f"{rank_model_id:05d}"
        # Construct the object name for ranking
        object_name = f"ranks/{formatted_rank_model_id}/data/ranking/aggregate/{filename}"

        # Fetch the content of the specified JSON file
        data = cmd.get_file_from_minio(request.app.minio_client, "datasets", object_name)

        if data is None:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"File {filename} not found.",
                http_status_code=404,
            )

        file_content = ""
        for chunk in data.stream(32 * 1024):
            file_content += chunk.decode('utf-8')

        json_content = json.loads(file_content)
    

        # Ensure all fields are present, set default values if not
        json_content.setdefault('flagged', None)
        json_content.setdefault('flagged_by_user', None)
        json_content.setdefault('flagged_time', None)

        # Successfully return the content of the JSON file
        return response_handler.create_success_response_v1(
            response_data={"json_content": json_content},
            http_status_code=200
        )
    except Exception as e:
        # Handle exceptions and return an error response
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string="Internal Server Error",
            http_status_code=500,
        )
  

@router.post("/rank-training/add-irrelevant-image",
             description="changed with /rank-training/add-irrelevant-image-v1 ",
             status_code=200,
             response_model=StandardSuccessResponseV1[IrrelevantResponse],
             tags=["deprecated3"],
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
def add_irrelevant_image(request: Request, job_uuid: str = Query(...), rank_model_id: int = Query(...)):
    api_response_handler = ApiResponseHandlerV1(request)

    # Fetch the image details from the completed_jobs_collection
    job = request.app.completed_jobs_collection.find_one({"uuid": job_uuid})
    if not job:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Job with UUID {job_uuid} not found in the completed jobs collection",
            http_status_code=404
        )

    rank = request.app.rank_collection.find_one({"rank_model_id": rank_model_id})
    if not rank:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Rank model not found in the rank models collection",
            http_status_code=404
        )

    # Check if the image is already marked as irrelevant
    existing_entry = request.app.irrelevant_images_collection.find_one({"uuid": job_uuid, "rank_model_id": rank_model_id})
    if existing_entry:
        existing_entry.pop('_id')
        return api_response_handler.create_success_response_v1(
            response_data=existing_entry,
            http_status_code=200
        )

    # Extract the relevant details
    image_data = {
        "uuid": job["uuid"],
        "file_hash": job["task_output_file_dict"]["output_file_hash"],
        "rank_model_id": rank_model_id,
        "image_source": generated_image
    }

    # Insert the UUID data into the irrelevant_images_collection
    inserted_id = request.app.irrelevant_images_collection.insert_one(image_data).inserted_id
    inserted_image_data = request.app.irrelevant_images_collection.find_one({"_id": inserted_id})

    # Remove the '_id' field from the response data
    if '_id' in inserted_image_data:
        inserted_image_data.pop('_id')

    return api_response_handler.create_success_response_v1(
        response_data=inserted_image_data,
        http_status_code=200
    )

@router.post("/rank-training/add-irrelevant-image-v1",
             description="Adds an image UUID to the irrelevant images collection",
             status_code=200,
             response_model=StandardSuccessResponseV1[IrrelevantResponseV1],
             tags=["rank-training"],
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
def add_irrelevant_image_v1(
    request: Request,
    job_uuid: str = Query(...),
    rank_model_id: int = Query(...),
    image_source: str = Query(..., regex="^(generated_image|extract_image|external_image)$")
):
    api_response_handler = ApiResponseHandlerV1(request)

    # Determine the appropriate collection based on image_source
    if image_source == "generated_image":
        collection = request.app.completed_jobs_collection
        projection = {"task_output_file_dict.output_file_hash": 1}
    elif image_source == "extract_image":
        collection = request.app.extracts_collection
        projection = {"image_hash": 1}
    elif image_source == "external_image":
        collection = request.app.external_images_collection
        projection = {"image_hash": 1}
    else:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string="Invalid image source provided.",
            http_status_code=422
        )

    image_query = {
        '$or': [
            {'uuid': job_uuid},
            {'uuid': uuid.UUID(job_uuid)}
        ]
    }

    # Fetch the image details from the determined collection
    job = collection.find_one(image_query, projection)
    if not job:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Job with UUID {job_uuid} not found in the {image_source} collection",
            http_status_code=404
        )

    rank = request.app.rank_collection.find_one({"rank_model_id": rank_model_id})
    if not rank:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string=f"Rank model not found in the rank models collection",
            http_status_code=404
        )

    # Check if the image is already marked as irrelevant
    existing_entry = request.app.irrelevant_images_collection.find_one({"uuid": job_uuid, "rank_model_id": rank_model_id})
    if existing_entry:
        existing_entry.pop('_id')
        return api_response_handler.create_success_response_v1(
            response_data=existing_entry,
            http_status_code=200
        )

    # Extract the relevant details
    if image_source == "generated_image":
        file_hash = job["task_output_file_dict"]["output_file_hash"]
    else:
        file_hash = job["image_hash"]

    image_data = {
        "uuid": job_uuid,
        "file_hash": file_hash,
        "rank_model_id": rank_model_id,
        "image_source": image_source
    }

    # Insert the UUID data into the irrelevant_images_collection
    inserted_id = request.app.irrelevant_images_collection.insert_one(image_data).inserted_id
    inserted_image_data = request.app.irrelevant_images_collection.find_one({"_id": inserted_id})

    # Remove the '_id' field from the response data
    if '_id' in inserted_image_data:
        inserted_image_data.pop('_id')

    return api_response_handler.create_success_response_v1(
        response_data=inserted_image_data,
        http_status_code=200
    )



@router.get("/rank-training/list-irrelevant-images", 
            description="changed with /rank-training/list-irrelevant-images-v1",
            tags=["deprecated3"],
            status_code=200,
            response_model=StandardSuccessResponseV1[ListIrrelevantResponse],
            responses=ApiResponseHandlerV1.listErrors([500]))
def list_irrelevant_images(request: Request):
    response_handler = ApiResponseHandlerV1(request)
    try:
        # Query all the rank models
        image_cursor = list(request.app.irrelevant_images_collection.find({}))

        # Convert each rank document to rankmodel and then to a dictionary
        for doc in image_cursor:
            doc.pop('_id', None)  

        return response_handler.create_success_response_v1(
            response_data={"images": image_cursor}, 
            http_status_code=200,
            )

    except Exception as e:
        return response_handler.create_error_response_v1(error_code=ErrorCode.OTHER_ERROR, 
                                                         error_string="Internal server error", 
                                                         http_status_code=500,
                            
                                                         )

@router.get("/rank-training/list-irrelevant-images-v1", 
            description="List irrelevant images with optional rank filter and pagination",
            tags=["rank-training"],
            status_code=200,
            response_model=StandardSuccessResponseV1[ListIrrelevantResponse],
            responses=ApiResponseHandlerV1.listErrors([500]))
def list_irrelevant_images_v1(
    request: Request,
    rank_model_id: Optional[int] = Query(None, description="Filter by rank model ID"),
    limit: int = Query(20, description="Limit on the number of results returned"),
    offset: int = Query(0, description="Offset for pagination"),
    order: str = Query("desc", description="Sort order: 'asc' for ascending, 'desc' for descending")
):
    response_handler = ApiResponseHandlerV1(request)
    try:
        # Build the query with optional rank_model_id filter
        query = {}
        if rank_model_id is not None:
            query["rank_model_id"] = rank_model_id

        # Decide the sort order based on the 'order' parameter
        sort_order = -1 if order == "desc" else 1

        # Query the irrelevant_images_collection with pagination
        image_cursor = request.app.irrelevant_images_collection.find(query).sort("rank_model_id", sort_order).skip(offset).limit(limit)

        # Convert each document to a dictionary and remove '_id' field
        image_list = []
        for doc in image_cursor:
            doc.pop('_id', None)
            image_list.append(doc)

        return response_handler.create_success_response_v1(
            response_data={"images": image_list}, 
            http_status_code=200,
        )

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string="Internal server error", 
            http_status_code=500,
        )


@router.delete("/rank-training/remove-irrelevant-image",
             description="changed with /rank-training/remove-irrelevant-image-v1 ",
             status_code=200,
             response_model=StandardSuccessResponseV1[WasPresentResponse],
             tags=["deprecated3"],
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
def unset_irrelevant_image(request: Request, job_uuid: str = Query(...), rank_model_id: int = Query(...)):
    api_response_handler = ApiResponseHandlerV1(request)

    # Check if the job exists in the irrelevant_images_collection
    query = {"uuid": job_uuid, "rank_model_id": rank_model_id}
    result = request.app.irrelevant_images_collection.delete_one(query)
    # Return a standard response with wasPresent set to true if there was a deletion
    return api_response_handler.create_success_delete_response_v1(result.deleted_count != 0)

@router.delete("/rank-training/remove-irrelevant-image-v1",
               description="Removes an image UUID from the irrelevant images collection",
               status_code=200,
               response_model=StandardSuccessResponseV1[WasPresentResponse],
               tags=["rank-training"],
               responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
def unset_irrelevant_image_v1(request: Request, job_uuid: str = Query(...), rank_model_id: int = Query(...), image_source: str = Query(..., regex="^(generated_image|extract_image|external_image)$")):
    api_response_handler = ApiResponseHandlerV1(request)

    # Check if the job exists in the irrelevant_images_collection
    query = {"uuid": job_uuid, "rank_model_id": rank_model_id, "image_source": image_source}
    result = request.app.irrelevant_images_collection.delete_one(query)
    # Return a standard response with wasPresent set to true if there was a deletion
    return api_response_handler.create_success_delete_response_v1(result.deleted_count != 0)

    


@router.get("/rank-training/list-selection-data-with-scores", 
            tags=['rank-training'], 
            description="List rank selection datapoints with detailed scores",
            response_model=StandardSuccessResponseV1[ResponseImageInfoV1],
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
def list_selection_data_with_scores(
    request: Request,
    model_type: str = Query(..., regex="^(linear|elm-v1)$"),
    rank_model_id: int = Query(None),  # rank_model_id parameter for filtering
    include_flagged: bool = Query(False),  # Parameter to include or exclude flagged documents
    limit: int = Query(10, alias="limit"),
    offset: int = Query(0, alias="offset"),  # Added for pagination
    sort_by: str = Query("delta_score"),  # Default sorting parameter
    order: str = Query("asc")  # Parameter for sort order
):
    response_handler = ApiResponseHandlerV1(request)
    
    try:
        # Connect to the MongoDB collections
        ranking_collection = request.app.ranking_datapoints_collection
        jobs_collection = request.app.completed_jobs_collection

        # Build query filter based on dataset and ensure delta_score exists for the model_type
        query_filter = {}
        if rank_model_id is not None:
            query_filter["rank_model_id"] = rank_model_id

        if not include_flagged:
            query_filter["flagged"] = {"$ne": True}

        # Ensure delta_score for the model_type exists and is not null
        query_filter[f"delta_score.{model_type}"] = {"$exists": True, "$ne": None}

        # Prepare sorting
        sort_order = 1 if order == "asc" else -1
        # Adjust sorting query for nested delta_score by model_type
        sort_query = [("delta_score." + model_type, sort_order)] if sort_by == "delta_score" else [(sort_by, sort_order)]

        # Fetch and sort data with pagination
        cursor = ranking_collection.find(query_filter).sort(sort_query).skip(offset).limit(limit)


        selection_data = []
        doc_count = 0
        for doc in cursor:
            doc_count += 1
            print(f"Processing document {doc['_id']}")
            # Check if the document is flagged
            is_flagged = doc.get("flagged", False)
            selection_file_name = doc["file_name"]
            delta_score = doc.get("delta_score", {}).get(model_type, None)
            selected_image_index = doc["selected_image_index"]
            selected_image_hash = doc["selected_image_hash"]
            selected_image_path = doc["image_1_metadata"]["file_path"] if selected_image_index == 0 else doc["image_2_metadata"]["file_path"]
            # Determine unselected image hash and path based on selected_image_index
            if selected_image_index == 0:
                unselected_image_hash = doc["image_2_metadata"]["file_hash"]
                unselected_image_path = doc["image_2_metadata"]["file_path"]
            else:
                unselected_image_hash = doc["image_1_metadata"]["file_hash"]
                unselected_image_path = doc["image_1_metadata"]["file_path"]
                
            # Fetch scores from completed_jobs_collection for both images
            selected_image_job = jobs_collection.find_one({"task_output_file_dict.output_file_hash": selected_image_hash})
            unselected_image_job = jobs_collection.find_one({"task_output_file_dict.output_file_hash": unselected_image_hash})

            # Skip this job if task_attributes_dict is missing
            if not selected_image_job or "task_attributes_dict" not in selected_image_job or not unselected_image_job or "task_attributes_dict" not in unselected_image_job:
                print(f"Skipping document {doc['_id']} due to missing job data or task_attributes_dict.")
                continue

            # Extract scores for both images
            selected_image_scores = selected_image_job["task_attributes_dict"][model_type]
            unselected_image_scores = unselected_image_job["task_attributes_dict"][model_type]
            
            selection_data.append({
                "selected_image": {
                    "selected_image_path": selected_image_path,
                    "selected_image_hash": selected_image_hash,
                    "selected_image_clip_sigma_score": selected_image_scores.get("image_clip_sigma_score", None),
                    "selected_text_embedding_sigma_score": selected_image_scores.get("text_embedding_sigma_score", None)
                },
                "unselected_image": {
                    "unselected_image_path": unselected_image_path,
                    "unselected_image_hash": unselected_image_hash,
                    "unselected_image_clip_sigma_score": unselected_image_scores.get("image_clip_sigma_score", None),
                    "unselected_text_embedding_sigma_score": unselected_image_scores.get("text_embedding_sigma_score", None)
                },
                "selection_datapoint_file_name": selection_file_name,
                "delta_score": delta_score,
                "flagged": is_flagged 
            })
            print(f"Finished processing document {doc['_id']}.")

        print(f"Total documents processed: {doc_count}. Selection data count: {len(selection_data)}")    
        return response_handler.create_success_response_v1(
            response_data={"selections": selection_data},
            http_status_code=200
        )

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )  


@router.get("/rank-training/get-random-images-by-classifier-score", 
            tags=['rank-training'], 
            description="Returns random images filtering by rank scores",
            response_model=StandardSuccessResponseV1[ListScoreImageTask],
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
def get_random_image_date_range(
    request: Request,
    rank_id: int = None,
    start_date: str = None,
    end_date: str = None,
    min_score: float = 0.6,
    size: int = None,
    prompt_generation_policy: Optional[str] = None  # Optional query parameter
):
    api_response_handler = ApiResponseHandlerV1(request)

    query = {
        '$or': [
            {'task_type': 'image_generation_sd_1_5'},
            {'task_type': 'inpainting_sd_1_5'},
            {'task_type': 'image_generation_kandinsky'},
            {'task_type': 'inpainting_kandinsky'},
            {'task_type': 'img2img_generation_kandinsky'}
        ]
    }

    if start_date and end_date:
        query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
    elif start_date:
        query['task_creation_time'] = {'$gte': start_date}
    elif end_date:
        query['task_creation_time'] = {'$lte': end_date}

    # Include prompt_generation_policy in the query if provided
    if prompt_generation_policy:
        query['prompt_generation_data.prompt_generation_policy'] = prompt_generation_policy

    # If rank_id is provided, adjust the query to consider classifier scores
    classifier_id = None
    uuids = []
    if rank_id is not None:
        rank = request.app.rank_collection.find_one({'rank_model_id': rank_id})
        if rank is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string="Rank model with this id doesn't exist",
                http_status_code=404
            )

        classifier_id = rank.get("classifier_id")
        if classifier_id is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string="This Rank has no relevance classifier model assigned to it",
                http_status_code=404
            )

        classifier_query = {'classifier_id': classifier_id}
        if min_score is not None:
            classifier_query['score'] = {'$gte': min_score}

        classifier_scores = request.app.image_classifier_scores_collection.find(classifier_query, {"uuid": 1})
        for score in classifier_scores:
            uuids.append(score['uuid'])

        if not uuids:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string="The relevance classifier model has no scores",
                http_status_code=404
            )

    # Shuffle the UUIDs array
    random.shuffle(uuids)

    # Pagination for handling large number of UUIDs
    batch_size = 5000  # Adjust as needed to fit within the document size limit
    all_documents = []
    total_docs_added = 0  # Track the number of documents added
    for i in range(0, len(uuids), batch_size):
        batch_uuids = uuids[i:i + batch_size]
        batch_query = query.copy()
        batch_query['uuid'] = {'$in': batch_uuids}
        aggregation_pipeline = [{"$match": batch_query}]
        if size:
            remaining_size = size - total_docs_added
            aggregation_pipeline.append({"$sample": {"size": remaining_size}})
        
        documents = list(request.app.completed_jobs_collection.aggregate(aggregation_pipeline))
        all_documents.extend(documents)
        total_docs_added += len(documents)

        if total_docs_added >= size:
            break

    # Retrieve the UUIDs of the selected documents
    document_uuids = [doc['uuid'] for doc in all_documents]

    # Fetch classifier scores only for the selected documents
    classifier_scores = list(request.app.image_classifier_scores_collection.find({
        'classifier_id': classifier_id,
        'uuid': {'$in': document_uuids}
    }))

    # Map UUIDs to their corresponding scores
    classifier_scores_map = {score['uuid']: score['score'] for score in classifier_scores}

    # Prepare image scores array
    image_scores = []
    for document in all_documents:
        uuid = document.get('uuid')
        score = classifier_scores_map.get(uuid, None)
        if score is not None:
            image_scores.append(score)

    # Prepare response without including classifier_score in the document data
    for document in all_documents:
        document.pop('_id', None)  # Remove the auto-generated field

    return api_response_handler.create_success_response_v1(
        response_data={"images": all_documents, "image_scores": image_scores},
        http_status_code=200
    )


@router.post("/rank-training/calculate-delta-scores", 
             status_code=200,
             description="Calculate and update delta scores for ranking datapoints, only calculates the scores that are missing in the datapoint and skips the ones that have already been calculated. ",
             response_model=StandardSuccessResponseV1[str],
             tags=["Rank Training"],
             responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def calculate_delta_scores(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        start_time = time.time()

        # Define the model types for which you want to calculate delta_scores
        model_types = ["linear", "elm-v1"]

        # Access collections
        ranking_collection = request.app.ranking_datapoints_collection
        jobs_collection = request.app.completed_jobs_collection

        processed_count = 0
        skipped_count = 0

        # Fetch all documents from ranking_collection
        for doc in ranking_collection.find({}):

            # Skip documents where delta_score already exists for all model_types
            if all(f"{model_type}" in doc.get("delta_score", {}) for model_type in model_types):
                skipped_count += 1
                continue

            selected_image_index = doc["selected_image_index"]
            selected_image_hash = doc["selected_image_hash"]
            unselected_image_hash = doc["image_2_metadata"]["file_hash"] if selected_image_index == 0 else doc["image_1_metadata"]["file_hash"]

            for model_type in model_types:
                # Proceed only if the delta_score for this model_type does not exist
                if f"delta_score.{model_type}" not in doc:
                    selected_image_job = jobs_collection.find_one({"task_output_file_dict.output_file_hash": selected_image_hash})
                    unselected_image_job = jobs_collection.find_one({"task_output_file_dict.output_file_hash": unselected_image_hash})

                    if selected_image_job and unselected_image_job and "task_attributes_dict" in selected_image_job and "task_attributes_dict" in unselected_image_job:
                        if model_type in selected_image_job["task_attributes_dict"] and model_type in unselected_image_job["task_attributes_dict"]:
                            selected_image_scores = selected_image_job["task_attributes_dict"][model_type]
                            unselected_image_scores = unselected_image_job["task_attributes_dict"][model_type]

                            if "image_clip_sigma_score" in selected_image_scores and "image_clip_sigma_score" in unselected_image_scores:
                                delta_score = selected_image_scores["image_clip_sigma_score"] - unselected_image_scores["image_clip_sigma_score"]

                                # Update the document in ranking_collection with the new delta_score under the specific model_type
                                update_field = f"delta_score.{model_type}"
                                ranking_collection.update_one(
                                    {"_id": doc["_id"]},
                                    {"$set": {update_field: delta_score}}
                                )
                                processed_count += 1

        end_time = time.time()
        total_time = end_time - start_time

        return response_handler.create_success_response_v1(
            response_data={
                "message": "Delta scores calculation and update complete.",
                "processed_count": processed_count,
                "skipped_count": skipped_count,
                "total_time": f"{total_time:.2f} seconds"
            },
            http_status_code=200
        )

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"An error occurred: {str(e)}",
            http_status_code=500
        )
         


@router.get("/rank-training/get-if-image-is-irrelevant",
            description="changed with rank-training/get-if-image-is-irrelevant-v1 ",
            status_code=200,
            response_model=StandardSuccessResponseV1[BoolIrrelevantResponse],
            tags=["deprecated3"],
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
def get_if_image_is_irrelevant(request: Request, job_uuid: str = Query(...), rank_model_id: int = Query(...)):
    api_response_handler = ApiResponseHandlerV1(request)
    
    try:
        # Check if the image is marked as irrelevant for the given rank model
        is_irrelevant = request.app.irrelevant_images_collection.find_one({"uuid": job_uuid, "rank_model_id": rank_model_id}) is not None

        # Return the result
        return api_response_handler.create_success_response_v1(
            response_data={"irrelevant": is_irrelevant},
            http_status_code=200
        )

    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f'Failed to check if image is irrelevant: {str(e)}',
            http_status_code=500
        )

@router.get("/rank-training/get-if-image-is-irrelevant-v1",
            description="Checks if an image is marked as irrelevant for a specific rank model",
            status_code=200,
            response_model=StandardSuccessResponseV1[BoolIrrelevantResponse],
            tags=["rank-training"],
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
def get_if_image_is_irrelevant_v1(request: Request, job_uuid: str = Query(...), rank_model_id: int = Query(...), image_source: str = Query(..., regex="^(generated_image|extract_image|external_image)$")):
    api_response_handler = ApiResponseHandlerV1(request)
    
    try:
        # Check if the image is marked as irrelevant for the given rank model and image source
        is_irrelevant = request.app.irrelevant_images_collection.find_one({"uuid": job_uuid, "rank_model_id": rank_model_id, "image_source": image_source}) is not None

        # Return the result
        return api_response_handler.create_success_response_v1(
            response_data={"irrelevant": is_irrelevant},
            http_status_code=200
        )

    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f'Failed to check if image is irrelevant: {str(e)}',
            http_status_code=500
        )


@router.get("/rank-training/get-datapoints-count-per-day",
            description="Get number of selection datapoints per day within the date range",
            response_model=StandardSuccessResponseV1[ListGenerationsCountPerDayResponse],
            tags=["rank-training"],
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def get_datapoints_count_per_day(
    request: Request,
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Convert the date strings to datetime objects
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Initialize the result dictionary
        num_by_dataset_and_day = {}

        # Fetch the list of rank folders from MinIO
        rank_folders = set()
        objects = request.app.minio_client.list_objects("datasets", prefix="ranks/", recursive=False)
        for obj in objects:
            folder = obj.object_name.split('/')[1]
            rank_folders.add(folder)

        # Iterate through each day within the date range
        current_date = start_date_dt
        while current_date <= end_date_dt:
            # Construct the query for the current day
            query_date = current_date.strftime("%Y-%m-%d")
            num_by_rank = {}
            for ranks in rank_folders:
                # Construct the MinIO path for selection datapoints
                datapoints_path = f"ranks/{ranks}/data/ranking/aggregate/{query_date}"

                # List objects in the datapoints path
                objects = request.app.minio_client.list_objects("datasets", prefix=datapoints_path, recursive=True)

                # Filter objects that match the current date
                num_datapoints = len([obj.object_name for obj in objects])

                # Store the result for the current day and dataset
                num_by_rank[ranks] = num_datapoints

            # Store the result for the current day in the dictionary
            num_by_dataset_and_day[current_date.strftime("%Y-%m-%d")] = num_by_rank

            # Move to the next day
            current_date += timedelta(days=1)

        return response_handler.create_success_response_v1(
            response_data={"results": num_by_dataset_and_day},
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"An error occurred: {str(e)}",
            http_status_code=500
        )
