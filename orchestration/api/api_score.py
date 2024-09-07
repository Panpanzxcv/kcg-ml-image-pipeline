from datetime import datetime, timedelta
import uuid
from fastapi import Request, APIRouter, HTTPException, Query
from typing import List, Optional

from pymongo import UpdateOne
from orchestration.api.mongo_schema.score_schemas import ScoreHelpers
from orchestration.api.mongo_schemas import OldRankingScoreListForBatchInsertion, RankingScore, RankingScoreListForBatchInsertion, ResponseRankingScore, ListRankingScore, ListOnlyRankingScore
from orchestration.api.utils.uuid64 import Uuid64
from .api_utils import ApiResponseHandler, ErrorCode, StandardSuccessResponse, WasPresentResponse, ApiResponseHandlerV1, StandardSuccessResponseV1, get_bucket_id_for_image_source

router = APIRouter()


@router.post("/score/set-image-rank-score", 
             tags=['deprecated3'], 
             description="changed with /image-scores/scores/set-rank-score")
async def set_image_rank_score(request: Request, ranking_score: RankingScore):
    # Check if image exists in the completed_jobs_collection using the provided uuid
    image_data = request.app.completed_jobs_collection.find_one(
        {"uuid": ranking_score.uuid},
        {"task_output_file_dict.output_file_hash": 1, "image_uuid": 1}
    )
    if not image_data:
        raise HTTPException(status_code=404, detail="Image with the given uuid not found in completed jobs collection.")

    image_uuid = image_data.get('image_uuid')
    image_hash = image_data.get('task_output_file_dict', {}).get('output_file_hash')
    if not image_uuid or not image_hash:
        raise HTTPException(status_code=422, detail="The image does not have a valid image uuid or hash.")

    # Check if the score already exists in image_rank_scores_collection
    query = {"uuid": ranking_score.uuid, "rank_model_id": ranking_score.rank_model_id}
    count = request.app.image_rank_scores_collection.count_documents(query)
    if count > 0:
        raise HTTPException(status_code=409, detail="Score for specific rank_model_id and uuid already exists.")
    
    additional_image_data = request.app.all_image_collection.find_one({"image_hash": image_hash, "bucket_id": 0}, {"bucket_id": 1, "dataset_id": 1})
    if not additional_image_data:
        raise HTTPException(status_code=422, detail="The image is not in the all-images collection.")
    
    image_bucket_id = additional_image_data.get('bucket_id')
    image_dataset_id = additional_image_data.get('dataset_id')
    if image_bucket_id == None or image_dataset_id == None:
        raise HTTPException(status_code=422, detail="The image does not have a valid bucket id or dataset id.")

    # Add the image_source property set to "generated_image"
    ranking_score_data = ranking_score.dict()
    ranking_score_data['image_source'] = "generated_image"
    ranking_score_data['image_hash'] = image_hash
    ranking_score_data['image_uuid'] = image_uuid
    ranking_score_data['bucket_id'] = image_bucket_id
    ranking_score_data['dataset_id'] = image_dataset_id

    # Insert the new ranking score
    request.app.image_rank_scores_collection.insert_one(ranking_score_data)

    return True


@router.post("/image-scores/scores/set-rank-score", 
             status_code=201,
             description="Sets the rank score of an image. The score can only be set one time per image/model combination",
             tags=["image scores"],  
             response_model=StandardSuccessResponseV1[ResponseRankingScore],
             responses=ApiResponseHandlerV1.listErrors([400, 422])) 
@router.post("/score/set-rank-score", 
             status_code=201,
             description="deprecated: use /image-scores/scores/set-rank-score",
             tags=["deprecated2"],  
             response_model=StandardSuccessResponseV1[RankingScore],
             responses=ApiResponseHandlerV1.listErrors([400, 422])) 
async def set_image_rank_score(
    request: Request, 
    ranking_score: RankingScore, 
    image_source: str = Query(..., regex="^(generated_image|extract_image|external_image)$")
):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    # Check if rank_id exists in rank_collection
    model_exists = request.app.rank_collection.find_one(
        {"rank_model_id": ranking_score.rank_id},
        {"_id": 1}
    )
    if not model_exists:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string="The provided rank_model_id does not exist in rank_collection.",
            http_status_code=400
        )
    
    # Check if the score already exists in image_rank_scores_collection
    query = {
        "uuid": ranking_score.uuid,
        "rank_model_id": ranking_score.rank_model_id
    }
    count = request.app.image_rank_scores_collection.count_documents(query)
    if count > 0:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string="Score for specific uuid, rank_model_id, and image_hash already exists.",
            http_status_code=400
        )

    # Determine the appropriate collection based on image_source
    if image_source == "generated_image":
        collection = request.app.completed_jobs_collection
    elif image_source == "extract_image":
        collection = request.app.extracts_collection
    elif image_source == "external_image":
        collection = request.app.external_images_collection
    else:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string="Invalid image source provided.",
            http_status_code=422
        )

    image_query = {
        '$or': [
            {'uuid': ranking_score.uuid},
            {'uuid': uuid.UUID(ranking_score.uuid)}
        ]
    }

    # Fetch additional data from the determined collection
    if image_source == "generated_image":
        image_data = collection.find_one(
            image_query,
            {"task_output_file_dict.output_file_hash": 1, "image_uuid": 1}
        )
        if not image_data or 'task_output_file_dict' not in image_data or 'output_file_hash' not in image_data['task_output_file_dict']:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=f"Image with UUID {ranking_score.uuid} not found or missing 'output_file_hash' in task_output_file_dict.",
                http_status_code=422
            )
        image_hash = image_data['task_output_file_dict']['output_file_hash']
    else:
        image_data = collection.find_one(image_query, {"image_hash": 1, "image_uuid": 1})
        if not image_data:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=f"Image with UUID {ranking_score.uuid} not found in {image_source} collection.",
                http_status_code=422
            )
        image_hash = image_data['image_hash']

    image_uuid = image_data.get('image_uuid', None)

    if image_uuid == None or image_hash == None:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string="The image does not have a valid image uuid or hash.",
            http_status_code=422
        )
    
    additional_image_data = request.app.all_image_collection.find_one({"image_hash": image_hash, "bucket_id": get_bucket_id_for_image_source(image_source)}, {"bucket_id": 1, "dataset_id": 1})
    if not additional_image_data:
        raise HTTPException(status_code=422, detail="The image is not in the all-images collection.")
    
    image_bucket_id = additional_image_data.get('bucket_id')
    image_dataset_id = additional_image_data.get('dataset_id')
    if image_bucket_id == None or image_dataset_id == None:
        raise HTTPException(status_code=422, detail="The image does not have a valid bucket id or dataset id.")

    # Insert the new ranking score
    ranking_score_data = ranking_score.dict()
    ranking_score_data['image_source'] = image_source
    ranking_score_data['image_hash'] = image_hash
    ranking_score_data["creation_time"] = datetime.utcnow().isoformat() 
    ranking_score_data['image_uuid'] = image_uuid
    ranking_score_data['bucket_id'] = image_bucket_id
    ranking_score_data['dataset_id'] = image_dataset_id
    request.app.image_rank_scores_collection.insert_one(ranking_score_data)

    ScoreHelpers.clean_rank_score_for_api_response(ranking_score_data)

    return api_response_handler.create_success_response_v1(
        response_data=ranking_score_data,
        http_status_code=201  
    )


@router.post("/image-scores/scores/set-rank-score-batch", 
             status_code=200,
             response_model=StandardSuccessResponseV1[ListRankingScore],
             description="Set rank image scores in a batch. This endpoint is slower than the v1 version, because it gets some of the values from the database.",
             tags=["image scores"], 
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def set_image_rank_score_batch(
    request: Request, 
    batch_scores: OldRankingScoreListForBatchInsertion
):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        initial_queries_for_adding = []
        initial_scores_to_add = []
        queries_for_getting_data = []

        for ranking_score in batch_scores.scores:
            try:
                bucket_id = get_bucket_id_for_image_source(ranking_score.image_source)
            except Exception as e:
                continue

            query = {
                "uuid": ranking_score.uuid,
                "rank_model_id": ranking_score.rank_model_id    
            }

            new_score_data = {
                "uuid": ranking_score.uuid,
                "rank_model_id": ranking_score.rank_model_id,
                "rank_id": ranking_score.rank_id,
                "score": ranking_score.score,
                "sigma_score": ranking_score.sigma_score,
                "image_hash": ranking_score.image_hash,
                "creation_time": datetime.utcnow().isoformat(),
                "image_source": ranking_score.image_source,
                "bucket_id": bucket_id
            }

            initial_queries_for_adding.append(query)
            initial_scores_to_add.append(new_score_data)

            queries_for_getting_data.append({'image_hash': ranking_score.image_hash, 'bucket_id': bucket_id})

        additional_image_data_query = { '$or': queries_for_getting_data }
        additional_image_data = request.app.all_image_collection.find(additional_image_data_query, {"uuid": 1, "image_hash": 1, "bucket_id": 1, "dataset_id": 1})

        additional_image_data_map = {}
        for img_data in additional_image_data:
            additional_image_data_map[str(img_data["bucket_id"]) + img_data["image_hash"]] = img_data
        
        response_data = []
        bulk_operations = []
        for i, score in enumerate(initial_scores_to_add):
            additional_data = additional_image_data_map.get(str(score["bucket_id"]) + score["image_hash"])
            if additional_data != None:
                score["dataset_id"] = additional_data.get("dataset_id")
                score["image_uuid"] = additional_data.get("uuid")
                response_data.append(score)

                update_operation = UpdateOne(
                    initial_queries_for_adding[i],
                    {"$set": score},
                    upsert=True
                )
                bulk_operations.append(update_operation)
        
        if bulk_operations:
            request.app.image_rank_scores_collection.bulk_write(bulk_operations)

        ScoreHelpers.clean_rank_score_list_for_api_response(response_data)

        return api_response_handler.create_success_response_v1(
            response_data=response_data,
            http_status_code=200  
        )

    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )


@router.post("/image-scores/scores/set-rank-score-batch-v1", 
             status_code=200,
             response_model=StandardSuccessResponseV1[ListRankingScore],
             description="Set rank image scores in a batch",
             tags=["image scores"], 
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def set_image_rank_score_batch(
    request: Request, 
    batch_scores: RankingScoreListForBatchInsertion
):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        bulk_operations = []
        response_data = []

        for ranking_score in batch_scores.scores:
            try:
                image_uuid = Uuid64.from_formatted_string(ranking_score.image_uuid)
            except Exception as e:
                continue

            query = {
                "uuid": ranking_score.uuid,
                "rank_model_id": ranking_score.rank_model_id    
            }

            new_score_data = {
                "uuid": ranking_score.uuid,
                "rank_model_id": ranking_score.rank_model_id,
                "rank_id": ranking_score.rank_id,
                "score": ranking_score.score,
                "sigma_score": ranking_score.sigma_score,
                "image_hash": ranking_score.image_hash,
                "creation_time": datetime.utcnow().isoformat(),
                "image_source": ranking_score.image_source,
                "image_uuid": image_uuid.to_mongo_value(),
                "bucket_id": ranking_score.bucket_id,
                "dataset_id": ranking_score.dataset_id,
            }

            update_operation = UpdateOne(
                query,
                {"$set": new_score_data},
                upsert=True
            )
            bulk_operations.append(update_operation)
            response_data.append(new_score_data)

        if bulk_operations:
            request.app.image_rank_scores_collection.bulk_write(bulk_operations)

        ScoreHelpers.clean_rank_score_list_for_api_response(response_data)

        return api_response_handler.create_success_response_v1(
            response_data={"scores": response_data},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )


@router.get("/image-scores/scores/get-image-rank-score", 
            description="Get image rank score by hash",
            status_code=200,
            tags=["image scores"],  
            response_model=StandardSuccessResponseV1[ResponseRankingScore],  
            responses=ApiResponseHandlerV1.listErrors([400,422]))
@router.get("/score/image-rank-score-by-hash", 
            description="deprectaed: use /image-scores/scores/get-image-rank-score ",
            status_code=200,
            tags=["deprecated2"],  
            response_model=StandardSuccessResponseV1[RankingScore],  
            responses=ApiResponseHandlerV1.listErrors([400,422]))
def get_image_rank_score_by_hash(
    request: Request, 
    image_hash: str = Query(..., description="The hash of the image to get score for"), 
    rank_model_id: int = Query(..., description="The rank model ID associated with the image score"),
    image_source: str = Query(..., description="The source of the image", regex="^(generated_image|extract_image|external_image)$")
):
    api_response_handler = ApiResponseHandlerV1(request)

    # Adjust the query to include rank_model_id and image_source
    query = {"image_hash": image_hash, "rank_model_id": rank_model_id, "image_source": image_source}
    item = request.app.image_rank_scores_collection.find_one(query)

    if item is None:
        # Return a standardized error response if not found
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string="Score for specified rank_model_id and image_hash does not exist.",
            http_status_code=404
        )

    ScoreHelpers.clean_rank_score_for_api_response(item)

    # Return a standardized success response
    return api_response_handler.create_success_response_v1(
        response_data=item,
        http_status_code=200
    )


@router.get("/score/get-image-rank-scores-by-model-id",
            tags = ['deprecated3'], description= "changed with /image-scores/scores/list-image-rank-scores-by-model-id")
def get_image_rank_scores_by_rank_model_id(request: Request, rank_model_id: int):
    # check if exist
    query = {"rank_model_id": rank_model_id}
    items = request.app.image_rank_scores_collection.find(query).sort("score", -1)
    if items is None:
        return []
    
    score_data = list(items)
    ScoreHelpers.clean_rank_score_list_for_api_response(score_data)

    return score_data

@router.get("/image-scores/scores/list-image-rank-scores-by-model-id",
            description="Get image rank scores by model id. Returns as descending order of scores",
            status_code=200,
            tags=["image scores"],  
            response_model=StandardSuccessResponseV1[ListRankingScore],  
            responses=ApiResponseHandlerV1.listErrors([422]))
def get_image_rank_scores_by_model_id(
    request: Request, 
    rank_model_id: int, 
    image_source: Optional[str] = Query(None, regex="^(generated_image|extract_image|external_image)$")
):
    api_response_handler = ApiResponseHandlerV1(request)
    
    # Check if exist
    query = {"rank_model_id": rank_model_id}
    if image_source:
        query["image_source"] = image_source

    items = list(request.app.image_rank_scores_collection.find(query).sort("score", -1))
    
    score_data = list(items)
    ScoreHelpers.clean_rank_score_list_for_api_response(score_data)
    
    # Return a standardized success response with the score data
    return api_response_handler.create_success_response_v1(
        response_data={'scores': score_data},
        http_status_code=200
    )
    
@router.get("/image-scores/scores/list-rank-scores",
            description="Get image rank scores by rank id with optional random sampling. The min and max score filters, as well as sorting, will use the field specified in the 'score_field' parameter.",
            status_code=200,
            tags=["image scores"],  
            response_model=StandardSuccessResponseV1[ListRankingScore],  
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
def list_rank_scores(
    request: Request, 
    rank_model_id: int, 
    score_field: str = Query(..., description="Score field for selecting if the data must be filtered and sorted by score or sigma score."),
    image_source: Optional[str] = Query(None, regex="^(generated_image|extract_image|external_image)$"),
    limit: int = Query(20, description="Limit for pagination"),
    offset: int = Query(0, description="Offset for pagination"),
    start_date: str = Query(None, description="Start date for filtering images"),
    end_date: str = Query(None, description="End date for filtering images"),
    sort_order: str = Query('asc', description="Sort order: 'asc' for ascending, 'desc' for descending"),
    min_score: float = Query(None, description="Minimum score for filtering"),
    max_score: float = Query(None, description="Maximum score for filtering"),
    time_interval: int = Query(None, description="Time interval in minutes or hours for filtering"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'"),
    random_sampling: bool = Query(False, description="Enable random sampling")
):
    api_response_handler = ApiResponseHandlerV1(request)
    try:
        # Calculate the time threshold based on the current time and the specified interval
        threshold_time = None
        if time_interval is not None:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        # Build the query
        query = {"rank_model_id": rank_model_id}
        if image_source:
            query["image_source"] = image_source
        
        if start_date and end_date:
            query['creation_time'] = {'$gte': start_date, '$lte': end_date}
        elif start_date:
            query['creation_time'] = {'$gte': start_date}
        elif end_date:
            query['creation_time'] = {'$lte': end_date}
        elif threshold_time:
            query['creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

        # Apply score filtering based on score_field, min_score, and max_score
        if min_score is not None or max_score is not None:
            score_filter = {}
            if min_score is not None:
                score_filter['$gte'] = min_score
            if max_score is not None:
                score_filter['$lte'] = max_score
            query[score_field] = score_filter

        # Modify behavior based on random_sampling parameter
        if random_sampling:
            pipeline = [
                {"$match": query}, 
                {"$sample": {"size": limit}},  
                {"$sort": {score_field: 1 if sort_order == 'asc' else -1}}  
            ]
            items = list(request.app.image_rank_scores_collection.aggregate(pipeline))
        else:
            items = list(request.app.image_rank_scores_collection\
                .find(query)\
                .sort(score_field, 1 if sort_order == 'asc' else -1)\
                .skip(offset).limit(limit))
        
        score_data = list(items)
        ScoreHelpers.clean_rank_score_list_for_api_response(score_data)
        
        # Return a standardized success response with the score data
        return api_response_handler.create_success_response_v1(
            response_data={'scores': score_data},
            http_status_code=200
        )
        
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )



@router.get("/image-scores/scores/get-image-rank-scores-by-hash",
            description="Get all rank scores assigned to a specific image by its hash.",
            status_code=200,
            tags=["image scores"],  
            response_model=StandardSuccessResponseV1[ListOnlyRankingScore], 
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def get_image_rank_scores(
    request: Request, 
    image_hash: str
):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Build the query to fetch scores for the given image hash
        query = {"image_hash": image_hash}

        # Fetch the scores from the database
        scores = list(request.app.image_rank_scores_collection.find(query))

        # Prepare the response structure
        score_list = []
        for score in scores:
            rank_model_id = score.get("rank_model_id")
            if rank_model_id is not None:
                # Create the desired response structure
                score_entry = {
                    "rank_model_id": rank_model_id,
                    "score": score.get("score"),
                    "sigma_score": score.get("sigma_score")
                }
                score_list.append(score_entry)
        
        # Return a standardized success response with the score data
        return api_response_handler.create_success_response_v1(
            response_data={"scores": score_list},
            http_status_code=200
        )
        
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )



@router.delete("/image-scores/scores/delete-image-rank-score", 
               description="Delete image rank score by specific hash.",
               status_code=200,
               tags=["image scores"], 
               response_model=StandardSuccessResponseV1[WasPresentResponse],
               responses=ApiResponseHandlerV1.listErrors([422]))
@router.delete("/score/image-rank-score-by-hash", 
               description="deprecated: use /image-scores/scores/delete-image-rank-score",
               status_code=200,
               tags=["deprecated2"], 
               response_model=StandardSuccessResponseV1[WasPresentResponse],
               responses=ApiResponseHandlerV1.listErrors([422]))
def delete_image_rank_score_by_hash(
    request: Request, 
    image_hash: str = Query(..., description="The hash of the image to delete score for"), 
    rank_model_id: int = Query(..., description="The rank model ID associated with the image score"),
    image_source: str = Query(..., description="The source of the image", regex="^(generated_image|extract_image|external_image)$")
):
    api_response_handler = ApiResponseHandlerV1(request)
    
    # Adjust the query to include rank_model_id and image_source
    query = {"image_hash": image_hash, "rank_model_id": rank_model_id, "image_source": image_source}
    res = request.app.image_rank_scores_collection.delete_one(query)
    # Return a standard response with wasPresent set to true if there was a deletion
    return api_response_handler.create_success_delete_response_v1(res.deleted_count != 0)
