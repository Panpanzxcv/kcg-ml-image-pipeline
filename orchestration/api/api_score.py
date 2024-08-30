from datetime import datetime, timedelta
from email.mime import image
import random
import uuid
from fastapi import Request, APIRouter, HTTPException, Query, Body
from typing import List, Optional, Tuple
from typing_extensions import Annotated

from pymongo import UpdateOne
from orchestration.api.mongo_schemas import Bin, RankingScore, ListBin, ListRankingScore, ListOnlyRankingScore, ResponseBinnedRankingScore, ResponseRankingScore
from orchestration.api.utils import select_random_values
from .api_utils import ApiResponseHandler, ErrorCode, StandardSuccessResponse, WasPresentResponse, ApiResponseHandlerV1, StandardSuccessResponseV1

router = APIRouter()


@router.post("/score/set-image-rank-score", 
             tags=['deprecated3'], 
             description="changed with /image-scores/scores/set-rank-score")
async def set_image_rank_score(request: Request, ranking_score: RankingScore):
    # Check if image exists in the completed_jobs_collection
    image_data = request.app.completed_jobs_collection.find_one(
        {"task_output_file_dict.output_file_hash": ranking_score.image_hash},
        {"task_output_file_dict.output_file_hash": 1}
    )
    if not image_data:
        raise HTTPException(status_code=404, detail="Image with the given hash not found in completed jobs collection.")

    # Check if the score already exists in image_rank_scores_collection
    query = {"image_hash": ranking_score.image_hash, "rank_model_id": ranking_score.rank_model_id}
    count = request.app.image_rank_scores_collection.count_documents(query)
    if count > 0:
        raise HTTPException(status_code=409, detail="Score for specific rank_model_id and image_hash already exists.")

    # Add the image_source property set to "generated_image"
    ranking_score_data = ranking_score.dict()
    ranking_score_data['image_source'] = "generated_image"

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
            {"task_output_file_dict.output_file_hash": 1}
        )
        if not image_data or 'task_output_file_dict' not in image_data or 'output_file_hash' not in image_data['task_output_file_dict']:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=f"Image with UUID {ranking_score.uuid} not found or missing 'output_file_hash' in task_output_file_dict.",
                http_status_code=422
            )
        image_hash = image_data['task_output_file_dict']['output_file_hash']
    else:
        image_data = collection.find_one(image_query, {"image_hash": 1})
        if not image_data:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=f"Image with UUID {ranking_score.uuid} not found in {image_source} collection.",
                http_status_code=422
            )
        image_hash = image_data['image_hash']

    # Check if the score already exists in image_rank_scores_collection
    query = {
        "uuid": ranking_score.uuid,
        "image_hash": image_hash,
        "rank_model_id": ranking_score.rank_model_id
    }
    count = request.app.image_rank_scores_collection.count_documents(query)
    if count > 0:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string="Score for specific uuid, rank_model_id and image_hash already exists.",
            http_status_code=400
        )

    # Insert the new ranking score
    ranking_score_data = ranking_score.dict()
    ranking_score_data['image_source'] = image_source
    ranking_score_data['image_hash'] = image_hash
    ranking_score_data["creation_time"] = datetime.utcnow().isoformat() 
    request.app.image_rank_scores_collection.insert_one(ranking_score_data)

    ranking_score_data.pop('_id', None)

    return api_response_handler.create_success_response_v1(
        response_data=ranking_score_data,
        http_status_code=201  
    )

@router.post("/image-scores/scores/set-rank-score-batch", 
             status_code=200,
             response_model=StandardSuccessResponseV1[ListRankingScore],
             description="Set rank image scores in a batch",
             tags=["image scores"], 
             responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def set_image_rank_score_batch(
    request: Request, 
    batch_scores: ListRankingScore
):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        bulk_operations = []
        response_data = []

        for ranking_score in batch_scores.scores:
            query = {
                "uuid": ranking_score.uuid,
                "image_hash": ranking_score.image_hash,
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
                "image_source": ranking_score.image_source
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

    # Remove the auto generated '_id' field before returning
    item.pop('_id', None)

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
    
    score_data = []
    for item in items:
        # remove the auto generated field
        item.pop('_id', None)
        score_data.append(item)

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
    
    score_data = []
    for item in items:
        # Remove the auto generated '_id' field
        item.pop('_id', None)
        score_data.append(item)
    
    # Return a standardized success response with the score data
    return api_response_handler.create_success_response_v1(
        response_data={'scores': score_data},
        http_status_code=200
    )
    
@router.get("/image-scores/scores/list-rank-scores",
            description="Get image rank scores by rank id with optional random sampling.",
            status_code=200,
            tags=["image scores"],  
            response_model=StandardSuccessResponseV1[ListRankingScore],  
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
def list_rank_scores(
    request: Request, 
    rank_model_id: int, 
    score_field: str = Query(..., description="Score field for selecting if the data must be sorted by score or sigma score."),
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
            
        if min_score and max_score:
            query['score'] = {
                '$gte': min_score,
                '$lte': max_score
            }
        elif min_score:
            query['score'] = { '$gte': min_score }
        elif max_score:
            query['score'] = { '$lte': max_score }

        # Modify behavior based on random_sampling parameter
        if random_sampling:
            query_filter = {"$match": query}
            sampling_stage = {"$sample": {"size": limit}}
            pipeline = [query_filter, sampling_stage]
            items = list(request.app.image_rank_scores_collection.aggregate(pipeline))
        else:
            items = list(request.app.image_rank_scores_collection\
                .find(query)\
                .sort(score_field, 1 if sort_order == 'asc' else -1)\
                .skip(offset).limit(limit))
        
        # Remove the auto generated '_id' field and prepare the score data
        score_data = []
        for item in items:
            # Remove the auto generated '_id' field
            item.pop('_id', None)
            score_data.append(item)
        
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


@router.post("/image-scores/scores/list-rank-scores-with-binning", 
               description="Get images grouped by rank score with binning",
               status_code=200,
               tags=["image scores"], 
               response_model=StandardSuccessResponseV1[ResponseBinnedRankingScore],
               responses=ApiResponseHandlerV1.listErrors([422]))
async def get_images_grouped_by_rank_score_with_binning(
    request: Request, 
    bins: List[int] = Body(description="The bins to use for grouping, e.x., [0, 5, 10]"),
    rank_model_id: int = Body(), 
    score_field: str = Body(),
    bucket_ids: Optional[List[int]] = Body(default=None),
    dataset_ids: Optional[List[int]] = Body(default=None),
    random_size: Optional[int] = Body(default=None),
    max_count: int = Body(default=20),
):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Check if rank_id exists in rank_collection
        model_exists = request.app.rank_collection.find_one(
            {"rank_model_id": rank_model_id},
            {"_id": 1}
        )
        if not model_exists:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="The provided rank_model_id does not exist in rank_collection.",
                http_status_code=400
            )
        # sort bins in ascending order
        bins.sort()
        
        # build rank score query to get images with rank scores in the given bins
        rank_score_query = {}
        rank_score_query['score'] = {
            '$gte': min(bins),
            '$lte': max(bins)
        }
        # rank_score_query['score_field'] = score_field
        rank_score_query["rank_model_id"] = rank_model_id
        rank_scores = request.app.image_rank_scores_collection.find(rank_score_query)
        
        # If random_size is provided, select random values from the rank_scores
        if random_size:
            rank_scores = select_random_values(array=rank_scores, random_size=random_size)
            
        # filter images based on bucket_ids and dataset_ids
        images = []
        query_for_all_images = {} # query for all images collection to filter out images that are not in the bucket_ids or dataset_ids
        if bucket_ids or dataset_ids:
            query_conditions = []
            if bucket_ids:
                query_conditions.append({"bucket_id": {"$in": bucket_ids}})
            if dataset_ids:
                query_conditions.append({"dataset_id": {"$in": dataset_ids}})
            if query_conditions:
                query_for_all_images = {"$or": query_conditions}
        for rank_score in rank_scores:
            query_for_all_images["uuid"] = rank_score["uuid"]
            image_data = request.app.all_image_collection.find_one(query_for_all_images)
            if image_data:
                image_data = dict(image_data)
                images.append({
                    "uuid": rank_score["uuid"],
                    "rank_model_id": rank_model_id,
                    "rank_id": rank_score["rank_id"],
                    "score": rank_score["score"],
                    "sigma_score": rank_score["sigma_score"],
                })
        
        # group images by rank score
        binned_images = [[] for i in range(len(bins - 1))]
        for image in images:
            for i in range(len(bins - 1)):
                if image["score"] >= bins[i] and image["score"] < bins[i + 1]:
                    binned_images[i].append(image)
                    break
        # trim each bin to max_count
        for binned_image in binned_images:
            if len(binned_image) > max_count:
                binned_image = binned_image[:max_count]
        
        # Return a standardized success response with the score data
        return api_response_handler.create_success_response_v1(
            response_data={},
            http_status_code=200
        )
        
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )
    pass