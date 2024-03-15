from fastapi import Request, APIRouter, Query, HTTPException
from datetime import datetime, timedelta
from .api_utils import PrettyJSONResponse, ApiResponseHandler, ErrorCode, StandardErrorResponse, StandardSuccessResponse, ImageData, StandardSuccessResponseV1, ApiResponseHandlerV1, ModelsAndScoresResponse
from .mongo_schemas import Task
from typing import Optional


router = APIRouter()


CACHE = {}
CACHE_EXPIRATION_DELTA = timedelta(hours=12)

@router.get("/tasks/attributes", 
         responses=ApiResponseHandler.listErrors([404, 500]),
         description="List unique score types from task_attributes_dict")
def list_task_attributes(request: Request, dataset: str = Query(..., description="Dataset to filter tasks")):
    api_handler = ApiResponseHandler(request)
    try:
        # Check if data is in cache and not expired
        cache_key = f"task_attributes_{dataset}"
        if cache_key in CACHE and datetime.now() - CACHE[cache_key]['timestamp'] < CACHE_EXPIRATION_DELTA:
            return api_handler.create_success_response(CACHE[cache_key]['data'], 200)

        # Fetch data from the database for the specified dataset
        tasks_cursor = request.app.completed_jobs_collection.find(
            {"task_input_dict.dataset": dataset, "task_attributes_dict": {"$exists": True, "$ne": {}}},
            {'task_attributes_dict': 1}
        )

        # Use a set for score field names and a list for model names
        score_fields = set()
        model_names = []

        # Iterate through cursor and add unique score field names and model names
        for task in tasks_cursor:
            task_attr_dict = task.get('task_attributes_dict', {})
            if isinstance(task_attr_dict, dict):  # Check if task_attr_dict is a dictionary
                for model, scores in task_attr_dict.items():
                    if model not in model_names:
                        model_names.append(model)
                    score_fields.update(scores.keys())

        # Convert set to a list to make it JSON serializable
        score_fields_list = list(score_fields)

        # Store data in cache with timestamp
        CACHE[cache_key] = {
            'timestamp': datetime.now(),
            'data': {
                "Models": model_names,
                "Scores": score_fields_list
            }
        }

        # Return success response
        return api_handler.create_success_response({
            "Models": model_names,
            "Scores": score_fields_list
        }, 200)

    except Exception as exc:
        print(f"Exception occurred: {exc}")
        return api_handler.create_error_response(
            ErrorCode.OTHER_ERROR,
            "Internal Server Error",
            500
        )





@router.get("/image_by_rank/image-list-sorted-by-score", response_class=PrettyJSONResponse)
def image_list_sorted_by_score(
    request: Request,
    dataset: str = Query(...),
    limit: int = 20,
    offset: int = 0,
    start_date: str = None,
    end_date: str = None,
    sort_order: str = 'asc',
    model_id: int = Query(...),
    min_score: float = None,
    max_score: float = None,
    time_interval: int = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours")
):

    # Calculate the time threshold based on the current time and the specified interval
    if time_interval is not None:
        current_time = datetime.utcnow()
        if time_unit == "minutes":
            threshold_time = current_time - timedelta(minutes=time_interval)
        elif time_unit == "hours":
            threshold_time = current_time - timedelta(hours=time_interval)
        else:
            raise HTTPException(status_code=400, detail="Invalid time unit. Use 'minutes' or 'hours'.")
    else:
        threshold_time = None

    # Decide the sort order based on the 'sort_order' parameter
    sort_order = -1 if sort_order == "desc" else 1

    # Query to get all scores of the specified model and sort them
    scores_query = {"model_id": model_id}
    if min_score and max_score:
        scores_query['score'] = {'$gte': min_score, '$lte': max_score}
    elif min_score:
        scores_query['score'] = {'$gte': min_score}
    elif max_score:
        scores_query['score'] = {'$lte': max_score}
    scores_data = list(request.app.image_scores_collection.find(scores_query, 
    {'_id': 0, 'image_hash': 1, 'score': 1}).sort("score", sort_order))

    images_data = []

    # Query to filter images based on dataset, date, and threshold_time
    imgs_query = {"task_input_dict.dataset": dataset}

    # Update the query based on provided start_date, end_date, and threshold_time
    if start_date and end_date:
        imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
    elif start_date:
        imgs_query['task_creation_time'] = {'$gte': start_date}
    elif end_date:
        imgs_query['task_creation_time'] = {'$lte': end_date}
    elif threshold_time:
        imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

    # Loop to get filtered list of images and their scores
    for data in scores_data:
        # Adding filter based on image hash
        imgs_query['task_output_file_dict.output_file_hash'] = data['image_hash']
        img = request.app.completed_jobs_collection.find_one(imgs_query)

        # Only appending image to response if it is within date range
        if img is not None:
            images_data.append({
                'image_path': img['task_output_file_dict']['output_file_path'],
                'image_hash': data['image_hash'],
                'score': data['score']
            })
    
    # Applying offset and limit for pagination
    images_data = images_data[offset:offset+limit]

    return images_data


@router.get("/image_by_rank/image-list-sorted", 
            response_model=StandardSuccessResponse[ImageData],
            status_code=200,
            responses=ApiResponseHandler.listErrors([500]),
            description="List sorted images from jobs collection")
def image_list_sorted_by_score_v1(
    request: Request,
    model_type: str = Query(..., description="Model type to filter the scores, e.g., 'linear' or 'elm-v1'"),
    score_field: str = Query(..., description="Score field to sort by"),
    dataset: str = Query(..., description="Dataset to filter the images"),
    limit: int = Query(20, description="Limit for pagination"),
    offset: int = Query(0, description="Offset for pagination"),
    start_date: str = Query(None, description="Start date for filtering images"),
    end_date: str = Query(None, description="End date for filtering images"),
    sort_order: str = Query('asc', description="Sort order: 'asc' for ascending, 'desc' for descending"),
    min_score: float = Query(None, description="Minimum score for filtering"),
    max_score: float = Query(None, description="Maximum score for filtering"),
    time_interval: int = Query(None, description="Time interval in minutes or hours for filtering"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    api_handler = ApiResponseHandler(request)
    try:
        
        # Calculate the time threshold based on the current time and the specified interval
        threshold_time = None
        if time_interval is not None:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        # Construct query based on filters
        imgs_query = {"task_input_dict.dataset": dataset,
                      f"task_attributes_dict.{model_type}.{score_field}": {"$exists": True}}
        
        if start_date and end_date:
            imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
        elif start_date:
            imgs_query['task_creation_time'] = {'$gte': start_date}
        elif end_date:
            imgs_query['task_creation_time'] = {'$lte': end_date}
        elif threshold_time:
            imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

        # Fetch data from the database
        completed_jobs = list(request.app.completed_jobs_collection.find(imgs_query))
        
        # Process and filter data
        images_scores = []
        for job in completed_jobs:
            task_attr = job.get('task_attributes_dict', {}).get(model_type, {})
            score = task_attr.get(score_field)
            if score is not None and (min_score is None or score >= min_score) and (max_score is None or score <= max_score):
                images_scores.append({
                    'image_path': job['task_output_file_dict']['output_file_path'],
                    'image_hash': job['task_output_file_dict']['output_file_hash'],
                    score_field: score
                })

        # Sort and paginate data
        images_scores.sort(key=lambda x: x[score_field], reverse=(sort_order == 'desc'))
        images_data = images_scores[offset:offset + limit]

        # Return success response
        return api_handler.create_success_response(images_data, 200)
    except Exception as exc:
        return api_handler.create_error_response(ErrorCode.OTHER_ERROR, "Internal Server Error", 500)


@router.get("/image_by_rank/image-list-sampled-sorted", 
            response_model=StandardSuccessResponse[ImageData],
            status_code=200,
            responses=ApiResponseHandler.listErrors([500]),
            description="List randomly sampled and sorted images from jobs collection")
def image_list_sampled_sorted(
    request: Request,
    model_type: str = Query(..., description="Model type to filter the scores, e.g., 'linear' or 'elm-v1'"),
    score_field: str = Query(..., description="Score field to sort by"),
    dataset: str = Query(..., description="Dataset to filter the images"),
    sampling_size: Optional[int] = Query(None, description="Number of images to randomly sample"),
    start_date: str = Query(None, description="Start date for filtering images"),
    end_date: str = Query(None, description="End date for filtering images"),
    sort_order: str = Query('asc', description="Sort order: 'asc' for ascending, 'desc' for descending"),
    min_score: float = Query(None, description="Minimum score for filtering"),
    max_score: float = Query(None, description="Maximum score for filtering"),
    time_interval: int = Query(None, description="Time interval in minutes or hours for filtering"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    api_handler = ApiResponseHandler(request)
    try:
        # Construct query based on filters
        imgs_query = {"task_input_dict.dataset": dataset,
                      f"task_attributes_dict.{model_type}.{score_field}": {"$exists": True}}

        # Calculate the time threshold based on the current time and the specified interval
        threshold_time = None
        if time_interval is not None:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        # Date range filtering
        if start_date and end_date:
            imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
        elif start_date:
            imgs_query['task_creation_time'] = {'$gte': start_date}
        elif end_date:
            imgs_query['task_creation_time'] = {'$lte': end_date}
        elif threshold_time:
            imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

        # Build aggregation pipeline
        aggregation_pipeline = [{"$match": imgs_query}]
        if sampling_size is not None:
            aggregation_pipeline.append({"$sample": {"size": sampling_size}})

        # Fetch data using the aggregation pipeline
        jobs = list(request.app.completed_jobs_collection.aggregate(aggregation_pipeline))

        # Process and filter data
        images_scores = []
        for job in jobs:
            task_attr = job.get('task_attributes_dict', {}).get(model_type, {})
            score = task_attr.get(score_field)
            if score is not None and (min_score is None or score >= min_score) and (max_score is None or score <= max_score):
                images_scores.append({
                    'image_path': job['task_output_file_dict']['output_file_path'],
                    'image_hash': job['task_output_file_dict']['output_file_hash'],
                    score_field: score
                })

        # Sort data
        images_scores.sort(key=lambda x: x[score_field], reverse=(sort_order == 'desc'))

        # Return success response
        return api_handler.create_success_response(images_scores, 200)
    except Exception as exc:
        return api_handler.create_error_response(ErrorCode.OTHER_ERROR, str(exc), 500)
    

@router.get("/image_by_rank/image-list-sorted-by-percentile", response_class=PrettyJSONResponse)
def image_list_sorted_by_percentile(
    request: Request,
    dataset: str = Query(...),
    limit: int = 20,
    offset: int = 0,
    start_date: str = None,
    end_date: str = None,
    sort_order: str = 'asc',
    model_id: int = Query(...),
    min_percentile: float = None,
    max_percentile: float = None,
    time_interval: int = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours")
):

    # Calculate the time threshold based on the current time and the specified interval
    if time_interval is not None:
        current_time = datetime.utcnow()
        if time_unit == "minutes":
            threshold_time = current_time - timedelta(minutes=time_interval)
        elif time_unit == "hours":
            threshold_time = current_time - timedelta(hours=time_interval)
        else:
            raise HTTPException(status_code=400, detail="Invalid time unit. Use 'minutes' or 'hours'.")
    else:
        threshold_time = None

    # Decide the sort order based on the 'sort_order' parameter
    sort_order = -1 if sort_order == "desc" else 1

    # Query to get all percentiles of the specified model and sort them
    percentiles_query = {"model_id": model_id}
    if min_percentile and max_percentile:
        percentiles_query['percentile'] = {'$gte': min_percentile, '$lte': max_percentile}
    elif min_percentile:
        percentiles_query['percentile'] = {'$gte': min_percentile}
    elif max_percentile:
        percentiles_query['percentile'] = {'$lte': max_percentile}

    percentiles_data = list(request.app.image_percentiles_collection.find(percentiles_query, 
    {'_id': 0, 'image_hash': 1, 'percentile': 1}).sort("percentile", sort_order))

    images_data = []

    # Query to filter images based on dataset, date, and threshold_time
    imgs_query = {"task_input_dict.dataset": dataset}

    # Update the query based on provided start_date, end_date, and threshold_time
    if start_date and end_date:
        imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
    elif start_date:
        imgs_query['task_creation_time'] = {'$gte': start_date}
    elif end_date:
        imgs_query['task_creation_time'] = {'$lte': end_date}
    elif threshold_time:
        imgs_query['task_creation_time'] = {'$gte': threshold_time}

    # Loop to get filtered list of images and their percentiles
    for data in percentiles_data:
        # Adding filter based on image hash
        imgs_query['task_output_file_dict.output_file_hash'] = data['image_hash']
        img = request.app.completed_jobs_collection.find_one(imgs_query)

        # Only appending image to response if it is within date range
        if img is not None:
            images_data.append({
                'image_path': img['task_output_file_dict']['output_file_path'],
                'image_hash': data['image_hash'],
                'percentile': data['percentile']
            })
    
    # Applying offset and limit for pagination
    images_data = images_data[offset:offset+limit]

    return images_data


@router.get("/image_by_rank/image-list-sorted-by-residual", response_class=PrettyJSONResponse)
def image_list_sorted_by_residual(
    request: Request,
    dataset: str = Query(...),
    limit: int = 20,
    offset: int = 0,
    start_date: str = None,
    end_date: str = None,
    sort_order: str = 'asc',
    model_id: int = Query(...),
    min_residual: float = None,
    max_residual: float = None,
    time_interval: int = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours")
):

    # Calculate the time threshold based on the current time and the specified interval
    if time_interval is not None:
        current_time = datetime.utcnow()
        if time_unit == "minutes":
            threshold_time = current_time - timedelta(minutes=time_interval)
        elif time_unit == "hours":
            threshold_time = current_time - timedelta(hours=time_interval)
        else:
            raise HTTPException(status_code=400, detail="Invalid time unit. Use 'minutes' or 'hours'.")
    else:
        threshold_time = None

    # Decide the sort order based on the 'sort_order' parameter
    sort_order = -1 if sort_order == "desc" else 1

    # Query to get all residuals of the specified model and sort them
    residuals_query = {"model_id": model_id}
    if min_residual and max_residual:
        residuals_query['residual'] = {'$gte': min_residual, '$lte': max_residual}
    elif min_residual:
        residuals_query['residual'] = {'$gte': min_residual}
    elif max_residual:
        residuals_query['residual'] = {'$lte': max_residual}
    residuals_data = list(request.app.image_residuals_collection.find(residuals_query, 
    {'_id': 0, 'image_hash': 1, 'residual': 1}).sort("residual", sort_order))

    images_data = []

    # Query to filter images based on dataset, date, and threshold_time
    imgs_query = {"task_input_dict.dataset": dataset}

    # Update the query based on provided start_date, end_date, and threshold_time
    if start_date and end_date:
        imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
    elif start_date:
        imgs_query['task_creation_time'] = {'$gte': start_date}
    elif end_date:
        imgs_query['task_creation_time'] = {'$lte': end_date}
    elif threshold_time:
        imgs_query['task_creation_time'] = {'$gte': threshold_time}

    # Loop to get filtered list of images and their residuals
    for data in residuals_data:
        # Adding filter based on image hash
        imgs_query['task_output_file_dict.output_file_hash'] = data['image_hash']
        img = request.app.completed_jobs_collection.find_one(imgs_query)

        # Only appending image to response if it is within date range
        if img is not None:
            images_data.append({
                'image_path': img['task_output_file_dict']['output_file_path'],
                'image_hash': data['image_hash'],
                'residual': data['residual']
            })
    
    # Applying offset and limit for pagination
    images_data = images_data[offset:offset+limit]

    return images_data


# new apis

@router.get("/tasks/attributes-v1", 
         response_model=StandardSuccessResponseV1[ModelsAndScoresResponse],
         responses=ApiResponseHandlerV1.listErrors([404, 500]),
         description="List unique score types from task_attributes_dict")
def list_task_attributes_v1(request: Request, dataset: str = Query(..., description="Dataset to filter tasks")):
    api_handler = ApiResponseHandlerV1(request)
    try:
        # Check if data is in cache and not expired
        cache_key = f"task_attributes_{dataset}"
        if cache_key in CACHE and datetime.now() - CACHE[cache_key]['timestamp'] < CACHE_EXPIRATION_DELTA:
            return api_handler.create_success_response_v1(response_data=CACHE[cache_key]['data'], http_status_code=200)

        # Fetch data from the database for the specified dataset
        tasks_cursor = request.app.completed_jobs_collection.find(
            {"task_input_dict.dataset": dataset, "task_attributes_dict": {"$exists": True, "$ne": {}}},
            {'task_attributes_dict': 1}
        )

        # Use a set for score field names and a list for model names
        score_fields = set()
        model_names = []

        # Iterate through cursor and add unique score field names and model names
        for task in tasks_cursor:
            task_attr_dict = task.get('task_attributes_dict', {})
            if isinstance(task_attr_dict, dict):  # Check if task_attr_dict is a dictionary
                for model, scores in task_attr_dict.items():
                    if model not in model_names:
                        model_names.append(model)
                    score_fields.update(scores.keys())

        # Convert set to a list to make it JSON serializable
        score_fields_list = list(score_fields)

        # Store data in cache with timestamp
        CACHE[cache_key] = {
            'timestamp': datetime.now(),
            'data': {
                "Models": model_names,
                "Scores": score_fields_list
            }
        }

        # Return success response
        return api_handler.create_success_response_v1(response_data={
            "Models": model_names,
            "Scores": score_fields_list
        }, http_status_code=200)

    except Exception as exc:
        print(f"Exception occurred: {exc}")
        return api_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string="Internal Server Error",
            http_status_code=500
        )


@router.get("/image-by-rank/image-list-sorted-by-score-v1",
            tags= ['images by rank'],
            responses=ApiResponseHandlerV1.listErrors([404, 500]),
         description="List unique score types from task_attributes_dict")
def image_list_sorted_by_score(
    request: Request,
    dataset: str = Query(...),
    limit: int = 20,
    offset: int = 0,
    start_date: str = None,
    end_date: str = None,
    sort_order: str = 'asc',
    model_id: int = Query(...),
    min_score: float = None,
    max_score: float = None,
    time_interval: int = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours")
):
    response_handler = ApiResponseHandlerV1(request)
    try:
        threshold_time = None
        if time_interval:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        sort_order_mongo = -1 if sort_order == "desc" else 1
        scores_query = {"model_id": model_id}

        if min_score is not None and max_score is not None:
            scores_query['score'] = {'$gte': min_score, '$lte': max_score}
        elif min_score is not None:
            scores_query['score'] = {'$gte': min_score}
        elif max_score is not None:
            scores_query['score'] = {'$lte': max_score}

        scores_data = list(request.app.image_scores_collection.find(
            scores_query,
            {'_id': 0, 'image_hash': 1, 'score': 1}
        ).sort("score", sort_order_mongo))

        images_data = []

        for data in scores_data:
            imgs_query = {
                "task_input_dict.dataset": dataset,
                "task_output_file_dict.output_file_hash": data['image_hash']
            }

            if start_date and end_date:
                imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
            elif start_date:
                imgs_query['task_creation_time'] = {'$gte': start_date}
            elif end_date:
                imgs_query['task_creation_time'] = {'$lte': end_date}
            if threshold_time:
                imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

            img = request.app.completed_jobs_collection.find_one(imgs_query)

            if img:
                images_data.append({
                    'image_path': img['task_output_file_dict']['output_file_path'],
                    'image_hash': data['image_hash'],
                    'score': data['score']
                })

        paginated_data = images_data[offset:offset + limit]

        return response_handler.create_success_response_v1(response_data={"images": paginated_data}, http_status_code=200)

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )
    

@router.get("/image-by-rank/image-list-sorted-v1", 
            response_model=StandardSuccessResponseV1[ImageData],
            tags= ['images by rank'],
            status_code=200,
            responses=ApiResponseHandlerV1.listErrors([500]),
            description="List sorted images from jobs collection")
def image_list_sorted_by_score_v1(
    request: Request,
    model_type: str = Query(..., description="Model type to filter the scores, e.g., 'linear' or 'elm-v1'"),
    score_field: str = Query(..., description="Score field to sort by"),
    dataset: str = Query(..., description="Dataset to filter the images"),
    limit: int = Query(20, description="Limit for pagination"),
    offset: int = Query(0, description="Offset for pagination"),
    start_date: str = Query(None, description="Start date for filtering images"),
    end_date: str = Query(None, description="End date for filtering images"),
    sort_order: str = Query('asc', description="Sort order: 'asc' for ascending, 'desc' for descending"),
    min_score: float = Query(None, description="Minimum score for filtering"),
    max_score: float = Query(None, description="Maximum score for filtering"),
    time_interval: int = Query(None, description="Time interval in minutes or hours for filtering"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    api_handler = ApiResponseHandlerV1(request)
    try:
        
        # Calculate the time threshold based on the current time and the specified interval
        threshold_time = None
        if time_interval is not None:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        # Construct query based on filters
        imgs_query = {"task_input_dict.dataset": dataset,
                      f"task_attributes_dict.{model_type}.{score_field}": {"$exists": True}}
        
        if start_date and end_date:
            imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
        elif start_date:
            imgs_query['task_creation_time'] = {'$gte': start_date}
        elif end_date:
            imgs_query['task_creation_time'] = {'$lte': end_date}
        elif threshold_time:
            imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

        # Fetch data from the database
        completed_jobs = list(request.app.completed_jobs_collection.find(imgs_query))
        
        # Process and filter data
        images_scores = []
        for job in completed_jobs:
            task_attr = job.get('task_attributes_dict', {}).get(model_type, {})
            score = task_attr.get(score_field)
            if score is not None and (min_score is None or score >= min_score) and (max_score is None or score <= max_score):
                images_scores.append({
                    'image_path': job['task_output_file_dict']['output_file_path'],
                    'image_hash': job['task_output_file_dict']['output_file_hash'],
                    score_field: score
                })

        # Sort and paginate data
        images_scores.sort(key=lambda x: x[score_field], reverse=(sort_order == 'desc'))
        images_data = images_scores[offset:offset + limit]

        # Return success response
        return api_handler.create_success_response_v1(response_data=images_data, http_status_code=200)
    except Exception as exc:
        return api_handler.create_error_response_v1(error_code=ErrorCode.OTHER_ERROR, error_string="Internal Server Error", http_status_code=500)


@router.get("/image-by-rank/image-list-sampled-sorted-v1", 
            response_model=StandardSuccessResponseV1[ImageData],
            tags= ['images by rank'],
            status_code=200,
            responses=ApiResponseHandlerV1.listErrors([500]),
            description="List randomly sampled and sorted images from jobs collection")
def image_list_sampled_sorted(
    request: Request,
    model_type: str = Query(..., description="Model type to filter the scores, e.g., 'linear' or 'elm-v1'"),
    score_field: str = Query(..., description="Score field to sort by"),
    dataset: str = Query(..., description="Dataset to filter the images"),
    sampling_size: Optional[int] = Query(None, description="Number of images to randomly sample"),
    start_date: str = Query(None, description="Start date for filtering images"),
    end_date: str = Query(None, description="End date for filtering images"),
    sort_order: str = Query('asc', description="Sort order: 'asc' for ascending, 'desc' for descending"),
    min_score: float = Query(None, description="Minimum score for filtering"),
    max_score: float = Query(None, description="Maximum score for filtering"),
    time_interval: int = Query(None, description="Time interval in minutes or hours for filtering"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    api_handler = ApiResponseHandlerV1(request)
    try:
        # Construct query based on filters
        imgs_query = {"task_input_dict.dataset": dataset,
                      f"task_attributes_dict.{model_type}.{score_field}": {"$exists": True}}

        # Calculate the time threshold based on the current time and the specified interval
        threshold_time = None
        if time_interval is not None:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        # Date range filtering
        if start_date and end_date:
            imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
        elif start_date:
            imgs_query['task_creation_time'] = {'$gte': start_date}
        elif end_date:
            imgs_query['task_creation_time'] = {'$lte': end_date}
        elif threshold_time:
            imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

        # Build aggregation pipeline
        aggregation_pipeline = [{"$match": imgs_query}]
        if sampling_size is not None:
            aggregation_pipeline.append({"$sample": {"size": sampling_size}})

        # Fetch data using the aggregation pipeline
        jobs = list(request.app.completed_jobs_collection.aggregate(aggregation_pipeline))

        # Process and filter data
        images_scores = []
        for job in jobs:
            task_attr = job.get('task_attributes_dict', {}).get(model_type, {})
            score = task_attr.get(score_field)
            if score is not None and (min_score is None or score >= min_score) and (max_score is None or score <= max_score):
                images_scores.append({
                    'image_path': job['task_output_file_dict']['output_file_path'],
                    'image_hash': job['task_output_file_dict']['output_file_hash'],
                    score_field: score
                })

        # Sort data
        images_scores.sort(key=lambda x: x[score_field], reverse=(sort_order == 'desc'))

        # Return success response
        return api_handler.create_success_response_v1(response_data=images_scores, http_status_code=200)
    except Exception as exc:
        return api_handler.create_error_response_v1(error_code=ErrorCode.OTHER_ERROR, error_string="Internal Server error", http_status_code=500)


@router.get("/image-by-rank/image-list-sorted-by-percentile-v1", 
            status_code=200,
            tags= ['images by rank'],
            responses=ApiResponseHandlerV1.listErrors([500]),
            description="List images sorted by percentile")
def image_list_sorted_by_percentile_v1(
    request: Request,
    dataset: str = Query(...),
    limit: int = 20,
    offset: int = 0,
    start_date: str = None,
    end_date: str = None,
    sort_order: str = 'asc',
    model_id: int = Query(...),
    min_percentile: float = None,
    max_percentile: float = None,
    time_interval: int = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours")
):
    response_handler = ApiResponseHandlerV1(request)
    try:
        threshold_time = None
        if time_interval:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        sort_order_mongo = -1 if sort_order == "desc" else 1

        percentiles_query = {"model_id": model_id}
        if min_percentile is not None and max_percentile is not None:
            percentiles_query['percentile'] = {'$gte': min_percentile, '$lte': max_percentile}
        elif min_percentile is not None:
            percentiles_query['percentile'] = {'$gte': min_percentile}
        elif max_percentile is not None:
            percentiles_query['percentile'] = {'$lte': max_percentile}

        percentiles_data = list(request.app.image_percentiles_collection.find(
            percentiles_query,
            {'_id': 0, 'image_hash': 1, 'percentile': 1}
        ).sort("percentile", sort_order_mongo))

        images_data = []

        for data in percentiles_data:
            imgs_query = {
                "task_input_dict.dataset": dataset,
                "task_output_file_dict.output_file_hash": data['image_hash']
            }

            if start_date and end_date:
                imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
            elif start_date:
                imgs_query['task_creation_time'] = {'$gte': start_date}
            elif end_date:
                imgs_query['task_creation_time'] = {'$lte': end_date}
            if threshold_time:
                imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

            img = request.app.completed_jobs_collection.find_one(imgs_query)

            if img:
                images_data.append({
                    'image_path': img['task_output_file_dict']['output_file_path'],
                    'image_hash': data['image_hash'],
                    'percentile': data['percentile']
                })

        paginated_data = images_data[offset:offset + limit]

        return response_handler.create_success_response_v1(response_data={"images": paginated_data}, http_status_code=200)

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string="Internal Server Error",
            http_status_code=500,
        )        
    
@router.get("/image-by-rank/image-list-sorted-by-residual-v1",
            status_code=200,
            tags= ['images by rank'],
            responses=ApiResponseHandlerV1.listErrors([500]),
            description="List images sorted by percentile")
def image_list_sorted_by_residual(
    request: Request,
    dataset: str = Query(...),
    limit: int = 20,
    offset: int = 0,
    start_date: str = None,
    end_date: str = None,
    sort_order: str = 'asc',
    model_id: int = Query(...),
    min_residual: float = None,
    max_residual: float = None,
    time_interval: int = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours")
):
    response_handler = ApiResponseHandlerV1(request)
    try:
        threshold_time = None
        if time_interval:
            current_time = datetime.utcnow()
            delta = timedelta(minutes=time_interval) if time_unit == "minutes" else timedelta(hours=time_interval)
            threshold_time = current_time - delta

        sort_order_mongo = -1 if sort_order == "desc" else 1

        residuals_query = {"model_id": model_id}
        if min_residual is not None and max_residual is not None:
            residuals_query['residual'] = {'$gte': min_residual, '$lte': max_residual}
        elif min_residual is not None:
            residuals_query['residual'] = {'$gte': min_residual}
        elif max_residual is not None:
            residuals_query['residual'] = {'$lte': max_residual}

        residuals_data = list(request.app.image_residuals_collection.find(
            residuals_query,
            {'_id': 0, 'image_hash': 1, 'residual': 1}
        ).sort("residual", sort_order_mongo))

        images_data = []

        for data in residuals_data:
            imgs_query = {
                "task_input_dict.dataset": dataset,
                "task_output_file_dict.output_file_hash": data['image_hash']
            }

            if start_date and end_date:
                imgs_query['task_creation_time'] = {'$gte': start_date, '$lte': end_date}
            elif start_date:
                imgs_query['task_creation_time'] = {'$gte': start_date}
            elif end_date:
                imgs_query['task_creation_time'] = {'$lte': end_date}
            if threshold_time:
                imgs_query['task_creation_time'] = {'$gte': threshold_time.strftime("%Y-%m-%dT%H:%M:%S")}

            img = request.app.completed_jobs_collection.find_one(imgs_query)

            if img:
                images_data.append({
                    'image_path': img['task_output_file_dict']['output_file_path'],
                    'image_hash': data['image_hash'],
                    'residual': data['residual']
                })

        paginated_data = images_data[offset:offset + limit]

        return response_handler.create_success_response_v1(response_data={"images": paginated_data}, http_status_code=200)

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string="Internal Server Error",
            http_status_code=500,
        )