from fastapi import Request, APIRouter, Query, HTTPException
from datetime import datetime, timedelta
from .api_utils import PrettyJSONResponse, ApiResponseHandler, ErrorCode, StandardErrorResponse, StandardSuccessResponse
from .mongo_schemas import Task

router = APIRouter()



@router.get("/tasks/attributes", 
         responses=ApiResponseHandler.listErrors([404,500]),
         description="List fields from task_attribute_dict")
def list_task_attributes(request: Request):
    api_handler = ApiResponseHandler(request)
    try:
        # Fetch data from the database
        tasks_cursor = request.app.completed_jobs_collection.find({}, {'task_attribute_dict': 1})
        
        # Use a set to hold unique field names
        field_names = set()
        
        # Iterate through cursor and add unique field names
        for task in tasks_cursor:
            task_attr_dict = task.get('task_attribute_dict', {})
            field_names.update(task_attr_dict.keys())

        # Convert set to a list to make it JSON serializable
        field_names_list = list(field_names)

        # Return success response
        return api_handler.create_success_response(field_names_list, 200)
    except HTTPException as http_exc:
        # If an HTTPException is raised, create and return an error response
        return api_handler.create_error_response(
            ErrorCode.INVALID_PARAMS,
            str(http_exc.detail),
            http_exc.status_code
        )
    except Exception as exc:
        # For any other exception, create and return a generic error response
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


@router.get("/image_by_rank/image-list-sorted-by-score-v1", 
            response_model=StandardSuccessResponse[Task],
            status_code = 200,
            responses=ApiResponseHandler.listErrors([500]),
            description="List sorted images from jobs collection" )
def image_list_sorted_by_score_v1(
    request: Request,
    dataset: str = Query(...),
    limit: int = Query(20),
    offset: int = Query(0),
    start_date: str = Query(None),
    end_date: str = Query(None),
    sort_order: str = Query('asc'),
    score_field: str = Query(...),
    min_score: float = Query(None),
    max_score: float = Query(None),
    time_interval: int = Query(None),
    time_unit: str = Query("minutes")
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
        imgs_query = {"task_input_dict.dataset": dataset}
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
            task_attr = job.get('task_attribute_dict', {})
            score = task_attr.get(score_field)
            if score is not None and (min_score is None or score >= min_score) and (max_score is None or score <= max_score):
                images_scores.append({
                    'image_path': job['task_output_file_dict']['output_file_path'],
                    'image_hash': job['task_output_file_dict']['output_file_hash'],
                    **{score_field: score}  # Unpacks the {score_field: score} dict into the current dict
                })

        # Sort and paginate data
        images_scores.sort(key=lambda x: x['score'], reverse=(sort_order == 'desc'))
        images_data = images_scores[offset:offset+limit]

        # Return success response
        return api_handler.create_success_response(images_data, 200)
    except Exception as exc:
        return api_handler.create_error_response(ErrorCode.OTHER_ERROR, "Interval Server Error", 500)

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
