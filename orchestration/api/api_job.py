import io
import os
import sys
from fastapi import Request, APIRouter, HTTPException, Query, Body
import numpy as np
import msgpack
from utility.path import separate_bucket_and_file_path
from utility.minio import cmd
import uuid
from datetime import datetime, timedelta
from orchestration.api.mongo_schemas import KandinskyTask, Task
from orchestration.api.api_dataset import get_sequential_id
import pymongo
from .api_utils import PrettyJSONResponse
from typing import List
import json
import paramiko
from typing import Optional, Dict
import csv
from .api_utils import ApiResponseHandler, ErrorCode, StandardSuccessResponse, AddJob, WasPresentResponse
from pymongo import UpdateMany
from bson import ObjectId


router = APIRouter()


# -------------------- Get -------------------------

def convert_objectid_to_str(doc):
    # Convert ObjectId fields to strings for JSON serialization
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

@router.get("/queue/image-generation/get-job")
def get_job(request: Request, task_type=None, model_type="sd_1_5"):
    # Define the base query
    base_query = {}
    if task_type:
        base_query["task_type"] = task_type
    if model_type:
        base_query["task_type"] = {"$regex": model_type} 
    
    # Prioritize jobs where task_input_dict.dataset is "variants"
    priority_query = base_query.copy()
    priority_query["task_input_dict.dataset"] = "variants"
    
    job = request.app.pending_jobs_collection.find_one(priority_query, sort=[("task_creation_time", pymongo.ASCENDING)])
    
    # If no priority job is found, fallback to the base query
    if job is None:
        job = request.app.pending_jobs_collection.find_one(base_query, sort=[("task_creation_time", pymongo.ASCENDING)])

    if job is None:
        raise HTTPException(status_code=204)

    # Proceed with the rest of the endpoint as before
    request.app.pending_jobs_collection.delete_one({"uuid": job["uuid"]})
    job.pop('_id', None)
    job["task_start_time"] = datetime.now().isoformat()
    request.app.in_progress_jobs_collection.insert_one(job)
    job = convert_objectid_to_str(job)
    
    return job

@router.get("/queue/image-generation/job",
            status_code=200,
            tags=["jobs"],
            description="add job in in-progress",
            response_model=StandardSuccessResponse[Task],
            responses=ApiResponseHandler.listErrors([400, 422, 500]))
def get_job(request: Request, task_type: str = None):
    api_response_handler = ApiResponseHandler(request)
    try:
        query = {}
        if task_type is not None:
            query = {"task_type": task_type}

        # Query to find the n newest elements based on the task_creation_time
        job = request.app.pending_jobs_collection.find_one(query, sort=[("task_creation_time", pymongo.ASCENDING)])

        if job is None:
            # Use ApiResponseHandler for standardized error response
            return api_response_handler.create_error_response(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="No job found.",
                http_status_code=400
            )

        # Proceed to delete from pending and add to in-progress collections
        request.app.pending_jobs_collection.delete_one({"uuid": job["uuid"]})
        request.app.in_progress_jobs_collection.insert_one(job)

        # Remove the auto-generated '_id' field
        job.pop('_id', None)

        # Convert datetime fields to ISO 8601 string format
        if 'task_creation_time' in job and isinstance(job['task_creation_time'], datetime):
            job['task_creation_time'] = job['task_creation_time'].isoformat()


        # Use ApiResponseHandler for standardized success response
        return api_response_handler.create_success_response(
            response_data=job,
            http_status_code=200
        )

    except Exception as e:
        # Log the error and return a standardized error response
        return api_response_handler.create_error_response(
            ErrorCode.OTHER_ERROR,
            str(e),
            500
        )


 # --------------------- Add ---------------------------
@router.post("/queue/image-generation/add", description="Add a job to db")
def add_job(request: Request, task: Task):
    if task.uuid in ["", None]:
        # generate since its empty
        task.uuid = str(uuid.uuid4())

    # add task creation time
    task.task_creation_time = datetime.now()

    # check if file_path is blank
    if (task.task_input_dict is None or "file_path" not in task.task_input_dict or task.task_input_dict["file_path"] in [
        '', "[auto]", "[default]"]) and "dataset" in task.task_input_dict:
        dataset_name = task.task_input_dict["dataset"]
        # get file path
        sequential_id_arr = get_sequential_id(request, dataset=dataset_name)
        new_file_path = "{}.jpg".format(sequential_id_arr[0])
        task.task_input_dict["file_path"] = new_file_path

    request.app.pending_jobs_collection.insert_one(task.to_dict())

    return {"uuid": task.uuid, "creation_time": task.task_creation_time}


@router.post("/queue/image-generation/add-kandinsky", description="Add a kandinsky job to db")
def add_job(request: Request, kandinsky_task: KandinskyTask):
    task= kandinsky_task.job

    if task.uuid in ["", None]:
        # generate since its empty
        task.uuid = str(uuid.uuid4())

    # add task creation time
    task.task_creation_time = datetime.now()

    # check if file_path is blank
    if (task.task_input_dict is None or "file_path" not in task.task_input_dict or task.task_input_dict["file_path"] in [
        '', "[auto]", "[default]"]) and "dataset" in task.task_input_dict:
        dataset_name = task.task_input_dict["dataset"]
        # get file path
        sequential_id_arr = get_sequential_id(request, dataset=dataset_name)
        new_file_path = "{}.jpg".format(sequential_id_arr[0])
        task.task_input_dict["file_path"] = new_file_path
    
    # upload input image embeddings to minIO
    image_embedding_data={
        "job_uuid": task.uuid,
        "dataset": task.task_input_dict["dataset"],
        "image_embedding": kandinsky_task.positive_embedding,
        "negative_image_embedding": kandinsky_task.negative_embedding
    }
    
    output_file_path = os.path.join(task.task_input_dict["dataset"], task.task_input_dict['file_path'])
    image_embeddings_path = output_file_path.replace(".jpg", "_embedding.msgpack")

    msgpack_string = msgpack.packb(image_embedding_data, default=encode_ndarray, use_bin_type=True, use_single_float=True)

    buffer = io.BytesIO()
    buffer.write(msgpack_string)
    buffer.seek(0)

    cmd.upload_data(request.app.minio_client, "datasets", image_embeddings_path, buffer) 

    request.app.pending_jobs_collection.insert_one(task.to_dict())

    return {"uuid": task.uuid, "creation_time": task.task_creation_time}

def encode_ndarray(obj):
    if isinstance(obj, np.ndarray):
        return {'__ndarray__': obj.tolist()}
    return obj


@router.post("/queue/image-generation", 
             description="Add a job to db",
             status_code=200,
             tags=["jobs"],
             response_model=StandardSuccessResponse[AddJob],
             responses=ApiResponseHandler.listErrors([500]))
def add_job(request: Request, task: Task):
    api_response_handler = ApiResponseHandler(request)
    try:
        if task.uuid in ["", None]:
            # Generate UUID since it's empty
            task.uuid = str(uuid.uuid4())

        # Add task creation time
        task.task_creation_time = datetime.now()

        # Check if file_path is blank and dataset is provided
        if (task.task_input_dict is None or "file_path" not in task.task_input_dict or task.task_input_dict["file_path"] in ['', "[auto]", "[default]"]) and "dataset" in task.task_input_dict:
            dataset_name = task.task_input_dict["dataset"]
            sequential_id_arr = get_sequential_id(request, dataset=dataset_name)
            new_file_path = "{}.jpg".format(sequential_id_arr[0])
            task.task_input_dict["file_path"] = new_file_path

        # Insert task into pending_jobs_collection
        request.app.pending_jobs_collection.insert_one(task.dict())


        # Convert datetime to ISO 8601 formatted string for JSON serialization
        creation_time_iso = task.task_creation_time.isoformat() if task.task_creation_time else None
        # Use ApiResponseHandler for standardized success response
        return api_response_handler.create_success_response(
            response_data={"uuid": task.uuid, "creation_time": creation_time_iso},
            http_status_code=200
        )

    except Exception as e:
        # Log the error and return a standardized error response
        return api_response_handler.create_error_response(
            ErrorCode.OTHER_ERROR,
            str(e),
            500
        )


@router.get("/queue/image-generation/get-jobs-count-last-hour")
def get_jobs_count_last_hour(request: Request, dataset):

    # Calculate the timestamp for one hour ago
    current_time = datetime.now()
    time_ago = current_time - timedelta(hours=1)

    # Query the collection to count the documents created in the last hour
    pending_query = {"task_input_dict.dataset": dataset, "task_creation_time": {"$gte": time_ago}}
    in_progress_query = {"task_input_dict.dataset": dataset, "task_creation_time": {"$gte": time_ago}}
    completed_query = {"task_input_dict.dataset": dataset, "task_completion_time": {"$gte": time_ago.strftime('%Y-%m-%d %H:%M:%S')}}

    count = 0

    # Take into account pending & in progress & completed jobs
    pending_count = request.app.pending_jobs_collection.count_documents(pending_query)
    in_progress_count = request.app.in_progress_jobs_collection.count_documents(in_progress_query)
    completed_count = request.app.completed_jobs_collection.count_documents(completed_query)


    count += pending_count
    count += in_progress_count
    count += completed_count

    return count


@router.get("/queue/image-generation/get-jobs-count-last-n-hour")
def get_jobs_count_last_n_hour(request: Request, dataset, hours: int):

    # Calculate the timestamp for one hour ago
    current_time = datetime.now()
    time_ago = current_time - timedelta(hours=hours)

    # Query the collection to count the documents created in the last hour
    pending_query = {"task_input_dict.dataset": dataset, "task_creation_time": {"$gte": time_ago}}
    in_progress_query = {"task_input_dict.dataset": dataset, "task_creation_time": {"$gte": time_ago}}
    completed_query = {"task_input_dict.dataset": dataset, "task_completion_time": {"$gte": time_ago.strftime('%Y-%m-%d %H:%M:%S')}}

    count = 0

    # Take into account pending & in progress & completed jobs
    pending_count = request.app.pending_jobs_collection.count_documents(pending_query)
    in_progress_count = request.app.in_progress_jobs_collection.count_documents(in_progress_query)
    completed_count = request.app.completed_jobs_collection.count_documents(completed_query)

    count += pending_count
    count += in_progress_count
    count += completed_count

    return count


# -------------- Get jobs count ----------------------
@router.get("/queue/image-generation/pending-count")
def get_pending_job_count(request: Request):
    count = request.app.pending_jobs_collection.count_documents({})
    return count


@router.get("/queue/image-generation/in-progress-count")
def get_in_progress_job_count(request: Request):
    count = request.app.in_progress_jobs_collection.count_documents({})
    return count


@router.get("/queue/image-generation/completed-count")
def get_completed_job_count(request: Request):
    count = request.app.completed_jobs_collection.count_documents({})
    return count


@router.get("/queue/image-generation/failed-count")
def get_failed_job_count(request: Request):
    count = request.app.failed_jobs_collection.count_documents({})
    return count


# ----------------- delete jobs ----------------------
@router.delete("/queue/image-generation/clear-all-pending")
def clear_all_pending_jobs(request: Request):
    request.app.pending_jobs_collection.delete_many({})

    return True

@router.delete("/queue/image-generation/clear-pending", status_code=200)
def clear_pending_jobs_by_task_type(task_type: str, request: Request) -> Dict[str, str]:
    # Perform deletion of pending jobs by the specified task_type
    deletion_result = request.app.pending_jobs_collection.delete_many({"task_type": task_type})

    # Return a response indicating how many documents were deleted
    return {"message": f"Deleted {deletion_result.deleted_count} pending jobs with task_type '{task_type}'."}


@router.delete("/queue/image-generation/all-pending",
               description="remove all pending jobs",
               response_model=StandardSuccessResponse[WasPresentResponse],
               tags=["jobs"],
               responses=ApiResponseHandler.listErrors([500]))
def clear_all_pending_jobs(request: Request):
    api_response_handler = ApiResponseHandler(request)
    try:
        was_present = request.app.pending_jobs_collection.count_documents({}) > 0
        request.app.pending_jobs_collection.delete_many({})

        return api_response_handler.create_success_response(
            response_data={"wasPresent": was_present},
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )
    

@router.delete("/queue/image-generation/clear-all-in-progress")
def clear_all_in_progress_jobs(request: Request):
    request.app.in_progress_jobs_collection.delete_many({})

    return True

@router.delete("/queue/image-generation/all-in-progress",
               description="remove all in-progress jobs",
               response_model=StandardSuccessResponse[WasPresentResponse],
               tags=["jobs"],
               responses=ApiResponseHandler.listErrors([500]))
def clear_all_in_progress_jobs(request: Request):
    api_response_handler = ApiResponseHandler(request)
    try:
        was_present = request.app.in_progress_jobs_collection.count_documents({}) > 0
        request.app.in_progress_jobs_collection.delete_many({})

        return api_response_handler.create_success_response(
            response_data={"wasPresent": was_present},
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )


@router.delete("/queue/image-generation/clear-all-failed")
def clear_all_failed_jobs(request: Request):
    request.app.failed_jobs_collection.delete_many({})

    return True

@router.delete("/queue/image-generation/all-failed",
               description="remove all failed jobs",
               response_model=StandardSuccessResponse[WasPresentResponse],
               tags=["jobs"],
               responses=ApiResponseHandler.listErrors([500]))
def clear_all_in_progress_jobs(request: Request):
    api_response_handler = ApiResponseHandler(request)
    try:
        was_present = request.app.failed_jobs_collection.count_documents({}) > 0
        request.app.failed_jobs_collection.delete_many({})

        return api_response_handler.create_success_response(
            response_data={"wasPresent": was_present},
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )


@router.delete("/queue/image-generation/clear-all-completed")
def clear_all_completed_jobs(request: Request):
    request.app.completed_jobs_collection.delete_many({})

    return True


@router.delete("/queue/image-generation/all-completed",
               description="remove all completed jobs",
               response_model=StandardSuccessResponse[WasPresentResponse],
               tags=["jobs"],
               responses=ApiResponseHandler.listErrors([500]))
def clear_all_in_progress_jobs(request: Request):
    api_response_handler = ApiResponseHandler(request)
    try:
        was_present = request.app.completed_jobs_collection.count_documents({}) > 0
        request.app.completed_jobs_collection.delete_many({})

        return api_response_handler.create_success_response(
            response_data={"wasPresent": was_present},
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )

@router.delete("/queue/image-generation/delete-completed")
def delete_completed_job(request: Request, uuid):
    query = {"uuid": uuid}
    request.app.completed_jobs_collection.delete_one(query)

    return True

@router.delete("/queue/image-generation/completed",
               description="remove all pending jobs",
               response_model=StandardSuccessResponse[WasPresentResponse],
               tags=["jobs"],
               responses=ApiResponseHandler.listErrors([500]))
def delete_completed_job(request: Request, uuid: str):
    api_response_handler = ApiResponseHandler(request)
    try:
        job = request.app.completed_jobs_collection.find_one({"uuid": uuid})
        was_present = bool(job)
        if job:
            request.app.completed_jobs_collection.delete_one({"uuid": uuid})
        return api_response_handler.create_success_response(
            response_data=WasPresentResponse(wasPresent=was_present),
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )
 # --------------------- List ----------------------

@router.get("/queue/image-generation/list-pending", response_class=PrettyJSONResponse)
def get_list_pending_jobs(request: Request):
    jobs = list(request.app.pending_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/list-in-progress", response_class=PrettyJSONResponse)
def get_list_in_progress_jobs(request: Request):
    jobs = list(request.app.in_progress_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/list-completed", response_class=PrettyJSONResponse)
def get_list_completed_jobs(request: Request, limit: Optional[int] = Query(10, alias="limit")):
    # Use the limit parameter in the find query to limit the results
    jobs = list(request.app.completed_jobs_collection.find({}).limit(limit))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/list-completed-by-task-type", response_class=PrettyJSONResponse)
def get_list_completed_jobs_by_dataset(request: Request, task_type, limit: Optional[int] = Query(10, alias="limit")):
    # Use the limit parameter in the find query to limit the results
    jobs = list(request.app.completed_jobs_collection.find({"task_type": task_type}).limit(limit))

    for job in jobs:
        job.pop('_id', None)

    return jobs

@router.get("/queue/image-generation/list-completed-by-dataset", response_class=PrettyJSONResponse)
def get_list_completed_jobs_by_dataset(request: Request, dataset, limit: Optional[int] = Query(10, alias="limit")):
    # Use the limit parameter in the find query to limit the results
    jobs = list(request.app.completed_jobs_collection.find({"task_input_dict.dataset": dataset}).limit(limit))

    for job in jobs:
        job.pop('_id', None)

    return jobs

@router.get("/queue/image-generation/list-completed-by-dataset-and-task-type", response_class=PrettyJSONResponse)
def get_list_completed_jobs_by_dataset_and_task_type(request: Request, dataset: str, task_type: str):
    # Use the limit parameter in the find query to limit the results
    jobs = list(request.app.completed_jobs_collection.find({"task_input_dict.dataset": dataset,"task_type": task_type}))

    job_data=[]
    for job in jobs:
        job_uuid = job.get("uuid")
        file_path = job.get("task_output_file_dict", {}).get("output_file_path")

        if not job_uuid or not file_path:
            continue

        job_info = {
            "job_uuid": job_uuid,
            "file_path": file_path
        }

        job_data.append(job_info)

    return job_data

@router.get("/queue/image-generation/list-by-date", response_class=PrettyJSONResponse)
def get_list_completed_jobs_by_date(
    request: Request,
    start_date: str = Query(..., description="Start date for filtering jobs"), 
    end_date: str = Query(..., description="End date for filtering jobs"),
    min_clip_sigma_score: float = Query(None, description="Minimum CLIP sigma score to filter jobs")
):
    print(f"Start Date: {start_date}, End Date: {end_date}")

    query = {
        "task_creation_time": {
            "$gte": start_date,
            "$lt": end_date
        }
    }

    # Add condition to filter by min_clip_sigma_score if provided
    if min_clip_sigma_score is not None:
        query["task_attributes_dict.image_clip_sigma_score"] = {"$gte": min_clip_sigma_score}

    jobs = list(request.app.completed_jobs_collection.find(query))

    datasets = {}
    for job in jobs:
        dataset_name = job.get("task_input_dict", {}).get("dataset")
        job_uuid = job.get("uuid")
        file_path = job.get("task_output_file_dict", {}).get("output_file_path")
        clip_sigma_score = job.get("task_attributes_dict",{}).get("image_clip_sigma_score")

        if not dataset_name or not job_uuid or not file_path:
            continue

        if dataset_name not in datasets:
            datasets[dataset_name] = []

        job_info = {
            "job_uuid": job_uuid,
            "file_path": file_path, 
            "clip_sigma_score": clip_sigma_score
        }

        datasets[dataset_name].append(job_info)

    return datasets

@router.get("/queue/image-generation/list-by-dataset", response_class=PrettyJSONResponse)
def get_list_completed_jobs_by_dataset(
    request: Request,
    dataset: str= Query(..., description="Dataset name"),  
    model_type: str= Query("elm-v1", description="Model type, elm-v1 or linear"),  
    min_clip_sigma_score: float = Query(None, description="Minimum CLIP sigma score to filter jobs"),
    size: int = Query(1, description="Number of images to return")
):

    query = {
        "task_input_dict.dataset": dataset
    }

    # Add condition to filter by min_clip_sigma_score if provided
    if min_clip_sigma_score is not None:
        query[f"task_attributes_dict.{model_type}.image_clip_sigma_score"] = {"$gte": min_clip_sigma_score}

    # jobs = list(request.app.completed_jobs_collection.find(query))
        
    # Use $match to filter documents based on dataset, creation time, and prompt_generation_policy
    documents = request.app.completed_jobs_collection.aggregate([
        {"$match": query},
        {"$sample": {"size": size}}
    ])

    # Convert cursor type to list
    jobs = list(documents)    

    datasets = []
    for job in jobs:
        job_uuid = job.get("uuid")
        file_hash = job.get('task_output_file_dict', {}).get('output_file_hash'),
        file_path = job.get("task_output_file_dict", {}).get("output_file_path")
        clip_sigma_score = job.get("task_attributes_dict",{}).get(model_type, {}).get("image_clip_sigma_score")

        if not job_uuid or not file_path:
            continue

        job_info = {
            "job_uuid": job_uuid,
            "image_hash": file_hash,
            "file_path": file_path, 
            "clip_sigma_score": clip_sigma_score
        }

        datasets.append(job_info)

    return datasets


@router.get("/queue/image-generation/list-failed", response_class=PrettyJSONResponse)
def get_list_failed_jobs(request: Request):
    jobs = list(request.app.failed_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/count-completed")
def count_completed(request: Request, dataset: str = None):

    jobs = list(request.app.completed_jobs_collection.find({
        'task_input_dict.dataset': dataset
    }))

    return len(jobs)


@router.get("/queue/image-generation/count-by-task-type")
def count_by_task_type(request: Request, task_type: str = "image_generation_task"):
    # Get the completed jobs collection
    completed_jobs_collection = request.app.completed_jobs_collection

    # Define the query to count documents with a specific task_type
    count = completed_jobs_collection.count_documents({'task_type': task_type})

    # Fetch documents with the specified task_type
    documents = completed_jobs_collection.find({'task_type': task_type})

    # Convert ObjectId to string for JSON serialization
    documents_list = [{k: str(v) if isinstance(v, ObjectId) else v for k, v in doc.items()} for doc in documents]

    # Return the count and documents
    return PrettyJSONResponse(content={"count": count, "documents": documents_list})

@router.get("/queue/image-generation/count-pending")
def count_completed(request: Request, dataset: str = None):

    jobs = list(request.app.pending_jobs_collection.find({
        'task_input_dict.dataset': dataset
    }))

    return len(jobs)

@router.get("/queue/image-generation/count-in-progress")
def count_completed(request: Request, dataset: str = None):

    jobs = list(request.app.in_progress_jobs_collection.find({
        'task_input_dict.dataset': dataset
    }))

    return len(jobs)

# ---------------- Update -------------------


@router.put("/queue/image-generation/update-completed-jobs-with-better-name", response_class=PrettyJSONResponse)
def update_completed_jobs_with_better_name(request: Request, task_type_mapping: Dict[str, str]):
    # Use the limit parameter in the find query to limit the results
    total_count_updated = 0
    
    for key, value in task_type_mapping.items():
        result = request.app.completed_jobs_collection.update_many(
            {"task_type": key},
            {
                "$set": {
                    "task_type": value
                },
            }
        )
        total_count_updated += result.matched_count
    
    return total_count_updated


@router.put("/queue/image-generation/update-completed", description="Update in progress job and mark as completed.")
def update_job_completed(request: Request, task: Task):
    # check if exist
    job = request.app.in_progress_jobs_collection.find_one({"uuid": task.uuid})
    if job is None:
        return False
    
    # add to completed
    request.app.completed_jobs_collection.insert_one(task.to_dict())

    # remove from in progress
    request.app.in_progress_jobs_collection.delete_one({"uuid": task.uuid})

    return True


@router.put("/queue/image-generation/update-failed", description="Update in progress job and mark as failed.")
def update_job_failed(request: Request, task: Task):
    # check if exist
    job = request.app.in_progress_jobs_collection.find_one({"uuid": task.uuid})
    if job is None:
        return False

    # add to failed
    request.app.failed_jobs_collection.insert_one(task.to_dict())

    # remove from in progress
    request.app.in_progress_jobs_collection.delete_one({"uuid": task.uuid})

    return True

@router.delete("/queue/image-generation/cleanup-completed-and-orphaned")
def cleanup_completed_and_orphaned_jobs(request: Request):

    jobs = request.app.completed_jobs_collection.find({})
    for job in jobs:
        file_exists = True
        try:
            file_path = job['task_output_file_dict']['output_file_path']
            bucket_name, file_path = separate_bucket_and_file_path(file_path)
            file_exists = cmd.is_object_exists(request.app.minio_client, bucket_name, file_path)
        except Exception as e:
            file_exists = False

        if not file_exists:
            # remove from in progress
            request.app.completed_jobs_collection.delete_one({"uuid": job['uuid']})

    return True



# --------------- Job generation rate ---------------------

@router.get("/job/get-dataset-job-per-second")
def get_job_generation_rate(request: Request, dataset: str, sample_size : int):

    # 1. Take last N=50 image generations (only time stamp when it was submitted)
    # 2. Sort by Time Stamp
    # 3. Use TimeStamp of Oldest, divided by N=50;
    # to get Images/Second = ImageTaskGenerationRate (images/second estimate), over window of last N=50 images
    query = {
        'task_input_dict.dataset': dataset
    }
    # Query to find the n newest elements based on the task_completion_time
    jobs = list(request.app.completed_jobs_collection.find(query).sort("task_creation_time",
                                                                    pymongo.DESCENDING).limit(sample_size))

    total_jobs = len(jobs)
    if total_jobs == 0:
        return 1.0

    job_per_second = 0.0
    for job in jobs:
        task_start_time = job['task_start_time']
        task_completion_time = job['task_completion_time']

        task_start_time = datetime.strptime(task_start_time, '%Y-%m-%d %H:%M:%S')
        task_completion_time = datetime.strptime(task_completion_time, '%Y-%m-%d %H:%M:%S')

        difference_in_seconds = (task_completion_time - task_start_time).total_seconds()
        this_job_per_second = 1.0 / difference_in_seconds
        job_per_second += this_job_per_second / total_jobs

    return job_per_second


# --------------- Job info ---------------------
@router.get("/job/get-completed-job-by-hash")
def get_completed_job_by_hash(request: Request, image_hash):
    query = {"task_output_file_dict.output_file_hash": image_hash}
    job = request.app.completed_jobs_collection.find_one(query)

    if job is None:
        return None

    job.pop('_id', None)

    return job

@router.get("/job/get-job/{uuid}", response_class=PrettyJSONResponse)
def get_job_by_uuid(request: Request, uuid: str):
    # Assuming the job's UUID is stored in the 'uuid' field
    query = {"uuid": uuid}
    job = request.app.completed_jobs_collection.find_one(query)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Remove the '_id' field to avoid issues with JSON serialization
    job.pop('_id', None)

    return job

@router.get("/job/get-jobs", response_class=PrettyJSONResponse)
def get_jobs_by_uuids(request: Request, uuids: List[str] = Query(None)):
    # Assuming the job's UUID is stored in the 'uuid' field
    query = {"uuid": {"$in": uuids}}
    jobs = request.app.completed_jobs_collection.find(query)
    if jobs is None:
        raise HTTPException(status_code=404, detail="Job not found")

    job_list = []
    for job in jobs:
        job.pop('_id', None)
        job_list.append(job)

    return job_list

# --------------- Get Job With Required Fields ---------------------

@router.get("/get-image-generation/by-hash/{image_hash}", response_class=PrettyJSONResponse)
def get_job_by_image_hash(request: Request, image_hash: str, fields: List[str] = Query(None)):
    # Create a projection object that understands nested fields using dot notation
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    job = request.app.completed_jobs_collection.find_one({"task_output_file_dict.output_file_hash": image_hash}, projection)
    if job:
        # If specific fields are requested, filter the job dictionary
        if fields is not None:
            filtered_job = {}
            for field in fields:
                field_parts = field.split('.')
                if len(field_parts) == 1:
                    # Top-level field
                    filtered_job[field] = job.get(field)
                else:
                    # Nested fields
                    nested_field = job
                    for part in field_parts:
                        nested_field = nested_field.get(part, {})
                    if isinstance(nested_field, dict):
                        nested_field = None
                    filtered_job[field_parts[-1]] = nested_field
            return filtered_job
        return job
    else:
        print("Job Not Found")
    

@router.get("/get-image-generation/by-job-id/{job_id}", response_class=PrettyJSONResponse)
def get_job_by_job_id(request: Request, job_id: str, fields: List[str] = Query(None)):
    # Create a projection object that understands nested fields using dot notation
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    job = request.app.completed_jobs_collection.find_one({"uuid": job_id}, projection)
    if job:
        # If specific fields are requested, filter the job dictionary
        if fields is not None:
            filtered_job = {}
            for field in fields:
                field_parts = field.split('.')
                if len(field_parts) == 1:
                    # Top-level field
                    filtered_job[field] = job.get(field)
                else:
                    # Nested fields
                    nested_field = job
                    for part in field_parts:
                        nested_field = nested_field.get(part, {})
                    if isinstance(nested_field, dict):
                        nested_field = None
                    filtered_job[field_parts[-1]] = nested_field
            return filtered_job
        return job
    else:
        print("Job Not Found")


# --------------- Add completed job attributes ---------------------

@router.put("/job/add-attributes", description="Adds the attributes to a completed job.")
def add_attributes_job_completed(
    request: Request,
    image_hash: str = Body(..., embed=True),
    model_type: str = Body(..., embed=True),
    image_clip_score: float = Body(..., embed=True),
    image_clip_percentile: float = Body(..., embed=True),
    image_clip_sigma_score: float = Body(..., embed=True),
    text_embedding_score: float = Body(..., embed=True),
    text_embedding_percentile: float = Body(..., embed=True),
    text_embedding_sigma_score: float = Body(..., embed=True),
    image_clip_h_score: float = Body(..., embed=True),
    image_clip_h_percentile: float = Body(..., embed=True),
    image_clip_h_sigma_score: float = Body(..., embed=True),
    delta_sigma_score: float = Body(..., embed=True)
):
    query = {"task_output_file_dict.output_file_hash": image_hash}

    update_query = {"$set": {
        f"task_attributes_dict.{model_type}.image_clip_score": image_clip_score,
        f"task_attributes_dict.{model_type}.image_clip_percentile": image_clip_percentile,
        f"task_attributes_dict.{model_type}.image_clip_sigma_score": image_clip_sigma_score,
        f"task_attributes_dict.{model_type}.text_embedding_score": text_embedding_score,
        f"task_attributes_dict.{model_type}.text_embedding_percentile": text_embedding_percentile,
        f"task_attributes_dict.{model_type}.text_embedding_sigma_score": text_embedding_sigma_score,
        f"task_attributes_dict.{model_type}.image_clip_h_score": image_clip_h_score,
        f"task_attributes_dict.{model_type}.image_clip_h_percentile": image_clip_h_percentile,
        f"task_attributes_dict.{model_type}.image_clip_h_sigma_score": image_clip_h_sigma_score,
        f"task_attributes_dict.{model_type}.delta_sigma_score": delta_sigma_score
    }}

    result = request.app.completed_jobs_collection.update_one(query, update_query)

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if result.modified_count == 0:
        raise HTTPException(status_code=304, detail="Job not updated, possibly no change in data")

    return {"message": "Job attributes updated successfully."}


@router.post("/update-tasks", status_code=200)
async def update_task_definitions(request: Request):
    # Define the updates for 'image_generation_task' and 'inpainting_generation_task'
    update_operations = [
        UpdateMany(
            {"task_type": "image_generation_task"},
            {
                "$set": {
                    "task_type": "image_generation_sd_1_5"
                },
                "$rename": {
                    "sd_model_hash": "model_hash"
                }
            }
        ),
        UpdateMany(
            {"task_type": "inpainting_generation_task"},
            {
                "$set": {
                    "task_type": "inpainting_sd_1_5"
                },
                "$rename": {
                    "sd_model_hash": "model_hash"
                }
            }
        )
    ]

    # Perform the update operations
    result = request.app.completed_jobs_collection.bulk_write(update_operations)

    # Return the result of the update operation
    return {
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
        "acknowledged": result.acknowledged
    }


@router.post("/update-task-definitions/")
async def update_task_definitions(request:Request):
    # Update operation for 'image_generation_task'
    image_update_result = request.app.completed_jobs_collection.update_many(
        {"task_type": "image_generation_task"},
        {
            "$set": {
                "task_type": "image_generation_sd_1_5",
                "prompt_generation_data": {
                    "prompt_generation_policy": "$task_input_dict.prompt_generation_policy",
                    "prompt_scoring_model": "$task_input_dict.prompt_scoring_model",
                    "prompt_score": "$task_input_dict.prompt_score"
                }
            },
            "$rename": {"sd_model_hash": "model_hash"},
            "$unset": {
                "task_input_dict.prompt_generation_policy": "",
                "task_input_dict.prompt_scoring_model": "",
                "task_input_dict.prompt_score": ""
            }
        }
    )
    
    # Update operation for 'inpainting_generation_task'
    inpainting_update_result = request.app.completed_jobs_collection.update_many(
        {"task_type": "inpainting_generation_task"},
        {
            "$set": {
                "task_type": "inpainting_sd_1_5",
                "prompt_generation_data": {
                    "prompt_generation_policy": "$task_input_dict.prompt_generation_policy",
                    "prompt_scoring_model": "$task_input_dict.prompt_scoring_model",
                    "prompt_score": "$task_input_dict.prompt_score"
                }
            },
            "$rename": {"sd_model_hash": "model_hash"},
            "$unset": {
                "task_input_dict.prompt_generation_policy": "",
                "task_input_dict.prompt_scoring_model": "",
                "task_input_dict.prompt_score": ""
            }
        }
    )

    # Return the combined result of the update operations
    return {
        "image_update_matched_count": image_update_result.matched_count,
        "image_update_modified_count": image_update_result.modified_count,
        "inpainting_update_matched_count": inpainting_update_result.matched_count,
        "inpainting_update_modified_count": inpainting_update_result.modified_count,
    }


@router.get("/queue/image-generation/score-counts", response_class=PrettyJSONResponse)
def get_image_score_counts(request: Request):
    # Fetch all jobs
    jobs = list(request.app.completed_jobs_collection.find({}))
    
    # Initialize counts
    counts = {
        'linear': {'more_than_0': 0, 'more_than_1': 0, 'more_than_2': 0, 'more_than_3': 0, 'total': 0},
        'elm-v1': {'more_than_0': 0, 'more_than_1': 0, 'more_than_2': 0, 'more_than_3': 0, 'total': 0}
    }
    
    # Iterate through jobs to count based on image_clip_sigma_score
    for job in jobs:
        task_attributes_dict = job.get('task_attributes_dict')
        # Check if task_attributes_dict is None or if 'linear' or 'elm-v1' keys are missing
        if task_attributes_dict is None or not ('linear' in task_attributes_dict and 'elm-v1' in task_attributes_dict):
            continue

        # Now safe to assume 'task_attributes_dict' is not None and contains 'linear' and 'elm-v1'
        for model_type in ['linear', 'elm-v1']:
            score = task_attributes_dict[model_type].get("image_clip_sigma_score", None)
            if score is not None:
                counts[model_type]['total'] += 1
                if score > 0:
                    counts[model_type]['more_than_0'] += 1
                if score > 1:
                    counts[model_type]['more_than_1'] += 1
                if score > 2:
                    counts[model_type]['more_than_2'] += 1
                if score > 3:
                    counts[model_type]['more_than_3'] += 1
    
    # Return the counts
    return {'counts': counts}


@router.get("/queue/image-generation/pending-count-task-type", response_class=PrettyJSONResponse)
async def get_pending_job_count_task_type(request: Request):
    # MongoDB aggregation pipeline to group by `task_type` and count occurrences
    aggregation_pipeline = [
        {
            "$group": {
                "_id": "$task_type",
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "task_type": "$_id",
                "count": 1
            }
        },
        {
            "$sort": {"task_type": 1}
        }
    ]
    
    cursor = request.app.pending_jobs_collection.aggregate(aggregation_pipeline)
    results = list(cursor)
    
    # Transform the results to match the expected output
    formatted_results = [{"task_type": result["task_type"], "count": result["count"]} for result in results]
    
    return formatted_results


@router.get("/queue/tasks/times", response_class =PrettyJSONResponse)
def get_task_times(request: Request):
    task_type = "img2img_generation_kandinsky"
    
    # Query for the first 5 documents
    first_five = request.app.pending_jobs_collection.find(
        {"task_type": task_type},
        {"task_creation_time": 1, "_id": 0}  # Project only the task_creation_time field
    ).sort("task_creation_time", 1).limit(5)  # Sort ascending

    # Query for the last 5 documents
    last_five = request.app.pending_jobs_collection.find(
        {"task_type": task_type},
        {"task_creation_time": 1, "_id": 0}  # Project only the task_creation_time field
    ).sort("task_creation_time", -1).limit(5)  # Sort descending
    
    # Convert cursor to list using the list() function
    first_five_results = list(first_five)
    last_five_results = list(last_five)

    # Reverse the order of last_five_results to display them from earliest to latest
    last_five_results.reverse()

    return {
        "first_five": first_five_results,
        "last_five": last_five_results
    } 

@router.get("/completed-jobs/kandinsky/dataset-score-count", response_class=PrettyJSONResponse)
async def get_dataset_image_clip_h_sigma_score_count(request: Request):
    all_datasets_list = ["waifu","propaganda-poster",
                         "character", "environmental", "external-images",
                           "icons", "mech", "test-generations", "variants" ]  # This needs to be defined, either from a query or a predefined list

    aggregation_pipeline = [
        {
            "$match": {
                "task_type": "img2img_generation_kandinsky",
                "task_attributes_dict.elm-v1.image_clip_h_sigma_score": {"$exists": True}
            }
        },
        {
            "$group": {
                "_id": "$task_input_dict.dataset",
                "count": {"$sum": 1}
            }
        }
    ]

    cursor = request.app.completed_jobs_collection.aggregate(aggregation_pipeline)
    results = list(cursor)

    # Convert aggregation results into a dictionary for easy lookup
    results_dict = {result["_id"]: result["count"] for result in results}

    # Prepare final results, ensuring all datasets are included with a default count of 0
    formatted_results = [{"dataset": dataset, "count": results_dict.get(dataset, 0)} for dataset in all_datasets_list]

    return formatted_results


@router.get("/completed-jobs/duplicated-jobs-count-by-task-type", response_class=PrettyJSONResponse)
async def duplicated_jobs_count_by_task_type(request: Request):
    try:
        aggregation_pipeline = [
            {
                # Stage 1: Group by task_type and uuid to identify unique jobs
                "$group": {
                    "_id": {
                        "task_type": "$task_type",
                        "uuid": "$uuid"
                    },
                    "doc_count": {"$sum": 1}  # Count occurrences of each uuid within each task type
                }
            },
            {
                # Stage 2: Transform structure, counting total and unique jobs per task_type
                "$group": {
                    "_id": "$_id.task_type",
                    "total_jobs": {"$sum": 1},  # Count all occurrences, which includes duplicates
                    "unique_jobs": {
                        "$sum": {
                            "$cond": [{"$eq": ["$doc_count", 1]}, 1, 0]  # Count as unique if only appeared once
                        }
                    }
                }
            },
            {
                # Stage 3: Calculate the number of duplicated jobs per task_type
                "$project": {
                    "task_type": "$_id",
                    "_id": 0,
                    "total_jobs": 1,
                    "unique_jobs_count": "$unique_jobs",
                    "duplicated_jobs_count": {
                        "$subtract": ["$total_jobs", "$unique_jobs"]
                    }
                }
            }
        ]

        cursor = request.app.completed_jobs_collection.aggregate(aggregation_pipeline)
        results = list(cursor)

        # Format the response to include task_type and counts
        formatted_results = [{
            "task_type": result["task_type"],
            "total_jobs": result["total_jobs"],
            "unique_jobs_count": result["unique_jobs_count"],
            "duplicated_jobs_count": result["duplicated_jobs_count"]
        } for result in results]

        return formatted_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")