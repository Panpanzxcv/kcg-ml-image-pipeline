import io
import os
import sys
from fastapi import Request, APIRouter, HTTPException, Query, Body
import numpy as np
import msgpack
from pymongo import ReplaceOne, UpdateOne, UpdateMany, ASCENDING, DESCENDING
from utility.path import separate_bucket_and_file_path
from utility.minio import cmd
import uuid
from datetime import datetime, timedelta
from orchestration.api.mongo_schemas import KandinskyTask, Task, ListSigmaScoreResponse, ListTask, JobInfoResponse, ListTaskV1
from orchestration.api.api_dataset import get_sequential_id
import pymongo
from typing import List
import json
import paramiko
from typing import Optional, Dict
import csv
from .api_utils import ErrorCode, AddJob, WasPresentResponse, ApiResponseHandlerV1, StandardSuccessResponseV1, CountLastHour, CountResponse, insert_into_all_images_for_completed, PrettyJSONResponse, DoneResponse, generate_uuid, check_image_usage, remove_from_additional_collections, delete_files_from_minio
from bson import ObjectId
import time



router = APIRouter()


# -------------------- Get -------------------------

def convert_objectid_to_str(doc):
    # Convert ObjectId fields to strings for JSON serialization
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

@router.get("/queue/image-generation/get-job", tags = ['deprecated3'], description= "changed wtih /queue/image-generation/move-job-to-in-progress")
def get_job(request: Request, task_type=None, model_type="sd_1_5"):
    # Define the base query
    base_query = {}
    if model_type:
        base_query["task_type"] = {"$regex": model_type} 
    if task_type:
        base_query["task_type"] = task_type
    
    # Prioritize jobs where task_input_dict.dataset is "variants"
    priority_query = base_query.copy()
    priority_query["task_input_dict.dataset"] = {"$in": ["variants", "test-generations"]}
    
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



 # --------------------- Add ---------------------------
@router.post("/queue/image-generation/add", tags = ['deprecated3'], description= "changed wtih /queue/image-generation/add-job")
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


@router.post("/queue/image-generation/add-kandinsky", tags = ['deprecated3'], description= "changed with /queue/image-generation/add-kandinsky-job")
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





@router.get("/queue/image-generation/get-jobs-count-last-hour", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-jobs-count-last-n-hours-v1")
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


@router.get("/queue/image-generation/get-jobs-count-last-n-hour", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-jobs-count-last-n-hours-v1")
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
@router.get("/queue/image-generation/pending-count", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-pending-jobs-count")
def get_pending_job_count(request: Request):
    count = request.app.pending_jobs_collection.count_documents({})
    return count


@router.get("/queue/image-generation/in-progress-count", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-in-progress-jobs-count")
def get_in_progress_job_count(request: Request):
    count = request.app.in_progress_jobs_collection.count_documents({})
    return count


@router.get("/queue/image-generation/completed-count", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-completed-jobs-count")
def get_completed_job_count(request: Request):
    count = request.app.completed_jobs_collection.count_documents({})
    return count


@router.get("/queue/image-generation/failed-count", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-failed-jobs-count")
def get_failed_job_count(request: Request):
    count = request.app.failed_jobs_collection.count_documents({})
    return count


# ----------------- delete jobs ----------------------


@router.delete("/queue/image-generation/delete-completed", tags = ['deprecated3'], description= "changed with /queue/image-generation/delete-completed-by-uuid")
def delete_completed_job(request: Request, uuid):
    query = {"uuid": uuid}
    request.app.completed_jobs_collection.delete_one(query)

    return True


@router.delete(
    "/queue/image-generation/delete-completed-by-uuid",
    description="Remove a completed job by UUID.",
    response_model=StandardSuccessResponseV1[WasPresentResponse],
    tags=["jobs-standardized"],
    responses=ApiResponseHandlerV1.listErrors([422, 500]),
)
async def delete_completed_job(request: Request, uuid: str):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        job = request.app.completed_jobs_collection.find_one({"uuid": uuid})

        if job is None:
            print("Job not found in completed_jobs_collection.")
            return api_response_handler.create_success_delete_response_v1(
                False,
                http_status_code=200,
            )

        # Check if the image is used in ranking datapoints or has a tag assigned
        image_hash = job.get("task_output_file_dict", {}).get("output_file_hash")
        file_path = job.get("task_output_file_dict", {}).get("output_file_path")

        if not image_hash or not file_path:
            print("No valid image hash or file path found in the job.")
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="No valid image hash or file path found in the job.",
                http_status_code=422
            )

        # Use the helper function to check image usage
        is_safe_to_delete, error_message = check_image_usage(request, image_hash)

        if not is_safe_to_delete:
            print(error_message)
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=error_message,
                http_status_code=422
            )

        # Remove the image data from additional collections
        remove_from_additional_collections(request, image_hash, bucket_id=0, image_source="generated_image")

        path_parts = file_path.split("/", 1)
        if len(path_parts) < 2:
            print(f"Error: Path format is not correct for file_path: {file_path}")
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="The file path format is incorrect; expected 'bucket_name/object_name'.",
                http_status_code=422
            )

        bucket_name = path_parts[0]
        object_name = path_parts[1]

        # Delete the related files from MinIO
        if bucket_name and object_name:
            print(f"Removing object {object_name} and associated files from bucket {bucket_name}")
            delete_files_from_minio(request.app.minio_client, bucket_name, object_name)

        # Finally, delete the image from completed_jobs_collection
        print(f"Removing job with uuid: {uuid} from completed_jobs_collection")
        request.app.completed_jobs_collection.delete_one({"uuid": uuid})

        return api_response_handler.create_success_delete_response_v1(
            True,
            http_status_code=200,
        )

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )


 # --------------------- List ----------------------

@router.get("/queue/image-generation/list-pending", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-pending-jobs")
def get_list_pending_jobs(request: Request):
    jobs = list(request.app.pending_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/list-in-progress", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-in-progress-jobs")
def get_list_in_progress_jobs(request: Request):
    jobs = list(request.app.in_progress_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/list-completed", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-completed-jobs")
def get_list_completed_jobs(request: Request, limit: Optional[int] = Query(10, alias="limit")):
    # Use the limit parameter in the find query to limit the results
    jobs = list(request.app.completed_jobs_collection.find({}).limit(limit))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/list-completed-by-task-type", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-completed-jobs")
def get_list_completed_jobs_by_dataset(request: Request, task_type, limit: Optional[int] = Query(10, alias="limit")):
    # Use the limit parameter in the find query to limit the results
    jobs = list(request.app.completed_jobs_collection.find({"task_type": task_type}).limit(limit))

    for job in jobs:
        job.pop('_id', None)

    return jobs

@router.get("/queue/image-generation/list-completed-by-dataset", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-completed-jobs")
def get_list_completed_jobs_by_dataset(request: Request, dataset, limit: Optional[int] = Query(10, alias="limit")):
    # Use the limit parameter in the find query to limit the results
    jobs = list(request.app.completed_jobs_collection.find({"task_input_dict.dataset": dataset}).limit(limit))

    for job in jobs:
        job.pop('_id', None)

    return jobs

@router.get("/queue/image-generation/list-completed-by-dataset-and-task-type", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-completed-jobs")
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

@router.get("/queue/image-generation/list-by-date", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-completed-jobs-ordered-by-dataset")
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

@router.get("/queue/image-generation/list-by-dataset", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-completed-jobs-using-random-sampling")
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


@router.get("/queue/image-generation/list-failed", tags = ['deprecated3'], description= "changed with /queue/image-generation/list-failed-jobs")
def get_list_failed_jobs(request: Request):
    jobs = list(request.app.failed_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/queue/image-generation/count-completed", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-completed-jobs-count")
def count_completed(request: Request, dataset: str = None):

    jobs = list(request.app.completed_jobs_collection.find({
        'task_input_dict.dataset': dataset
    }))

    return len(jobs)


@router.get("/queue/image-generation/count-by-task-type", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-completed-jobs-count")
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

@router.get("/queue/image-generation/count-pending", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-pending-jobs-count")
def count_completed(request: Request, dataset: str = None):

    jobs = list(request.app.pending_jobs_collection.find({
        'task_input_dict.dataset': dataset
    }))

    return len(jobs)

@router.get("/queue/image-generation/count-in-progress", tags = ['deprecated3'], description= "changed with /queue/image-generation/get-in-progress-jobs-count")
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


@router.put("/queue/image-generation/update-completed", 
            tags=['deprecated3'], 
            description="changed with /queue/image-generation/set-in-progress-job-as-completed")
def update_job_completed(request: Request, task: Task):
    # Check if the job exists in the in-progress collection
    job = request.app.in_progress_jobs_collection.find_one({"uuid": task.uuid})
    if job is None:
        return False
    
    # Extract the dataset name from the task_input_dict
    dataset_name = job.get("task_input_dict", {}).get("dataset")

    # Find the dataset in the collection
    dataset_result = request.app.datasets_collection.find_one({"dataset_name": dataset_name, "bucket_id": 0})
    if not dataset_result:
        return False  # Handle missing dataset scenario

    dataset_id = dataset_result.get("dataset_id")

    # Insert the job details into the all-images collection and get the generated image_uuid
    all_images_collection = request.app.all_image_collection
    image_uuid = insert_into_all_images_for_completed(job, dataset_id, all_images_collection)

    # Update the task object with the new image_uuid
    task_dict = task.to_dict()
    if image_uuid:
        task_dict["image_uuid"] = image_uuid

    # Add the updated task to the completed_jobs_collection
    request.app.completed_jobs_collection.insert_one(task_dict)

    # Remove the job from the in-progress collection
    request.app.in_progress_jobs_collection.delete_one({"uuid": task.uuid})

    return True



@router.put("/queue/image-generation/update-failed",tags = ['deprecated3'], description= "changed with /queue/image-generation/set-in-progress-job-as-failed")
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

@router.delete("/queue/image-generation/cleanup-completed-and-orphaned", tags = ['deprecated3'], description= "changed with /queue/image-generation/remove-all-orphaned-completed-jobs")
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

@router.get("/get-image-generation/by-hash/{image_hash}", tags = ["deprecated3"], description="changed with /queue/image-generation/get-completed-jobs-data-by-hash/{image_hash}, NOTE: the new endpoint returns the properties using a different name format when the 'fields' param contains properties. ")
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
    

@router.get("/get-image-generation/by-job-id/{job_id}",tags = ["deprecated3"], description="changed with /queue/image-generation/get-completed-jobs-data-by-uuid/{uuid, NOTE: the new endpoint returns the properties using a different name format when the 'fields' param contains properties. ")
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


@router.put("/job/add-attributes-witout-embeddings", description="Adds the attributes to a completed job without embedding.")
def add_attributes_job_completed(
    request: Request,
    image_hash: str = Body(..., embed=True),
    model_type: str = Body(..., embed=True),
    image_clip_score: float = Body(..., embed=True),
    image_clip_percentile: float = Body(..., embed=True),
    image_clip_sigma_score: float = Body(..., embed=True),
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


@router.get("/jobs/find-last-duplicate-uuid")
async def find_last_duplicate_uuid(request: Request):
    task_type = "clip_calculation_task_kandinsky"
    aggregation_pipeline = [
        {
            "$match": {"task_type": task_type}
        },
        {
            "$group": {
                "_id": "$uuid",
                "count": {"$sum": 1},
                "task_creation_time": {"$last": "$task_creation_time"}  # Change $first to $last
            }
        },
        {
            "$match": {"count": {"$gt": 1}}
        },
        {
            "$sort": {"task_creation_time": DESCENDING}  # Change ASCENDING to DESCENDING
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "uuid": "$_id",
                "_id": 0,
                "task_creation_time": 1
            }
        }
    ]

    cursor = request.app.completed_jobs_collection.aggregate(aggregation_pipeline)
    duplicated_job = next(cursor, None)

    if not duplicated_job:
        raise HTTPException(status_code=404, detail="No duplicated UUID found for the specified task type.")

    return duplicated_job


# New apis


@router.get("/queue/image-generation/move-job-to-in-progress",
            status_code=200,
            tags=["jobs-standardized"],
            description="add job in in-progress",
            response_model=StandardSuccessResponseV1[Task],
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def get_job(request: Request, task_type=None, model_type="sd_1_5"):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    base_query = {}
    if task_type:
        base_query["task_type"] = task_type
    if model_type:
        base_query["task_type"] = {"$regex": model_type} 
    
    # Prioritize jobs where task_input_dict.dataset is "variants"
    priority_query = base_query.copy()
    priority_query["task_input_dict.dataset"] = {"$in": ["variants", "test-generations"]}
    
    job = request.app.pending_jobs_collection.find_one(priority_query, sort=[("task_creation_time", pymongo.ASCENDING)])
    
    # If no priority job is found, fallback to the base query
    if job is None:
        job = request.app.pending_jobs_collection.find_one(base_query, sort=[("task_creation_time", pymongo.ASCENDING)])

    if job is None:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND,
            error_string="job not found",
            http_status_code=404
        )

    # Proceed with the rest of the endpoint as before
    request.app.pending_jobs_collection.delete_one({"uuid": job["uuid"]})
    job.pop('_id', None)
    job["task_start_time"] = datetime.now().isoformat()
    request.app.in_progress_jobs_collection.insert_one(job)
    job = convert_objectid_to_str(job)
    
    return api_response_handler.create_success_response_v1(
                response_data=job,
                http_status_code=200
            )
    

@router.post("/queue/image-generation/add-job",
    description="Adds an image generation job to the pending queue. If no UUID is provided, an UUID is generated automatically. If no file path is provided, or the provided file path is \"\", '[auto]' or '[default]', the file path is generated automatically.",
    status_code=200,
    tags=["jobs-standardized"],
    response_model=StandardSuccessResponseV1[AddJob],
    responses=ApiResponseHandlerV1.listErrors([422, 500]),
)
async def add_job(request: Request, task: Task):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        if task.uuid in ["", None]:
            # Generate UUID since it's empty
            task.uuid = str(uuid.uuid4())  # Generate UUID if it's empty

        # Add task creation time
        task.task_creation_time = datetime.now()  # Add task creation time

        # Check if 'file_path' requires a dataset name
        requires_dataset = task.task_input_dict is None or (
            "file_path" not in task.task_input_dict or task.task_input_dict["file_path"] in ['', "[auto]", "[default]"]
        )

        # If 'file_path' requires a dataset and no dataset is provided, return 422
        if requires_dataset and "dataset" not in task.task_input_dict:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="Dataset name is required when file_path is blank or set to '[auto]' or '[default]'.",
                http_status_code=422,
            )

        # If dataset is provided, generate the new file path
        if requires_dataset and "dataset" in task.task_input_dict:
            dataset_name = task.task_input_dict["dataset"]
            sequential_id_arr = get_sequential_id(request, dataset=dataset_name)
            new_file_path = "{}.jpg".format(sequential_id_arr[0])
            task.task_input_dict["file_path"] = new_file_path

        # Insert task into pending_jobs_collection
        request.app.pending_jobs_collection.insert_one(task.dict())

        # Convert datetime to ISO 8601 formatted string for JSON serialization
        creation_time_iso = task.task_creation_time.isoformat() if task.task_creation_time else None
        return api_response_handler.create_success_response_v1(
            response_data={"uuid": task.uuid, "creation_time": creation_time_iso},
            http_status_code=200,
        )

    except Exception as e:
        # Log the error and return a standardized error response
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )


@router.post("/queue/image-generation/add-kandinsky-job", 
             description="Add a kandinsky job to the pending queue. If no UUID is provided, an UUID is generated automatically. If no file path is provided, or the provided file path is \"\", '[auto]' or '[default]', the file path is generated automatically.",
             status_code=200,
             tags=["jobs-standardized"],
             response_model=StandardSuccessResponseV1[AddJob],
             responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def add_job(request: Request, kandinsky_task: KandinskyTask):
    try:
        api_response_handler = await ApiResponseHandlerV1.createInstance(request)

        task= kandinsky_task.job

        if not task.dataset:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="The 'dataset' field is required and cannot be empty.",
                http_status_code=422,
            )

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

        creation_time_iso = task.task_creation_time.isoformat() if task.task_creation_time else None

        return api_response_handler.create_success_response_v1(
            response_data={"uuid": task.uuid, "creation_time": creation_time_iso},
            http_status_code=200
        )
    
    except Exception as e:
        # Log the error and return a standardized error response
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )
 
@router.get("/queue/image-generation/get-jobs-count-last-n-hours-v1",
            tags=["jobs-standardized"],
            response_model=StandardSuccessResponseV1[CountLastHour],
            status_code=200,
            description="Gets how many image generation jobs were created in the last N hours for a specific dataset. If the 'hours' parameter is not set, returns the full count of all jobs without time filtering.",
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def get_jobs_count_last_n_hour(request: Request, dataset: str, hours: int = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        query = {"task_input_dict.dataset": dataset}
        
        if hours is not None:
            # Calculate the timestamp for N hours ago
            current_time = datetime.now()
            time_ago = current_time - timedelta(hours=hours)
            query["task_creation_time"] = {"$gte": time_ago.strftime('%Y-%m-%d %H:%M:%S')}

        # Take into account pending & in progress & completed jobs
        pending_count = request.app.pending_jobs_collection.count_documents(query)
        in_progress_count = request.app.in_progress_jobs_collection.count_documents(query)
        completed_count = request.app.completed_jobs_collection.count_documents(query)
        failed_count = request.app.failed_jobs_collection.count_documents(query)

        counts = {
            "pending_count": pending_count,
            "in_progress_count": in_progress_count,
            "completed_count": completed_count,
            "failed_count": failed_count
        }

        return response_handler.create_success_response_v1(
            response_data={"jobs_count": counts},  
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=f"Failed to get jobs count: {str(e)}",
            http_status_code=500
        )
 
    

@router.get("/queue/image-generation/list-pending-jobs", 
            response_model=StandardSuccessResponseV1[ListTask],
            status_code = 200,
            tags=["jobs-standardized"])
async def get_list_pending_jobs(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    jobs = list(request.app.pending_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)
        job['task_creation_time'] = job['task_creation_time'].isoformat()
        job['task_start_time'] = (
            job['task_start_time'].isoformat() 
            if isinstance(job['task_start_time'], datetime) 
            else None
        )
        job['task_completion_time'] = (
            job['task_completion_time'].isoformat() 
            if isinstance(job['task_completion_time'], datetime) 
            else None
        )

    return response_handler.create_success_response_v1(response_data={"jobs": jobs}, http_status_code=200)


@router.get("/queue/image-generation/list-in-progress-jobs", 
            response_model=StandardSuccessResponseV1[ListTask],
            status_code = 200,
            tags=["jobs-standardized"])
async def get_list_in_progress_jobs(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    jobs = list(request.app.in_progress_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)
        job['task_creation_time'] = job['task_creation_time'].isoformat()
        job['task_start_time'] = (
            job['task_start_time'].isoformat() 
            if isinstance(job['task_start_time'], datetime) 
            else None
        )
        job['task_completion_time'] = (
            job['task_completion_time'].isoformat() 
            if isinstance(job['task_completion_time'], datetime) 
            else None
        )

    return response_handler.create_success_response_v1(response_data={"jobs": jobs}, http_status_code=200)


@router.get("/queue/image-generation/list-completed-jobs", 
            response_model=StandardSuccessResponseV1[ListTask],
            status_code=200,
            tags=["jobs-standardized"],
            summary="List completed jobs with optional filters for task type and dataset")
async def get_list_completed_jobs(
    request: Request,
    task_type: Optional[str] = Query(None, description="Filter jobs by task type"),
    dataset: Optional[str] = Query(None, description="Filter jobs by dataset"),
    limit: int = Query(10, description="Limit on the number of results returned", alias="limit")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    # Build the MongoDB query based on provided filters
    query = {}
    if task_type:
        query["task_type"] = task_type
    if dataset:
        query["task_input_dict.dataset"] = dataset

    # Retrieve jobs from the completed jobs collection based on the constructed query and limit
    jobs = list(request.app.completed_jobs_collection.find(query).limit(limit))

    # Remove MongoDB's internal '_id' field from the output
    for job in jobs:
        job.pop('_id', None)

    return response_handler.create_success_response_v1(response_data={"jobs": jobs}, http_status_code=200)

@router.get("/queue/image-generation/list-failed-jobs", 
            response_model=StandardSuccessResponseV1[ListTask],
            status_code = 200,
            tags=["jobs-standardized"])
async def get_list_failed_jobs(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    jobs = list(request.app.failed_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return response_handler.create_success_response_v1(response_data={"jobs": jobs}, http_status_code=200)



@router.get("/queue/image-generation/list-completed-jobs-ordered-by-dataset", 
            response_model=StandardSuccessResponseV1[JobInfoResponse],
            tags=["jobs-standardized"],
            status_code=200,
            description="List completed jobs by job creation date. If no dataset is specified, jobs from all datasets are included.",
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def get_list_completed_jobs_by_date(
    request: Request,
    dataset: Optional[str] = Query(None, description="Dataset input"),
    start_date: str = Query(..., description="Start date for filtering jobs"), 
    end_date: str = Query(..., description="End date for filtering jobs"),
    min_clip_sigma_score: Optional[float] = Query(None, description="Minimum CLIP sigma score to filter jobs")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        print(f"Start Date: {start_date}, End Date: {end_date}")

        # Build the initial query
        query = {
            "task_creation_time": {
                "$gte": start_date,
                "$lt": end_date
            }
        }

        # Conditionally add dataset to the query if provided
        if dataset:
            query["task_input_dict.dataset"] = dataset

        # Add condition to filter by min_clip_sigma_score if provided
        if min_clip_sigma_score is not None:
            query["task_attributes_dict.image_clip_sigma_score"] = {"$gte": min_clip_sigma_score}

        # Print query to debug
        print(f"Query: {query}")

        # Execute query
        jobs = list(request.app.completed_jobs_collection.find(query))

        # Process jobs to organize by dataset
        datasets = {}
        for job in jobs:
            dataset_name = job.get("task_input_dict", {}).get("dataset")
            job_uuid = job.get("uuid")
            file_hash = job.get('task_output_file_dict', {}).get('output_file_hash') 
            file_path = job.get("task_output_file_dict", {}).get("output_file_path")
            clip_sigma_score = job.get("task_attributes_dict", {}).get("image_clip_sigma_score")

            if not dataset_name or not job_uuid or not file_path:
                continue

            if dataset_name not in datasets:
                datasets[dataset_name] = []

            job_info = {
                "job_uuid": job_uuid,
                "file_hash": file_hash,
                "file_path": file_path, 
                "clip_sigma_score": clip_sigma_score
            }

            datasets[dataset_name].append(job_info)

        # Convert datasets to a list of dictionaries for the response
        datasets_list = [{"dataset_name": name, "jobs": jobs} for name, jobs in datasets.items()]

        return response_handler.create_success_response_v1(response_data={"job_info": datasets_list}, http_status_code=200)
    except Exception as e:
        print(f"Error during API execution: {str(e)}")
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=f"Failed to list jobs by date: {str(e)}",
            http_status_code=500
        )



@router.get("/queue/image-generation/get-completed-jobs-using-random-sampling", 
            response_model=StandardSuccessResponseV1[ListSigmaScoreResponse],
            tags=["jobs-standardized"],
            description="returns list of randomly selected completed jobs",
            responses=ApiResponseHandlerV1.listErrors([422, 500])
            )
async def get_list_completed_jobs_by_dataset(
    request: Request,
    dataset: str= Query(..., description="Dataset name"),  
    model_type: str= Query("elm-v1", description="Model type, elm-v1 or linear"),  
    min_clip_sigma_score: Optional[float] = Query(None, description="Minimum CLIP sigma score to filter jobs"),
    sampling_size: int = Query(1, description="Number of images to return")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        query = {
            "task_input_dict.dataset": dataset
        }

        # Add condition to filter by min_clip_sigma_score if provided
        if min_clip_sigma_score is not None:
            query[f"task_attributes_dict.{model_type}.image_clip_sigma_score"] = {"$gte": min_clip_sigma_score}

        # Use $match and $sample to filter documents based on query and randomly select a specified sampling_size of documents
        documents = request.app.completed_jobs_collection.aggregate([
            {"$match": query},
            {"$sample": {"size": sampling_size}}
        ])

        # Convert cursor type to list
        jobs = list(documents)    

        datasets = []
        for job in jobs:
            job_uuid = job.get("uuid")
            file_hash = job.get('task_output_file_dict', {}).get('output_file_hash')  
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

        return response_handler.create_success_response_v1(response_data={"jobs": datasets}, http_status_code=200)
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=f"Failed to list jobs by dataset: {str(e)}",
            http_status_code=500
        )        
    

@router.get("/queue/image-generation/get-completed-jobs-count", 
            response_model=StandardSuccessResponseV1[CountResponse],
            status_code=200,
            tags=["jobs-standardized"],
            description="Count the number of completed jobs optionally filtered by dataset.",
            responses=ApiResponseHandlerV1.listErrors([422]))
async def count_completed(request: Request, dataset: Optional[str] = None, task_type: Optional[str] = None):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    query = {'task_input_dict.dataset': dataset, 'task_type': task_type} if dataset and task_type else {'task_input_dict.dataset': dataset} if dataset else {'task_type': task_type} if task_type else {}
    count = request.app.completed_jobs_collection.count_documents(query)
    
    return response_handler.create_success_response_v1(response_data={"count": count}, http_status_code=200)

@router.get("/queue/image-generation/get-pending-jobs-count", 
            response_model=StandardSuccessResponseV1[CountResponse],
            status_code=200,
            tags=["jobs-standardized"],
            description="Count the number of pending jobs optionally filtered by dataset.",
            responses=ApiResponseHandlerV1.listErrors([422]))
async def count_pending(request: Request, dataset: Optional[str] = None):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    query = {'task_input_dict.dataset': dataset} if dataset else {}
    count = request.app.pending_jobs_collection.count_documents(query)
    
    return response_handler.create_success_response_v1(response_data={"count": count}, http_status_code=200)


@router.get("/queue/image-generation/get-in-progress-jobs-count", 
            response_model=StandardSuccessResponseV1[CountResponse],
            status_code=200,
            tags=["jobs-standardized"],
            description="Count the number of in-progress jobs optionally filtered by dataset.",
            responses=ApiResponseHandlerV1.listErrors([422]))
async def count_in_progress(request: Request, dataset: Optional[str] = None):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    query = {'task_input_dict.dataset': dataset} if dataset else {}
    count = request.app.in_progress_jobs_collection.count_documents(query)
    
    return response_handler.create_success_response_v1(response_data={"count": count}, http_status_code=200)

@router.get("/queue/image-generation/get-failed-jobs-count", 
            response_model=StandardSuccessResponseV1[CountResponse],
            description="count jobs in failed collection",
            status_code = 200,
            tags=["jobs-standardized"])
async def get_failed_job_count(request: Request,  dataset: Optional[str] = None):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    query = {'task_input_dict.dataset': dataset} if dataset else {}
    count = request.app.failed_jobs_collection.count_documents(query)

    return response_handler.create_success_response_v1(response_data={"count": count}, http_status_code=200)



@router.put("/queue/image-generation/set-in-progress-job-as-completed", 
            response_model=StandardSuccessResponseV1[DoneResponse],
            status_code=200,
            tags=["jobs-standardized"],
            description="Update an in-progress job and mark as completed.",
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def update_job_completed(request: Request, uuid: str):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Retrieve the job with the given UUID
        job = request.app.in_progress_jobs_collection.find_one({"uuid": uuid})
        if job is None:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND, 
                error_string="Job not found",
                http_status_code=404
            )

        # Extract the dataset name from the task_input_dict
        dataset_name = job.get("task_input_dict", {}).get("dataset")

        # Find the dataset in the collection
        dataset_result = request.app.datasets_collection.find_one({"dataset_name": dataset_name, "bucket_id": 0})
        if not dataset_result:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND, 
                error_string="Dataset not found",
                http_status_code=404
            )

        dataset_id = dataset_result.get("dataset_id")

        # Insert into all-images collection and get the generated image_uuid
        all_images_collection = request.app.all_image_collection
        image_uuid = insert_into_all_images_for_completed(job, dataset_id, all_images_collection)

        # Update job document with image_uuid
        if image_uuid:
            job["image_uuid"] = image_uuid

        # Move the job to the completed jobs collection with the new image_uuid
        request.app.completed_jobs_collection.insert_one(job)

        # Remove the job from the in-progress collection
        request.app.in_progress_jobs_collection.delete_one({"uuid": uuid})

        return response_handler.create_success_response_v1(
            response_data={"Done": True},
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=f"Failed to update job as completed: {str(e)}",
            http_status_code=500
        )



@router.put("/queue/image-generation/set-in-progress-job-as-failed", 
            response_model=StandardSuccessResponseV1[DoneResponse],
            status_code=200,
            tags=["jobs-standardized"],
            description="Update an in-progress job and mark as failed.",
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def update_job_failed(request: Request, uuid: str):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Retrieve the job with the given UUID
        job = request.app.in_progress_jobs_collection.find_one({"uuid": uuid})

        if job is None:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string="Job not found",
                http_status_code=404,
            )

        # Move the job to the failed jobs collection and delete it from in-progress
        request.app.failed_jobs_collection.insert_one(job)  # Save the existing job data
        request.app.in_progress_jobs_collection.delete_one({"uuid":uuid})

        # Return a success response indicating the job was marked as failed
        return response_handler.create_success_response_v1(
            response_data={"Done": True},
            http_status_code=200,
        )

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Failed to mark job as failed: {str(e)}",
            http_status_code=500,
        )

@router.delete("/queue/image-generation/remove-all-orphaned-completed-jobs", 
               response_model=StandardSuccessResponseV1[CountResponse],
               status_code=200,
               tags=["jobs-standardized"],
               description="Removes completed jobs with missing output files.",
               responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def cleanup_completed_and_orphaned_jobs(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        jobs = request.app.completed_jobs_collection.find({})
        count_removed = 0
        for job in jobs:
            try:
                file_path = job['task_output_file_dict']['output_file_path']
                bucket_name, file_path = separate_bucket_and_file_path(file_path)
                file_exists = cmd.is_object_exists(request.app.minio_client, bucket_name, file_path)
            except Exception:
                file_exists = False

            if not file_exists:
                request.app.completed_jobs_collection.delete_one({"uuid": job['uuid']})
                count_removed += 1  

        # Using WasPresentResponse model to indicate if any jobs were removed
        was_present = count_removed > 0
        return response_handler.create_success_response_v1(
            response_data={ "count": count_removed},
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=f"Failed to cleanup completed and orphaned jobs: {str(e)}",
            http_status_code=500
        )

@router.get("/job/get-completed-job-by-hash-v1", 
            response_model=StandardSuccessResponseV1[Task],
            status_code=200,
            tags=["deprecated3"],
            description="changed with /queue/image-generation/get-completed-jobs-data-by-hash/{image_hash}",
            responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
async def get_completed_job_by_hash(request: Request, image_hash: str):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    job = request.app.completed_jobs_collection.find_one({"task_output_file_dict.output_file_hash": image_hash})

    if job is None:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND, 
            error_string="Job not found",
            http_status_code=404)

    job.pop('_id', None)
    return response_handler.create_success_response_v1(response_data=job, http_status_code=200)


@router.get("/job/get-completed-job-by-uuid", 
            response_model=StandardSuccessResponseV1[Task],
            status_code=200,
            tags=["deprecated3"],
            description="the replacement is '/queue/image-generation/get-completed-jobs-data-by-uuid/{uuid}'",
            responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
async def get_job_by_uuid(request: Request, uuid: str):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    job = request.app.completed_jobs_collection.find_one({"uuid": uuid})

    if job is None:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND, 
            error_string="Job not found",
            http_status_code=404)

    job.pop('_id', None)
    return response_handler.create_success_response_v1(response_data=job, http_status_code=200)

@router.get("/job/list-completed-jobs-by-uuid", 
            response_model=StandardSuccessResponseV1[ListTask],
            status_code=200,
            tags=["jobs-standardized"],
            description="Retrieves multiple jobs by their UUIDs.",
            responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
async def get_jobs_by_uuids(request: Request, uuids: List[str] = Query(...)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    jobs_cursor = request.app.completed_jobs_collection.find({"uuid": {"$in": uuids}})

    jobs = list(jobs_cursor)
    if not jobs:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND, 
            error_string="Jobs not found",
            http_status_code=404)

    for job in jobs:
        job.pop('_id', None)

    return response_handler.create_success_response_v1(response_data={"jobs": jobs}, http_status_code=200)


@router.get("/queue/image-generation/get-completed-jobs-data-by-hash/{image_hash}", 
            response_model=StandardSuccessResponseV1[Task],
            status_code=200,
            tags=["jobs-standardized"],
            description="Retrieves the data of a completed job by image hash. It returns the full data by default, but it can return only some properties by listing them using the 'fields' param",
            responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
@router.get("/get-image-generation/by-hash-v1/{image_hash}", 
            response_model=StandardSuccessResponseV1[dict],
            status_code=200,
            tags=["deprecated3"],
            description="changed with /queue/image-generation/get-completed-jobs-data-by-hash/{image_hash}",
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def get_job_by_image_hash(request: Request, image_hash: str, fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    job = request.app.completed_jobs_collection.find_one({"task_output_file_dict.output_file_hash": image_hash}, projection)
    if job:
        return response_handler.create_success_response_v1(response_data=job, http_status_code=200)
    else:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND, 
            error_string="Job not found",
            http_status_code=404
        )


@router.get("/queue/image-generation/get-completed-jobs-data-by-hashes", 
            response_model=StandardSuccessResponseV1[ListTaskV1],
            status_code=200,
            tags=["deprecated3"],
            description="changed with /queue/image-generation/get-completed-jobs-data-by-hashes-v1",
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_jobs_by_image_hashes(request: Request, image_hashes: List[str] = Query(...), fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    # Use the $in operator to find all matching documents in one query
    jobs = list(request.app.completed_jobs_collection.find({"task_output_file_dict.output_file_hash": {"$in": image_hashes}}, projection))

    # Return the data found in the success response
    return response_handler.create_success_response_v1(response_data=jobs, http_status_code=200)


@router.get("/queue/image-generation/get-completed-jobs-data-by-uuid/{uuid}", 
            response_model=StandardSuccessResponseV1[Task],
            status_code=200,
            tags=["jobs-standardized"],
            description="Retrieves the data of a completed job by uuid. It returns the full data by default, but it can return only some properties by listing them using the 'fields' param",
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_job_by_uuid(request: Request, uuid: str, fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    job = request.app.completed_jobs_collection.find_one({"uuid": uuid}, projection)
    if job:
        return response_handler.create_success_response_v1(response_data=job, http_status_code=200)
    else:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND, 
            error_string="Job not found",
            http_status_code=404
        )

@router.get("/get-image-generation/by-hash-v2/{image_hash}",
            response_model=StandardSuccessResponseV1[dict],
            status_code=200,
            tags=["deprecated3"],
            description="changed with /queue/image-generation/get-completed-jobs-data-by-hash/{image_hash}",
            responses=ApiResponseHandlerV1.listErrors([404, 500]),
)
async def get_job_by_image_hash(request: Request, image_hash: str, fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        # Define a projection for MongoDB based on the requested fields
        # The endpoint projection ensures full paths are specified
        projection = {field: 1 for field in fields} if fields else {}
        projection["_id"] = 0

        # Query the database to retrieve the job
        job = request.app.completed_jobs_collection.find_one(
            {"task_output_file_dict.output_file_hash": image_hash}, projection
        )

        # Initialize response data
        response_data = {}

        # Populate the response data with the correct field paths
        if job:
            for field in fields:
                # Access nested fields through a dynamic approach
                path_parts = field.split('.')
                current_data = job

                # Traverse the nested path to retrieve the value
                for part in path_parts:
                    if part in current_data:
                        current_data = current_data[part]
                    else:
                        current_data = None
                        break  # If any part doesn't exist, break out

                # Only add to response_data if a valid value was found
                if current_data is not None:
                    response_data[field] = current_data

            return response_handler.create_success_response_v1(
                response_data=response_data,
                http_status_code=200,
            )
        else:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Job with image hash '{image_hash}' not found",
                http_status_code=404,
            )


    except Exception as e:
        print("Exception occurred:", e)  # Debugging print statement to check the exception
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )


@router.get("/get-image-generation/by-job-id-v1/{job_id}", 
            response_model=StandardSuccessResponseV1[dict],
            status_code=200,
            tags=["deprecated3"],
            description="changed with /queue/image-generation/get-completed-jobs-data-by-uuid/{uuid}",
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def get_job_by_job_id(request: Request, job_id: str, fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    job = request.app.completed_jobs_collection.find_one({"uuid": job_id}, projection)
    if job:
        return response_handler.create_success_response_v1(response_data=job, http_status_code=200)
    else:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND, 
            error_string="Job not found",
            http_status_code=404
        )


@router.get("/queue/image-generation/count-non-empty-task-attributes", 
            response_model=StandardSuccessResponseV1[CountResponse],
            status_code=200,
            tags=["jobs-standardized"],
            description="Counts the number of jobs where task_attributes_dict is not empty.",
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def count_non_empty_task_attributes(request: Request, task_type: str = "image_generation_task"):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Count documents where task_attributes_dict is not empty
        count = request.app.completed_jobs_collection.count_documents({
            'task_type': task_type, 
            'task_attributes_dict': {'$exists': True, '$ne': {}}
        })

        return response_handler.create_success_response_v1(response_data={"count": count}, http_status_code=200)

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=f"Failed to count documents: {str(e)}",
            http_status_code=500
        )

@router.post("/update-jobs",
             description="Adds the 'ranking_count', 'safe_to_delete' and 'tag_count' properties to the completed jobs that still don't have them",
             tags= ["utility"],
             response_model=StandardSuccessResponseV1[CountResponse],
             responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def update_completed_jobs(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    # Use projection to fetch only necessary fields
    cursor = request.app.completed_jobs_collection.find(
        {"safe_to_delete": {"$exists": False}},
        {'task_output_file_dict': 1}
    )

    updated_count = 0

    for job in cursor:
        output_file_hash = job.get('task_output_file_dict', {}).get('output_file_hash', '')

        # Count occurrences in image_tags_collection
        tag_count = request.app.image_tags_collection.count_documents({'image_hash': output_file_hash})

        start_time = time.time()
        count1 = request.app.image_pair_ranking_collection.count_documents({'image_1_metadata.file_hash': output_file_hash})
        count2 = request.app.image_pair_ranking_collection.count_documents({'image_2_metadata.file_hash': output_file_hash})
        ranking_count = count1 + count2
        print("Time to count rankings: {:.2f} seconds".format(time.time() - start_time))

        # Determine if the image is safe to delete
        safe_to_delete = tag_count == 0 and ranking_count == 0

        # Update the job document individually
        update_result = request.app.completed_jobs_collection.update_one(
            {'_id': ObjectId(job['_id'])},
            {'$set': {
                'ranking_count': ranking_count,
                'safe_to_delete': safe_to_delete,
                'tag_count': tag_count,
            }}
        )

        if update_result.modified_count > 0:
            updated_count += 1

    # Return a message about how many documents were updated
    if updated_count > 0:
        return response_handler.create_success_response_v1(response_data=updated_count, http_status_code=200)
    else:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=f"Failed to count documents",
            http_status_code=500
        )



@router.get("/count-updated-jobs",
            response_model=StandardSuccessResponseV1[CountResponse],
            tags= ["utility"],
            description="count updated jobs",
            responses=ApiResponseHandlerV1.listErrors([422]))
async def get_completed_job_count(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    # Count documents where "safe_to_delete" field exists
    count = request.app.completed_jobs_collection.count_documents({"safe_to_delete": {"$exists": True}})
    
    return response_handler.create_success_response_v1(response_data=count, http_status_code=200)


@router.get("/queue/image-generation/get-completed-jobs-data-by-hashes-v1", 
            response_model=StandardSuccessResponseV1[ListTaskV1],
            status_code=200,
            tags=["jobs-standardized"],
            description="Retrieves the data of completed jobs by a list of image hashes. It returns the full data by default, but it can return only some properties by listing them using the 'fields' param",
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_jobs_by_image_hashes(request: Request, image_hashes: List[str] = Query(...), fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    # Use the $in operator to find all matching documents in one query
    jobs = list(request.app.completed_jobs_collection.find({"task_output_file_dict.output_file_hash": {"$in": image_hashes}}, projection))

    # Return the data found in the success response
    return response_handler.create_success_response_v1(response_data={'images': jobs}, http_status_code=200)