from fastapi import Request, APIRouter, HTTPException, Query
import uuid
from datetime import datetime
from orchestration.api.mongo_schemas import TrainingTask
from orchestration.api.api_dataset import get_sequential_id
from .api_utils import PrettyJSONResponse

router = APIRouter()

# -------------------- Get -------------------------
@router.get("/model-training/queue/get-job")
def get_job(request: Request, model_task: str = None):
    query = {}
    if model_task:
        query = {"model_task": model_task}

    # find
    job = request.app.training_pending_jobs_collection.find_one(query)
    if job is None:
        raise HTTPException(status_code=204)

    # delete from pending
    request.app.training_pending_jobs_collection.delete_one({"uuid": job["uuid"]})
    # add to in progress
    request.app.training_in_progress_jobs_collection.insert_one(job)

    # remove the auto generated field
    job.pop('_id', None)

    return job

 # --------------------- Add ---------------------------
@router.post("/model-training/queue/add-training-job", description="Add a job to db")
def add_job(request: Request, training_task: TrainingTask):
    if training_task.uuid in ["", None]:
        # generate since its empty
        training_task.uuid = str(uuid.uuid4())

    # add task creation time
    training_task.task_creation_time = datetime.now()
    request.app.training_pending_jobs_collection.insert_one(training_task.to_dict())

    return {"uuid": training_task.uuid, "creation_time": training_task.task_creation_time}




# -------------- Get jobs count ----------------------
@router.get("/model-training/queue/pending-job-count")
def get_pending_job_count(request: Request):
    count = request.app.training_pending_jobs_collection.count_documents({})
    return {"pending_job_count": count}


@router.get("/model-training/queue/inprogress-job-count")
def get_in_progress_job_count(request: Request):
    count = request.app.training_in_progress_jobs_collection.count_documents({})
    return {"in_progress_job_count": count}


@router.get("/model-training/queue/completed-job-count")
def get_completed_job_count(request: Request):
    count = request.app.training_completed_jobs_collection.count_documents({})
    return {"completed_job_count": count}


@router.get("/model-training/queue/failed-job-count")
def get_failed_job_count(request: Request):
    count = request.app.training_failed_jobs_collection.count_documents({})
    return {"failed_job_count": count}



# ----------------- delete jobs ----------------------
@router.delete("/model-training/queue/clear-pending-jobs")
def clear_all_pending_jobs(request: Request):
    request.app.training_pending_jobs_collection.delete_many({})

    return True


@router.delete("/model-training/queue/clear-all-in-progress-jobs")
def clear_all_in_progress_jobs(request: Request, dataset: str = Query(...)):  
    if dataset == "all":
        request.app.training_in_progress_jobs_collection.delete_many({})
    else:
        request.app.training_in_progress_jobs_collection.delete_many({"dataset": dataset})

    return True


@router.delete("/model-training/queue/clear-all-failed-jobs")
def clear_all_failed_jobs(request: Request, dataset: str = Query(...)):  
    if dataset == "all":
        request.app.training_failed_jobs_collection.delete_many({})
    else:
        request.app.training_failed_jobs_collection.delete_many({"dataset": dataset})

    return True

@router.delete("/model-training/queue/clear-all-completed-jobs")
def clear_all_completed_jobs(request: Request, dataset: str = Query(...)): 
    if not dataset:
        raise HTTPException(status_code=400, detail="Dataset parameter is required.")

    request.app.training_completed_jobs_collection.delete_many({"dataset": dataset})

    return True



 # --------------------- List ----------------------
@router.get("/model-training/queue/list-pending-jobs", response_class=PrettyJSONResponse)
def get_list_pending_jobs(request: Request):
    jobs = list(request.app.training_pending_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/model-training/queue/list-inprogress-jobs", response_class=PrettyJSONResponse)
def get_list_in_progress_jobs(request: Request):
    jobs = list(request.app.training_in_progress_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/model-training/queue/list-completed-jobs", response_class=PrettyJSONResponse)
def get_list_completed_jobs(request: Request):
    jobs = list(request.app.training_completed_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


@router.get("/model-training/queue/list-failed-jobs", response_class=PrettyJSONResponse)
def get_list_failed_jobs(request: Request):
    jobs = list(request.app.training_failed_jobs_collection.find({}))

    for job in jobs:
        job.pop('_id', None)

    return jobs


# ---------------- Update -------------------


@router.put("/model-training/queue/update-job-status-to-completed", description="Update in progress job and mark as completed.")
def update_job_completed(request: Request, training_task: TrainingTask):
    # check if exist
    job = request.app.training_in_progress_jobs_collection.find_one({"uuid": training_task.uuid})
    if job is None:
        return False

    # add to completed
    request.app.training_completed_jobs_collection.insert_one(training_task.to_dict())

    # remove from in progress
    request.app.training_in_progress_jobs_collection.delete_one({"uuid": training_task.uuid})

    return True


@router.put("/model-training/queue/update-job-status-to-failed", description="Update in progress job and mark as failed.")
def update_job_failed(request: Request, training_task: TrainingTask):
    # check if exist
    job = request.app.training_in_progress_jobs_collection.find_one({"uuid": training_task.uuid})
    if job is None:
        return False

    # add to failed
    request.app.training_failed_jobs_collection.insert_one(training_task.to_dict())

    # remove from in progress
    request.app.training_in_progress_jobs_collection.delete_one({"uuid": training_task.uuid})

    return True
