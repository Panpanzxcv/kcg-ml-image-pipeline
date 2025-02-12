from fastapi import Request, HTTPException, APIRouter, Response, Query, status
from datetime import datetime, timedelta
import math
import random
import pymongo
from utility.minio import cmd
from orchestration.api.mongo_schema.active_learning_schemas import  ActiveLearningPolicy, ActiveLearningQueuePair
from .api_utils import PrettyJSONResponse, ApiResponseHandler
import os
from fastapi.responses import JSONResponse
from pymongo.collection import Collection
from datetime import datetime, timezone
from typing import List
from io import BytesIO
from bson import ObjectId
from typing import Optional
import json

router = APIRouter()

@router.post("/active-learning-queue/add-queue-pair-to-mongo")
def add_queue_pair(request: Request, queue_pair: ActiveLearningQueuePair):
    # Validate and retrieve the active learning policy using the active_learning_policy_id
    policy = request.app.active_learning_policies_collection.find_one(
        {"active_learning_policy_id": queue_pair.active_learning_policy_id}
    )
    if not policy:
        raise HTTPException(status_code=404, detail=f"Active learning policy with ID {queue_pair.active_learning_policy_id} not found")

    # Function to extract job details
    def extract_job_details(job_uuid, suffix):
        job = request.app.completed_jobs_collection.find_one({"uuid": job_uuid})
        if not job:
            raise HTTPException(status_code=422, detail=f"Job {job_uuid} not found")

        output_file_path = job["task_output_file_dict"]["output_file_path"]
        task_creation_time = job["task_creation_time"]
        path_parts = output_file_path.split('/')
        if len(path_parts) < 4:
            raise HTTPException(status_code=500, detail="Invalid output file path format")

        return {
            f"job_uuid_{suffix}": job_uuid,
            f"file_name_{suffix}": path_parts[-1],
            f"image_path_{suffix}": output_file_path,
            f"image_hash_{suffix}": job["task_output_file_dict"]["output_file_hash"],
            f"job_creation_time_{suffix}": task_creation_time,
        }

    # Extract job details for both jobs
    job_details_1 = extract_job_details(queue_pair.image1_job_uuid, "1")
    job_details_2 = extract_job_details(queue_pair.image2_job_uuid, "2")

    # Prepare the document to insert into the active learning queue pairs collection
    combined_job_details = {
        "active_learning_policy_id": queue_pair.active_learning_policy_id,
        "active_learning_policy": policy["active_learning_policy"],  # Retrieved from the policies collection
        "dataset": job_details_1['image_path_1'].split('/')[1],
        "metadata": queue_pair.metadata,
        "generator_string": queue_pair.generator_string,
        "creation_date": datetime.utcnow().isoformat(),
        "images": [
            {
                "job_uuid_1": job_details_1["job_uuid_1"],
                "file_name_1": job_details_1["file_name_1"],
                "image_path_1": job_details_1["image_path_1"],
                "image_hash_1": job_details_1["image_hash_1"],
                "job_creation_time_1": job_details_1["job_creation_time_1"],
            },
            {
                "job_uuid_2": job_details_2["job_uuid_2"],
                "file_name_2": job_details_2["file_name_2"],
                "image_path_2": job_details_2["image_path_2"],
                "image_hash_2": job_details_2["image_hash_2"],
                "job_creation_time_2": job_details_2["job_creation_time_2"],
            }
        ]
    }

    # Insert the combined job details into MongoDB collection
    request.app.active_learning_queue_pairs_collection.insert_one(combined_job_details)

    return {"status": "success", "message": "Queue pair added successfully to MongoDB"}



@router.get("/active-learning-queue/list-queue-pairs-from-mongo", response_class=PrettyJSONResponse)
def list_queue_pairs(request: Request, dataset: Optional[str] = None, limit: int = 10, offset: int = 0):
    # Build the query based on whether a dataset is provided
    query = {"dataset": dataset} if dataset else {}
    
    # Execute the query with the filter if dataset is provided
    queue_pairs_cursor = request.app.active_learning_queue_pairs_collection.find(query).skip(offset).limit(limit)
    
    # Convert the cursor to a list of dictionaries and drop the _id field
    queue_pairs = []
    for pair in queue_pairs_cursor:
        # Drop the _id field from the response
        pair.pop('_id', None)
        queue_pairs.append(pair)

    # Directly return the list of modified dictionaries
    return queue_pairs


@router.get("/active-learning-queue/get-random-queue-pair-from-mongo", response_class=PrettyJSONResponse)
def random_queue_pair(request: Request, size: int = 1, dataset: Optional[str] = None, active_learning_policy: Optional[str] = None):
    # Define the aggregation pipeline
    pipeline = []

    # Filters based on dataset and active_learning_policy
    match_filter = {}
    if dataset:
        match_filter["dataset"] = dataset
    if active_learning_policy:
        match_filter["active_learning_policy"] = active_learning_policy

    if match_filter:
        pipeline.append({"$match": match_filter})

    # Add the random sampling stage to the pipeline
    pipeline.append({"$sample": {"size": size}})

    # Use MongoDB's aggregation framework to randomly select documents
    random_pairs_cursor = request.app.active_learning_queue_pairs_collection.aggregate(pipeline)

    # Convert the cursor to a list of dictionaries
    random_pairs = []
    for pair in random_pairs_cursor:
        pair['_id'] = str(pair['_id'])  # Convert _id ObjectId to string
        random_pairs.append(pair)

    return random_pairs



@router.delete("/active-learning-queue/delete-queue-pair-from-mongo")
def delete_queue_pair(request: Request, id: str):
    # Convert the string ID to ObjectId
    try:
        obj_id = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")

    # Delete the document with the specified _id
    result = request.app.active_learning_queue_pairs_collection.delete_one({"_id": obj_id})

    # Check if a document was deleted
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Document not found")

    return {"status": "success", "message": f"Deleted queue pair with _id: {id} from MongoDB"}


@router.get("/active-learning-queue/count-queue-pairs")
def count_queue_pairs(request: Request, dataset: Optional[str] = None, active_learning_policy: Optional[str] = None):
    # Build a query filter based on the provided parameters
    query_filter = {}
    if dataset:
        query_filter["dataset"] = dataset
    if active_learning_policy:
        query_filter["active_learning_policy"] = active_learning_policy

    # Count the documents in the collection based on the query filter
    count = request.app.active_learning_queue_pairs_collection.count_documents(query_filter)

    # Return the count in a JSON response
    return count


@router.get("/active-learning/uncertainty-sampling-pair-v1", 
            response_class=PrettyJSONResponse,
            tags= ['deprecated3'],
            description="it is not used any more, so there is no replacement.")
def get_ranking_comparison(
    request: Request,
    dataset: str,  
    score_type: str,  # Added score_type parameter to choose between clip_sigma_score and embedding_sigma_score
    min_score: float,
    max_score: float,
    threshold: float
):
    if score_type not in ["clip_sigma_score", "embedding_sigma_score"]:
        raise HTTPException(status_code=400, detail="Invalid score_type parameter")

    image_rank_scores_collection: Collection = request.app.image_rank_scores_collection

    try:
        # Fetch a random image score within the score range and the specified dataset
        first_image_cursor = image_rank_scores_collection.aggregate([
            {"$match": {
                "score": {"$gte": min_score, "$lte": max_score},
                "dataset": dataset  # Filter by dataset
            }},
            {"$sample": {"size": 1}}
        ])
        first_image_score = next(first_image_cursor, None)

        if not first_image_score:
            {"images": []}

        # Calculate the score range for the second image using the selected score_type
        base_score = first_image_score[score_type]  # Use dynamic score_type

        # Fetch candidate images for the second image within the specified dataset
        candidates_cursor = image_rank_scores_collection.find({
            score_type: {"$gte": min_score, "$lte": max_score},
            "image_hash": {"$ne": first_image_score['image_hash']},
            "dataset": dataset  # Filter by dataset
        })

        # Compute probabilities using sigmoid function based on the score_type
        candidates = list(candidates_cursor)
        total_probability = 0
        for candidate in candidates:
            score_diff = abs(candidate[score_type] - base_score)  # Use dynamic score_type
            probability = 1 / (1 + math.exp((score_diff - threshold) / 50))
            candidate['probability'] = probability
            total_probability += probability

        # Select the second image based on computed probabilities
        if total_probability == 0:
            {"images": []}

        random_choice = random.uniform(0, total_probability)
        cumulative = 0
        for candidate in candidates:
            cumulative += candidate['probability']
            if cumulative >= random_choice:
                second_image_score = candidate
                break

    except StopIteration:
        return JSONResponse(
            status_code=500,
            content={"message": "Error fetching images from the database."}
        )

    # Prepare the images for the response
    images = [
        {
            "image_hash": first_image_score['image_hash'],
            "image_score": first_image_score[score_type]  # Use dynamic score_type
        },
        {
            "image_hash": second_image_score['image_hash'],
            "image_score": second_image_score[score_type]  # Use dynamic score_type
        }
    ]

    return {"images": images}


@router.get("/active-learning/uncertainty-sampling-pair-v2", 
            response_class=PrettyJSONResponse,
            tags= ['deprecated3'],
            description="it is not used any more, so there is no replacement.")
def get_ranking_comparison(
    request: Request,
    dataset: str,  
    score_type: str,
    model: str,  
    min_score: float,
    max_score: float,
    threshold: float
):

    # Input Validations
    if score_type not in ["image_clip_sigma_score", "text_embedding_sigma_score"]:
        raise HTTPException(status_code=422, detail="Invalid score_type parameter")

    if model not in ["linear", "elm-v1"]:
        raise HTTPException(status_code=422, detail="Invalid model parameter")

    try:
        min_score = min_score
        max_score = max_score

        base_score_field = f"task_attributes_dict.{model}.{score_type}"

        # Find first image
        first_image_cursor = request.app.completed_jobs_collection.aggregate([
            {"$match": {
                base_score_field: {"$gte": min_score, "$lte": max_score},
                "task_input_dict.dataset": dataset
            }},
            {"$sample": {"size": 1}}
        ])

        first_image_score = next(first_image_cursor, None)
        if not first_image_score:
            print("No first image found matching criteria.")
            return {"images": []}

        if 'task_attributes_dict' not in first_image_score or model not in first_image_score['task_attributes_dict']:
            print(f"task_attributes_dict.{model} not found in the fetched document")
            return {"images": []}

        base_score = float(first_image_score['task_attributes_dict'][model][score_type])

        lower_bound = base_score - threshold
        upper_bound = base_score + threshold

        # Find candidates for second image
        candidates_cursor = request.app.completed_jobs_collection.find({
            base_score_field: {"$gte": lower_bound, "$lte": upper_bound},
            "task_output_file_dict.output_file_hash": {"$ne": first_image_score['task_output_file_dict']['output_file_hash']},
            "task_input_dict.dataset": dataset
        })

        candidates = list(candidates_cursor)

        if not candidates:
            print("No candidates found for second image.")
            return {"images": []}

        second_image_score = random.choice(candidates)
        print(f"Selected second image with hash: {second_image_score['task_output_file_dict']['output_file_hash']}")

        images = [
            {
                "image_hash": first_image_score['task_output_file_dict']['output_file_hash'],
                "image_score": first_image_score['task_attributes_dict'][model][score_type]
            },
            {
                "image_hash": second_image_score['task_output_file_dict']['output_file_hash'],
                "image_score": second_image_score['task_attributes_dict'][model][score_type]
            }
        ]

        print(f"Returning images: {images}")
        return {"images": images}

    except StopIteration:
        print("Error fetching images from the database.")
        raise HTTPException(status_code=500, detail="Internal server error")