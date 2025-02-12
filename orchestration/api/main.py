import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pymongo
from bson.objectid import ObjectId
from fastapi.responses import JSONResponse
from .api_utils import ApiResponseHandlerV1, PrettyJSONResponse, ApiResponseHandler, ErrorCode,  StandardErrorResponseV1, StandardSuccessResponse
from fastapi.exceptions import RequestValidationError
from fastapi import status, Request
from dotenv import dotenv_values
from datetime import datetime
from orchestration.api.api_clip import router as clip_router
from orchestration.api.api_dataset import router as dataset_router
from orchestration.api.api_inpainting_dataset import router as inpainting_dataset_router
from orchestration.api.api_image import router as image_router
from orchestration.api.api_job_stats import router as job_stats_router
from orchestration.api.api_job import router as job_router
from orchestration.api.api_ranking import router as ranking_router
from orchestration.api.api_training import router as training_router
from orchestration.api.api_model import router as model_router
from orchestration.api.api_tag import router as tag_router
from orchestration.api.api_dataset_settings import router as dataset_settings_router
from orchestration.api.api_users import router as user_router
from orchestration.api.api_score import router as score_router
from orchestration.api.api_sigma_score import router as sigma_score_router
from orchestration.api.api_residual import router as residual_router
from orchestration.api.api_percentile import router as percentile_router
from orchestration.api.api_residual_percentile import router as residual_percentile_router
from orchestration.api.api_image_by_rank import router as image_by_rank_router
from orchestration.api.api_queue_ranking import router as queue_ranking_router
from orchestration.api.api_active_learning import router as active_learning 
from orchestration.api.api_active_learning_policy import router as active_learning_policy_router
from orchestration.api.api_worker import router as worker_router
from orchestration.api.api_inpainting_job import router as inpainting_job_router
from orchestration.api.api_server_utility import router as server_utility_router
from orchestration.api.api_classifier_score import router as classifier_score_router
from orchestration.api.api_classifier import router as classifier_router
from orchestration.api.api_ranking_model import router as ranking_model_router
from orchestration.api.api_ab_rank import router as ab_rank_router
from orchestration.api.api_rank_active_learning import router as rank_router
from orchestration.api.api_rank_active_learning_policy import router as rank_active_learning_policy_router
from orchestration.api.api_image_hashes import router as image_hashes_router
from orchestration.api.api_external_images import router as external_images_router
from orchestration.api.api_extracts import router as extracts_router
from orchestration.api.api_ingress_videos import router as ingress_videos_router
from orchestration.api.api_bucket import router as bucket_router
from orchestration.api.api_all_images import router as all_images
from orchestration.api.api_video_game import router as video_game_router
from orchestration.api.api_clustered_image import router as image_clustered_router
from orchestration.api.api_cluster_model import router as cluster_model_router
from utility.minio import cmd

config = dotenv_values("./orchestration/api/.env")
app = FastAPI(title="Orchestration API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(clip_router)
app.include_router(dataset_router)
app.include_router(inpainting_dataset_router)
app.include_router(image_router)
app.include_router(image_by_rank_router)
app.include_router(job_router)
app.include_router(job_stats_router)
app.include_router(ranking_router)
app.include_router(training_router)
app.include_router(model_router)
app.include_router(tag_router)
app.include_router(dataset_settings_router)
app.include_router(user_router)
app.include_router(score_router)
app.include_router(sigma_score_router)
app.include_router(residual_router)
app.include_router(percentile_router)
app.include_router(residual_percentile_router)
app.include_router(queue_ranking_router)
app.include_router(active_learning)
app.include_router(active_learning_policy_router)
app.include_router(worker_router)
app.include_router(inpainting_job_router)
app.include_router(server_utility_router)
app.include_router(classifier_score_router)
app.include_router(classifier_router)
app.include_router(ranking_model_router)
app.include_router(ab_rank_router)
app.include_router(rank_router)
app.include_router(rank_active_learning_policy_router)
app.include_router(image_hashes_router)
app.include_router(external_images_router)
app.include_router(extracts_router)
app.include_router(ingress_videos_router)
app.include_router(bucket_router)
app.include_router(all_images)
app.include_router(video_game_router)
app.include_router(image_clustered_router)
app.include_router(cluster_model_router)



def get_minio_client(minio_ip_addr, minio_access_key, minio_secret_key):
    # check first if minio client is available
    minio_client = None
    while minio_client is None:
        # check minio server
        if cmd.is_minio_server_accessible(address= minio_ip_addr):
            minio_client = cmd.connect_to_minio_client(minio_ip_addr= minio_ip_addr, access_key=minio_access_key, secret_key=minio_secret_key)
            return minio_client


def add_models_counter():
    # add counter for models
    try:
        app.counters_collection.insert_one({"_id": "models", "seq": 0})
    except Exception as e:
        print("models counter already exists.")

    return True

def create_collection_if_not_exists(db, collection_name):
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
        print(f"Collection '{collection_name}' created.")
    else:
        print(f"Collection '{collection_name}' already exists.")

def create_index_if_not_exists(collection, index_key, index_name):
    existing_indexes = collection.index_information()
    
    if index_name not in existing_indexes:
        collection.create_index(index_key, name=index_name)
        print(f"Index '{index_name}' created on collection '{collection.name}'.")
    else:
        print(f"Index '{index_name}' already exists on collection '{collection.name}'.")


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print(exc.errors())

    error_string = ""
    for err in exc.errors():
        error_string += "(" + err["loc"][1] + " param in " + err["loc"][0] + ": " + err["msg"] + ") "

    response_handler = ApiResponseHandlerV1.createInstanceWithBody(request, exc.body)

    return response_handler.create_error_response_v1(
        error_code=ErrorCode.INVALID_PARAMS,
        error_string="Validation Error " + error_string,
        http_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    )


@app.on_event("startup")
def startup_db_client():
    # add creation of mongodb here for now
    app.mongodb_client = pymongo.MongoClient(config["DB_URL"], uuidRepresentation='standard')
    app.mongodb_db = app.mongodb_client["orchestration-job-db"]
    app.users_collection = app.mongodb_db["users"]
    app.pending_jobs_collection = app.mongodb_db["pending-jobs"]
    app.in_progress_jobs_collection = app.mongodb_db["in-progress-jobs"]
    app.completed_jobs_collection = app.mongodb_db["completed-jobs"]

    completed_jobs_hash_index=[
    ('task_output_file_dict.output_file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.completed_jobs_collection ,completed_jobs_hash_index, 'completed_jobs_hash_index')

    completed_jobs_createdAt_index=[
    ('task_creation_time', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.completed_jobs_collection ,completed_jobs_createdAt_index, 'completed_jobs_createdAt_index')
    
    completed_jobs_compound_index=[
    ('task_input_dict.dataset', pymongo.ASCENDING),
    ('task_creation_time', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.completed_jobs_collection ,completed_jobs_compound_index, 'completed_jobs_compound_index')

    pending_jobs_task_creation_time_index=[
    ("task_creation_time", pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.pending_jobs_collection ,pending_jobs_task_creation_time_index, 'pending_jobs_task_creation_time_index')
    
    pending_jobs_task_type_index=[
    ("task_type", pymongo.ASCENDING),
    ("task_creation_time", pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.pending_jobs_collection ,pending_jobs_task_type_index, 'pending_jobs_task_type_index')
    
    pending_jobs_task_type_and_dataset_index=[
    ("task_type", pymongo.ASCENDING),
    ("task_input_dict.dataset", pymongo.ASCENDING),
    ("task_creation_time", pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.pending_jobs_collection ,pending_jobs_task_type_and_dataset_index, 'pending_jobs_task_type_and_dataset_index')

    completed_jobs_uuid_index=[
    ('uuid', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.completed_jobs_collection ,completed_jobs_uuid_index, 'completed_jobs_uuid_index')


    app.failed_jobs_collection = app.mongodb_db["failed-jobs"]

    #inpainting jobs
    app.pending_inpainting_jobs_collection = app.mongodb_db["pending-inpainting-jobs"]
    app.in_progress_inpainting_jobs_collection = app.mongodb_db["in-progress-inpainting-jobs"]
    app.completed_inpainting_jobs_collection = app.mongodb_db["completed-inpainting-jobs"]

    # used to store sequential ids of generated images
    app.dataset_sequential_id_collection = app.mongodb_db["dataset-sequential-id"]
    # used to store sequential ids of extracted images
    app.extract_data_batch_sequential_id = app.mongodb_db["extract-data-batch-sequential-id"]
    # used to store sequential ids of generated images
    app.inpainting_dataset_sequential_id_collection = app.mongodb_db["inpainting-dataset-sequential-id"]
    # used store the sequential ids of self training data
    app.self_training_sequential_id_collection = app.mongodb_db["self-training-sequential-id"]

    # for training jobs
    app.training_pending_jobs_collection = app.mongodb_db["training-pending-jobs"]
    app.training_in_progress_jobs_collection = app.mongodb_db["training-in-progress-jobs"]
    app.training_completed_jobs_collection = app.mongodb_db["training-completed-jobs"]
    app.training_failed_jobs_collection = app.mongodb_db["training-failed-jobs"]
    app.video_game_collection = app.mongodb_db["video-game"]

    # dataset rate
    app.dataset_config_collection = app.mongodb_db["dataset_config"]

    app.all_image_collection = app.mongodb_db["all-images"]

    all_images_hash_index=[
    ('image_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.all_image_collection ,all_images_hash_index, 'all_images_hash_index')

    all_images_hash_and_bucket_index=[
    ('image_hash', pymongo.ASCENDING),
    ('bucket_id', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.all_image_collection ,all_images_hash_and_bucket_index, 'all_images_hash_and_bucket_index')


    # bucket collection

    app.buckets_collection = app.mongodb_db["buckets"]

    app.datasets_collection = app.mongodb_db["datasets"]

    # external dataset

    app.external_datasets_collection = app.mongodb_db["external-datasets"]

    app.extract_datasets_collection = app.mongodb_db["extract-datasets"]

    # ab ranking
    app.rank_collection = app.mongodb_db["rank_definitions"]
    app.rank_model_categories_collection = app.mongodb_db["rank_categories"]


    # tags
    app.tag_definitions_collection = app.mongodb_db["tag_definitions"]
    app.image_tags_collection = app.mongodb_db["image_tags"]

    # image-clustering
    app.clustered_images_collection = app.mongodb_db["clustered_images"]
    app.cluster_model_collection = app.mongodb_db["cluster_models"]
    
    tagged_images_hash_index=[
    ('image_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_tags_collection ,tagged_images_hash_index, 'tagged_images_hash_index')

    tagged_images_source_index=[
    ('image_source', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_tags_collection ,tagged_images_source_index, 'tagged_images_source_index')

    app.tag_categories_collection = app.mongodb_db["tag_categories"]

    # pseudo tags
    app.pseudo_tag_definitions_collection = app.mongodb_db["pseudo_tag_definitions"]
    app.pseudo_tag_categories_collection = app.mongodb_db["pseudo_tag_categories"]
    app.uuid_pseudo_tag_count_collection = app.mongodb_db["pseudo_tag_count"]

    # external image collection
    app.external_images_collection = app.mongodb_db["external_images"]

    external_images_dataset_index=[
    ('dataset', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.external_images_collection ,external_images_dataset_index, 'external_images_dataset_index')

    external_images_hashx=[
    ('image_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.external_images_collection ,external_images_hashx, 'external_images_hashx')
    
    external_images_creation_time_index=[
    ('upload_date', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.external_images_collection ,external_images_creation_time_index, 'external_images_creation_time_index')

    external_images_compound_index = [
    ('dataset', pymongo.ASCENDING),
    ('upload_date', pymongo.ASCENDING),
    ('image_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.external_images_collection, external_images_compound_index, 'external_images_compound_index')

    app.extracts_collection = app.mongodb_db["extracts"]
    
    extracts_dataset_index=[
    ('dataset', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.extracts_collection ,extracts_dataset_index, 'extracts_dataset_index')

    extracts_dataset_hashx=[
    ('image_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.extracts_collection ,extracts_dataset_hashx, 'extracts_dataset_hashx')
    
    extracts_creation_time_index=[
    ('upload_date', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.extracts_collection ,extracts_creation_time_index, 'extracts_creation_time_index')

    extracts_old_uuid_index=[
    ('old_uuid_string', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.extracts_collection ,extracts_old_uuid_index, 'extracts_old_uuid_index')

    extracts_source_hash_index = [
    ('source_image_hash', pymongo.ASCENDING),
    ('upload_date', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.extracts_collection, extracts_source_hash_index, 'extracts_source_hash_index')

    app.ingress_video_collection = app.mongodb_db["ingress_videos"]
    app.external_dataset_sequential_id = app.mongodb_db["external_dataset_sequential_id"]


    #classifier
    app.classifier_models_collection = app.mongodb_db["classifier_models"]
    
    #ranking models
    create_collection_if_not_exists(app.mongodb_db, "ranking_models")
    app.ranking_models_collection = app.mongodb_db["ranking_models"]

    # delta score
    app.datapoints_delta_score_collection = app.mongodb_db["datapoints_delta_score"]

    # workers
    app.workers_collection = app.mongodb_db["workers"]

    # models
    app.models_collection = app.mongodb_db["models"]

    # counters
    app.counters_collection = app.mongodb_db["counters"]
    add_models_counter()

    app.uuid_tag_count_collection = app.mongodb_db["tag_count"]

    # scores
    create_collection_if_not_exists(app.mongodb_db, "image_rank_scores")
    app.image_rank_scores_collection = app.mongodb_db["image_rank_scores"]

    rank_scores_hash_index=[
    ("image_hash", pymongo.ASCENDING),
    ]
    create_index_if_not_exists(app.image_rank_scores_collection , rank_scores_hash_index, "rank_scores_hash_index")


    rank_scores_index=[
    ("uuid", pymongo.ASCENDING),
    ("image_hash", pymongo.ASCENDING),
    ("rank_model_id", pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_rank_scores_collection , rank_scores_index, "rank_scores_index")

    # scores for image classfier
    app.image_classifier_scores_collection = app.mongodb_db["image_classifier_scores"]

    # active learning
    app.active_learning_policies_collection = app.mongodb_db["active-learning-policies"]
    app.active_learning_queue_pairs_collection = app.mongodb_db["queue-pairs"]

    # rank active learning
    app.rank_active_learning_pairs_collection = app.mongodb_db["rank_pairs"]

    app.irrelevant_images_collection = app.mongodb_db["irrelevant_images"]


    # ranking data points

    app.ranking_datapoints_collection = app.mongodb_db["ranking_datapoints"]

    pair_ranking_hash_index = [

        ('image_1_metadata.file_hash', pymongo.ASCENDING),
        ('image_2_metadata.file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.ranking_datapoints_collection, pair_ranking_hash_index, 'pair_ranking_hash_index')

    pair_ranking_hash_index_1 = [

        ('image_1_metadata.file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.ranking_datapoints_collection, pair_ranking_hash_index_1, 'pair_ranking_hash_index_1')

    pair_ranking_hash_index_2 = [

        ('image_2_metadata.file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.ranking_datapoints_collection, pair_ranking_hash_index_2, 'pair_ranking_hash_index_2')

    # rank active learning policy

    app.rank_active_learning_policies_collection = app.mongodb_db["rank_active_learning_policy"]


    # image hash
    app.image_hashes_collection = app.mongodb_db["image_hashes"]

    # Initialize next_image_global_id with the maximum value from the collection
    if app.image_hashes_collection is not None:
        max_image_doc = app.image_hashes_collection.find_one(sort=[("image_global_id", pymongo.DESCENDING)])
        app.max_image_global_id = max_image_doc["image_global_id"] if max_image_doc else 0
    else:
        app.max_image_global_id = 0

    # classifier scores classifier_id, tag_id, image_hash
    classifier_image_hash_index=[
    ('image_hash', pymongo.ASCENDING),
    ('classifier_id', pymongo.ASCENDING),
    ('tag_id', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection , classifier_image_hash_index, 'classifier_image_hash_index')

    # classifier scores classifier_id, tag_id
    classifier_image_classifier_index=[
    ('classifier_id', pymongo.ASCENDING),
    ('tag_id', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection , classifier_image_classifier_index, 'classifier_image_classifier_index')

    # index for classifier_id in image_classifier_scores
    classifier_image_classifier_id_index=[
    ('classifier_id', pymongo.ASCENDING),
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection , classifier_image_classifier_id_index, 'classifier_image_classifier_id_index')

    # index for tag_id in image_classifier_scores
    classifier_image_tag_id_index=[
    ('tag_id', pymongo.ASCENDING),
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection , classifier_image_tag_id_index, 'classifier_image_tag_id_index')

    # index for image_hash in image_classifier_scores
    classifier_image_hash_index=[
    ('image_hash', pymongo.ASCENDING),
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection , classifier_image_hash_index, 'classifier_image_image_hash_index')

    # index for score in image_classifier_scores
    classifier_image_score_index=[
    ('score', pymongo.ASCENDING),
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection , classifier_image_score_index, 'classifier_image_score_index')
    classifier_image_uuid_index=[
    ('uuid', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection , classifier_image_uuid_index, 'classifier_image_uuid_index')

    classifier_task_score_index = [
    ('classifier_id', pymongo.ASCENDING),
    ('task_type', pymongo.ASCENDING),
    ('score', pymongo.DESCENDING) 
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection, classifier_task_score_index, 'classifier_task_score_index')

    classifier_score_index = [
    ('classifier_id', pymongo.ASCENDING),
    ('score', pymongo.DESCENDING) 
    ]
    create_index_if_not_exists(app.image_classifier_scores_collection, classifier_score_index, 'classifier_score_index')

    # sigma scores
    app.image_sigma_scores_collection = app.mongodb_db["image-sigma-scores"]

    sigma_scores_index = [
        ('model_id', pymongo.ASCENDING),
        ('sigma-score', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_sigma_scores_collection, sigma_scores_index, 'sigma_scores_index')

    hash_index = [
        ('model_id', pymongo.ASCENDING),
        ('image_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_sigma_scores_collection, hash_index, 'sigma_score_hash_index')

    # residuals
    app.image_residuals_collection = app.mongodb_db["image-residuals"]

    residuals_index=[
    ('model_id', pymongo.ASCENDING), 
    ('residual', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_residuals_collection ,residuals_index, 'residuals_index')
    create_index_if_not_exists(app.image_residuals_collection ,hash_index, 'residual_hash_index')

    image_hash_index = [
        ('image_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_hashes_collection ,image_hash_index, 'image_hash_index')

    # percentiles
    app.image_percentiles_collection = app.mongodb_db["image-percentiles"]

    percentiles_index=[
    ('model_id', pymongo.ASCENDING), 
    ('percentile', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_percentiles_collection ,percentiles_index, 'percentiles_index')
    create_index_if_not_exists(app.image_percentiles_collection ,hash_index, 'percentile_hash_index')

    # residual percentiles
    app.image_residual_percentiles_collection = app.mongodb_db["image-residual-percentiles"]

    # image rank use count - the count the image is used in selection datapoint
    app.image_rank_use_count_collection = app.mongodb_db["image-rank-use-count"]

    app.image_pair_ranking_collection = app.mongodb_db["image_pair_ranking"]

    pair_rank_hash_index = [

        ('image_1_metadata.file_hash', pymongo.ASCENDING),
        ('image_2_metadata.file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_pair_ranking_collection, pair_rank_hash_index, 'pair_rank_hash_index')

    pair_rank_hash_index_1 = [

        ('image_1_metadata.file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_pair_ranking_collection, pair_rank_hash_index_1, 'pair_rank_hash_index_1')

    pair_rank_hash_index_2 = [

        ('image_2_metadata.file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.image_pair_ranking_collection, pair_rank_hash_index_2, 'pair_rank_hash_index_2')

    # create index for ingress_video_collection
    ingress_video_hash_index = [

        ('file_hash', pymongo.ASCENDING)
    ]
    create_index_if_not_exists(app.ingress_video_collection, ingress_video_hash_index,
    'ingress_video_hash_index')


    print("Connected to the MongoDB database!")

    # get minio client
    app.minio_client = get_minio_client(minio_ip_addr=config["MINIO_ADDRESS"],
                                        minio_access_key=config["MINIO_ACCESS_KEY"],
                                        minio_secret_key=config["MINIO_SECRET_KEY"])


@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()