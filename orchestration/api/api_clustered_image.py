from typing import Optional
from fastapi import APIRouter, Query, Request

from orchestration.api.controller.cluster_model_repo import find_cluster_model, find_cluster_model_by_model_id
from orchestration.api.controller.clustered_images_repo import (
    add_clustered_image,
    delete_all_images, 
    find_clustered_image_by_image_uuid,
    find_clustered_images_by_pipeline,
    update_clustered_image, 
    delete_clustered_image_by_image_uuid
)

from orchestration.api.mongo_schema.clustering_schemas import ClusterModel, ClusteredImageMetadata

from orchestration.api.api_utils import ApiResponseHandlerV1, StandardSuccessResponseV1, ErrorCode

router = APIRouter()

@router.post("/clustered-images/add-clustered-image",
            description="Add a clustered image",
            tags=["Clustered Image"],
            response_model=StandardSuccessResponseV1[ClusteredImageMetadata],
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def add_clustered_image_endpoint(request: Request, clustered_image: ClusteredImageMetadata):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        existed = find_clustered_image_by_image_uuid(
            request=request, 
            image_uuid=clustered_image.image_uuid
        )
        
        if existed:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=f"Clustered images with image uuid {clustered_image.image_uuid} already exists",
                http_status_code=422
            )
        
        result = add_clustered_image(
            request=request, 
            clustered_image=clustered_image
        )
        
        return api_response_handler.create_success_response_v1(
            response_data=result,
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )

@router.get("/clustered-images/get-clustered-images-by-cluster-id",
            description="Get list of clustered image by cluster id",
            tags=["Clustered Image"],
            response_model=StandardSuccessResponseV1[ClusteredImageMetadata],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def get_clustered_image_by_image_uuid_endpoint(request: Request, 
        model_name: str,
        cluster_id: int,
        cluster_level: int,
        limit: Optional[int] = Query(20, description="Limit for pagination"),
        offset: Optional[int] = Query(0, description="Offset for pagination"),
        sort_order: Optional[str] = Query(None, description="Sort order: 'asc' for ascending, 'desc' for descending")):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        cluster_model = find_cluster_model(request, model_name=model_name, cluster_level=cluster_level)
        if cluster_model is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=f"Cluster model with model name {model_name} and cluster level {cluster_level} is not found",
                http_status_code=404
            )
        aggregate_pipeline = [
            {"$match": {"cluster_id": cluster_id, "model_id": cluster_model["model_id"]}},
            {"$skip": offset},
            {"$limit": limit},
            {"$project": {"_id": 0}}
        ]
        if sort_order:
            aggregate_pipeline.append({
                "$sort": {"distance": 1 if sort_order == 'asc' else -1}
            })
        
        result = find_clustered_images_by_pipeline(
            request=request,
            aggregate_pipeline=aggregate_pipeline,
        )

        if result is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"No clustered image found with cluster_id {cluster_id}",
                http_status_code=404
            )

        return api_response_handler.create_success_response_v1(
            response_data=result,
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )

@router.get("/clustered-images/get-clustered-image-by-image-uuid",
            description="Get a clustered image by image uuid",
            tags=["Clustered Image"],
            response_model=StandardSuccessResponseV1[ClusteredImageMetadata],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def get_clustered_image_by_image_uuid_endpoint(request: Request, image_uuid: int):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        result = find_clustered_image_by_image_uuid(
            request=request, 
            image_uuid=image_uuid
        )
        
        if result is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Clustered image with image_uuid {image_uuid} does not exist",
                http_status_code=404
            )
        
        return api_response_handler.create_success_response_v1(
            response_data=result,
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )
    
@router.put("/clustered-images/update-clustered-image",
            description="Update clustered image",
            tags=["Clustered Image"],
            response_model=StandardSuccessResponseV1[ClusteredImageMetadata],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def update_clustered_image_endpoint(request: Request, clustered_image: ClusteredImageMetadata):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
    
        existed = find_clustered_image_by_image_uuid(
            request=request, 
            image_uuid=clustered_image.image_uuid
        )
        
        if existed is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Clustered image with image_uuid {clustered_image.image_uuid} does not exist",
                http_status_code=404
            )
        
        result = update_clustered_image(request=request, clustered_image=clustered_image)
        
        return api_response_handler.create_success_response_v1(
            response_data=result,
            http_status_code=200
        )
        
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )

    
@router.delete("/clustered-images/delete-clustered-image-by-image-uuid",
            description="Delete clustered image by image uuid",
            tags=["Clustered Image"],
            response_model=StandardSuccessResponseV1[ClusteredImageMetadata],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def delete_clustered_image_by_image_uuid_endpoint(request: Request, image_uuid: int):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        deleted_count = delete_clustered_image_by_image_uuid(request=request, image_uuid=image_uuid)
        
        if deleted_count == 0:
            return api_response_handler.create_success_delete_response_v1(
                wasPresent=False,
                http_status_code=200
            )
            
        return api_response_handler.create_success_delete_response_v1(
                wasPresent=True, 
                http_status_code=200
            )
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )
        
@router.delete("/clustered-images/delete-all",
            description="Delete clustered image by image uuid",
            tags=["Clustered Image"],
            response_model=StandardSuccessResponseV1[ClusteredImageMetadata],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def delete_all_clustered_images_endpoint(request: Request, image_uuid: int):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        deleted_count = delete_all_images(request=request)
        
        if deleted_count == 0:
            return api_response_handler.create_success_delete_response_v1(
                wasPresent=False,
                http_status_code=200
            )
            
        return api_response_handler.create_success_delete_response_v1(
                wasPresent=True, 
                http_status_code=200
            )
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )