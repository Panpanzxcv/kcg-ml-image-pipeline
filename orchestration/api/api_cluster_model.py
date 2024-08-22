from fastapi import APIRouter, Request

from orchestration.api.mongo_schema.clustering_schemas import ClusterModel
from orchestration.api.controller.cluster_model_repo import (
    add_cluster_model,
    find_cluster_model,
    find_cluster_model_by_model_id,
    update_cluster_model,
    delete_cluster_model_by_model_id,
)
from orchestration.api.api_utils import ApiResponseHandlerV1, StandardSuccessResponseV1, ErrorCode
from orchestration.api.utils.datetime_utils import get_current_datetime_str

router = APIRouter()

@router.post("/cluster-model/add-cluster-model",
            description="Add a cluster model",
            tags=["Cluster Model"],
            response_model=StandardSuccessResponseV1[ClusterModel],
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def add_cluster_model_endpoint(request: Request, cluster_model: ClusterModel):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        existed = find_cluster_model(request=request, 
                                    model_name=cluster_model.model_name, 
                                    cluster_level=cluster_model.cluster_level)
        
        if existed:
            cluster_model.model_id = existed["model_id"]
            result = update_cluster_model(request=request, model=cluster_model)
        else:
            result = add_cluster_model(request=request, cluster_model=cluster_model)
        
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

@router.get("/cluster-model/get-cluster-model-by-model-id",
            description="Get cluster model by model id",
            tags=["Cluster Model"],
            response_model=StandardSuccessResponseV1[ClusterModel],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def get_cluster_model_by_model_id_endpoint(request: Request, model_id: int):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        result = find_cluster_model_by_model_id(request=request, model_id=model_id)
        
        if result is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Cluster model with model id {model_id} does not exist",
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
    
@router.put("/cluster-model/update-cluster-model",
            description="Update cluster model",
            tags=["Cluster Model"],
            response_model=StandardSuccessResponseV1[ClusterModel],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def update_cluster_model_endpoint(request: Request, model: ClusterModel):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        existed = find_cluster_model_by_model_id(request=request, model_id=model.model_id)
        
        if existed is None:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"Cluster Model with model id {model.model_id} does not exist",
                http_status_code=404
            )
        
        result = update_cluster_model(request=request, model=model)
        
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

    
@router.delete("/cluster-model/delete-cluster-model-by-model-id",
            description="Delete cluster model by model id",
            tags=["Cluster Model"],
            response_model=StandardSuccessResponseV1[ClusterModel],
            responses=ApiResponseHandlerV1.listErrors([404, 500]))
async def delete_cluster_model_by_model_id_endpoint(request: Request, model_id: int):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        deleted_count = delete_cluster_model_by_model_id(request=request, model_id=model_id)
        
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