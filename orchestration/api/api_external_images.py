
from fastapi import APIRouter, Body, Request, HTTPException, Query, status
from typing import Optional
from utility.path import separate_bucket_and_file_path
from .api_utils import ApiResponseHandlerV1, StandardSuccessResponseV1, ErrorCode, WasPresentResponse, DeletedCount, validate_date_format, TagListForImages, TagCountResponse, TagListForImagesV1, insert_into_all_images, generate_uuid,  check_image_usage, remove_from_additional_collections, delete_files_from_minio
from .mongo_schemas import ExternalImageData, ImageHashRequest, ListExternalImageData, ListImageHashRequest, ExternalImageDataV1, ListExternalImageDataV1, ListDatasetV1, ListExternalImageDataWithSimilarityScore, Dataset, ListExternalImageDataV2, ListDataset
from orchestration.api.mongo_schema.tag_schemas import ExternalImageTag, ListExternalImageTag, ImageTag, ListImageTag
from typing import List
from datetime import datetime, timedelta
from pymongo import UpdateOne
from utility.minio import cmd
import random
import uuid
from .api_clip import http_clip_server_get_cosine_similarity_list
from .api_utils import get_next_external_dataset_seq_id, update_external_dataset_seq_id, get_minio_file_path, PrettyJSONResponse
import asyncio


router = APIRouter()

external_image = "external_image"


@router.post("/external-images/add-external-image", 
            description="Add an external image data with a randomly generated UUID by uuid4",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[ExternalImageDataV1],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500])) 
async def add_external_image_data(request: Request, image_data: ExternalImageData):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        '''
        objects = cmd.get_list_of_objects(request.app.minio_client, "datasets")
        dataset_path = f'{image_data.dataset}'
        
        if dataset_path not in objects:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=f"Dataset '{image_data.dataset}' does not exist.",
                http_status_code=422,
            )
        '''

        # Check if the dataset exists
        dataset_result = request.app.datasets_collection.find_one({"dataset_name": image_data.dataset, "bucket_id": 2})
        if not dataset_result:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND, 
                error_string=f"{image_data.dataset} dataset does not exist",
                http_status_code=422
            )
        
        dataset_id = dataset_result["dataset_id"]

        # Check if the image data already exists
        existed = request.app.external_images_collection.find_one({
            "image_hash": image_data.image_hash
        })
        if existed:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="Image data with this hash already exists.",
                http_status_code=422
            )

        # Prepare the image data for insertion
        image_data_dict = image_data.to_dict()
        image_data_dict['uuid'] = str(uuid.uuid4())
        image_data_dict['upload_date'] = str(datetime.now())

        # Set MinIO path using sequential ID
        next_seq_id = get_next_external_dataset_seq_id(request, bucket="external", dataset=image_data.dataset)
        image_data_dict['file_path'] = get_minio_file_path(next_seq_id,
                                                "external",            
                                                image_data.dataset, 
                                                image_data.image_format)

        # Insert into the all-images collection and retrieve the image_uuid
        all_images_collection = request.app.all_image_collection
        image_uuid = insert_into_all_images(image_data_dict, dataset_id, all_images_collection)

        # Add the image_uuid to the image_data_dict and insert into the external images collection
        if image_uuid:
            image_data_dict['image_uuid'] = image_uuid

        request.app.external_images_collection.insert_one(image_data_dict)

        image_data_dict.pop('_id', None)

        # Update the sequential ID
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, 
                                            update_external_dataset_seq_id, 
                                            request, "external", image_data.dataset, next_seq_id)
        
        return api_response_handler.create_success_response_v1(
            response_data=image_data_dict,
            http_status_code=200
        )
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )


@router.post("/external-images/add-external-image-list", 
            description="Add list of external image data",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[ListExternalImageData],  
            responses=ApiResponseHandlerV1.listErrors([422, 500]))
async def add_external_image_data_list(request: Request, image_data_list: List[ExternalImageData]):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    updated_image_data_list = []
    try:
        for image_data in image_data_list:
            # Check if the dataset exists
            dataset_result = request.app.datasets_collection.find_one({"dataset_name": image_data.dataset,"bucket_id": 2 })
            if not dataset_result:
                return api_response_handler.create_error_response_v1(
                    error_code=ErrorCode.ELEMENT_NOT_FOUND, 
                    error_string=f"Dataset {image_data.dataset} does not exist",
                    http_status_code=422
                )
            
            dataset_id = dataset_result["dataset_id"]

            # Check if the image data already exists
            existed = request.app.external_images_collection.find_one({
                "image_hash": image_data.image_hash
            })

            if existed:
                return api_response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string=f"Image data with hash {image_data.image_hash} already exists.",
                    http_status_code=422
                )
            else:
                # Prepare the image data for insertion
                image_data_dict = image_data.to_dict()
                image_data_dict['uuid'] = str(uuid.uuid4())
                image_data_dict['upload_date'] = str(datetime.now())

                # Set MinIO path using sequential ID
                next_seq_id = get_next_external_dataset_seq_id(request, bucket="external", dataset=image_data.dataset)
                image_data_dict['file_path'] = get_minio_file_path(next_seq_id, 
                                                        "external",
                                                        image_data.dataset, 
                                                        image_data.image_format)
                
                # Insert into the all-images collection and retrieve the image_uuid
                all_images_collection = request.app.all_image_collection
                image_uuid = insert_into_all_images(image_data_dict, dataset_id, all_images_collection)

                # Add the image_uuid to the image_data_dict and insert into the external images collection
                if image_uuid:
                    image_data_dict['image_uuid'] = image_uuid

                request.app.external_images_collection.insert_one(image_data_dict)

                # Update sequential ID
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, 
                                                    update_external_dataset_seq_id, 
                                                    request, "external", image_data.dataset, next_seq_id)

                # Add the updated image_data_dict to the updated_image_data_list
                updated_image_data_list.append(image_data_dict)

        # Remove the _id field from the response data
        response_data = [image_data_dict for image_data_dict in updated_image_data_list]
        for data in response_data:
            data.pop('_id', None)

        return api_response_handler.create_success_response_v1(
            response_data={"data": response_data},
            http_status_code=200
        )
    
    except Exception as e:
        print(e)
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )

    
@router.get("/external-images/get-external-image-list", 
            description="changed with /external-images/get-external-image-list-v1",
            tags=["deprecated3"],  
            response_model=StandardSuccessResponseV1[ListExternalImageData],  
            responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
async def get_external_image_data_list(request: Request, image_hash_list: List[str]):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        list_external_images = []
        for image_hash in image_hash_list:

            result = request.app.external_images_collection.find_one({
                "image_hash": image_hash
            }, {"_id": 0})
        
            if result is not None:
                list_external_images.append(result)

        return api_response_handler.create_success_response_v1(
            response_data={"data": list_external_images},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )
    

@router.post("/external-images/get-external-image-list-v1", 
             description="Get list of external image data by image hash list",
             tags=["external-images"],  
             response_model=StandardSuccessResponseV1[ListExternalImageData],  
             responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
async def get_external_image_data_list(request: Request, body: ListImageHashRequest):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        list_external_images = []
        for image_hash in body.image_hash_list:

            result = request.app.external_images_collection.find_one({
                "image_hash": image_hash
            }, {"_id": 0})
        
            if result is not None:
                list_external_images.append(result)

        return api_response_handler.create_success_response_v1(
            response_data={"data": list_external_images},
            http_status_code=200
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )
     
        
@router.get("/external-images/get-all-external-image-list", 
            description="Get all external image data. If 'dataset' parameter is set, it only returns images from that dataset, and if the 'size' parameter is set, a random sample of that size will be returned.",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[ListExternalImageDataV2],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_all_external_image_data_list(request: Request, dataset: str=None, size: int = None):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        query={}
        if dataset:
            query['dataset']= dataset

        aggregation_pipeline = [{"$match": query}]

        if size:
            aggregation_pipeline.append({"$sample": {"size": size}})

        image_data_list = list(request.app.external_images_collection.aggregate(aggregation_pipeline))

        for image_data in image_data_list:
            image_data.pop('_id', None)  # Remove the auto-generated field

        return api_response_handler.create_success_response_v1(
            response_data={"data": image_data_list},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )


@router.get("/external-images/get-all-external-image-list-v1", 
            description="Get all external image data. If 'dataset' parameter is set, it only returns images from those datasets, and if the 'size' parameter is set, a random sample of that size will be returned.",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[ListExternalImageDataV2],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_all_external_image_data_list_v1(
    request: Request, 
    dataset: Optional[List[str]] = Query(None, description="Dataset(s) to filter images"),
    size: int = Query(None, description="Number of random images to return")
):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        query = {}
        if dataset:
            query['dataset'] = {"$in": dataset}

        aggregation_pipeline = [{"$match": query}]

        if size:
            aggregation_pipeline.append({"$sample": {"size": size}})

        image_data_list = list(request.app.external_images_collection.aggregate(aggregation_pipeline))

        for image_data in image_data_list:
            image_data.pop('_id', None)  # Remove the auto-generated field

        return api_response_handler.create_success_response_v1(
            response_data={"data": image_data_list},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )

@router.get("/external-images/get-external-image-list-without-extracts", 
            description="Get only external images that don't have any images extracted from them. If 'dataset' parameter is set, it only returns images from that dataset, and if the 'size' parameter is set, a random sample of that size will be returned.",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[List[ExternalImageData]],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_external_image_list_without_extracts(request: Request, dataset: str = None, size: int = None):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    try:
        # Base query for external images
        query = {}
        if dataset:
            query['dataset'] = dataset

        # Aggregation pipeline to find images without corresponding extracts
        aggregation_pipeline = [
            {"$match": query},
            {
                "$lookup": {
                    "from": "extracts",  # Name of the extracts collection
                    "localField": "image_hash",     # The field in external_images_collection
                    "foreignField": "source_image_hash",  # The field in extracts_collection
                    "as": "extracts"                # The array to store the joined results
                }
            },
            {"$match": {"extracts": {"$size": 0}}},  # Filter to include only those without extracts
        ]

        # Apply sampling if the size parameter is provided
        if size:
            aggregation_pipeline.append({"$sample": {"size": size}})

        # Execute the aggregation
        image_data_list = list(request.app.external_images_collection.aggregate(aggregation_pipeline))

        # Remove '_id' field from results
        for image_data in image_data_list:
            image_data.pop('_id', None)

        return api_response_handler.create_success_response_v1(
            response_data={"data": image_data_list},
            http_status_code=200
        )

    except Exception as e:
        # Return a structured error response
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )




@router.delete("/external-images/delete-external-image", 
            description="Delete an external image data if it's not used in a selection datapoint or has a tag assigned",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[WasPresentResponse],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def delete_external_image_data(request: Request, image_hash: str):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        # Check if the image is used in ranking datapoints or has a tag assigned
        is_safe_to_delete, error_message = check_image_usage(request, image_hash)

        if not is_safe_to_delete:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string=error_message,
                http_status_code=422
            )

        # Fetch the image document to get the file path before deletion
        image_document = request.app.external_images_collection.find_one({"image_hash": image_hash})
        if not image_document:
            return api_response_handler.create_success_delete_response_v1(
                False, 
                http_status_code=200
            )

        # Extract the file path
        file_path = image_document.get("file_path")
        if not file_path:
            return api_response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="No valid file path found for the image.",
                http_status_code=422
            )

        # Remove the image data from additional collections
        remove_from_additional_collections(request, image_hash, bucket_id=2, image_source="external_image")

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
            delete_files_from_minio(request.app.minio_client, bucket_name, object_name)

        # Finally, delete the image from external_images_collection
        result = request.app.external_images_collection.delete_one({"image_hash": image_hash})

        if result.deleted_count == 0:
            return api_response_handler.create_success_delete_response_v1(
                False, 
                http_status_code=200
            )

        return api_response_handler.create_success_delete_response_v1(
            True, 
            http_status_code=200
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )






@router.delete("/external-images/delete-external-image-list", 
            description="Delete a list of external image data if they are not used in a selection datapoint or have a tag assigned",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[DeletedCount],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def delete_external_image_data_list(request: Request, image_hash_list: List[str]):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        deleted_count = 0
        for image_hash in image_hash_list:
            # Use the helper function to check image usage
            is_safe_to_delete, error_message = check_image_usage(request, image_hash)

            if not is_safe_to_delete:
                return api_response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string=error_message,
                    http_status_code=422
                )

            # Fetch the image document to get the file path before deletion
            image_document = request.app.external_images_collection.find_one({"image_hash": image_hash})
            if not image_document:
                continue  # Skip this image if it doesn't exist

            # Extract the file path
            file_path = image_document.get("file_path")
            if not file_path:
                return api_response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="No valid file path found for the image.",
                    http_status_code=422
                )

            # Remove the image data from additional collections
            remove_from_additional_collections(request, image_hash, bucket_id=2, image_source="external_image")

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
                delete_files_from_minio(request.app.minio_client, bucket_name, object_name)

            # Perform the deletion in MongoDB as the last step
            result = request.app.external_images_collection.delete_one({"image_hash": image_hash})

            if result.deleted_count > 0:
                deleted_count += 1
            
        return api_response_handler.create_success_response_v1(
            response_data={'deleted_count': deleted_count},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )



    

@router.put("/external-images/add-task-attributes-v1",
              description="Add or update the task attributes of an external image, No old attibute will be deleted, this function only adds and overwrites",
              tags=["external-images"],  
              response_model=StandardSuccessResponseV1[ExternalImageDataV1],  
              responses=ApiResponseHandlerV1.listErrors([404,422, 500]))    
@router.patch("/external-images/add-task-attributes",
              description="changed with /external-images/add-task-attributes-v1",
              tags=["deprecated3"],  
              response_model=StandardSuccessResponseV1[ListExternalImageData],  
              responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
async def add_task_attributes(request: Request, image_hash: str, data_dict: dict = Body(...)):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
     
    try: 
        image = request.app.external_images_collection.find_one(
            {"image_hash": image_hash},
        )

        if image:
            task_attributs_dict= image['task_attributes_dict']
            for key, value in data_dict.items():
                task_attributs_dict[key]= value

            image = request.app.external_images_collection.find_one_and_update(
                {"image_hash": image_hash},
                {"$set": {"task_attributes_dict": task_attributs_dict}},
                return_document=True
            )

            image.pop('_id', None)

            return api_response_handler.create_success_response_v1(
                response_data={"data": image},
                http_status_code=200  
            )       

        else:
            return api_response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS, 
                    error_string="There is no external image data with image hash: {}".format(image_hash), 
                    http_status_code=400)
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )



@router.post("/external-images/update-uuids",
             status_code=200,
             tags=["external-images"],  
             response_model=StandardSuccessResponseV1,  
             responses=ApiResponseHandlerV1.listErrors([404, 422]))
def update_external_images(request: Request):
    api_response_handler = ApiResponseHandlerV1(request)

    # Fetch all items from the external_images_collection
    items = list(request.app.external_images_collection.find())

    updated_count = 0
    for item in items:
        # Generate a new UUID
        new_uuid = str(uuid.uuid4())

        # Construct the updated document with uuid before image_hash
        updated_item = {"uuid": new_uuid, **item}
        updated_item.pop('_id', None)  # Remove the '_id' field to avoid duplication issues

        # Perform the update operation
        result = request.app.external_images_collection.update_one(
            {"_id": item["_id"]},
            {"$set": updated_item}
        )
        if result.modified_count > 0:
            updated_count += 1

    if updated_count == 0:
        raise HTTPException(status_code=404, detail="No items updated")

    # Return a standardized success response with the update result
    return api_response_handler.create_success_response_v1(
        response_data={'updated_count': updated_count},
        http_status_code=200
    ) 


@router.post("/external-images/add-tag-to-external-image",
             status_code=201,
             tags=["deprecated3"],  
             description="changed with /tags/add-tag-to-image-v2",
             response_model=StandardSuccessResponseV1[ImageTag], 
             responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
def add_tag_to_image(request: Request, tag_id: int, image_hash: str, tag_type: int, user_who_created: str):
    response_handler = ApiResponseHandlerV1(request)
    try:
        date_now = datetime.now().isoformat()
    
        existing_tag = request.app.tag_definitions_collection.find_one({"tag_id": tag_id})
        if not existing_tag:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND, 
                error_string="Tag does not exist!", 
                http_status_code=400
            )

        image = request.app.external_images_collection.find_one(
            {'image_hash': image_hash},
            {"file_path": 1, "image_uuid": 1}  # Include image_uuid in the projection
        )
        if not image:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND, 
                error_string="No image found with the given hash", 
                http_status_code=400
            )

        file_path = image.get("file_path", "")
        image_uuid = image.get("image_uuid", None)  # Get the image_uuid if available
        
        # Check if the tag is already associated with the image
        existing_image_tag = request.app.image_tags_collection.find_one({
            "tag_id": tag_id, 
            "image_hash": image_hash, 
            "image_source": external_image
        })
        if existing_image_tag:
            # Remove the '_id' field before returning the response
            existing_image_tag.pop('_id', None)
            # Return a success response indicating that the tag has already been added to the image
            return response_handler.create_success_response_v1(
                response_data=existing_image_tag, 
                http_status_code=200
            )

        # Add new tag to image
        image_tag_data = {
            "tag_id": tag_id,
            "file_path": file_path,  
            "image_hash": image_hash,
            "image_uuid": image_uuid,  # Include the image_uuid field
            "tag_type": tag_type,
            "image_source": external_image,
            "user_who_created": user_who_created,
            "tag_count": 1,  # Since this is a new tag for this image, set count to 1
            "creation_time": date_now
        }
        result = request.app.image_tags_collection.insert_one(image_tag_data)
        
        # Add the generated _id to the image_tag_data
        image_tag_data['_id'] = str(result.inserted_id)

        # Remove the '_id' field before returning the response
        image_tag_data.pop('_id', None)

        return response_handler.create_success_response_v1(
            response_data=image_tag_data, 
            http_status_code=200
        )

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e), 
            http_status_code=500
        )



@router.delete("/external-images/remove-tag-from-external-image",
               status_code=200,
               tags=["deprecated3"],
               description="changed with /tags/remove-tag-from-image-v1/{tag_id}",
               response_model=StandardSuccessResponseV1[WasPresentResponse],
               responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
def remove_tag_from_image(request: Request, tag_id: int, image_hash: str):
    response_handler = ApiResponseHandlerV1(request)
    try:
        # Remove the tag
        result = request.app.image_tags_collection.delete_one({
            "tag_id": tag_id, 
            "image_hash": image_hash, 
            "image_source": external_image
        })
        # Return a standard response with wasPresent set to true if there was a deletion
        return response_handler.create_success_delete_response_v1(result.deleted_count != 0)

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e), 
            http_status_code=500
        )



@router.get("/external-images/get-images-by-tag-id", 
            tags=["deprecated3"], 
            status_code=200,
            description="changed with /tags/get-images-by-tag-id-v1",
            response_model=StandardSuccessResponseV1[ListExternalImageTag], 
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
def get_external_images_by_tag_id(
    request: Request, 
    tag_id: int,
    start_date: str = None,
    end_date: str = None,
    order: str = Query("desc", description="Order in which the data should be returned. 'asc' for oldest first, 'desc' for newest first")
):
    response_handler = ApiResponseHandlerV1(request)
    try:
        # Validate start_date and end_date
        if start_date:
            validated_start_date = validate_date_format(start_date)
            if validated_start_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS, 
                    error_string="Invalid start_date format. Expected format: YYYY-MM-DDTHH:MM:SS", 
                    http_status_code=400,
                )
        if end_date:
            validated_end_date = validate_date_format(end_date)
            if validated_end_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS, 
                    error_string="Invalid end_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=400,
                )

        # Build the query
        query = {"tag_id": tag_id, "image_source": external_image}
        if start_date and end_date:
            query["creation_time"] = {"$gte": validated_start_date, "$lte": validated_end_date}
        elif start_date:
            query["creation_time"] = {"$gte": validated_start_date}
        elif end_date:
            query["creation_time"] = {"$lte": validated_end_date}

        # Decide the sort order
        sort_order = -1 if order == "desc" else 1

        # Execute the query
        image_tags_cursor = request.app.image_tags_collection.find(query).sort("creation_time", sort_order)

        # Process the results
        image_info_list = []
        for tag_data in image_tags_cursor:
            if "image_hash" in tag_data and "user_who_created" in tag_data and "file_path" in tag_data:
                image_tag = ImageTag(
                    tag_id=int(tag_data["tag_id"]),
                    file_path=tag_data["file_path"], 
                    image_hash=str(tag_data["image_hash"]),
                    tag_type=int(tag_data["tag_type"]),
                    user_who_created=tag_data["user_who_created"],
                    creation_time=tag_data.get("creation_time", None)
                )
                image_info_list.append(image_tag.model_dump())  # Convert to dictionary

        # Return the list of images in a standard success response
        return response_handler.create_success_response_v1(
            response_data={"images": image_info_list}, 
            http_status_code=200,
        )

    except Exception as e:
        # Log the exception details here, if necessary
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, error_string="Internal Server Error", http_status_code=500
        )



@router.get("/external-images/get-tag-list-for-image", 
            response_model=StandardSuccessResponseV1[TagListForImages], 
            description="changed with /tags/get-tag-list-for-image-v2",
            tags=["deprecated3"],
            status_code=200,
            responses=ApiResponseHandlerV1.listErrors([400, 404, 422, 500]))
def get_tag_list_for_external_image(request: Request, file_hash: str):
    response_handler = ApiResponseHandlerV1(request)
    try:
        # Fetch image tags based on image_hash
        image_tags_cursor = request.app.image_tags_collection.find({"image_hash": file_hash, "image_source": external_image})
        
        # Process the results
        tags_list = []
        for tag_data in image_tags_cursor:
            # Find the tag definition
            tag_definition = request.app.tag_definitions_collection.find_one({"tag_id": tag_data["tag_id"]})
            if tag_definition:
                # Find the tag category and determine if it's deprecated
                category = request.app.tag_categories_collection.find_one({"tag_category_id": tag_definition.get("tag_category_id")})
                deprecated_tag_category = category['deprecated'] if category else False
                
                # Create a dictionary representing TagDefinition with tag_type and deprecated_tag_category
                tag_definition_dict = {
                    "tag_id": tag_definition["tag_id"],
                    "tag_string": tag_definition["tag_string"],
                    "tag_type": tag_data.get("tag_type"),
                    "tag_category_id": tag_definition.get("tag_category_id"),
                    "tag_description": tag_definition["tag_description"],
                    "tag_vector_index": tag_definition.get("tag_vector_index", -1),
                    "deprecated": tag_definition.get("deprecated", False),
                    "deprecated_tag_category": deprecated_tag_category,
                    "user_who_created": tag_definition["user_who_created"],
                    "creation_time": tag_definition.get("creation_time", None)
                }

                tags_list.append(tag_definition_dict)
        
        # Return the list of tags including 'deprecated_tag_category'
        return response_handler.create_success_response_v1(
            response_data={"tags": tags_list},
            http_status_code=200,
        )
    except Exception as e:
        # Optional: Log the exception details here
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )



@router.get("/external-images/get-images-count-by-tag-id",
            status_code=200,
            tags=["deprecated3"],
            description="changed with tags/get-images-count-by-tag-id-v1",
            response_model=StandardSuccessResponseV1[TagCountResponse],
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
def get_images_count_by_tag_id(request: Request, tag_id: int):
    response_handler = ApiResponseHandlerV1(request)
    try :
        # Build the query to include the image_source as "external_image"
        query = {"tag_id": tag_id, "image_source": external_image}
        count = request.app.image_tags_collection.count_documents(query)

        # Return the count even if it is zero
        return response_handler.create_success_response_v1(
            response_data={"tag_id": tag_id, "count": count},
            http_status_code=200
        )

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string="Internal server error",
            http_status_code=500
        )


@router.get("/external-images/list-images-v1",
            status_code=200,
            tags=["deprecated3"],
            response_model=StandardSuccessResponseV1[List[ExternalImageData]],
            description="changed with /external-images/list-images-v2 ",
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def list_external_images_v1(
    request: Request,
    dataset: Optional[str] = Query(None, description="Dataset to filter the results by"),
    limit: int = Query(20, description="Limit on the number of results returned"),
    offset: int = Query(0, description="Offset for the results to be returned"),
    start_date: Optional[str] = Query(None, description="Start date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    order: str = Query("desc", description="Order in which the data should be returned. 'asc' for oldest first, 'desc' for newest first"),
    time_interval: Optional[int] = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        # Calculate the time threshold based on the current time and the specified interval
        if time_interval is not None:
            current_time = datetime.utcnow()
            if time_unit == "minutes":
                threshold_time = current_time - timedelta(minutes=time_interval)
            elif time_unit == "hours":
                threshold_time = current_time - timedelta(hours=time_interval)
            else:
                raise HTTPException(status_code=400, detail="Invalid time unit. Use 'minutes' or 'hours'.")

            # Convert threshold_time to a string in ISO format
            threshold_time_str = threshold_time.isoformat(timespec='milliseconds')
        else:
            threshold_time_str = None

        # Validate start_date and end_date
        if start_date:
            validated_start_date = validate_date_format(start_date)
            if validated_start_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid start_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=400
                )
        if end_date:
            validated_end_date = validate_date_format(end_date)
            if validated_end_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid end_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=400
                )

        # Build the query
        query = {}
        if start_date and end_date:
            query["upload_date"] = {"$gte": validated_start_date, "$lte": validated_end_date}
        elif start_date:
            query["upload_date"] = {"$gte": validated_start_date}
        elif end_date:
            query["upload_date"] = {"$lte": validated_end_date}
        elif threshold_time_str:
            query["upload_date"] = {"$gte": threshold_time_str}

        # Add dataset filter if specified
        if dataset:
            query["dataset"] = dataset

        # Decide the sort order
        sort_order = -1 if order == "desc" else 1

        # Query the external_images_collection using the constructed query
        images_cursor = request.app.external_images_collection.find(query).sort("upload_date", sort_order).skip(offset).limit(limit)

        # Collect the metadata for the images that match the query
        images_metadata = []
        for image in images_cursor:
            image.pop('_id', None)  # Remove the auto-generated field
            images_metadata.append(image)

        return response_handler.create_success_response_v1(
            response_data=images_metadata,
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )

@router.get("/external-images/list-images-v2",
            status_code=200,
            tags=["external-images"],
            response_model=StandardSuccessResponseV1[ListExternalImageDataV1],
            description="List external images with optional filtering and pagination",
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def list_external_images_v1(
    request: Request,
    dataset: Optional[str] = Query(None, description="Dataset to filter the results by"),
    limit: int = Query(20, description="Limit on the number of results returned"),
    offset: int = Query(0, description="Offset for the results to be returned"),
    start_date: Optional[str] = Query(None, description="Start date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    order: str = Query("desc", description="Order in which the data should be returned. 'asc' for oldest first, 'desc' for newest first"),
    time_interval: Optional[int] = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        # Calculate the time threshold based on the current time and the specified interval
        if time_interval is not None:
            current_time = datetime.utcnow()
            if time_unit == "minutes":
                threshold_time = current_time - timedelta(minutes=time_interval)
            elif time_unit == "hours":
                threshold_time = current_time - timedelta(hours=time_interval)
            else:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid time unit. Use 'minutes' or 'hours'.",
                    http_status_code=400)

            # Convert threshold_time to a string in ISO format
            threshold_time_str = threshold_time.isoformat(timespec='milliseconds')
        else:
            threshold_time_str = None

        # Validate start_date and end_date
        if start_date:
            validated_start_date = validate_date_format(start_date)
            if validated_start_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid start_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=400
                )
        if end_date:
            validated_end_date = validate_date_format(end_date)
            if validated_end_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid end_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=400
                )

        # Build the query
        query = {}
        if start_date and end_date:
            query["upload_date"] = {"$gte": validated_start_date, "$lte": validated_end_date}
        elif start_date:
            query["upload_date"] = {"$gte": validated_start_date}
        elif end_date:
            query["upload_date"] = {"$lte": validated_end_date}
        elif threshold_time_str:
            query["upload_date"] = {"$gte": threshold_time_str}

        # Add dataset filter if specified
        if dataset:
            query["dataset"] = dataset

        # Decide the sort order
        sort_order = -1 if order == "desc" else 1

        # Query the external_images_collection using the constructed query
        images_cursor = request.app.external_images_collection.find(query).sort("upload_date", sort_order).skip(offset).limit(limit)

        # Collect the metadata for the images that match the query
        images_metadata = []
        for image in images_cursor:
            image.pop('_id', None)  # Remove the auto-generated field
            images_metadata.append(image)

        return response_handler.create_success_response_v1(
            response_data={"images": images_metadata},
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )
    
@router.get("/external-images/list-images-v3",
            status_code=200,
            tags=["external-images"],
            response_model=StandardSuccessResponseV1[ListExternalImageDataV1],
            description="List external images with optional filtering and pagination",
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def list_external_images_v3(
    request: Request,
    dataset: Optional[List[str]] = Query(None, description="Dataset(s) to filter the results by"),
    limit: int = Query(20, description="Limit on the number of results returned"),
    offset: int = Query(0, description="Offset for the results to be returned"),
    start_date: Optional[str] = Query(None, description="Start date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    order: str = Query("desc", description="Order in which the data should be returned. 'asc' for oldest first, 'desc' for newest first"),
    time_interval: Optional[int] = Query(None, description="Time interval in minutes or hours"),
    time_unit: str = Query("minutes", description="Time unit, either 'minutes' or 'hours'")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        # Calculate the time threshold based on the current time and the specified interval
        if time_interval is not None:
            current_time = datetime.utcnow()
            if time_unit == "minutes":
                threshold_time = current_time - timedelta(minutes=time_interval)
            elif time_unit == "hours":
                threshold_time = current_time - timedelta(hours=time_interval)
            else:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid time unit. Use 'minutes' or 'hours'.",
                    http_status_code=400)

            # Convert threshold_time to a string in ISO format
            threshold_time_str = threshold_time.isoformat(timespec='milliseconds')
        else:
            threshold_time_str = None

        # Validate start_date and end_date
        if start_date:
            validated_start_date = validate_date_format(start_date)
            if validated_start_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid start_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=400
                )
        if end_date:
            validated_end_date = validate_date_format(end_date)
            if validated_end_date is None:
                return response_handler.create_error_response_v1(
                    error_code=ErrorCode.INVALID_PARAMS,
                    error_string="Invalid end_date format. Expected format: YYYY-MM-DDTHH:MM:SS",
                    http_status_code=400
                )

        # Build the query
        query = {}
        if start_date and end_date:
            query["upload_date"] = {"$gte": validated_start_date, "$lte": validated_end_date}
        elif start_date:
            query["upload_date"] = {"$gte": validated_start_date}
        elif end_date:
            query["upload_date"] = {"$lte": validated_end_date}
        elif threshold_time_str:
            query["upload_date"] = {"$gte": threshold_time_str}

        # Add dataset filter if specified
        if dataset:
            query["dataset"] = {"$in": dataset}

        # Decide the sort order
        sort_order = -1 if order == "desc" else 1

        # Query the external_images_collection using the constructed query
        images_cursor = request.app.external_images_collection.find(query).sort("upload_date", sort_order).skip(offset).limit(limit)

        # Collect the metadata for the images that match the query
        images_metadata = []
        for image in images_cursor:
            image.pop('_id', None)  # Remove the auto-generated field
            images_metadata.append(image)

        return response_handler.create_success_response_v1(
            response_data={"images": images_metadata},
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )

        
@router.get("/external-images/get-unique-datasets", 
            description="Get all unique dataset names in the external images collection.",
            tags=["external-images"],  
            response_model=StandardSuccessResponseV1[ListDatasetV1],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_unique_datasets(request: Request):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Use aggregation pipeline to get unique dataset names
        aggregation_pipeline = [
            {"$group": {"_id": "$dataset"}},
            {"$project": {"_id": 0, "dataset": "$_id"}}
        ]

        datasets_cursor = request.app.external_images_collection.aggregate(aggregation_pipeline)
        datasets = [doc["dataset"] for doc in datasets_cursor]

        return api_response_handler.create_success_response_v1(
            response_data={"datasets": datasets},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )        


@router.get("/external-images/get-image-details-by-hash/{image_hash}", 
            response_model=StandardSuccessResponseV1[ExternalImageData],
            status_code=200,
            tags=["external-images"],
            description="Retrieves the details of an external image by image hash. It returns the full data by default, but it can return only some properties by listing them using the 'fields' param",
            responses=ApiResponseHandlerV1.listErrors([404,422, 500]))
async def get_image_details_by_hash(request: Request, image_hash: str, fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    # Create a projection for the MongoDB query
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    # Find the image by hash
    image_data = request.app.external_images_collection.find_one({"image_hash": image_hash}, projection)
    if image_data:
        return response_handler.create_success_response_v1(response_data=image_data, http_status_code=200)
    else:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.ELEMENT_NOT_FOUND, 
            error_string="Image not found",
            http_status_code=404
        )

@router.get("/external-images/get-image-details-by-hashes", 
            response_model=StandardSuccessResponseV1[ListExternalImageDataV1],
            status_code=200,
            tags=["external-images"],
            description="Retrieves the details of external images by image hashes. It returns the full data by default, but it can return only some properties by listing them using the 'fields' param",
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_image_details_by_hashes(request: Request, image_hashes: List[str] = Query(...), fields: List[str] = Query(None)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    
    # Create a projection for the MongoDB query
    projection = {field: 1 for field in fields} if fields else {}
    projection['_id'] = 0  # Exclude the _id field

    # Use the $in operator to find all matching documents in one query
    image_data_list = list(request.app.external_images_collection.find({"image_hash": {"$in": image_hashes}}, projection))

    # Return the data found in the success response
    return response_handler.create_success_response_v1(response_data={"images":image_data_list}, http_status_code=200)     


@router.get("/external-images/get-random-images-with-clip-search",
            tags=["external-images"],
            description="Gets as many random external images as set in the size param, scores each image with CLIP according to the value of the 'phrase' param and then returns the list sorted by the similarity score. NOTE: before using this endpoint, make sure to register the phrase using the '/clip/add-phrase' endpoint.",
            response_model=StandardSuccessResponseV1[ListExternalImageDataWithSimilarityScore],
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def get_random_external_image_similarity(
    request: Request,
    phrase: str = Query(..., description="Phrase to compare similarity with"),
    dataset: Optional[str] = Query(None, description="Dataset to filter images"),
    similarity_threshold: float = Query(0, description="Minimum similarity threshold"),
    start_date: Optional[str] = Query(None, description="Start date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    size: int = Query(..., description="Number of random images to return")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        query = {}

        if dataset:
            query['dataset'] = dataset

        if start_date and end_date:
            query['upload_date'] = {'$gte': start_date, '$lte': end_date}
        elif start_date:
            query['upload_date'] = {'$gte': start_date}
        elif end_date:
            query['upload_date'] = {'$lte': end_date}

        aggregation_pipeline = [{"$match": query}]
        if size:
            aggregation_pipeline.append({"$sample": {"size": size}})

        images = list(request.app.external_images_collection.aggregate(aggregation_pipeline))

        image_path_list = []
        for image in images:
            image.pop('_id', None)  # Remove the auto-generated field
            bucket_name, file_path= separate_bucket_and_file_path(image['file_path'])
            image_path_list.append(file_path)

        similarity_score_list = http_clip_server_get_cosine_similarity_list("external", image_path_list, phrase)
        print(similarity_score_list)

        if similarity_score_list is None or 'similarity_list' not in similarity_score_list:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.OTHER_ERROR,
                error_string=str(e),
                http_status_code=500
            )

        similarity_score_list = similarity_score_list['similarity_list']

        if len(images) != len(similarity_score_list):
            return response_handler.create_success_response_v1(response_data={"images": []}, http_status_code=200)

        filtered_images = []
        for i in range(len(images)):
            image_similarity_score = similarity_score_list[i]
            image = images[i]

            if image_similarity_score >= similarity_threshold:
                image["similarity_score"] = image_similarity_score
                filtered_images.append(image)

        return response_handler.create_success_response_v1(response_data={"images": filtered_images}, http_status_code=200)

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )
    
@router.get("/external-images/get-random-images-with-clip-search-v1",
            tags=["external-images"],
            description="Gets as many random external images as set in the size param, scores each image with CLIP according to the value of the 'phrase' param and then returns the list sorted by the similarity score. NOTE: before using this endpoint, make sure to register the phrase using the '/clip/add-phrase' endpoint.",
            response_model=StandardSuccessResponseV1[ListExternalImageDataWithSimilarityScore],
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def get_random_external_image_similarity_v1(
    request: Request,
    phrase: str = Query(..., description="Phrase to compare similarity with"),
    dataset: Optional[List[str]] = Query(None, description="Dataset(s) to filter images"),
    similarity_threshold: float = Query(0, description="Minimum similarity threshold"),
    start_date: Optional[str] = Query(None, description="Start date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date for filtering results (YYYY-MM-DDTHH:MM:SS)"),
    size: int = Query(..., description="Number of random images to return")
):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    try:
        query = {}

        if dataset:
            query['dataset'] = {"$in": dataset}

        if start_date and end_date:
            query['upload_date'] = {'$gte': start_date, '$lte': end_date}
        elif start_date:
            query['upload_date'] = {'$gte': start_date}
        elif end_date:
            query['upload_date'] = {'$lte': end_date}

        aggregation_pipeline = [{"$match": query}]
        if size:
            aggregation_pipeline.append({"$sample": {"size": size}})

        images = list(request.app.external_images_collection.aggregate(aggregation_pipeline))

        image_path_list = []
        for image in images:
            image.pop('_id', None)  # Remove the auto-generated field
            bucket_name, file_path = separate_bucket_and_file_path(image['file_path'])
            image_path_list.append(file_path)

        similarity_score_list = http_clip_server_get_cosine_similarity_list("external", image_path_list, phrase)
        print(similarity_score_list)

        if similarity_score_list is None or 'similarity_list' not in similarity_score_list:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.OTHER_ERROR,
                error_string="Error retrieving similarity scores",
                http_status_code=500
            )

        similarity_score_list = similarity_score_list['similarity_list']

        if len(images) != len(similarity_score_list):
            return response_handler.create_success_response_v1(response_data={"images": []}, http_status_code=200)

        filtered_images = []
        for i in range(len(images)):
            image_similarity_score = similarity_score_list[i]
            image = images[i]

            if image_similarity_score >= similarity_threshold:
                image["similarity_score"] = image_similarity_score
                filtered_images.append(image)

        return response_handler.create_success_response_v1(response_data={"images": filtered_images}, http_status_code=200)

    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500
        )



@router.post("/external-images/add-new-dataset",
            description="changed with /datasets/add-new-dataset",
            tags=["deprecated_datasets"],
            response_model=StandardSuccessResponseV1[Dataset],  
            responses=ApiResponseHandlerV1.listErrors([400, 422]))
async def add_new_dataset(request: Request, dataset: Dataset):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    if request.app.external_datasets_collection.find_one({"dataset_name": dataset.dataset_name}):
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string='Dataset already exists',
            http_status_code=400
        )

    # Find the current highest dataset_id
    highest_dataset = request.app.external_datasets_collection.find_one(
        sort=[("dataset_id", -1)]
    )
    next_dataset_id = (highest_dataset["dataset_id"] + 1) if highest_dataset else 0

    # Add the dataset_id to the dataset
    dataset_dict = dataset.to_dict()
    dataset_dict["dataset_id"] = next_dataset_id

    # Insert the new dataset with dataset_id
    request.app.external_datasets_collection.insert_one(dataset_dict)

    return response_handler.create_success_response_v1(
        response_data={"dataset_name": dataset.dataset_name, "dataset_id": next_dataset_id}, 
        http_status_code=200
    )



@router.get("/external-images/list-datasets",
            description="changed with /datasets/list-datasets-v1",
            tags=["deprecated_datasets"],
            response_model=StandardSuccessResponseV1[ListDataset],  
            responses=ApiResponseHandlerV1.listErrors([422]))
async def list_datasets(request: Request):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    datasets = list(request.app.external_datasets_collection.find({}))
    for dataset in datasets:
        dataset.pop('_id', None)

    return response_handler.create_success_response_v1(
                response_data={'datasets': datasets}, 
                http_status_code=200
            )               


@router.delete("/external-images/remove-dataset",
               description="changed with datasets/remove-dataset-v1",
               tags=["deprecated_datasets"],
               response_model=StandardSuccessResponseV1[WasPresentResponse],  
               responses=ApiResponseHandlerV1.listErrors([422]))
async def remove_dataset(request: Request, dataset: str = Query(...)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)

    # Check if the dataset contains any objects (images)
    image_count = request.app.external_datasets_collection.count_documents({"dataset_name": dataset, "images": {"$exists": True, "$ne": []}})
    if image_count > 0:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.INVALID_PARAMS,
            error_string=f"Dataset '{dataset}' contains images and cannot be deleted.",
            http_status_code=422
        )

    # Attempt to delete the dataset
    dataset_result = request.app.external_datasets_collection.delete_one({"dataset_name": dataset})
    # Return a standard response with wasPresent set to true if there was a deletion
    return response_handler.create_success_delete_response_v1(dataset_result.deleted_count != 0)


@router.post("/external-images/get-tag-list-for-multiple-external-images", 
             response_model=StandardSuccessResponseV1[TagListForImagesV1], 
             description="/tags/get-tag-list-for-multiple-images-v1",
             tags=["deprecated3"],
             status_code=200,
             responses=ApiResponseHandlerV1.listErrors([400, 404, 422, 500]))
async def get_tag_list_for_multiple_images(request: Request, file_hashes: List[str]):
    response_handler = ApiResponseHandlerV1(request)
    try:
        all_tags_list = []
        
        for file_hash in file_hashes:
            # Fetch image tags based on image_hash
            image_tags_cursor = request.app.image_tags_collection.find({"image_hash": file_hash, "image_source": external_image})
            
            # Process the results
            tags_list = []
            for tag_data in image_tags_cursor:
                # Find the tag definition
                tag_definition = request.app.tag_definitions_collection.find_one({"tag_id": tag_data["tag_id"]})
                if tag_definition:
                    # Find the tag category and determine if it's deprecated
                    category = request.app.tag_categories_collection.find_one({"tag_category_id": tag_definition.get("tag_category_id")})
                    deprecated_tag_category = category['deprecated'] if category else False
                    
                    # Create a dictionary representing TagDefinition with tag_type and deprecated_tag_category
                    tag_definition_dict = {
                        "tag_id": tag_definition["tag_id"],
                        "tag_string": tag_definition["tag_string"],
                        "tag_type": tag_data.get("tag_type"),
                        "tag_category_id": tag_definition.get("tag_category_id"),
                        "tag_description": tag_definition["tag_description"],
                        "tag_vector_index": tag_definition.get("tag_vector_index", -1),
                        "deprecated": tag_definition.get("deprecated", False),
                        "deprecated_tag_category": deprecated_tag_category,
                        "user_who_created": tag_definition["user_who_created"],
                        "creation_time": tag_definition.get("creation_time", None)
                    }

                    tags_list.append(tag_definition_dict)

            all_tags_list.append({"file_hash": file_hash, "tags": tags_list})
        
        # Return the list of tag lists for each image
        return response_handler.create_success_response_v1(
            response_data={"images": all_tags_list},
            http_status_code=200,
        )
    except Exception as e:
        # Optional: Log the exception details here
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=str(e),
            http_status_code=500,
        )  

@router.get("/external-images/get_random_image_by_classifier_score", response_class=PrettyJSONResponse)
def get_random_image_date_range(
    request: Request,
    rank_id: int = None,
    start_date: str = None,
    end_date: str = None,
    min_score: float = 0.6,
    size: int = None,
):
    query = {}

    if start_date and end_date:
        query['upload_date'] = {'$gte': start_date, '$lte': end_date}
    elif start_date:
        query['upload_date'] = {'$gte': start_date}
    elif end_date:
        query['upload_date'] = {'$lte': end_date}

    # If rank_id is provided, adjust the query to consider classifier scores
    if rank_id is not None:
        # Get rank data
        rank = request.app.rank_collection.find_one({'rank_model_id': rank_id})
        if rank is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Rank model with this id doesn't exist")

        # Get the relevance classifier model id
        classifier_id = rank["classifier_id"]
        if classifier_id is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="This Rank has no relevance classifier model assigned to it")

        classifier_query = {'classifier_id': classifier_id, 'image_source': 'external_image'}
        if min_score is not None:
            classifier_query['score'] = {'$gte': min_score}
            
        # Fetch image hashes from classifier_scores collection that match the criteria
        classifier_scores = list(request.app.image_classifier_scores_collection.find(classifier_query))
        if not classifier_scores:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="The relevance classifier model has no scores.")
        limited_image_hashes = random.sample([score['image_hash'] for score in classifier_scores], min(size, len(classifier_scores)))

        # Break down the image hashes into smaller batches
        BATCH_SIZE = 1000  # Adjust batch size as needed
        all_documents = []

        for i in range(0, len(limited_image_hashes), BATCH_SIZE):
            batch_image_hashes = limited_image_hashes[i:i+BATCH_SIZE]
            batch_query = query.copy()
            batch_query['image_hash'] = {'$in': batch_image_hashes}

            aggregation_pipeline = [{"$match": batch_query}]
            if size:
                aggregation_pipeline.append({"$sample": {"size": size}})
            
            batch_documents = request.app.external_images_collection.aggregate(aggregation_pipeline)
            batch_docs_list = list(batch_documents)
            all_documents.extend(batch_docs_list)
            if len(all_documents) >= size:
                break

        documents = all_documents[:size]
    else:
        aggregation_pipeline = [{"$match": query}]
        if size:
            aggregation_pipeline.append({"$sample": {"size": size}})

        documents = request.app.external_images_collection.aggregate(aggregation_pipeline)
        documents = list(documents)

    for document in documents:
        document.pop('_id', None)  # Remove the auto-generated field

    return documents

@router.get("/external-images/get-external-image-count", 
            description="Returns the count of external images where image_hash exists",
            tags=["external-images"],
            response_model=StandardSuccessResponseV1[int],  
            responses=ApiResponseHandlerV1.listErrors([404, 422, 500]))
async def get_external_image_count(request: Request):
    api_response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Count documents where image_hash exists in the external_images collection
        count = await request.app.external_images_collection.count_documents({
            "image_hash": {"$exists": True}
        })

        return api_response_handler.create_success_response_v1(
            response_data={"count": count},
            http_status_code=200  
        )
    
    except Exception as e:
        return api_response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR, 
            error_string=str(e),
            http_status_code=500
        )
