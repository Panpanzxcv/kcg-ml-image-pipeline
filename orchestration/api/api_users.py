from orchestration.api.deps import is_authenticated,is_admin
from orchestration.api.mongo_schema.user_schemas import User, LoginRequest
from .api_utils import PrettyJSONResponse, ApiResponseHandlerV1, StandardSuccessResponseV1, ErrorCode, generate_uuid
from fastapi import status, HTTPException, Depends, APIRouter, Request, Query
from datetime import datetime
from fastapi.security import OAuth2PasswordRequestForm
from orchestration.api.utils.uuid64 import Uuid64
from orchestration.api.jwt import (
    get_hashed_password,
    create_access_token,
    create_refresh_token,
    verify_password
)
from uuid import uuid4

router = APIRouter()

@router.post('/users/create', summary="Create new user")
def create_user(request: Request, data: User):
    # querying database to check if user already exists
    user = request.app.users_collection.find_one({"username": data.username})
    if user is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this name already exists"
        )

    uuid = Uuid64.create_new_uuid()

    user = {
        'username': data.username,
        'password': get_hashed_password(data.password),
        'role': data.role,
        'is_active': True,
        'uuid': uuid.to_mongo_value()
    }
    request.app.users_collection.insert_one(user)
    # remove the auto generated field
    user.pop('_id', None)
    return user


@router.post('/users/login', summary="Create access and refresh tokens for user")
def login(request: Request, form_data: OAuth2PasswordRequestForm = Depends()):
    user=request.app.users_collection.find_one({"username": form_data.username})
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect username or password"
        )

    hashed_pass = user['password']
    if not verify_password(form_data.password, hashed_pass):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect username or password"
        )
    
    return {
        "access_token": create_access_token(user['username']),
        "refresh_token": create_refresh_token(user['username']),
    }

@router.post('/users/login-v1', summary="Create access and refresh tokens for user")
def login(request: Request, login_request: LoginRequest):
    user = request.app.users_collection.find_one({"username": login_request.username})
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect username or password"
        )

    hashed_pass = user['password']
    if not verify_password(login_request.password, hashed_pass):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect username or password"
        )
    
    return {
        "access_token": create_access_token(user['username']),
        "refresh_token": create_refresh_token(user['username']),
    }



# Deactivate a user by username
@router.put("/users/deactivate")
def deactivate_user(request:Request, username: str= Query(...), user: User = Depends(is_admin)):
    # Define the update to apply to the document
    update = {
        "$set": {
            "is_active": False
        }
    }
    request.app.users_collection.update_one({"username":username}, update)
    return {'message':f"user {username} deactivated successfully"}

# Reactivate a user by username
@router.put("/users/reactivate")
def reactivate_user(request:Request, username: str= Query(...), user: User = Depends(is_admin)):
    # Define the update to apply to the document
    update = {
        "$set": {
            "is_active": True
        }
    }
    request.app.users_collection.update_one({"username":username}, update)
    return {'message':f"user {username} reactivated successfully"}

# Delete a user by username
@router.delete("/users/delete")
def delete_user(request:Request, username: str= Query(...), user: User = Depends(is_admin)):
    request.app.users_collection.delete_one({"username":username})
    return {'message':f"user {username} deleted successfully"}

#list of users
@router.get('/users/list')
def list_users(request:Request, user: User = Depends(is_admin)):
    users = list(request.app.users_collection.find({}))

    for user in users:
        user.pop('_id', None)

    return users

@router.get('/users/list-v1')
def list_users(request:Request):
    users = list(request.app.users_collection.find({}))

    for user in users:
        user.pop('_id', None)

    return users

@router.post('/users/create-v1', 
             summary="Create new user",
             tags=["users"],
             response_model=StandardSuccessResponseV1[str],
             responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def create_user_v1(request: Request, data: User):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Querying database to check if user already exists
        uuid = Uuid64.create_new_uuid()
        user = request.app.users_collection.find_one({"username": data.username})
        if user is not None:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="User with this name already exists",
                http_status_code=400
            )

        user = {
            'username': data.username,
            'password': get_hashed_password(data.password),
            'role': data.role,
            'is_active': True,
            'uuid': uuid.to_mongo_value()
        }
        request.app.users_collection.insert_one(user)
        # Remove the auto generated field
        user.pop('_id', None)
        
        return response_handler.create_success_response_v1(
            response_data=user,
            http_status_code=201
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Failed to create user: {str(e)}",
            http_status_code=500
        )



@router.post('/users/login-v2', 
             summary="Create access and refresh tokens for user",
             tags=["users"],
             response_model=StandardSuccessResponseV1[str],
             responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def login_v1(request: Request, login_request: LoginRequest):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        user = request.app.users_collection.find_one({"username": login_request.username})
        if user is None:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string="Incorrect username or password",
                http_status_code=400
            )

        hashed_pass = user['password']
        if not verify_password(login_request.password, hashed_pass):
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.INVALID_PARAMS,
                error_string="Incorrect username or password",
                http_status_code=400
            )

        tokens = {
            "access_token": create_access_token(user['username']),
            "refresh_token": create_refresh_token(user['username']),
        }
        return response_handler.create_success_response_v1(
            response_data=tokens,
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Failed to login: {str(e)}",
            http_status_code=500
        )

@router.put("/users/deactivate-v1", 
            summary="Deactivate a user by username",
            tags=["users"],
            response_model=StandardSuccessResponseV1[str],
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def deactivate_user(request: Request, username: str = Query(...), user: User = Depends(is_admin)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Define the update to apply to the document
        update = {
            "$set": {
                "is_active": False
            }
        }
        result = request.app.users_collection.update_one({"username": username}, update)
        
        if result.matched_count == 0:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"User {username} not found",
                http_status_code=400
            )

        return response_handler.create_success_response_v1(
            response_data={"message": f"user {username} deactivated successfully"},
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Failed to deactivate user: {str(e)}",
            http_status_code=500
        )

@router.put("/users/reactivate-v1", 
            summary="Reactivate a user by username",
            tags=["users"],
            response_model=StandardSuccessResponseV1[str],
            responses=ApiResponseHandlerV1.listErrors([400, 422, 500]))
async def reactivate_user(request: Request, username: str = Query(...), user: User = Depends(is_admin)):
    response_handler = await ApiResponseHandlerV1.createInstance(request)
    try:
        # Define the update to apply to the document
        update = {
            "$set": {
                "is_active": True
            }
        }
        result = request.app.users_collection.update_one({"username": username}, update)
        
        if result.matched_count == 0:
            return response_handler.create_error_response_v1(
                error_code=ErrorCode.ELEMENT_NOT_FOUND,
                error_string=f"User {username} not found",
                http_status_code=400
            )

        return response_handler.create_success_response_v1(
            response_data={"message": f"user {username} reactivated successfully"},
            http_status_code=200
        )
    except Exception as e:
        return response_handler.create_error_response_v1(
            error_code=ErrorCode.OTHER_ERROR,
            error_string=f"Failed to reactivate user: {str(e)}",
            http_status_code=500
        )
