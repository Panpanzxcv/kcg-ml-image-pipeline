from fastapi import Request, HTTPException, APIRouter, Response, Query
router = APIRouter()




@router.get("/list-phrase")
def get_rate(request: Request,
             limit: int = 20,
             offset: int = 0):
    clip_server = request.app.clip_server

    phrase_list = clip_server.get_phrase_list(offset, limit)

    return phrase_list

@router.get("/clip-vector")
def get_clip_vector(request: Request,
             phrase : str):
    clip_server = request.app.clip_server

    clip_vector = clip_server.get_clip_vector(phrase)

    return clip_vector

@router.get("/clip-vector-from-image-path")
def clip_vector_from_image_path(request: Request,
             image_path : str):
    clip_server = request.app.clip_server

    clip_vector = clip_server.get_image_clip_from_minio(image_path, 'datasets')

    return clip_vector

@router.post("/add-phrase")
def add_job(request: Request, phrase : str):
    clip_server = request.app.clip_server

    clip_server.add_phrase(phrase)

    return True
