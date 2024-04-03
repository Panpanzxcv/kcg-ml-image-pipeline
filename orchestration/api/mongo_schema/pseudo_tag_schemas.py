from pydantic import BaseModel, Field, constr, validator
from typing import List, Union, Optional
import re
from datetime import datetime

class ImagePseudoTag(BaseModel):
        uuid: Union[str, None]
        classifier_id: int
        image_hash: str
        tag_id: int
        score: float
        creation_time: Union[str, None] = None

        def to_dict(self):
            return {
                "uuid": self.uuid,
                "classifier_id": self.classifier_id,
                "image_hash": self.image_hash,
                "tag_id": self.tag_id,
                "score": self.score,
                "creation_time" : self.creation_time
            }


NonEmptyString = constr(strict=True, min_length=1)

class ListImagePseudoTag(BaseModel):
    images: List [ImagePseudoTag]

class ImagePseudoTagRequest(BaseModel):
        uuid: Union[str, None]
        classifier_id: int
        score: float
        
        def to_dict(self):
            return {
                "uuid": self.uuid,
                "classifier_id": self.classifier_id,
                "score": self.score
            }