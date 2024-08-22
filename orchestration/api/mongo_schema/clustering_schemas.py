from datetime import date
from pydantic import BaseModel, Field
from typing import Optional

class ClusteredImageMetadata(BaseModel):
    model_id: int = Field(..., description="Model ID")
    image_uuid: int = Field(..., description="Image UUID (int64)")
    level: int = Field(..., description="Level")
    cluster_id: int = Field(..., description="Cluster ID(int16)")
    distance_to_cluster: Optional[float] = Field(None, description="Distance from center of cluster")

    def to_dict(self):
        return {
            "model_id": self.model_id,
            "image_uuid": self.image_uuid,
            "level": self.level,
            "cluster_id": self.cluster_id,
            "distance_to_cluster": self.distance_to_cluster
        }

class ClusterModel(BaseModel):
    model_id: int = Field(..., description="Model ID (uint64)")
    model_name: str = Field(..., description="Name of the model")
    model_path: str = Field(..., description="Path in MinIO")
    cluster_level: int = Field(..., description="Number of clusters")
    creation_date: date = Field(date.today(), description="Creation date of the model")
    
    def to_dict(self):
        return {
            "model_id": self.model_id,
            "model_name": self.model_name,
            "model_path": self.model_path,
            "cluster_level": self.cluster_level,
            "creation_date": self.creation_date
        }