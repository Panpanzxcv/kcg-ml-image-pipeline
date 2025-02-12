import msgpack
import numpy as np


class PromptEmbedding:
    job_uuid: str
    creation_time: str
    dataset: str
    file_path: str
    file_hash: str
    positive_prompt: str
    negative_prior_prompt: str
    negative_decoder_prompt: str
    positive_embedding: list
    negative_prior_embedding: list
    negative_decoder_embedding: list
    positive_attention_mask: list = None
    negative_prior_mask: list = None
    negative_decoder_mask: list = None

    def __init__(self, job_uuid, creation_time, dataset, file_path, file_hash, positive_prompt, 
                 negative_prior_prompt, negative_decoder_prompt, positive_embedding, 
                 negative_prior_embedding, negative_decoder_embedding, positive_attention_mask=None, 
                 negative_prior_mask=None, negative_decoder_mask=None):
        self.job_uuid = job_uuid
        self.creation_time = creation_time
        self.dataset = dataset
        self.file_path = file_path
        self.file_hash = file_hash
        self.positive_prompt = positive_prompt
        self.negative_prior_prompt = negative_prior_prompt
        self.negative_decoder_prompt = negative_decoder_prompt
        self.positive_embedding = positive_embedding
        self.negative_prior_embedding = negative_prior_embedding
        self.negative_decoder_embedding = negative_decoder_embedding
        self.positive_attention_mask = positive_attention_mask
        self.negative_prior_mask = negative_prior_mask
        self.negative_decoder_mask = negative_decoder_mask


    def serialize(self):
        # Convert object to a dictionary
        return {
            "job_uuid": self.job_uuid,
            "creation_time": self.creation_time,
            "dataset": self.dataset,
            "file_path": self.file_path,
            "file_hash": self.file_hash,
            "positive_prompt": self.positive_prompt,
            "negative_prior_prompt": self.negative_prior_prompt,
            "negative_decoder_prompt": self.negative_decoder_prompt,
            "positive_embedding": self.positive_embedding,
            "negative_prior_embedding": self.negative_prior_embedding,
            "negative_decoder_embedding": self.negative_decoder_embedding,
            "positive_attention_mask": self.positive_attention_mask,
            "negative_prior_mask": self.negative_prior_mask,
            "negative_decoder_mask": self.negative_decoder_mask
        }

    @classmethod
    def deserialize(cls, data):
        # Convert dictionary back to object
        positive_attention_mask = None
        if "positive_attention_mask" in data:
            positive_attention_mask = data["positive_attention_mask"]

        negative_prior_mask = None
        if "negative_prior_mask" in data:
            negative_prior_mask = data["negative_prior_mask"]
        
        negative_decoder_mask = None
        if "negative_decoder_mask" in data:
            negative_decoder_mask = data["negative_decoder_mask"]

        return cls(data["job_uuid"],
                   data["creation_time"],
                   data["dataset"],
                   data["file_path"],
                   data["file_hash"],
                   data["positive_prompt"],
                   data["negative_prior_prompt"],
                   data["negative_decoder_prompt"],
                   data["positive_embedding"],
                   data["negative_prior_embedding"],
                   data["negative_decoder_embedding"],
                   positive_attention_mask,
                   negative_prior_mask,
                   negative_decoder_mask)

    def get_msgpack_string(self):
        serialized = self.serialize()
        return msgpack.packb(serialized, default=encode_ndarray, use_bin_type=True, use_single_float=True)

    @classmethod
    def from_msgpack_string(cls, msgpack_string):
        data = msgpack.unpackb(msgpack_string.encode('latin1'), object_hook=decode_ndarray, raw=False)
        return cls.deserialize(data)

    @classmethod
    def from_msgpack_bytes(cls, msgpack_string):
        data = msgpack.unpackb(msgpack_string, object_hook=decode_ndarray, raw=False)
        return cls.deserialize(data)


def encode_ndarray(obj):
    if isinstance(obj, np.ndarray):
        return {'__ndarray__': obj.tolist()}
    return obj

def decode_ndarray(packed_obj):
    if '__ndarray__' in packed_obj:
        return np.array(packed_obj['__ndarray__'])
    return packed_obj