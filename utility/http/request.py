import requests
import json
from fastapi import Request
from pymongo import MongoClient

# Connect to the MongoDB database
client = MongoClient('mongodb://192.168.3.1:32017/')
db = client['orchestration-job-db']

# Access the specific collection
comleted_jobs_collection = db["completed-jobs"]

# SERVER_ADDRESS = 'http://123.176.98.90:8764'
SERVER_ADDRESS = 'http://192.168.3.1:8111'


def http_get_list_completed_jobs():
    url = SERVER_ADDRESS + "/queue/image-generation/list-completed"
    response = None

    try:
        response = requests.get(url)

        if response.status_code == 200:
            job_json = response.json()
            return job_json

    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()
            
    return None

# Get request to get sequential id of a dataset
def http_get_sequential_id(dataset_name: str, limit: int):
    url = SERVER_ADDRESS + "/dataset/sequential-id/{0}?limit={1}".format(dataset_name, limit)
    response = None

    try:
        response = requests.get(url)
        if response.status_code == 200:
            job_json = response.json()
            return job_json
        
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None

# Get request to get the self training sequential id of a dataset
def http_get_self_training_sequential_id(dataset_name: str):
    url = SERVER_ADDRESS + "/dataset/self-training-sequential-id/{0}".format(dataset_name)
    response = None

    try:
        response = requests.get(url)
        if response.status_code == 200:
            job_json = response.json()
            return job_json["sequential_id"]
        
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


def http_add_model(model_card):
    url = SERVER_ADDRESS + "/models/add"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.post(url, data=model_card, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}")
        print("model_id=", response.content)
        return response.content
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None

def http_add_classifier_model(model_card):
    url = SERVER_ADDRESS + "/classifier/register-tag-classifier"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.post(url, data=model_card, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}")
        print("classifier data=", response.content)
        return response.content
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None

def http_get_model_id(model_hash):
    url = SERVER_ADDRESS + "/models/get-id?model_hash={}".format(model_hash)
    response = None

    try:
        response = requests.get(url)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}")

        return int(response.content)
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None

def http_get_classifier_model_list():
    url = SERVER_ADDRESS + "/classifier/list-classifiers"
    response = None
    try:
        response = requests.get(url)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}")
            return []
        return response.json()["response"]
    except Exception as e:
        print('request exception ', e)
        
    finally:
        if response:
            response.close()

    return None


def http_add_score(score_data):
    url = SERVER_ADDRESS + "/score/set-image-rank-score"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.post(url, json=score_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None

def http_add_classifier_score(score_data):
    url = SERVER_ADDRESS + "/classifier-score/set-image-classifier-score"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None
    try:
        response = requests.post(url, json=score_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


def http_add_sigma_score(sigma_score_data):
    url = SERVER_ADDRESS + "/sigma-score/set-image-rank-sigma-score"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.post(url, json=sigma_score_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


def http_add_residual(residual_data):
    url = SERVER_ADDRESS + "/job/add-selected-residual"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.put(url, json=residual_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


def http_add_percentile(percentile_data):
    url = SERVER_ADDRESS + "/percentile/set-image-rank-percentile"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.post(url, json=percentile_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


def http_add_residual_percentile(residual_percentile_data):
    url = SERVER_ADDRESS + "/residual-percentile/set-image-rank-residual-percentile"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.post(url, json=residual_percentile_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


# Get list of all dataset names
def http_get_dataset_names():
    url = SERVER_ADDRESS + "/dataset/list"
    response = None

    try:
        response = requests.get(url)

        if response.status_code == 200:
            data_json = response.json()
            return data_json

    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


# Get completed job
def http_get_completed_job_by_image_hash(image_hash):
    url = SERVER_ADDRESS + "/job/get-completed-job-by-hash?image_hash={}".format(image_hash)
    response = None

    try:
        response = requests.get(url)

        if response.status_code == 200:
            data_json = response.json()
            return data_json

    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


def http_add_score_attributes(model_type,
                              img_hash,
                              image_clip_score,
                              image_clip_percentile,
                              image_clip_sigma_score,
                              text_embedding_score,
                              text_embedding_percentile,
                              text_embedding_sigma_score,
                              image_clip_h_score,
                              image_clip_h_percentile,
                              image_clip_h_sigma_score,
                              delta_sigma_score):
    data = {
        "image_hash": img_hash,
        "model_type": model_type,
        "image_clip_score": image_clip_score,
        "image_clip_percentile": image_clip_percentile,
        "image_clip_sigma_score": image_clip_sigma_score,
        "text_embedding_score": text_embedding_score,
        "text_embedding_percentile": text_embedding_percentile,
        "text_embedding_sigma_score": text_embedding_sigma_score,
        "image_clip_h_score":image_clip_h_score,
        "image_clip_h_percentile":image_clip_h_percentile,
        "image_clip_h_sigma_score":image_clip_h_sigma_score,
        "delta_sigma_score": delta_sigma_score
    }

    url = SERVER_ADDRESS + "/job/add-attributes"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.put(url, json=data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None


def http_add_score_attributes_v1(model_type, img_hash, image_clip_score, image_clip_percentile,
                              image_clip_sigma_score, text_embedding_score, text_embedding_percentile,
                              text_embedding_sigma_score, image_clip_h_score, image_clip_h_percentile,
                              image_clip_h_sigma_score, delta_sigma_score):
    
    query = {"task_output_file_dict.output_file_hash": img_hash}
    job = comleted_jobs_collection.find_one(query)
    
    if not job:
        print(f"Failed to fetch job data for image hash {img_hash}. Job not found.")
        return
        
    task_type = job.get("task_type")

    if "kandinsky" in task_type:
        url = SERVER_ADDRESS + "/job/add-attributes-witout-embeddings"
    else:
        url = SERVER_ADDRESS + "/job/add-attributes"

    # Prepare the data payload excluding text embedding attributes if calling the without-embeddings endpoint
    if url.endswith("witout-embeddings"):
        data = {
            "image_hash": img_hash,
            "model_type": model_type,
            "image_clip_score": image_clip_score,
            "image_clip_percentile": image_clip_percentile,
            "image_clip_sigma_score": image_clip_sigma_score,
            "image_clip_h_score": image_clip_h_score,
            "image_clip_h_percentile": image_clip_h_percentile,
            "image_clip_h_sigma_score": image_clip_h_sigma_score,
            "delta_sigma_score": delta_sigma_score
        }
    else:
        data = {
            "image_hash": img_hash,
            "model_type": model_type,
            "image_clip_score": image_clip_score,
            "image_clip_percentile": image_clip_percentile,
            "image_clip_sigma_score": image_clip_sigma_score,
            "text_embedding_score": text_embedding_score,
            "text_embedding_percentile": text_embedding_percentile,
            "text_embedding_sigma_score": text_embedding_sigma_score,
            "image_clip_h_score": image_clip_h_score,
            "image_clip_h_percentile": image_clip_h_percentile,
            "image_clip_h_sigma_score": image_clip_h_sigma_score,
            "delta_sigma_score": delta_sigma_score
        }

    headers = {"Content-Type": "application/json"}
    response = None

    try:
        response = requests.put(url, json=data, headers=headers)

        if response.status_code != 200:
            print(f"Request failed with status code: {response.status_code}: {str(response.content)}")
        else:
            print("Attributes updated successfully.")
    except Exception as e:
        print('Request exception', e)
    finally:
        if response:
            response.close()

    return None

# update delta scores for ranking data
def http_update_ranking_delta_scores():

    url = SERVER_ADDRESS + "/calculate-delta-scores"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data
    response = None

    try:
        response = requests.post(url, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}: {str(response.content)}")
    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None

# Get completed job
def http_get_completed_job_by_uuid(job_uuid):
    url = SERVER_ADDRESS + "/job/get-job/{}".format(job_uuid)
    response = None

    try:
        response = requests.get(url)

        if response.status_code == 200:
            data_json = response.json()
            return data_json

    except Exception as e:
        print('request exception ', e)

    finally:
        if response:
            response.close()

    return None

# Get completed jobs
def http_get_completed_jobs_by_uuids(job_uuids):
    count = 0
    batch_uuids = ""
    for uuid in job_uuids:
        if count!=0:
            batch_uuids += "&uuids="

        batch_uuids += "{}".format(uuid)
        count += 1

    url = SERVER_ADDRESS + "/job/get-jobs?uuids={}".format(batch_uuids)
    response = None

    try:
        response = requests.get(url)

        if response.status_code == 200:
            data_json = response.json()
            return data_json

    except Exception as e:
        print('request exception ', e)
    finally:
        if response:
            response.close()

    return None

def http_get_tag_list():
    url = SERVER_ADDRESS + "/tags/list-tag-definitions"
    try:
        response = requests.get(url)

        if response.status_code == 200:
            data_json = response.json()
            return data_json["response"]["tags"]

    except Exception as e:
        print('request exception ', e)


def http_get_tagged_images(tag_id):
    url = SERVER_ADDRESS + "/tags/{}/images".format(tag_id)
    try:
        response = requests.get(url)

        if response.status_code == 200:
            data_json = response.json()
            return data_json["response"]["images"]

    except Exception as e:
        print('request exception ', e)


def http_get_random_image_list(dataset, size):
    url = SERVER_ADDRESS + "/image/get_random_image_list?dataset={}&size={}".format(dataset, size)
    try:
        response = requests.get(url)

        if response.status_code == 200:
            data_json = response.json()
            return data_json["images"]

    except Exception as e:
        print('request exception ', e)

