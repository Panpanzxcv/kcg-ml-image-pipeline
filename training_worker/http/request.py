import requests

SERVER_ADRESS = 'http://192.168.3.1:8111'


# Get request to get an available job
def http_get_job(worker_type: str = None):
    url = SERVER_ADRESS + "/training/get-job"
    if worker_type is not None:
        url = url + "?task_type={}".format(worker_type)

    try:
        response = requests.get(url)
    except Exception as e:
        print('request exception ', e)

    if response.status_code == 200:
        job_json = response.json()
        return job_json

    return None


# Get request to get sequential id of a dataset
def http_get_sequential_id(dataset_name: str, limit: int):
    url = SERVER_ADRESS + "/dataset/sequential-id/{0}?limit={1}".format(dataset_name, limit)

    try:
        response = requests.get(url)
    except Exception as e:
        print('request exception ', e)

    if response.status_code == 200:
        job_json = response.json()
        return job_json

    return None


def http_add_job(job):
    url = SERVER_ADRESS + "/training/add"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data

    try:
        response = requests.post(url, json=job, headers=headers)
    except Exception as e:
        print('request exception ', e)

    if response.status_code != 201 and response.status_code != 200:
        print(f"POST request failed with status code: {response.status_code}")


def http_update_job_completed(job):
    url = SERVER_ADRESS + "/training/update-completed"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data

    try:
        response = requests.put(url, json=job, headers=headers)
    except Exception as e:
        print('request exception ', e)

    if response.status_code != 200:
        print(f"request failed with status code: {response.status_code}")


def http_update_job_failed(job):
    url = SERVER_ADRESS + "/training/update-failed"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data

    try:
        response = requests.put(url, json=job, headers=headers)
    except Exception as e:
        print('request exception ', e)

    if response.status_code != 200:
        print(f"request failed with status code: {response.status_code}")


def http_add_model(model_card):
    url = SERVER_ADRESS + "/models/add"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data

    try:
        response = requests.post(url, data=model_card, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}")
        print("model_id=", response.content)
        return response.content
    except Exception as e:
        print('request exception ', e)

    return None


def http_add_score(score_data):
    url = SERVER_ADRESS + "/score/set-image-rank-score"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data

    try:
        response = requests.post(url, json=score_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}")
    except Exception as e:
        print('request exception ', e)

    return None


def http_add_residual(residual_data):
    url = SERVER_ADRESS + "/residual/set-image-rank-residual"
    headers = {"Content-type": "application/json"}  # Setting content type header to indicate sending JSON data

    try:
        response = requests.post(url, json=residual_data, headers=headers)

        if response.status_code != 200:
            print(f"request failed with status code: {response.status_code}")
    except Exception as e:
        print('request exception ', e)

    return None
