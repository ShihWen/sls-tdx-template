import json
import boto3
import os
import requests
import datetime
import time
import pandas as pd
import awswrangler as wr

# TDX helper class
class Auth():

    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        content_type = 'application/x-www-form-urlencoded'
        grant_type = 'client_credentials'

        return{
            'content-type' : content_type,
            'grant_type' : grant_type,
            'client_id' : self.app_id,
            'client_secret' : self.app_key
        }


class Data():

    def __init__(self, app_id, app_key, auth_response):
        self.app_id = app_id
        self.app_key = app_key
        self.auth_response = auth_response

    def get_data_header(self):
        auth_JSON = json.loads(self.auth_response.text)
        #print(f"method auth_JSON in class Data, the auth_JSON is {auth_JSON}")
        access_token = auth_JSON.get('access_token')
        #print(f"method get_data_header in class Data, the access_token is {access_token}")

        return{
            'authorization': 'Bearer '+access_token
        }

def get_tdx_result(app_id=os.environ['tdx_client_id_1'], 
                   app_key=os.environ['tdx_client_secret_1'], 
                   auth_url=os.environ['tdx_auth_url'],
                   url=None):
  a = Auth(app_id, app_key)
  auth_response = requests.post(auth_url, a.get_auth_header())
  d = Data(app_id, app_key, auth_response)
  data_response = requests.get(url, headers=d.get_data_header()) 
  
  return json.loads(data_response.text)


def get_existing_file(data_type:str)->list:
    '''
    Args: 
        --
    
    Returns:
        A list of existing station and exit file
    '''
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('online-data-lake-thirty-three')

    bucket_object = bucket.objects.filter(Prefix=f'{data_type}/tdx')
    if bucket_object != []:
        objects = [obj.key.split('/')[1] for obj in bucket_object]
        return objects#'_'.join(objects)
    return []


