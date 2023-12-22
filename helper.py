import json
import boto3
import os
import requests
import datetime

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

def get_tdx_result(app_id=os.environ['tdx_client_id'], 
                   app_key=os.environ['tdx_client_secret'], 
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


def get_road_data(category:str, output_folder:str)->dict:
    '''
    Download geo data into S3 by comparing the UpdateDate(data_dt) in the S3 

    Args:
        existing_version: check update date of existing files

        category: category of district data api, includes city, town and village.

        output_folder: specify sub-path under bucket online-data-lake-thirty-three
    
    Returns:
        A dictionary of downloaded file presents by update date, and length of downloaded file
        ex. 20210101
    '''
    url_checker = f"{os.environ[f'tdx_{category}_api']}/0?%24top=1&%24format=GEOJSON"

    jdata_checker = get_tdx_result(url=url_checker)
    update_dt = jdata_checker['features'][0]['properties']['model']['InfoDate'].split('T')[0]
    update_dt = update_dt.replace('-','')


    json_out = []
    for road_class in [0, 1, 3]: # road class
        url_formal = f"{os.environ[f'tdx_{category}_api']}/{road_class}?%24top=99999&%24format=GEOJSON"     
        jdata = get_tdx_result(url=url_formal)
        

        print(f"feature number of road class {road_class}: {len(jdata['features'])}")
        ingest_dt = datetime.datetime.now().strftime("%Y/%m/%d")
        ingest_time = datetime.datetime.now().strftime("%H:%M:%S")

        for feat in jdata['features']:
            obj = {}
            obj['type'] = feat['type']
            geometry_string = json.dumps(feat['geometry'])
            obj['geometry'] = geometry_string

            for k, v in feat['properties']['model'].items():
                obj[k] = v
            
            obj['ingestDate'] = ingest_dt
            obj['ingestTime'] = ingest_time
            
            json_out.append(obj)

    length = len(json_out)
    json_obj = '\n'.join(map(json.dumps, json_out))
    
    s3_client = boto3.client('s3')
    ingest_dt = datetime.datetime.now().strftime("%Y%m%d")
    s3_client.put_object(Body=json_obj,
                        Bucket=os.environ['s3_bucket'],
                        Key=f'{output_folder}/tdx_{category}_update_dt_{ingest_dt}.json')
    
    return {'ingest_dt':ingest_dt, f'num_of_{category}':length}