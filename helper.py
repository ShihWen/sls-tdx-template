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


def get_road_id():
    '''
    function briefing

    Args:
        [arg_name]: explaination
    
    Returns:
    '''
    city_list = ["Taipei", "Taichung", "Keelung", "Tainan", "Kaohsiung",
                "NewTaipei", "YilanCounty", "Taoyuan", "Chiayi", "HsinchuCounty",
                "MiaoliCounty", "NantouCounty", "ChanghuaCounty", "Hsinchu", "YunlinCounty",
                "ChiayiCounty", "PingtungCounty", "HualienCounty", "TaitungCounty",
                "KinmenCounty", "PenghuCounty", "LienchiangCounty"]

    df_list = []

    for city in city_list:
        print(f"processing {city}...")
        request_url = f"{os.environ['api_road_id']}/{city}"\
                        "?%24top=99999"\
                        "&%24format=JSON"
        jdata = get_tdx_result(url=request_url)
        df_road_id = pd.DataFrame(jdata)
        df_list.append(df_road_id)
        time.sleep(1.5)

    df_road_all = pd.concat(df_list)
    ingest_dt = datetime.datetime.now().strftime("%Y%m%d")

    wr.s3.to_parquet(
        df=df_road_all,
        path=f"s3://{os.environ['s3_bucket']}/line-string",
        dataset=True,
        filename_prefix=f'road_id_{ingest_dt}_'
    )

    raw_dict = dict(df_road_all['CityName'].value_counts())
    return_dict = {}

    for k, v in raw_dict.items():
            return_dict[k] = str(v)

    return {'road_id_cnt_by_city':return_dict}

def load_road_id():
    '''
    function briefing

    Args:
        [arg_name]: explaination
    
    Returns:
    '''
    read_dt = datetime.datetime.now().strftime("%Y%m%d")
    return wr.s3.read_parquet(path=f's3://online-data-lake-thirty-three/line-string/road_id_{read_dt}_*', 
                              columns=['RoadID','CityID','CityName'])


def get_link_id(city_list:list):
    '''
    function briefing

    Args:
        [arg_name]: explaination
    
    Returns:
    '''
    df_road = load_road_id()
    road_list = list(df_road[df_road['CityName'].isin(city_list)]['RoadID'])
    link_id_df = []
    counter = 0
    got_cnt = 0
    no_cnt = 0
    error_cnt = 0

    apis = [{"tdx_client_id":os.environ['tdx_client_id_1'],"tdx_client_secret":os.environ['tdx_client_secret_1']},
            {"tdx_client_id":os.environ['tdx_client_id_2'],"tdx_client_secret":os.environ['tdx_client_secret_2']},
            {"tdx_client_id":os.environ['tdx_client_id_3'],"tdx_client_secret":os.environ['tdx_client_secret_3']}]

    while counter < len(road_list):
        try:
            #sys.stdout.write(f'/rprocessing road_id number {counter}')
            request_url = "https://tdx.transportdata.tw/api/basic/v2/Road/Link/RoadID"\
                        f"/{road_list[counter]}"\
                        "?%24select=LinkID%2CRoadID%2CRoadName%2CRoadClass"\
                        "&%24format=JSON"
            api_idx = 0 #counter%3
            jdata = get_tdx_result(app_id=apis[api_idx]['tdx_client_id'],
                                   app_key=apis[api_idx]['tdx_client_secret'],
                                   url=request_url)
            counter += 1
            print(f'\ndone processing road_id number {counter}, using api {api_idx}')
            
            if jdata == []:
                no_cnt += 1
                print('...no link_id')
                time.sleep(1)
            else:
                link_id_df.append(pd.DataFrame(jdata))
                got_cnt += 1
                print(f'...got link_id: {got_cnt}/{counter}')
                time.sleep(1)

        except TypeError:
            error_cnt += 1
            counter += 1
            print(f'TypeError:{error_cnt}/{counter}')
            
            time.sleep(1)

    #df_link = pd.concat(link_id_df)

    # raw_dict = dict(df_road['CityName'].value_counts())
    # return_dict = {}

    # for k, v in raw_dict.items():
    #         return_dict[k] = str(v)

    return {'finished':'!'}



