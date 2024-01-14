import json
import os
import re
import requests
import datetime


# Json helper class
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


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



def geometry_generator(feature_string:str, city:str)->dict:
    '''
    converting string location data into geojson format

    Args:
        feature_string: string returned by TDX api
    
    Returns:
        a dictionary in for value of geometry key for geojson file
    '''
    geo_type = re.match('[a-zA-z]+',feature_string).group(0)
    result_coordinates = []
    #print(f"geo_type:{geo_type}")
    if geo_type == 'LINESTRING':
        #print(f"LINESTRING")
        start_idx = feature_string.find('(')
        end_idx = feature_string.find(')')
        if city in ["Taipei","NewTaipei"]:
            loc_list = feature_string[start_idx+1:end_idx].split(', ')
        else:
            loc_list = feature_string[start_idx+1:end_idx].split(',')
        # loc_list:['121.625813 25.059209', '121.627575 25.060679', '121.628098 25.061084', ...]
        #print(f"loc_list:{loc_list}")

        for loc in loc_list:
            split_loc = loc.split(' ')
            num_split_loc = list(map(float, split_loc))

            result_coordinates.append(num_split_loc)

    elif geo_type == 'MULTILINESTRING':
        #print(f"{feature_string}")
        start_idx = [idx for idx, s in enumerate(feature_string) if '(' in s][1:]
        end_idx = [idx for idx, s in enumerate(feature_string) if ')' in s][:-1]

        loc_list_group = []
        for i in range(len(start_idx)):
            if city in ["Taipei","NewTaipei"]:
                loc_list = feature_string[start_idx[i]+1:end_idx[i]].split(', ')
            else:
                loc_list = feature_string[start_idx[i]+1:end_idx[i]].split(',')
            loc_list_group.append(loc_list)
            #display(loc_list_group)

        for loc_list in loc_list_group:
            #print(loc_list)
            refined_log_list = []
            for loc in loc_list:
                split_loc = loc.split(' ')
                num_split_loc = list(map(float, split_loc))
                refined_log_list.append(num_split_loc)
            result_coordinates.append(refined_log_list)

    return {"type":geo_type, "coordinates":result_coordinates}


def json_generator(tdx_data:list, city)->str:
    '''
    merge exising tdx data with geometry value generated from geometry_generator, 
    and generate a newline-delimited json file in string

    Args:
        tdx_data: string returned by TDX api
    
    Returns:
        a dictionary in for value of geometry key for geojson file
    '''
    lst = []
    for route in tdx_data:
        flat = flatten_json(route)
        new_route_obj = {}
        for k, v in flat.items():   
            if k != 'Geometry':
                new_route_obj[k] = v
            else:
                new_route_obj["geometry"] = json.dumps(geometry_generator(v, city))
            
            new_route_obj["ingest_time"] = datetime.datetime.now().strftime("%Y%m%d")
            new_route_obj["city"] = city

        lst.append(new_route_obj)

    return '\n'.join(map(json.dumps, lst))