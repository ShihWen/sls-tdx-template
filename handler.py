import datetime
import os
import boto3
from helper import get_tdx_result, json_generator


def get_tdx_bus_route(event, context):
    city_key = [#"Taipei"
                #"NewTaipei"
                "Taoyuan",
                "Taichung",
                "Tainan",
                "Kaohsiung",
                "Keelung",
                "KinmenCounty",           
                "Hsinchu",
                "HsinchuCounty",          
                "MiaoliCounty",
                "NantouCounty",
                "ChanghuaCounty",
                "YunlinCounty",
                "ChiayiCounty",
                "Chiayi",
                "PingtungCounty",
                "HualienCounty",
                "TaitungCounty",
                "YilanCounty",
                "PenghuCounty"
                ]
    result = {}
    for city in city_key:
        print(f'processing {city}...')
        request_url = "https://tdx.transportdata.tw/api/basic/v2/Bus/Shape/City/"\
                        f"{city}"\
                        "?%24top=9999"\
                        "&%24format=JSON"


        data = get_tdx_result(url=request_url)
        json_obj = json_generator(tdx_data=data, city=city)
        ingest_time = datetime.datetime.now().strftime("%Y%m%d")
        s3_client = boto3.client('s3')

        s3_client.put_object(Body=json_obj,
                            Bucket=os.environ['s3_bucket'],
                            Key=f"line-string/tdx_bus_route_bq_format__{city}_{ingest_time}.json")
        
        result[f"routes_cnt_{city}"] = str(len(data)) 
    event['result'] = result


    return event


