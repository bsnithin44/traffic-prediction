import datetime
import logging
import azure.functions as func

import pandas as pd
from pandas.io.json import json_normalize
import requests,pytz
from azure.storage.blob import BlockBlobService
import datetime


my_account_name = 'nithindev'
my_account_key = 'AJCUenkBC+LYslpZXFlJqha/pcNHWNBNoDQg/+rHb7QFktedIKuczFhMfbJPl2fn3lO6+xNkBDoXtm7QBSukFA=='
api_key = 'AIzaSyAYejgqWu2uW9r53I5GGAVFWYHWKFAE8h4'

blobservice = BlockBlobService(account_name=my_account_name,account_key=my_account_key)
try:
    blobservice.create_container('junction-one')
except:
    pass

params = {}
params['road_a1'] = {}
params['road_b1'] = {}
params['road_c1'] = {}
params['road_a1']['start'] = '12.956956, 77.701577'
params['road_a1']['end'] ='12.956066, 77.714548'
params['road_b1']['start'] = '12.964115, 77.717878'
params['road_b1']['end']= '12.955928, 77.714965'
params['road_c1']['start'] = '12.955790, 77.724528'
params['road_c1']['end'] = '12.955808, 77.714829'

def create_url(road_params,api_key):
    base_url = 'https://maps.googleapis.com/maps/api/distancematrix/json?'
    origin_ = f"origins={road_params['start']}&"
    destination_ = f"destinations={road_params['end']}&"
    key_ = f'key={api_key}'
    traffic = 'departure_time=now&'
    final_url = base_url + origin_ + destination_ + traffic + key_  
    return final_url

def modify_data(data,road_params,road):
    df =json_normalize(data['rows'][0]['elements'])
    df['road'] = road
    df['destination_addresses'] = data['destination_addresses'][0]
    df['origin_addresses'] = data['origin_addresses'][0]
    df['origin'] = road_params['start']
    df['destination'] = road_params['end']
    return df

def main_worker(params):
    try:
        del df
    except:
        pass
    for road in params:
        road_params = params[road]
        url  = create_url(road_params,api_key)
        r = requests.get(url)
        data = r.json()
        df_ = modify_data(data,road_params,road)
        try:
            df = df.append(df_)
        except:
            df = df_
        df = df.reset_index(drop=True)
        df['timestamp_UTC'] = datetime.datetime.now(tz=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
        df['timestamp_India'] = datetime.datetime.now(tz=pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
    a = datetime.datetime.now(tz=pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
    filename = f"junction-one {a}.csv"
    df.to_csv(filename,index=False)
    blobservice.create_blob_from_path(container_name='junction-one',file_path=filename,blob_name=filename)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    main_worker(params)
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
