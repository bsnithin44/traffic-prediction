import azure.functions as func
import pandas as pd
from pandas.io.json import json_normalize
import requests,pytz
from azure.storage.blob import BlockBlobService
from azure.storage.file import FileService
import datetime,json,logging




my_account_name = 'nithindev'
my_account_key = 'AJCUenkBC+LYslpZXFlJqha/pcNHWNBNoDQg/+rHb7QFktedIKuczFhMfbJPl2fn3lO6+xNkBDoXtm7QBSukFA=='
api_key = 'AIzaSyANcAT5bp-yvBY35DlVdUaGHAU8D3Rr0Po'
config_container = 'traffic-config'
config_blobname = 'road_config.json'
base_url = 'https://maps.googleapis.com/maps/api/distancematrix/json?'
key_ = f'key={api_key}'
traffic = 'departure_time=now&'

blobservice = BlockBlobService(account_name=my_account_name,account_key=my_account_key)
blobservice.get_blob_to_path(container_name=config_container,
                             blob_name=config_blobname,
                             file_path=config_blobname
                            )
logging.info('initialised')

blobservice.create_container('junction-123')
file_service = FileService(account_name=my_account_name,account_key=my_account_key)
file_service.create_share('traffic-data')
file_service.create_directory(share_name='traffic-data',directory_name='raw_data')
file_service.create_directory(share_name='traffic-data',directory_name='clean_data')
logging.info('created share and containers')

with open('road_config.json','r') as f:
    road_config = json.load(f)

def create_origins(road_config):
    origins_ = ''
    for jnc in road_config:
        for road in road_config[jnc]['roads']:
            x = road_config[jnc]['roads'][road]['start']
            origins_ = origins_ + f"{x}|"
    return f"origins={origins_}&"

def create_destinations(road_config):
    destinations_ = ''
    for jnc in road_config:
        destinations_ = destinations_ + f"{road_config[jnc]['center']}|"
    return f"destinations={destinations_}&"

def create_url(road_config):
    origin = create_origins(road_config)
    destination = create_destinations(road_config)
    final_url = base_url + origin + destination + traffic + key_  
    return final_url

def pull_data(url):
    i = 0
    while i<5:
        r = requests.get(url)
        if int(r.status_code) == 200:
            break
        i += 1
    return r.json()

def upload_rawdata(blobservice,data_json):
    ts_india = datetime.datetime.now(tz=pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
    dumpname = f"dump_{ts_india}.json"
    with open(dumpname,'w') as f:
        json.dump(data_json,f)
    blobservice.create_blob_from_path(container_name='junction-123',blob_name=dumpname,file_path=dumpname)
    
def process_data_slave(i,r,j,row,data_json):
    data_ = row['elements'][j]
    df = json_normalize(data_)
    df.columns = df.columns.str.replace(".","_")
    df['origin_address'] = data_json['origin_addresses'][i]
    df['desination_address'] = data_json['destination_addresses'][j]
    junction_ = f"junction-{j+1}"
    road = str(r+1)
    df['start_coord'] = road_config[junction_]['roads'][road]['start']
    df['end_coord'] = road_config[junction_]['roads'][road]['end']
    df['junction'] = junction_
    df['road'] = road
    return df

def process_data_master(data_json):
    for i,row in enumerate(data_json['rows']):
        if i <=3:
            r = i
            j = 0
        if i>3 and i<=6:
            r = i - 4
            j = 1
        elif i>6:
            r = i - 7
            j = 2
        df_ = process_data_slave(i,r,j,row,data_json)
        try:
            df = df.append(df_)
        except:
            df = df_
    ts_india = datetime.datetime.now(tz=pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
    df['ts_india'] = ts_india
    df['ts_utc'] = datetime.datetime.now(tz=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
    return df.reset_index(drop=True),ts_india

def main_worker():
    url = create_url(road_config)
    data_json = pull_data(url)
    upload_rawdata(blobservice,data_json)
    df,ts = process_data_master(data_json)

    csv_name = f"csv_{ts}.csv"
    df.to_csv(csv_name,index=False)
    file_service.create_file_from_path(share_name='traffic-data',
                                       directory_name='raw_data',
                                       local_file_path=csv_name,
                                       file_name=csv_name.replace(":","-"))

    try:
        file_service.get_file_to_path(share_name='traffic-data',
                                      directory_name='clean_data',
                                      file_name='master.csv',
                                     file_path='master_old.csv')
        df_master = pd.read_csv('master_old.csv')
        df_master = df_master.append(df)
        df_master = df_master.sort_values(['ts_india']).reset_index(drop=True)
        df_master.to_csv('master_new.csv',index=False)
        file_service.create_file_from_path(share_name='traffic-data',
                                          directory_name='clean_data',
                                          file_name='master.csv',
                                          local_file_path='master_new.csv')
        logging.info("appended and updated")
    except Exception as e:
        logging.debug(f"Error occurred {e}")
        df.to_csv('master_new.csv',index=False)
        file_service.create_file_from_path(share_name='traffic-data',
                                           directory_name='clean_data',
                                           local_file_path='master_new.csv',
                                           file_name='master.csv')

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    main_worker()
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
