SCRIPT_PURPOSE = '''
The Script is meant to harvest a large amount of tar based data 
From arxiv and then store it To S3 based Object store. 

ENVIRONMENT VARIABLES FOR AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY Expected
'''
from arxiv_miner.mining_engine import SourceHarvestingEngine
import boto3
import metaflow
import os
import click
from arxiv_miner.config import Config
import json
from arxiv_miner.logger import create_logger
from typing import List,Tuple
import random

logger = create_logger('Mass-S3-Paper-Source-Harvest')
# Create The S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# S3 Related Args
BUCKET_NAME = Config.bucket_name
ID_LIST_PREFIX = 'scraping_id_chunks'
ID_SOURCE_PATH = os.path.join(BUCKET_NAME,ID_LIST_PREFIX)

# Download paths
DEFAULT_DOWNLOAD_PATH = './data'

def get_chunk_name(chunk_id):
    return os.path.join(ID_SOURCE_PATH,f"completely-parsable-id-chunk-{chunk_id}.json")


def get_chunk_from_s3(sample=None):
    s3_obj_locs = s3_client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=ID_LIST_PREFIX)
    if len(s3_obj_locs['Contents']) == 0:
        raise Exception("No Contents Left In Bucket")
    selected_key = random.choice(s3_obj_locs['Contents'])
    download_object_key = selected_key['Key'] # get A random key
    boto_resp = s3_client.get_object(Bucket=BUCKET_NAME,Key=download_object_key)
    logger.info(f"Extracted Chunk From S3: {download_object_key}")
    id_list_json = json.loads(boto_resp['Body'].read()) # Download and save the object to Memory
    if sample is None:
        # delete the object from bucket so that other workers down do same works
        delete_resp = s3_client.delete_object(Bucket=BUCKET_NAME,Key=download_object_key)
        logger.info(delete_resp)
    return id_list_json


def store_source_to_s3(download_path_tuples:List[Tuple[str,str]]):
    for arxiv_id,file_path in download_path_tuples:
        obj_key = os.path.normpath(file_path)
        upload_resp = s3_client.upload_file(file_path,BUCKET_NAME,obj_key)
        logger.info(f"Uploaded Object To {obj_key}")
        
@click.command(help=SCRIPT_PURPOSE)
@click.option('--max-chunks',default=1,help='Maximum Chunks To Harvest from S3 Bucket')
@click.option('--download-rootpath',default=DEFAULT_DOWNLOAD_PATH,help='Root Path to which Source From Latex Will be Downloaded')
@click.option('--sample',default=None,help='Extract Sampled Data. It will not delete file from remote. Will Do a Dry Run of the code. Sample Value <100 ')
def harvest(max_chunks=1,download_rootpath=DEFAULT_DOWNLOAD_PATH,type=int,sample=None):
    for i in range(max_chunks):
        logger.info(f"Starting Harvest For Chuck Number {i} With Sample of {sample}")
        id_list_obj = get_chunk_from_s3(sample=sample)
        id_list = id_list_obj['id_list']
        if sample is not None:
            id_list = random.sample(id_list,int(sample))
        logger.info(f"Harvesting {len(id_list)} Samples From Arxiv")
        harvest_engine = SourceHarvestingEngine(
            id_list,
            download_rootpath,
            error_sleep_time=5
        )
        download_path_tuples = harvest_engine.harvest() # returns [(arxiv_id,download_path)]
        logger.info(f"Extracted Data : {len(download_path_tuples)}")
        store_source_to_s3(download_path_tuples)
        logger.info(f'Save {len(download_path_tuples)} Documents TO S3')

if __name__ == "__main__":
    harvest()