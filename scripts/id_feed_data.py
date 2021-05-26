"""
The Purpose Of this Script is To Connect to ArxivDatabase 
Mine Records and Send them back to The DB. 
"""
from arxiv_miner import \
    MiningProcess
from arxiv_miner import TextSearchFilter

import click
from arxiv_miner.logger import create_logger
from arxiv_miner.utils import load_json_from_file,save_json_to_file,dir_exists
import os
from arxiv_miner.config import Config
from arxiv_miner.cli import db_cli
import random
import time
import json
import pandas


DEFAULT_PATH = Config.mining_data_path
DEFAULT_DETEX_PATH = Config.detex_path

DEFAULT_MINING_INTERVAL=5
SLEEP_BETWEEEN_PORCS = 5
DEFAULT_MINING_LIMIT=30
DEFAULT_EMPTY_WAIT_TIME= 600
DEFAULT_SLEEP_INTERVAL_COUNT = 50
APP_NAME = 'ArXiv-Miner'
MINER_HELP = '''

The Purpose Of this Script is To Connect to ArxivDatabase,
Extract ID based Record Feed. 
'''
DEFAULT_FILE = 'id_list.json'
STORE_ROOT_PATH = 'extracted_id_data'

@db_cli.command(help=MINER_HELP)
@click.option('--id_dict_path',default=DEFAULT_FILE,help='File of Arxiv Id list to extract data Feed. ')
@click.option('--root_path',default=STORE_ROOT_PATH,help='File of Arxiv Id list to extract data Feed. ')
@click.option('--sample',default=None,type=int,help='Sample Of Ids from the selected `id_dict_path`')
@click.pass_context
def extract_ids(ctx, # click context object: populated from db_cli
                id_dict_path = DEFAULT_FILE,
                root_path=STORE_ROOT_PATH,
                sample=None
                ):
    backup_time = str(int(time.time()))
    
    raw_json_save_path = os.path.join(root_path,backup_time,'raw_research')
    parsed_json_save_path = os.path.join(root_path,backup_time,'parsed_research')
    
    
    logger = create_logger('Data Transfer')
    if not dir_exists(raw_json_save_path):
        os.makedirs(raw_json_save_path)
    if not dir_exists(parsed_json_save_path):
        os.makedirs(parsed_json_save_path)

    num_stored = 0
    logger.info("Starting Database Stream And Writing to Folder : %s"%root_path )

    database_client = ctx.obj['db_class'](**ctx.obj['db_args']) # Create Database 
    with open(id_dict_path,'r') as f :
        id_json = json.load(f)
    
    id_list = id_json['id_list']
    if sample is not None:
        id_list = random.sample(id_list,sample)
    for datatup in database_client.id_stream(id_list):
        arxiv_id,rec_obj,parsed_obj = datatup
        if rec_obj is not None:
            rec_pth = os.path.join(raw_json_save_path,arxiv_id+'.json')
            save_json_to_file(rec_obj.to_json(),rec_pth)
        if parsed_obj is not None:
            parsed_pth = os.path.join(parsed_json_save_path,arxiv_id+'.json')
            save_json_to_file(parsed_obj.to_json(),parsed_pth)
    
    logger.info("Completed Writing To %s"%os.path.join(root_path,backup_time))

if __name__ == "__main__":
    db_cli()
