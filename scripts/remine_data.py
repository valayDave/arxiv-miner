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
DEFAULT_FILE = 'id_list.json'
APP_NAME = 'ArXiv-Miner'
MINER_HELP = f'''

The Purpose Of this Script is To Connect to ArxivDatabase,
and Set mined to be for the records in {DEFAULT_FILE}
'''

STORE_ROOT_PATH = 'extracted_id_data'

@db_cli.command(help=MINER_HELP)
@click.option('--id_dict_path',default=DEFAULT_FILE,help='File of Arxiv Id list to extract data Feed. ')
@click.option('--sample',default=None,type=int,help='Sample Of Ids from the selected `id_dict_path`')
@click.pass_context
def remine_data(ctx, # click context object: populated from db_cli
                id_dict_path = DEFAULT_FILE,
                sample=None
                ):
    backup_time = str(int(time.time()))
    
    logger = create_logger('Data Reminer')
    
    num_stored = 0
    logger.info("Starting Database Stream " )

    database_client = ctx.obj['db_class'](**ctx.obj['db_args']) # Create Database 
    with open(id_dict_path,'r') as f :
        id_json = json.load(f)
    
    id_list = id_json['id_list']
    if sample is not None:
        id_list = random.sample(id_list,sample)
    
    for datatup in database_client.id_stream(id_list):
        arxiv_id,rec_obj,_ = datatup
        database_client.set_mined(rec_obj.identity,False)
        logger.info(f"Set {arxiv_id} to Unmined")

if __name__ == "__main__":
    db_cli()
