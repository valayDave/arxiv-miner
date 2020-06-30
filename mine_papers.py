"""
The Purpose Of this Script is To Connect to ArxivDatabase 
Mine Records and Send them back to The DB. 
"""
from arxiv_miner import \
    MiningProcess,\
    ArxivDatabaseServiceClient

import click
import os
from utils import Config

DEFAULT_PATH = Config.mining_data_path
DEFAULT_HOST = Config.database_host
DEFAULT_PORT = Config.database_port
DEFAULT_DETEX_PATH = Config.detex_path

DEFAULT_MINING_INTERVAL=5
DEFAULT_MINING_LIMIT=30
DEFAULT_EMPTY_WAIT_TIME= 600

MINER_HELP = '''
ArXiv MINER 

The Purpose Of this Script is To Connect to ArxivDatabase,
Mine Records and Send them back to The DB. 

'''

@click.command(help=MINER_HELP)
@click.option('--mining_data_path',default=DEFAULT_PATH,type=click.Path())
@click.option('--host',default=DEFAULT_HOST,help='ArxivDatabase Host')
@click.option('--port',default=DEFAULT_PORT,help='ArxivDatabase Port')
@click.option('--detex_path',default=DEFAULT_DETEX_PATH,help='Path To Detex Binary For Latex Processing')
@click.option('--mining_interval',default=DEFAULT_MINING_INTERVAL,help='Interval in Seconds To Wait If a Paper Was Not Mined')
@click.option('--mining_limit',default=DEFAULT_MINING_LIMIT,help='Maximum Number of Papers To Mine')
@click.option('--empty_wait_time',default=DEFAULT_EMPTY_WAIT_TIME,help='Time To Wait if No Unmined Records Were Returned')
def start_miner(mining_data_path,\
                host=DEFAULT_HOST,
                port=DEFAULT_PORT,
                detex_path=DEFAULT_DETEX_PATH,
                mining_interval=5,\
                mining_limit=30,
                empty_wait_time = 600
                ):
    database = ArxivDatabaseServiceClient(host=host,port=port)
    process = MiningProcess(database,\
                            mining_data_path,\
                            detex_path,\
                            mining_interval = mining_interval,\
                            mining_limit = mining_limit,\
                            empty_wait_time = empty_wait_time)
    process.start()
    process.join()

if __name__ == "__main__":
    start_miner()