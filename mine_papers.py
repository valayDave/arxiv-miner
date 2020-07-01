"""
The Purpose Of this Script is To Connect to ArxivDatabase 
Mine Records and Send them back to The DB. 
"""
from arxiv_miner import \
    MiningProcess,\
    ArxivDatabaseServiceClient
from arxiv_miner import ArxivElasticSeachDatabaseClient

import click
import os
from config import Config
from cli import db_cli

DEFAULT_PATH = Config.mining_data_path
DEFAULT_DETEX_PATH = Config.detex_path

DEFAULT_MINING_INTERVAL=5
DEFAULT_MINING_LIMIT=30
DEFAULT_EMPTY_WAIT_TIME= 600
APP_NAME = 'ArXiv-Miner'
MINER_HELP = '''

The Purpose Of this Script is To Connect to ArxivDatabase,
Mine Records and Send them back to The DB. 

'''

@db_cli.command(help=MINER_HELP)
@click.option('--mining_data_path',default=DEFAULT_PATH,type=click.Path())
@click.option('--forever', is_flag=True, help="Run the Miner Without any max Caps")
@click.option('--detex_path',default=DEFAULT_DETEX_PATH,help='Path To Detex Binary For Latex Processing')
@click.option('--mining_interval',default=DEFAULT_MINING_INTERVAL,help='Interval in Seconds To Wait If a Paper Was Not Mined')
@click.option('--mining_limit',default=DEFAULT_MINING_LIMIT,help='Maximum Number of Papers To Mine')
@click.option('--empty_wait_time',default=DEFAULT_EMPTY_WAIT_TIME,help='Time To Wait if No Unmined Records Were Returned')
@click.pass_context
def start_miner(ctx, # click context object: populated from db_cli
                mining_data_path,\
                forever=False,
                detex_path=DEFAULT_DETEX_PATH,
                mining_interval=5,\
                mining_limit=30,
                empty_wait_time = 600
                ):
    database_client = ctx.obj['db_class'](**ctx.obj['db_args']) # Create Database 
    if forever:
        mining_limit = None
    process = MiningProcess(database_client,\
                            mining_data_path,\
                            detex_path,\
                            mining_interval = mining_interval,\
                            mining_limit = mining_limit,\
                            empty_wait_time = empty_wait_time)
    process.start()
    process.join()


if __name__ == "__main__":
    db_cli()
    # run_wrapped_cli(db_cli,app_name=APP_NAME)