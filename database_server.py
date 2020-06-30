from arxiv_miner import ArxivDatabaseService
from rpyc.utils.server import ThreadedServer
import os
from utils import Config
import datetime
import click

DEFAULT_PATH = Config.data_path
DEFAULT_PORT = Config.database_port
DEFAULT_HOST = Config.database_host

DATABASE_HELP = '''
ArXiv Database

This will start an FS oriented Database for the ArxivRecords and uses
`rpyc` based Services to interact. 
'''

@click.command(help=DATABASE_HELP)
@click.option('--host',default=DEFAULT_HOST,help='ArxivDatabase HostName')
@click.option('--port',default=DEFAULT_PORT,help='ArxivDatabase Port')
@click.option('--data_path',default=DEFAULT_PATH,type=click.Path())
def start_server(data_path,\
                port = DEFAULT_PORT,
                host = DEFAULT_HOST):
    t = ThreadedServer(\
        ArxivDatabaseService(data_path),\
        port=port,\
        hostname=host,\
        protocol_config=Config.database_config)
    t.start()


if __name__ == "__main__":
    start_server()