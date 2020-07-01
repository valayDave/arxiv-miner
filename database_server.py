from arxiv_miner import ArxivFSDatabaseService as ArxivDatabaseService
from rpyc.utils.server import ThreadedServer
import os
from config import Config
import datetime
import click
from signal import signal,SIGINT


DEFAULT_PATH = Config.data_path
DEFAULT_PORT = Config.fs_database_port
DEFAULT_HOST = Config.fs_database_host

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
    db_service = ArxivDatabaseService(data_path)

    def stop_server(signal_received, frame):
        db_service.shutdown()
        t.close()

    signal(SIGINT, stop_server)
    t = ThreadedServer(\
        db_service,\
        port=port,\
        hostname=host,\
        protocol_config=Config.fs_database_config)
    t.start()

   
if __name__ == "__main__":
    start_server()