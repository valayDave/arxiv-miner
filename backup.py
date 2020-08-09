# coding: utf-8
from arxiv_miner import ArxivPaper, ResearchPaper,ResearchPaperFactory
from arxiv_miner.utils import load_json_from_file,save_json_to_file,dir_exists
import os 
from arxiv_miner import ArxivElasticSeachDatabaseClient
from config import Config
from arxiv_miner.logger import create_logger
from arxiv_miner.record import ArxivSematicParsedResearch 
import time
import os
import tarfile
import shutil
import click

BACKUP_HELP = '''

The Purpose Of this Script is To Connect to ArxivElasticsearchDatabase,
And create a Tar based Core ArxivRecord backup on FS.

'''
BASE_PATH = os.path.join(
                os.path.dirname(\
                    os.path.abspath(__file__),\
                    ),
                'backups'                
                )

@click.command(help=BACKUP_HELP)
@click.option('--host', default=Config.get_db_defaults()['host'], help='ArxivElasticsearchDatabase Host')
@click.option('--port', default=Config.get_db_defaults()['port'], help='ArxivElasticsearchDatabase Port')
@click.option('--root_path',default=BASE_PATH,help='Default Path to Save information')
@click.option('--print_every',default=100,help="Print Message After Saving X objects to FS")
def create_backup(\
                host = 'localhost',\
                port = 9200,
                root_path = BASE_PATH,
                print_every=100,
                ):
    click.secho(
        'Connecting To Elasticsearch on Host %s And Port %s'%(host,port),
        fg='green'
    )
    backup_time = str(int(time.time()))
    json_save_path = os.path.join(root_path,backup_time)
    logger = create_logger('Data Transfer')
    database = ArxivElasticSeachDatabaseClient(Config.elasticsearch_index,host=host,port=port)
    if not dir_exists(json_save_path):
        os.makedirs(json_save_path)
    num_stored = 0
    logger.info("Starting Database Stream")
    for rec in database.record_stream():
        arxiv_object = rec.to_json()
        rec_id = rec.identity.identity
        save_json_to_file(arxiv_object,os.path.join(
            json_save_path,
            rec_id+'.json'
        ))
        num_stored+=1
        if num_stored % print_every == 0:
            logger.info("Flushed %d Records to FS "%num_stored)

    tar_file_path  = os.path.join(root_path,backup_time+'.tar.gz')
    save_tar_file = tarfile.open(
        tar_file_path,
        'w:gz'
    )
    logger.info("Creating Tar File To Path %s"%tar_file_path)
    os.chdir(
        root_path
    )
    for name in os.listdir(backup_time):
        save_tar_file.add(os.path.join(backup_time,name))
    save_tar_file.close()
    shutil.rmtree(json_save_path)

if __name__ == '__main__':
    create_backup()