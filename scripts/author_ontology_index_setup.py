
# coding: utf-8
from arxiv_miner.database.elasticsearch import KeywordsTextSearch
from arxiv_miner import ArxivPaper
from arxiv_miner.utils import load_json_from_file,save_json_to_file,dir_exists
import os 
from arxiv_miner import ArxivElasticSeachDatabaseClient
from arxiv_miner.config import Config
from arxiv_miner.logger import create_logger
from arxiv_miner.ontology_miner import OntologyMiner,ONTOLOGY_MINABLE
from arxiv_miner.record import ArxivSematicParsedResearch, Author,Ontology
import time
import os
import tarfile
import shutil
import click

HELP = '''
To Move Ontology and Authors into one index so they become well searchable index

'''

@click.command(help=HELP)
@click.option('--host', default=Config.get_db_defaults()['host'], help='ArxivElasticsearchDatabase Host')
@click.option('--port', default=Config.get_db_defaults()['port'], help='ArxivElasticsearchDatabase Port')
@click.option('--print_every',default=100,help="Print Message After Saving X objects to FS")
@click.option('--limit',default=None, type=int, help="Cap the Limit Of Records to Mine and Send to DB.")
def move_data(\
    host = 'localhost',\
    port = 9200,\
    print_every=100,\
    limit=None):
    click.secho(
        'Connecting To Elasticsearch on Host %s And Port %s'%(host,port),
        fg='green'
    )
    backup_time = str(int(time.time()))
    
    logger = create_logger('Ontology Data Migration')
    database = KeywordsTextSearch(Config.elasticsearch_index,host=host,port=port)
    num_stored = 0
    for record in database.record_stream():
        
        database.set_many_authors([Author(name=auth) for auth in record.identity.authors])
        num_stored+=len(record.identity.authors)

        if num_stored % print_every == 0:
            logger.info("Flushed %d Parsed Records to FS "%num_stored)
    
    
if __name__=="__main__":
    move_data()