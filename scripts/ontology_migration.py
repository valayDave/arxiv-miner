# coding: utf-8
from arxiv_miner import ArxivPaper
from arxiv_miner.utils import load_json_from_file,save_json_to_file,dir_exists
import os 
from arxiv_miner import ArxivElasticSeachDatabaseClient,KeywordsTextSearch
from arxiv_miner.config import Config
from arxiv_miner.logger import create_logger
from arxiv_miner.ontology_miner import OntologyMiner,ONTOLOGY_MINABLE
from arxiv_miner.record import ArxivSematicParsedResearch,Ontology,Author
import time
import os
import tarfile
import shutil
import click

HELP = '''
ArXiv Databased Ontology Mining

The Purpose Of this Script is To Connect to ArxivElasticsearchDatabase,
Run the One-Time Ontology Miner For Migration To New Index. 

'''
if not ONTOLOGY_MINABLE:
    raise Exception("Ontology Not Minable. Please Setup CSO Classifer Properly. ")

@click.command(help=HELP)
@click.option('--host', default=Config.get_db_defaults()['host'], help='ArxivElasticsearchDatabase Host')
@click.option('--port', default=Config.get_db_defaults()['port'], help='ArxivElasticsearchDatabase Port')
@click.option('--print_every',default=100,help="Print Message After Saving X objects to FS")
@click.option('--limit',default=None, type=int, help="Cap the Limit Of Records to Mine and Send to DB.")
@click.option('--buffer-size',default=200, type=int, help="The Number Items to Keep in Batch For Ontology Processing.")
@click.option('--workers',default=1, type=int, help="Number of Workers For Ontology Processing.")
def create_backup(\
                host = 'localhost',\
                port = 9200,\
                print_every=100,\
                limit=None,\
                buffer_size=200,
                workers=1
                ):
    click.secho(
        'Connecting To Elasticsearch on Host %s And Port %s'%(host,port),
        fg='green'
    )
    backup_time = str(int(time.time()))
    
    logger = create_logger('Ontology Data Migration')
    database = KeywordsTextSearch(Config.elasticsearch_index,host=host,port=port)
    migrate_db = KeywordsTextSearch(Config.elasticsearch_index+"_with_ontology",host=host,port=port)
    num_stored = 0
    logger.info("Starting Database Stream")
    
    buffer = []
    for rec in database.parsed_research_stream():
        if len(buffer) < buffer_size:
            buffer.append(rec)
            continue
        
        id_ontology_list = OntologyMiner.mine_lots_of_papers(buffer,workers=workers)
        buffer = []
        srp = [
                ArxivSematicParsedResearch(
                    identity=recobj.identity,\
                    research_object=recobj.research_object,\
                    ontology = ontology
                ) for recobj,ontology in id_ontology_list
            ]
        migrate_db.set_many_parsed_research(srp)
        migrate_db.set_many_authors(
            [Author(name=xp) for xp in set([a for x in srp for a in x.identity.authors])]
        )
        migrate_db.set_many_ontology(
            [xp for xp in set([a for _,x in id_ontology_list for a in x.union])]
        )
        num_stored+=len(srp)
        # for recobj,ontology in id_ontology_list:
        #     parsed_research = ArxivSematicParsedResearch(\
        #         identity=recobj.identity,\
        #         research_object=recobj.research_object,\
        #         ontology = ontology
        #     )
        #     migrate_db.set_semantic_parsed_research(parsed_research)
        #     num_stored+=1

        if num_stored % print_every == 0:
            logger.info("Flushed %d Parsed Records to FS "%num_stored)
        if limit is not None and num_stored > limit:
            logger.info("Flushed %d Parsed Records to FS And Now Exiting"%num_stored)
            break
    

if __name__ == '__main__':
    create_backup()
    