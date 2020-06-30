from arxiv_miner import \
    ArxivDatabaseServiceClient,\
    MassDataHarvestingEngine,\
    DailyScrapingEngine,\
    ScrapingEngine,\
    DailyHarvestationProcess,\
    MassHarvestationProcess

from arxiv_miner.scraping_engine import CLASSES

import os
import click
import datetime
from functools import wraps
from utils import Config

DEFAULT_HOST = Config.database_host
DEFAULT_PORT = Config.database_port
DEFAULT_SELECTED_CLASS = 'cs'
DEFAULT_START_DATE = datetime.datetime.now().strftime(ScrapingEngine.date_format)
DEFAULT_END_DATE = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(ScrapingEngine.date_format)
DEFAUL_TIMEOUT_PER_DAILY_SCRAPE = 1000 # In Seconds
DEFAULT_TIMEOUT_PER_DATE_RANGE_SCRAPE = 5

CLASS_STR = '\n'.join(['\t'+c['name']+' : '+c['code']+'\n' for c in CLASSES])
SCRAPING_HELP = '''
ArXiv Scraper 

The Purpose of this Module is to :\n
- Fetch Data From http://export.arxiv.org/oai2?verb=ListRecords API \n
- extract ArxivRecords. \n
- Add them as `ArxivIdentity` to `ArxivDatabase`. \n

The Following CLASSES are Available for Scraping : \n
'''+CLASS_STR

@click.group(help=SCRAPING_HELP)
def cli():
    pass



def common_run_options(func):
    @click.option('--host',default=DEFAULT_HOST,help='ArxivDatabase Host')
    @click.option('--port',default=DEFAULT_PORT,help='ArxivDatabase Port')
    @click.option('--selected_class',default=DEFAULT_SELECTED_CLASS,help='Topic Of ArXiv Papers to Scrape')
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@cli.command(help='Start Scraping Engine that will Scrape Between Date Ranges')
@common_run_options
@click.option('--start_date',default=DEFAULT_START_DATE,help='Start Date in Y-m-d Format')
@click.option('--end_date',default=DEFAULT_END_DATE,help='End Date in Y-m-d Format')
@click.option('--timeout_per_scrape',default=DEFAULT_TIMEOUT_PER_DATE_RANGE_SCRAPE,help="Time To Wait Before Next Scraping")
def date_range(host=DEFAULT_HOST,\
                        port=DEFAULT_PORT,\
                        selected_class=DEFAULT_SELECTED_CLASS,\
                        start_date=DEFAULT_START_DATE,\
                        end_date=DEFAULT_END_DATE,\
                        timeout_per_scrape=DEFAULT_TIMEOUT_PER_DATE_RANGE_SCRAPE):

    database = ArxivDatabaseServiceClient(host=host,port=port)    
    harvester = MassDataHarvestingEngine.from_string_dates(
                                    database,\
                                    start_date=start_date,\
                                    selected_class=selected_class,\
                                    end_date=end_date,\
                                    timeout_per_scrape=timeout_per_scrape\
                                    )
    harvesting_process = MassHarvestationProcess(harvester)
    harvesting_process.start()
    harvesting_process.join()

@cli.command(help='Start Scraping Engine that will Scrape Data For The Day')
@common_run_options
@click.option('--timeout_per_scrape',default=DEFAUL_TIMEOUT_PER_DAILY_SCRAPE,help="Time To Wait Before Next Scraping")
def daily_harvest(host=DEFAULT_HOST,\
                port=DEFAULT_PORT,\
                selected_class=DEFAULT_SELECTED_CLASS,\
                timeout_per_scrape=DEFAUL_TIMEOUT_PER_DAILY_SCRAPE):

    database = ArxivDatabaseServiceClient(host=host,port=port)    
    harvester = DailyScrapingEngine(database,selected_class=selected_class)
    harvesting_process = DailyHarvestationProcess(harvester,timeout_per_scrape=timeout_per_scrape)
    harvesting_process.start()
    harvesting_process.join()

if __name__ =='__main__':
    cli()