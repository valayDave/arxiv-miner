from arxiv_miner import \
    MassDataHarvestingEngine,\
    DailyScrapingEngine,\
    ScrapingEngine,\
    DailyHarvestationProcess,\
    MassHarvestationProcess,\
    DailyHarvestationThread,\
    MassHarvestationThread


from arxiv_miner import ArxivElasticSeachDatabaseClient
# import multiprocessing as mp
# mp.set_start_method('spawn')

from arxiv_miner.scraping_engine import CLASSES

import os
import click
import datetime
from functools import wraps,partial
from arxiv_miner.config import Config
from arxiv_miner.cli import db_cli


DEFAULT_SELECTED_CLASS = 'cs'
DEFAULT_START_DATE = datetime.datetime.now().strftime(ScrapingEngine.date_format)
DEFAULT_END_DATE = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(ScrapingEngine.date_format)
DEFAUL_TIMEOUT_PER_DAILY_SCRAPE = 1000 # In Seconds
DEFAULT_TIMEOUT_PER_DATE_RANGE_SCRAPE = 5
DEFAULT_THREADMODE = False

CLASS_STR = '\n'.join(['\t'+c['name']+' : '+c['code']+'\n' for c in CLASSES])
APP_NAME = 'ArXiv-Scraper'
SCRAPING_HELP = '''
ArXiv Scraper 

The Purpose of this Module is to :\n
- Fetch Data From http://export.arxiv.org/oai2?verb=ListRecords API \n
- extract ArxivRecords. \n
- Add them as `ArxivIdentity` to `ArxivDatabase`. \n

The Following CLASSES are Available for Scraping : \n
'''+CLASS_STR


def common_run_options(func):
    @click.option('--selected_class',default=DEFAULT_SELECTED_CLASS,help='Topic Of ArXiv Papers to Scrape')
    @click.option('--thread-mode',is_flag=True,help='Run In Thread Mode Instead of Process Mode')
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@db_cli.command(help='Start Scraping Engine that will Scrape Between Date Ranges')
@common_run_options
@click.option('--start_date',default=DEFAULT_START_DATE,help='Start Date in Y-m-d Format')
@click.option('--end_date',default=DEFAULT_END_DATE,help='End Date in Y-m-d Format')
@click.option('--timeout_per_scrape',default=DEFAULT_TIMEOUT_PER_DATE_RANGE_SCRAPE,help="Time To Wait Before Next Scraping")
@click.pass_context
def date_range(ctx, # click context
                selected_class=DEFAULT_SELECTED_CLASS,\
                thread_mode=DEFAULT_THREADMODE,\
                start_date=DEFAULT_START_DATE,\
                end_date=DEFAULT_END_DATE,\
                timeout_per_scrape=DEFAULT_TIMEOUT_PER_DATE_RANGE_SCRAPE):
    database_client = ctx.obj['db_class'](**ctx.obj['db_args'])
    harvester = MassDataHarvestingEngine.from_string_dates(
                                    database_client,\
                                    start_date=start_date,\
                                    selected_class=selected_class,\
                                    end_date=end_date,\
                                    timeout_per_scrape=timeout_per_scrape\
                                    )
    hp = None
    if thread_mode:
        click.secho("Thread Mode Detected",fg='blue')
        hp = MassHarvestationThread(harvester)
    else:
        click.secho("Running Process Mode",fg='yellow')
        hp =  MassHarvestationProcess(harvester)
    harvesting_process = hp
    harvesting_process.start()
    harvesting_process.join()

@db_cli.command(help='Start Scraping Engine that will Scrape Data For The Day')
@common_run_options
@click.option('--timeout_per_scrape',default=DEFAUL_TIMEOUT_PER_DAILY_SCRAPE,help="Time To Wait Before Next Scraping")
@click.pass_context
def daily_harvest(ctx, # click context
                selected_class=DEFAULT_SELECTED_CLASS,\
                thread_mode=DEFAULT_THREADMODE,\
                timeout_per_scrape=DEFAUL_TIMEOUT_PER_DAILY_SCRAPE):
    database_client = ctx.obj['db_class'](**ctx.obj['db_args']) # Create Database 
    harvester = DailyScrapingEngine(database_client,selected_class=selected_class)
    hp = None
    if thread_mode:
        click.secho("Thread Mode Detected",fg='blue')
        hp = DailyHarvestationThread(harvester,timeout_per_scrape=timeout_per_scrape)
    else:
        click.secho("Running Process Mode",fg='yellow')
        hp =  DailyHarvestationProcess(harvester,timeout_per_scrape=timeout_per_scrape)
    harvesting_process = hp
    harvesting_process.start()
    harvesting_process.join()

if __name__ =='__main__':
    db_cli.help = SCRAPING_HELP
    db_cli()