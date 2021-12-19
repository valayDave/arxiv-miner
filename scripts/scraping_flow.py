from metaflow import FlowSpec, step, Parameter, IncludeFile
import click
from arxiv_miner.config import Config
import configparser
from arxiv_miner import ScrapingEngine, MassDataHarvestingEngine,KeywordsTextSearch
import datetime
from arxiv_miner.metaflow_db_connection import DatabaseParameters

DEFAULT_END_DATE = datetime.datetime.now().strftime(ScrapingEngine.date_format)
DEFAULT_START_DATE = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(ScrapingEngine.date_format)
DEFAULT_TIMEOUT_PER_DAILY_SCRAPE = 12*60*60 # 12 hours
DEFAULT_SELECTED_CLASS= 'cs'



class ScrapingFlow(FlowSpec,DatabaseParameters):

    start_date = Parameter('start_date',default=DEFAULT_START_DATE,help='Start Date in Y-m-d Format')
    end_date = Parameter('end_date',default=DEFAULT_END_DATE,help='End Date in Y-m-d Format')
    timeout_per_date = Parameter('timeout_per_date',default=DEFAULT_TIMEOUT_PER_DAILY_SCRAPE,help="Time To Wait Before Next Scraping")
    selected_topic = Parameter('arxiv-topic',default=DEFAULT_SELECTED_CLASS,help='Topic Of ArXiv Papers to Scrape')

    def get_harvester(self,db_conn):
        return MassDataHarvestingEngine.from_string_dates(
                        db_conn,\
                        start_date=self.start_date,\
                        selected_class=self.selected_topic,\
                        end_date=self.end_date,\
                        timeout_per_scrape=self.timeout_per_date\
                        )
    
    @step
    def start(self):
        db_con = self.get_connection()
        harvester = self.get_harvester(db_con)
        harvester()
        self.next(self.end)

    @step
    def end(self):
        print("Done")

if __name__ == "__main__":
    ScrapingFlow()