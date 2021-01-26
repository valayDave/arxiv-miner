""" 
Responsible for creating scraping and adding 
Records into a database. 

The Mining Engine Uses the Database To extract 
Information from Arxiv LateX papers
"""

import datetime
import time
from threading import Thread
from multiprocessing import Process,Event
from expiringdict import ExpiringDict
from signal import signal, SIGINT
from .record import ArxivIdentity,ArxivRecord,ArxivSematicParsedResearch
from .database import ArxivDatabase
from .paper import ResearchPaperFactory
from .logger import create_logger
from .scraper import Scraper
'''
How Will Scraping Take Place. ? 
    1. Once Running With a Configuration Of what Subject area to Scrape. 
    2. Outside Cron Will Control this bound object. 
    3. Bound Object will Scrape Information : 
        - Create Identities
        - Verify Existance in Database / Add to DB --> Identity With SemanticParsedResearch will be Created in DB will be used by the Miner
    4. 
'''
CLASSES = [
    {'name': 'Computer Science', 'code': 'cs'},
    {'name': 'Economics', 'code': 'econ'},
    {'name': 'Electrical Engineering and Systems Science', 'code': 'eess'},
    {'name': 'Mathematics', 'code': 'math'},
    {'name': 'Physics', 'code': 'physics'},
    {'name': 'Astrophysics', 'code': 'physics:astro-ph'},
    {'name': 'Condensed Matter', 'code': 'physics:cond-mat'},
    {'name': 'General Relativity and Quantum Cosmology', 'code': 'physics:gr-qc'},
    {'name': 'High Energy Physics - Experiment', 'code': 'physics:hep-ex'},
    {'name': 'High Energy Physics - Lattice', 'code': 'physics:hep-lat'},
    {'name': 'High Energy Physics - Phenomenology', 'code': 'physics:hep-ph'},
    {'name': 'High Energy Physics - Theory', 'code': 'physics:hep-th'},
    {'name': 'Mathematical Physics', 'code': 'physics:math-ph'},
    {'name': 'Nonlinear Sciences', 'code': 'physics:nlin'},
    {'name': 'Nuclear Experiment', 'code': 'physics:nucl-ex'},
    {'name': 'Nuclear Theory', 'code': 'physics:nucl-th'},
    {'name': 'Physics (Other)', 'code': 'physics:physics'},
    {'name': 'Quantum Physics', 'code': 'physics:quant-ph'},
    {'name': 'Quantitative Biology', 'code': 'q-bio'},
    {'name': 'Quantitative Finance', 'code': 'q-fin'},
    {'name': 'Statistics', 'code': 'stat'}
]
MAX_PAPERS_IN_CACHE = 2000
CACHE_PAPER_TTL = 60*12
MAX_BACK_DAYS = 365*5
# 1 Day ago
DEFAULT_SCRAPE_UNTIL = (datetime.datetime.now() - datetime.timedelta(days = 1))
# SCRAPE_UNTIL = 

class ScrapingEngine:
    """ 
    General purpose Scraping Engine Responsible For Extracting the Data for Different Usecases. 
    """
    daily_id_set = ExpiringDict(MAX_PAPERS_IN_CACHE,max_age_seconds=CACHE_PAPER_TTL)
    date_format = '%Y-%m-%d'

    def __init__(self,database:ArxivDatabase,selected_class='cs'):
        self.db = database
        self.selected_class = selected_class
        self.logger = create_logger(self.__class__.__name__)

    def __call__(self):
        """
        Create the `ArxivIdentity` Records and Add them to the database. 
        """
        raise NotImplementedError()    
    
    def update_cache(self,paper_id):
        self.daily_id_set[paper_id] = 1

    def filter_results(self,scraped_id_set:set):
        cached_keys = set(self.daily_id_set.keys())
        new_keys = scraped_id_set - cached_keys
        if len(new_keys) > 0:
            return list(new_keys)
        return []
    
    def _scrape(self,start_date,end_date):
        try:
            scraper = Scraper(category=self.selected_class, date_from=start_date,date_until=end_date)
            output = scraper.scrape()
            if output is 1:
                return []
        except Exception as e:
            raise e
        scraped_id_set = set(map(lambda x : x['id'],output))
        self.logger.info("Scraped %d Records From %s To %s"%(len(scraped_id_set),start_date,end_date))
        cache_miss = self.filter_results(scraped_id_set)
        db_check_records = list(filter(lambda x : x['id'] in cache_miss,output))
        return db_check_records 

    @staticmethod
    def _is_rejected_id(id_str):
        try:
            [int(i) for i in id_str.split('.')]
        except:
            return True
        return False

    def _save_oa2_identity(self,oa2_record):
        """_save_oa2_identity 
        Save's Identity or Puts it in the Cache. Else Doens't put it in cache.
        :param oa2_record: [description]
        :type oa2_record: [type]
        """
        if self._is_rejected_id(oa2_record['id']):
            return False
        db_retrieved_identity = self.db.query(oa2_record['id'])
        if db_retrieved_identity: # If we can find the Doc in DB then continue as there is no more need to check/Add
            self.update_cache(db_retrieved_identity.identity)
            return False
        
        # There was nothing in the DB so we create Identity. 
        retrieved_identity = ArxivIdentity.from_oa2_response(oa2_record)
        self.db.save_identity(retrieved_identity)
        self.update_cache(retrieved_identity.identity)
        # Save Semantic parsed result too. It will be used 
        self.db.set_semantic_parsed_research(\
            ArxivSematicParsedResearch(\
                identity=retrieved_identity,\
                research_object=ResearchPaperFactory.from_arxiv_record(ArxivRecord(identity=retrieved_identity))\
            )\
        )
        return True

class DailyScrapingEngine(ScrapingEngine):
    """ ## DailyScrapingEngine
    - Responsible for scraping Records for daily from Arxiv For a Particular Class. 
        - Default is CS. 
    - Will be used with a Cron to quickly extract Daily data and same object instance will be used.  
    - This will be responsible for Extracting the New records every day. 
    """

    def __init__(self, database,selected_class='cs'):
        super().__init__(database,selected_class=selected_class)
        

    def get_date_ranges(self):
        today = datetime.datetime.now().strftime(self.date_format) 
        yesterday = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime(self.date_format) 
        return (today,yesterday)

    def __call__(self):
        scrape_status = []
        today,yesterday  = self.get_date_ranges()
        try : 
            db_check_records = self._scrape(yesterday,today)
        except Exception as e:
            self.logger.error(e)
            time.sleep(self.timeout_per_scrape)
            self.logger.error("Failure At Scraping For Dates %s To %s"%(today,yesterday))
            self.logger.error("%s"%str(e))
            return 

        for cache_missed_paper in db_check_records:
            scrape_status.append(self._save_oa2_identity(cache_missed_paper))
        new_added = sum([1 for st in scrape_status if st])
        self.logger.info("Saved %d Records For %s To %s"%(new_added,yesterday,today))
        

class MassDataHarvestingEngine(ScrapingEngine):
    """MassDataHarvestingEngine
    Ment To harvest Data until a `scrape_until` date in the Past for a `selected_class`. 
    Requires a `selected_class` because of `arxivscraper.Scraper`'s requirements. 
    
    """
    
    def __init__(self, database,selected_class='cs',start_date = DEFAULT_SCRAPE_UNTIL, end_date = datetime.datetime.now(),timeout_per_scrape=5):
        super().__init__(database)
        self.scraping_dates = self.get_date_arr(start_date,end_date)
        self.timeout_per_scrape = timeout_per_scrape

    def get_date_arr(self,start_date:datetime.datetime,end_date:datetime.datetime):
        delta = end_date - start_date
        date_arr = []
        for i in range(delta.days + 1):
            if i==0:
                continue
            save_st_day = start_date + datetime.timedelta(days=i-1)
            save_end_day = start_date + datetime.timedelta(days=i)
            date_arr.append((save_st_day.strftime(self.date_format),save_end_day.strftime(self.date_format)))
        
        return date_arr
        
    def __call__(self):
        for start_date,end_date in self.scraping_dates:
            scrape_status = []
            try : 
                db_check_records = self._scrape(start_date,end_date)
            except:
                time.sleep(self.timeout_per_scrape)
                self.logger.error("Failure At Scraping For Dates %s To %s"%(start_date,end_date))
                continue
            for cache_missed_paper in db_check_records:
                scrape_status.append(self._save_oa2_identity(cache_missed_paper))
            new_added = sum([1 for st in scrape_status if st])
            self.logger.info("Saved %d Records For %s To %s"%(new_added,start_date,end_date))
            time.sleep(self.timeout_per_scrape) # Sleep For X Number of Seconds Before One Starts Again. 
                

    @classmethod
    def from_string_dates(cls,\
            database,\
            selected_class='cs',\
            start_date='2020-05-01',\
            end_date='2020-05-03',\
            timeout_per_scrape=5):
        
        start_date = datetime.datetime.strptime(start_date,cls.date_format)
        end_date = datetime.datetime.strptime(end_date,cls.date_format)
        return cls(database,\
            selected_class=selected_class,\
            start_date=start_date,\
            end_date=end_date,\
            timeout_per_scrape=timeout_per_scrape)

class HarvestingThread(Thread):
    def __init__(self,scraping_engine:ScrapingEngine):
        super().__init__(name=self.__class__.__name__)
        self.exit = Event()
        self.scraping_engine = scraping_engine
        self.scraping_engine.logger.info("Starting Scraping Engine")
        signal(SIGINT, self.shutdown)
    
    def shutdown(self,signal_received, frame):
        # Handle any cleanup here
        self.scraping_engine.logger.info('SIGINT or CTRL-C detected. Exiting gracefully')
        self.exit.set()
        exit(0)

    def run(self):
        raise NotImplementedError()

class DailyHarvestationThread(HarvestingThread):
    def __init__(self, scraping_engine,timeout_per_scrape=600):
        super().__init__(scraping_engine)
        self.timeout_per_scrape = timeout_per_scrape
    
    def run(self):
        while True:
            if self.exit.is_set():
                break
            self.scraping_engine()
            self.scraping_engine.logger.info("Sleeping For %d Seconds Post Scraping "%self.timeout_per_scrape)
            time.sleep(self.timeout_per_scrape)


class MassHarvestationThread(HarvestingThread):
    
    def __init__(self, scraping_engine):
        super().__init__(scraping_engine)
    
    def run(self):
        self.scraping_engine()

class HarvestingProcess(Process):
    def __init__(self,scraping_engine:ScrapingEngine):
        super().__init__(name=self.__class__.__name__)
        self.exit = Event()
        self.scraping_engine = scraping_engine
        self.scraping_engine.logger.info("Starting Scraping Engine")
        signal(SIGINT, self.shutdown)
    
    def shutdown(self,signal_received, frame):
        # Handle any cleanup here
        self.scraping_engine.logger.info('SIGINT or CTRL-C detected. Exiting gracefully')
        self.exit.set()
        exit(0)

    def run(self):
        raise NotImplementedError()

class DailyHarvestationProcess(HarvestingProcess):
    def __init__(self, scraping_engine,timeout_per_scrape=600):
        super().__init__(scraping_engine)
        self.timeout_per_scrape = timeout_per_scrape
    
    def run(self):
        while True:
            if self.exit.is_set():
                break
            self.scraping_engine()
            self.scraping_engine.logger.info("Sleeping For %d Seconds Post Scraping "%self.timeout_per_scrape)
            time.sleep(self.timeout_per_scrape)

class MassHarvestationProcess(HarvestingProcess):
    def __init__(self, scraping_engine):
        super().__init__(scraping_engine)
    
    def run(self):
        self.scraping_engine()