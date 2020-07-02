""" 
Scraping Engine creates the Identiy Data for the papers. 
The Mining Engine on Instantiation 
    - Will check For the New Papers to Mine. 
    - It will Create a 
"""
from .database import ArxivDatabase
from .record import ArxivRecord,ArxivIdentity
from .logger import create_logger
from .exception import ArxivAPIException
from .paper import ArxivPaper
import time
from multiprocessing import Process,Event
from signal import signal, SIGINT
import random
import string

def random_string(stringLength=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


class MiningEngine:
    """ 
    Run as Isolated Process 
        - Query Database For Unmined Paper
        - Mine Paper : With ArxivPaper Object. 
            - If Arxiv Acts Bitchy with 500 Errors Wait and Mine. 
    """
    def __init__(self,\
            database:ArxivDatabase,\
            data_root_path,\
            detex_path):

        self.db = database
        self.data_root_path = data_root_path
        self.detex_path = detex_path
        self.logger = create_logger(self.__class__.__name__+"__"+random_string())

    def mine_record(self,paper_record:ArxivRecord):
        paper_obj = ArxivPaper.from_arxiv_record(self.data_root_path,\
                                                    paper_record,\
                                                    detex_path=self.detex_path)
        try:
            paper_obj.mine_paper()
        except Exception as e:
            self.logger.error('Failed Mining Paper : %s\n\n%s'%(paper_obj.identity.identity,str(e)))
            return None
        
        return paper_obj
    
    def _paper_mining_logic(self):
        """_paper_mining_logic 
        1. Get unmined ArxivRecord
        2. Create `AxivPaper` from `ArxivRecord` and mine it 
        3. save Record and Mark as Mined. 
        """
        paper_record = self.db.get_unmined_paper()
        paper_mined = False
        if not paper_record: 
            return None,paper_mined
        
        self.logger.info("Mining Paper %s"%paper_record.identity.identity)
        paper_obj = self.mine_record(paper_record)
        if paper_obj is not None:
            paper_mined = True
            self.db.save_record(paper_obj.to_arxiv_record())
            paper_record = paper_obj.to_arxiv_record()

        self.db.set_mined(paper_record.identity,paper_mined)
        
        return paper_record,paper_mined


class MiningProcess(Process,MiningEngine):
    def __init__(self,
            database:ArxivDatabase,\
            data_root_path,\
            detex_path,\
            mining_interval=5,\
            mining_limit=30,
            empty_wait_time = 600):
        
        # Instantiate The Processes
        Process.__init__(self,daemon=False) # Making it a deamon process. 
        MiningEngine.__init__(self,database,data_root_path,detex_path)
        
        self.mining_interval = mining_interval
        self.empty_wait_time = empty_wait_time
        self.mining_limit = mining_limit
        self.num_mined = 0
        self.exit = Event()
        signal(SIGINT, self.shutdown)
        
    def run(self):
        """run 
        This will Run the MongoEngine's Miner
        """
        self.start_mining()

    def shutdown(self,signal_received, frame):
        # Handle any cleanup here
        self.logger.info('SIGINT or CTRL-C detected. Exiting gracefully')
        self.exit.set()
        exit(0)

    def start_mining(self):
        while True:
            if self.mining_limit is not None:
                if self.num_mined == self.mining_limit:
                    break
            if self.exit.is_set():
                break
            time.sleep(self.mining_interval)
            paper_record,mined_status = self._paper_mining_logic()
            self.num_mined+=1
            if not paper_record: # Sleep If DB says There are Unmined Papers
                self.logger.info("No Record Found Sleeping For %d"%self.empty_wait_time)
                time.sleep(self.empty_wait_time)
                continue
                
            if mined_status is False:
                self.logger.error('Couldnt Mine Paper  : %s'%paper_record.identity.identity)
                time.sleep(self.empty_wait_time)
                continue
            self.logger.info('Saved Paper To DB : %s Completed Mining %d Paper'%(paper_record.identity.identity,self.num_mined))

        
        self.logger.info('Miner Mined : %d'%self.num_mined)
    