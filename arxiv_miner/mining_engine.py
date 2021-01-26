""" 
Scraping Engine creates the Identiy Data for the papers. 
The Mining Engine on Instantiation 
    - Will check For the New Papers to Mine. 
    - It will Create a ArxivPaper 
                --> Download Latex 
                --> Parse Latex 
                --> Sematically Parse the Paper here too. 
                --> Mining Ontology Here Too
"""
from .database import ArxivDatabase
from .record import ArxivRecord,ArxivIdentity,ArxivSematicParsedResearch,Ontology,Author
from .logger import create_logger
from .exception import ArxivAPIException
from .ontology_miner import OntologyMiner
from .paper import ArxivPaper,ResearchPaperFactory
import time
from multiprocessing import Process,Event
from signal import signal, SIGINT
import random
import string
from typing import List

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
        3. Create `ArxivSematicParsedResearch` from `ArxivRecord` and save it .
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
        
        ontology = Ontology()
        if OntologyMiner.is_minable:
            ontology = OntologyMiner.mine_paper(paper_record.identity)
            try:
                self.db.set_many_ontology(ontology.union)
            except:
                self.logger.info("No Ontology Saved")
        else:
            self.logger.info("No Ontology Saved")

        try:
            self.db.set_many_authors( [Author(name=xp) for xp in paper_record.identity.authors])
        except:
            self.logger.info("No Author Saved")
        self.db.set_semantic_parsed_research(ArxivSematicParsedResearch(\
            identity=paper_record.identity,\
            research_object=ResearchPaperFactory.from_arxiv_record(paper_record),\
            ontology=ontology
        ))
        self.db.set_mined(paper_record.identity,paper_mined)
        
        return paper_record,paper_mined


class SourceHarvestingEngine:
    """ 
    Run as Isolated Process 
        - Given a List of Ids, It will download the Tar source and Put it into a folder. Otherwise it will wait for arxiv to Forgive :) 

    Args : 
        id_list : List[str] : list of strings that will be used for the 
    """
    def __init__(self,id_list:List[str],data_root_path,error_sleep_time=5,scrape_sleep_time=3):
        super().__init__()
        self.id_list = id_list
        self.data_root_path = data_root_path
        self.logger = create_logger(self.__class__.__name__+"__"+random_string())
        self.error_sleep_time = error_sleep_time
        self.scrape_sleep_time = scrape_sleep_time

    def harvest_one(self,arxiv_id:str):
        paper = ArxivPaper(arxiv_id,self.data_root_path,build_paper=False)
        download_path = paper.download_latex()
        return download_path

    def harvest(self):
        harvest_papers_paths = []
        retry_map = {

        }
        while len(self.id_list) > 0:
            arxiv_id = self.id_list.pop()
            try:
                download_path = self.harvest_one(arxiv_id)
                harvest_papers_paths.append((arxiv_id,download_path))
                time.sleep(self.scrape_sleep_time)
            except Exception as e: # Upon exception. Try 3 times by adding it back to list. If still Failure then dont use it. 
                self.logger.error(f"Latex Download {str(e)} For ID {arxiv_id}. Will be Sleeping for {self.error_sleep_time}")
                if arxiv_id in retry_map:
                    if retry_map[arxiv_id] > 3:
                        continue 
                    else:
                        retry_map[arxiv_id]+=1
                        self.id_list.append(arxiv_id)
                else:
                    retry_map[arxiv_id]=1
                    self.id_list.append(arxiv_id)
                time.sleep(self.error_sleep_time)
        
        return harvest_papers_paths
        
class MiningProcess(Process,MiningEngine):
    def __init__(self,
            database:ArxivDatabase,\
            data_root_path,\
            detex_path,\
            mining_interval=5,\
            mining_limit=30,
            empty_wait_time = 600,
            sleep_interval_count = 10):
        
        # Instantiate The Processes
        Process.__init__(self,daemon=False) # Making it a deamon process. 
        MiningEngine.__init__(self,database,data_root_path,detex_path)
        
        self.mining_interval = mining_interval
        self.empty_wait_time = empty_wait_time
        self.mining_limit = mining_limit
        self.num_mined = 0
        self.exit = Event()
        self.sleep_interval_count = sleep_interval_count
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
            # Sleep Every `sleep_interval_count` records 
            if self.num_mined % self.sleep_interval_count == 0 and self.num_mined > 0:
                time.sleep(self.empty_wait_time)
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
    