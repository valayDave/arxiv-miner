import os
import rpyc
from signal import signal,SIGINT
from ..record import ArxivRecord,ArxivIdentity,ArxivPaperStatus
from ..utils import dir_exists,save_json_to_file,load_json_from_file
from ..paper import ArxivPaper
from ..logger import create_logger
from ..exception import ArxivDatabaseConnectionException

class ArxivDatabase:
    """ 
    This Class Responsible for acting as a hook for getting 
    underlying Database client implementations. 
    The whole goal of this is to Adapter to is have minimal 
    effort in switching the data-storage layer without affecting the compute 
    layer.
    
    """
    
    def query(self,paper_id) -> ArxivRecord:
        """query [summary]
        Query Database for a paper_id. Return None is doesn't Exist. 
        :type paper_id: [str]
        """
        raise NotImplementedError()

    def save_identity(self,identity:ArxivIdentity):
        """save [summary]
        Save the identity to Database. One Can Overwrite the Values. 
        """
        raise NotImplementedError()

    def get_unmined_paper(self) -> ArxivRecord:
        """get_unmined_data 
        Extract one Unmined Paper from the collection of papers. 
        """
        raise NotImplementedError()

    def set_mined(self,identity:ArxivIdentity,mined_status:bool) -> None:
        """mark_mined 
        Set ArxivIdentity as Mined 
        """
        raise NotImplementedError()

    def save_record(self,record:ArxivRecord) -> None:
        """save_record 
        Save ArxivRecord which could be mined/unmined to database.
        """
        raise NotImplementedError()

    def pipeline_stats(self):
        raise NotImplementedError()

    def archive(self): # To Extract all the data from DB
        raise NotImplementedError()

