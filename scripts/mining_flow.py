from metaflow import FlowSpec, step, Parameter, IncludeFile,retry,catch
import click
from arxiv_miner.config import Config
import configparser
from arxiv_miner import MiningEngine
import datetime
import os
import stat
from arxiv_miner.metaflow_db_connection import DatabaseParameters


DEFAULT_MINING_LIMIT=300

class MiningFlow(FlowSpec,DatabaseParameters):

    detex_binary = IncludeFile('detex-binary',default='./detex',is_text=False,help='Path To Detex Binary For Latex Processing')
    mining_limit = Parameter('mining-limit',default=DEFAULT_MINING_LIMIT,help='Maximum Number of Papers To Mine')

    def _to_file(self,file_bytes,is_executable=False):
        """
        Returns path for a file. 
        """
        import tempfile
        latent_temp = tempfile.NamedTemporaryFile(delete=True)
        latent_temp.write(file_bytes)
        latent_temp.seek(0)
        if is_executable:
            os.chmod(latent_temp.name,stat.S_IEXEC)
        return latent_temp
        

    def get_miner(self,db_conn,detex_file_path,mining_data_path):
        return MiningEngine(
                        db_conn,\
                        mining_data_path,
                        detex_file_path
                        )
    
    @step
    def start(self):
        self.limit = list(range(0,self.mining_limit))
        self.next(self.mine_papers,foreach='limit')
    
    @catch(var='mining_failed')
    @retry(times=4,minutes_between_retries=10)
    @step
    def mine_papers(self):
        import tempfile
        self.mining_failed = False
        db_con = self.get_connection()
        detex_file = self._to_file(self.detex_binary,is_executable=True)
        with tempfile.TemporaryDirectory() as temp_dir:
            miner = self.get_miner(db_con,detex_file.name,temp_dir)
            record,bool_stat = miner.run()
            self.mined_paper = record.to_json()
            self.status = bool_stat
            self.id = record.identity.identity
        self.next(self.join)
    
    @step
    def join(self,inputs):
        self.success_ids = []
        for inpt in inputs:
            if not inpt.mining_failed:
                self.success_ids.append(inpt.id)
        self.next(self.end)

    @step
    def end(self):
        print("Was able to complete mining %d / %d" %(len(self.success_ids),int(self.mining_limit)))

if __name__ == "__main__":
    MiningFlow()