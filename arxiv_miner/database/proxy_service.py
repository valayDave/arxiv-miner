""" 
This module helps exposing the FS based database as "Service" based module via `rpyc`
`rpyc` helps create direct remote callable python objects. This helps expose db as a service.
"""
import rpyc
from ..record import ArxivRecord,ArxivIdentity,ArxivPaperStatus
from ..exception import ArxivDatabaseConnectionException
from .filesystem import ArxivFSDatabase
from .core import ArxivDatabase

class ArxivFSDatabaseService(rpyc.Service,ArxivFSDatabase):
    """ArxivFSDatabaseService 
    This service will help expose an FS based DB as a server for clients to start calling. 
    This is useful if one doesn't-want/cant use Elasticsearch and still wants to mine data. 
    """
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.logger.info('Database Sever Started On %s'%self.papers_path)

    def on_connect(self, conn):
        # code that runs when a connection is created
        # (to init the service, if needed)
        self.logger.info("[CONN OPEN]: DB Currently Has %d Papers "%len(self.paper_map))
        pass
    
    def shutdown(self):
        self.paper_map.save_map()
        self.logger.info("Shutting Down The Server. Total Papers Records(Mined/Scraped) %d"%len(self.paper_map))

    def on_disconnect(self, conn):
        # code that runs after the connection has already closed
        # (to finalize the service, if needed)
        self.logger.info("[CONN CLOSE]: DB Currently Has %d Papers "%len(self.paper_map))
        pass

    def exposed_query(self,paper_id): # this is an exposed method
        return self.query(paper_id)

    def exposed_save_identity(self,identity:ArxivIdentity):  # while this method is not exposed
        return self.save_identity(identity)
 
    def exposed_get_unmined_paper(self):
        return self.get_unmined_paper()

    def exposed_set_mined(self,identity:ArxivIdentity,mined_status:bool):
        return self.set_mined(identity,mined_status)

    def exposed_save_record(self,record:ArxivRecord):
        return self.save_record(record)


class ArxivDatabaseServiceClient(ArxivDatabase):
    """
    This is in case any Data-layer Adapter needs to be exposed as a remote service 
    with `rypc`. This 
    """
    
    def __init__(self,host='localhost',port=18861):
        try:
            self.conn = rpyc.connect(host, port,config={'allow_public_attrs': True, 'sync_request_timeout': 10})
            self.client = self.conn.root
        except Exception as e:
            raise ArxivDatabaseConnectionException(host,port,str(e))
        
    
    def query(self,paper_id):
       return self.client.query(paper_id)

    def save_identity(self,identity:ArxivIdentity):
       return self.client.save_identity(identity)

    def get_unmined_paper(self):
        return self.client.get_unmined_paper()

    def set_mined(self,identity:ArxivIdentity,mined_status:bool):
        return self.client.set_mined(identity,mined_status)

    def save_record(self,record:ArxivRecord):
        return self.client.save_record(record)
