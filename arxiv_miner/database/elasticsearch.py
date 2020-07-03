from .core import ArxivDatabase
from ..record import ArxivRecord,ArxivIdentity,ArxivPaperStatus
from ..paper import ArxivPaper
from ..logger import create_logger
from ..exception import \
        ArxivDatabaseConnectionException,\
        ElasticsearchMissingException,\
        ElasticsearchIndexMissingException
try:
    import elasticsearch # Do a Safe Import Because of DataLayer Integration
    from elasticsearch.exceptions import NotFoundError
    from elasticsearch_dsl import Search,Q
except ImportError:
    raise ElasticsearchMissingException()

import random


class ArxivElasticSeachDatabaseClient(ArxivDatabase):
    def __init__(self,index_name=None,host='localhost',port=9200):
        if index_name == None:
            raise ElasticsearchIndexMissingException()
        self.index_name = index_name
        self.status_index_name = index_name+'_status'
        self.es = elasticsearch.Elasticsearch([{"host": host, "port": port}])
        if not self.es.ping():
            raise ArxivDatabaseConnectionException(host,port,'')
    
    def _get_paper(self,paper_id):
        es_record = None
        try:
            record_obj = self._get_record(paper_id)
            status_obj = self._get_status(paper_id)
        except NotFoundError as e:
            return es_record,None
        
        return record_obj,status_obj
    
    def _get_record(self,paper_id):
        es_record = self.es.get(index=self.index_name,id=paper_id)
        return self._record_from_source(es_record)
    
    def _get_status(self,paper_id):
        es_status = self.es.get(index=self.status_index_name,id=paper_id)
        return self._status_from_source(es_status)

    def _status_from_source(self,es_dict):
        """_status_from_source 
        Creates the datastructures from the JSON dict returned from ES. 
        :returns ArxivPaperStatus 
        """
        record_json = es_dict['_source']
        status = ArxivPaperStatus.from_json(dict(record_json)) 
        return status
    
    def _record_from_source(self,es_dict):
        """_record_from_source 
        Creates the datastructures from the JSON dict returned from ES. 
        :returns ArxivRecord 
        """
        record_json = es_dict['_source']
        return ArxivRecord.from_json(record_json)

    def _save_paper(self,record:ArxivRecord,status:ArxivPaperStatus):
        # Save the Record and the Status.
        self._save_record(record)
        self._save_status(status,record.identity)

    def _save_status(self,status:ArxivPaperStatus,identity:ArxivIdentity):
        status_doc = status.to_json()
        self.es.index(index=self.status_index_name,id=identity.identity,body=status_doc)

    def _save_record(self,record:ArxivRecord):
        record_doc = record.to_json()
        self.es.index(index=self.index_name,id=record.identity.identity,body=record_doc)
 
    def query(self,paper_id) -> ArxivRecord:
        """query [summary]
        Query Database for a paper_id. Return None is doesn't Exist. 
        :type paper_id: [str]
        """
        record,_ = self._get_paper(paper_id)
        return record

    def save_identity(self,identity:ArxivIdentity):
        """save [summary]
        Save the identity to Database. One Can Overwrite the Values. 
        """
        self._save_paper(ArxivRecord(identity=identity),ArxivPaperStatus(scraped=True))
        
    def get_unmined_paper(self) -> ArxivRecord:
        """get_unmined_data 
        Extract one Random Unmined Paper from database 
        and mark that paper as mining=True
        Finds Latest Undone docs, Find's status and hits archieve. 
        """
        # Find Unprocessed Document : `paper_processing_meta` holds that data. 
        query = Search(using=self.es, index=self.index_name)\
            .query('bool',**{'paper_processing_meta':None})\
            .sort('-identity.published')\
            .source(['_id'])

        query = query[0:30] # Extract 50 records
        resp = query.execute()
        
        id_list = list(map(lambda x: x.meta.id, resp.hits))
        id_filter = Q('terms',**{'_id':id_list})
        # Search status index
        query = Q ('bool',\
                must=[ # Must not contain this field 
                    Q('match',**{'mining':False}), 
                    Q('match',**{'mined':False})  
                ]  
            )
        s = Search(using=self.es, index=self.status_index_name) \
            .query(query)\
            .filter(id_filter)

        search_resp = s.execute()
        if len(search_resp.hits) == 0:
            return None
        
        hit_choice = random.choice(search_resp.hits.hits)
        
        status_dict = hit_choice.to_dict() # elasticsearch-dsl directly returns the doc.
        status = ArxivPaperStatus.from_json(status_dict['_source'])
        status.mining = True # Mark as Mining in status index
        status.update()
        
        record = self._get_record(status_dict['_id'])
        self._save_status(status,record.identity) # Update DB Document to avoid reextraction
        return record

    def set_mined(self,identity:ArxivIdentity,mined_status:bool) -> None:
        # Marks The paper according to mining bool returned.
        status = self._get_status(identity.identity)
        status.mining = False
        status.mined = mined_status
        status.update()
        self._save_status(status,identity)

    def save_record(self,record:ArxivRecord) -> None:
        """save_record 
        Save ArxivRecord which could be mined/unmined to database.
        """
        self._save_record(record)

    def archive(self):
        query = Q ()
        s = Search(using=self.es, index=self.status_index_name) \
            .query(Q())
        for es_record in s.scan():
            es_record