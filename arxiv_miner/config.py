# TODO : Move this to configuration format where the entire thing comes from a YML file
import os
# global settings
# -----------------------------------------------------------------------------
class Config(object):
    default_database = 'elasticsearch' 
    elasticsearch_port = 9200
    elasticsearch_host = 'localhost'
    elasticsearch_index = 'arxiv_papers'
    es_auth = None # should be a tuple
    
    # Object Store 
    bucket_name = 'arxiv-papers-source-bucket'
    
    @classmethod
    def get_defaults(cls,db_str):
        if db_str == 'elasticsearch':
            return_dict = dict(\
                host=cls.elasticsearch_host,\
                port=cls.elasticsearch_port,\
                index_name = cls.elasticsearch_index)
            
            if cls.es_auth is not None:
                return_dict['auth']=cls.es_auth

            return return_dict
        else:
            return None

    @classmethod
    def get_db_defaults(cls):
        return cls.get_defaults(cls.default_database)
