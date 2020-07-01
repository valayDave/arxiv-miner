import os
# global settings
# -----------------------------------------------------------------------------
class Config(object):
    # Based on `arxiv_miner.database.SUPPORTED_DBS`
    default_database = 'elasticsearch' 

    #FS  Database Related Configuration
    data_path = os.path.abspath('./data')
    fs_database_port = 18861
    fs_database_host = 'localhost'
    fs_database_config = {
        'allow_public_attrs': True,\
        'sync_request_timeout': 10\
    }
    
    elasticsearch_port = 9200
    elasticsearch_host = 'localhost'
    elasticsearch_index = 'arxiv_papers'
    # Mining Related Configuration
    detex_path = os.path.abspath('./detex')
    mining_data_path = os.path.abspath('./mining_data/papers')

    @classmethod
    def get_defaults(cls,db_str):
        if db_str == 'elasticsearch':
            return dict(\
                host=cls.elasticsearch_host,\
                port=cls.elasticsearch_port,\
                index_name = cls.elasticsearch_index
                )
        elif db_str == 'fs':
            return dict(\
                # data_path= cls.data_path,
                host=cls.fs_database_host,\
                port=cls.fs_database_port,\
            )
        else:
            return None

    @classmethod
    def get_db_defaults(cls):
        return cls.get_defaults(cls.default_database)
