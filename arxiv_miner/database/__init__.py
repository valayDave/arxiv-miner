from .core import ArxivDatabase
from .elasticsearch import \
    ArxivElasticTextSearch,\
    ArxivElasticSeachDatabaseClient,\
    TextSearchFilter,\
    SearchResults,\
    FIELD_MAPPING

from .filesystem import ArxivFSDatabase
from .proxy_service import \
            ArxivFSDatabaseService,\
            ArxivDatabaseServiceClient

SUPPORTED_DBS = ['fs','elasticsearch']

class DatabaseNotSupported(Exception):
    headline = 'DB_CLIENT_NOT_FOUND'
    
    def __init__(self,given_client):
        msg = "Database Client Not Supported %s :\n"%given_client
        msg = msg+'Supported Clients : %s'.format(' '.join(SUPPORTED_DBS))
        super(DatabaseNotSupported, self).__init__(msg)

def get_database_client(client_name):
    if client_name not in SUPPORTED_DBS:
        raise DatabaseNotSupported(client_name)
    if client_name == 'fs':
        return ArxivDatabaseServiceClient
    elif client_name == 'elasticsearch':
        return ArxivElasticTextSearch
