from .core import ArxivDatabase
from .elasticsearch import \
    ArxivElasticTextSearch,\
    ArxivElasticSeachDatabaseClient,\
    DateAggregation,\
    TermsAggregation,\
    KeywordsTextSearch,\
    CategoryFilterItem,\
    TextSearchFilter,\
    SearchResults,\
    FIELD_MAPPING,\
    DATE_FIELD_NAME

SUPPORTED_DBS = ['elasticsearch']

class DatabaseNotSupported(Exception):
    headline = 'DB_CLIENT_NOT_FOUND'
    
    def __init__(self,given_client):
        msg = "Database Client Not Supported %s :\n"%given_client
        msg = msg+'Supported Clients : %s'.format(' '.join(SUPPORTED_DBS))
        super(DatabaseNotSupported, self).__init__(msg)

def get_database_client(client_name):
    if client_name not in SUPPORTED_DBS:
        raise DatabaseNotSupported(client_name)
    elif client_name == 'elasticsearch':
        return KeywordsTextSearch
