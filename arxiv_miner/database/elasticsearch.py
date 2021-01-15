from .core import ArxivDatabase
from ..record import \
        ArxivRecord,\
        ArxivIdentity,\
        ArxivPaperStatus,\
        ArxivSematicParsedResearch
from ..utils import get_date_range_from_today
from ..paper import ArxivPaper
from ..logger import create_logger
from ..exception import \
        ArxivDatabaseConnectionException,\
        ElasticsearchMissingException,\
        ElasticsearchIndexMissingException
try:
    import elasticsearch # Do a Safe Import Because of DataLayer Integration
    from elasticsearch.exceptions import NotFoundError
    from elasticsearch_dsl import Search,Q,A
except ImportError:
    raise ElasticsearchMissingException()

import random
import datetime
import dateparser
from typing import List
import json

DEFAULT_TIME_RANGE = 30
from luqum.elasticsearch import ElasticsearchQueryBuilder
from luqum.parser import parser


class ArxivElasticSeachDatabaseClient(ArxivDatabase):
    def __init__(self,index_name=None,host='localhost',port=9200):
        if index_name == None:
            raise ElasticsearchIndexMissingException()
        self.index_name = index_name
        self.status_index_name = index_name+'_status'
        self.parsed_research_index_name = index_name + '_parsed_research'
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

    def _get_parsed_research(self,paper_id):
        es_research = self.es.get(index=self.parsed_research_index_name,id=paper_id)
        return self._parsed_research_from_source(es_research)
    
    @staticmethod
    def _parsed_research_from_source(es_dict):
        """_status_from_source 
        Creates the datastructures from the JSON dict returned from ES. 
        :returns ArxivSematicParsedResearch
        """
        record_json = es_dict['_source']
        return ArxivSematicParsedResearch.from_json(record_json)

    @staticmethod
    def _status_from_source(es_dict):
        """_status_from_source 
        Creates the datastructures from the JSON dict returned from ES. 
        :returns ArxivPaperStatus 
        """
        record_json = es_dict['_source']
        status = ArxivPaperStatus.from_json(dict(record_json)) 
        return status
    
    @staticmethod
    def _record_from_source(es_dict):
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
 
    def _save_semantic_parsed_research(self,record:ArxivSematicParsedResearch):
        record_doc = record.to_json()
        self.es.index(index=self.parsed_research_index_name,id=record.identity.identity,body=record_doc)
    
    
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
            # .query('bool',**{'paper_processing_meta.mined':None})\
        search_obj = Search()
        final_filter = [
            Q('range',**TextSearchFilter._date_query('03/01/2019','12/01/2019'))
        ]
        search = Search(using=self.es, index=self.index_name)
                # .query('bool',filter=final_filter)\
        query= search\
                .query(~Q('exists',field='paper_processing_meta.mined'))\
                .sort('-identity.published')\
                .source(['_id'])

        query = query[0:100] # Extract 50 records
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

    def get_semantic_parsed_research(self,paper_id):
        try: 
            return self._get_parsed_research(paper_id)
        except:
            return None
        
    def set_semantic_parsed_research(self,record:ArxivSematicParsedResearch):
        self._save_semantic_parsed_research(record)
    
    def record_stream(self) -> ArxivRecord:
        """record_stream
        Stream All records from ES. 
        TODO : Make it filterable and Queryable in the future. 
        :yield: [ArxivRecord]
        """
        search_obj = Search(using=self.es, index=self.index_name)\
                            .query(Q())
        for hit in search_obj.scan():
            yield ArxivRecord.from_json(hit.to_dict())

    def id_stream(self,id_list):
        for arxiv_id in id_list:
            record_obj , _ = self._get_paper(paper_id=arxiv_id)
            parsed_obj = self.get_semantic_parsed_research(paper_id=arxiv_id)
            yield (arxiv_id,record_obj,parsed_obj)

    def parsed_research_stream(self):
        """
        Stream all Parsed Semantic Research from DB 
        """
        search_obj = Search(using=self.es, index=self.parsed_research_index_name)\
                            .query(Q())
        for hit in search_obj.scan():
            yield ArxivSematicParsedResearch.from_json(hit.to_dict())

        
    def archive(self):
        query = Q ()
        s = Search(using=self.es, index=self.status_index_name) \
            .query(Q())
        for es_record in s.scan():
            es_record


FIELD_MAPPING = {
    'identity.title':'Title',
    'identity.abstract':'Abstract',
    'research_object.introduction.text':'Introduction',
    'research_object.related_works.text':'Related_works',
    'research_object.methodology.text':'Methodology',
    'research_object.experiments.text':'Experiments',
    'research_object.results.text':'Results',
    'research_object.conclusion.text':'Conclusion',
    'research_object.limitations.text':'Limitations',
    'research_object.dataset.text':'Dataset'
}
TEXT_HIGHLIGHT = [
    'identity.title',
    'identity.abstract',
    'research_object.introduction.text',
    'research_object.related_works.text',
    'research_object.methodology.text',
    'research_object.experiments.text',
    'research_object.results.text',
    'research_object.conclusion.text',
    'research_object.limitations.text',
    'research_object.dataset.text',
    'research_object.unknown_sections.subsections.name.keyword',
]

CATEGORY_FIELD_NAME = 'identity.categories.keyword'
DATE_FIELD_NAME = 'identity.published'
SOURCE_FIELDS = [
    'identity.*',
    'research_object.parsing_stats'
]
class TextSearchFilter:
    """ 
    Used With the parsed Research index to quickly find and highlight data. 
    Lucene Text Query AAllowed 
    Eg. 
        string_match_query = '("brown fox" AND quick AND NOT dog)'
    """
    def __init__(self,\
                # Text filter opt
                string_match_query="",\
                text_filter_fields = [],\
                # Date filter opt
                start_date_key=None,\
                end_date_key = None,\
                date_filter_field = DATE_FIELD_NAME,\
                #Category filter opt
                category_filter_values =[],\
                category_field = CATEGORY_FIELD_NAME,\
                category_match_type= 'AND',\
                # Sort Key And Ordee
                sort_key=DATE_FIELD_NAME,
                sort_order='descending',\
                # highlight opts : Aggregation sets this as [] default. 
                highlights = TEXT_HIGHLIGHT,\
                highlight_fragments=60,\
                # Source fields,
                source_fields=[],\
                # Page settings 
                page_size=10,\
                page_number=1,
                scan=False# Full Dataset Traversal key # If scan==True, then no Pagination else paaginate
                ):
        
        self.text_search_query = self._text_search_query(string_match_query,text_filter_fields)
        self.date_filter = self._date_query(start_date_key,end_date_key,date_range_key=date_filter_field)
        self.category_filter = self._category_filter(category_filter_values,category_field,match_type=category_match_type)
        self.sort_key = sort_key
        self.sort_order = sort_order
        # if self.sort_key is not None : 
        #     self.sort_key = '-'+self.sort_key if sort_order == 'descending' else self.sort_key
        self.highlights = highlights
        self.highlight_fragments =highlight_fragments
        self.page_number = page_number
        self.page_size = page_size
        self.source_fields = source_fields
        self.scan = scan
        # self.sort_order = sort_order
    
    def __hash__(self): # For UI required Hashing to identify uniqueness of input. 
        return hash(''.join([str(v) for v in list(self.__dict__.values())]))

    @staticmethod
    def _category_filter(category_filter_values,category_field,match_type='AND'):
        combined_query = None
        if category_field is None or category_filter_values is None:
            return combined_query
        if len(category_filter_values) == 0:
            return combined_query
        # phrase_matches = []
        combined_query
        for cat in category_filter_values:
            ph = dict()
            ph[category_field] = cat
            if combined_query is None: 
                combined_query = Q('match_phrase',**ph)
            else:
                if match_type == 'AND':
                    combined_query = combined_query & Q('match_phrase',**ph)
                else:
                    combined_query = combined_query | Q('match_phrase',**ph)
            
        return combined_query.to_dict()

    @staticmethod
    def _date_query(start_date_key,end_date_key,date_range_key='identity.published'):
        default_start_date,default_end_date = get_date_range_from_today(DEFAULT_TIME_RANGE)
        if start_date_key is None:
            start_date_key = default_start_date.isoformat()
        else:
            start_date_key = dateparser.parse(start_date_key).isoformat()
        if end_date_key is None:
            end_date_key = default_end_date.isoformat()
        else:
            end_date_key = dateparser.parse(end_date_key).isoformat()

        unroll_json = dict()
        unroll_json[date_range_key] = {
            "gte" : start_date_key,
            "lte":end_date_key
        }
        return unroll_json # return range dict


    @staticmethod
    def _text_search_query(string_match_query,text_filter_fields):    
        """_text_search_query
        '("deep learning" AND "Transformer") OR ("data parallelism")' below is the trans. One needs same
         {
          "bool": {
            "should": [
              {
                "bool": {
                  "filter": [
                    {
                      "multi_match": {
                        "type": "phrase",
                        "query": "deep learning",
                        "lenient": true
                      }
                    },
                    {
                      "multi_match": {
                        "type": "phrase",
                        "query": "Transformer",
                        "lenient": true
                      }
                    }
                  ]
                }
              },
              {
                "multi_match": {
                  "type": "phrase",
                  "query": "data parallelism",
                  "lenient": true
                }
              }
            ],
            "minimum_should_match": 1
          }
        },
        """
        field_options = {
            '*': {
                'match_type':'multi_match',
                'type': 'phrase',
                "lenient": True,
            }
        }
        if len(text_filter_fields) > 0:
            field_options['*']['fields']=tuple(text_filter_fields)

        es_builder = ElasticsearchQueryBuilder(default_field='*',field_options=field_options)
        if string_match_query =="":
            return None
        
        tree = parser.parse(string_match_query)
        # Dict Return needs to Convert to Elasticsearch DSL after Return.
        return es_builder(tree) 

    def query(self):
        """query 
        Return elasticsearch Dict for searching
        """
        search_obj = Search()
        final_filter = [
            Q('range',**self.date_filter)
        ]
        # Text Query setting
        if self.text_search_query is not None:
            query_type = list(self.text_search_query.keys())[0]
            final_filter.append(\
                Q(query_type,**self.text_search_query[query_type])\
            )

        # Category Query setting
        if self.category_filter is not None:
            query_type = list(self.category_filter.keys())[0]
            final_filter.append(\
                Q(query_type,**self.category_filter[query_type])\
            )

        # final Query setting
        quer = Q('bool',must=final_filter)
        
        # Sort key setting
        if self.sort_key is not None:
            order = dict()
            order[self.sort_key] = dict(
                    order='desc' if self.sort_order=='descending' else 'asc'
            )
            search_obj = search_obj.query(quer).sort(order)
        # Highlight setting
        for highlight in self.highlights:
            search_obj = search_obj.highlight(highlight,fragment_size=self.highlight_fragments)

        if not self.scan:
            # pagination happens here if no scan
            if self.page_size > 0:
                page_start = self.page_size*(self.page_number-1)
                page_end = self.page_size*(self.page_number)
                search_obj = search_obj[page_start:page_end]
        else:
            search_obj = search_obj[:0]

        if len(self.source_fields) > 0 :
            search_obj = search_obj.source(includes=self.source_fields)
            
        print(json.dumps(search_obj.to_dict(),indent=4))
        return search_obj.to_dict()

class Aggregation(TextSearchFilter):
    """Aggregation 
    Builds on TextSearchFilter to add aggregations for the filtered search. 
    :param agg_name: Name sent to the aggregation bucket. 
    """
    def __init__(self,\
                no_docs =True,\
                agg_name = '',\
                **kwargs):
        super().__init__(highlights=[],**kwargs)
        self.no_docs = no_docs
        if agg_name == '':
            raise Exception("Aggregation Name Needed")
        self.agg_name = agg_name
    
    def query(self):
        return super().query()
    
    @staticmethod
    def transform_resp(doc_list:List[dict],metric_name='doc_count'):
        arr = []
        for doc in doc_list:
            arr.append(dict(
                key=doc['key'],
                metric=doc[metric_name]
            ))
        return arr

class DateAggregation(Aggregation):
    
    def __init__(self,\
                calendar_interval= "day",
                agg_name='date_distribution',
                date_agg_field=DATE_FIELD_NAME,
                time_zone= "America/Phoenix",
                min_doc_count= 1,
                **kwargs):
        super().__init__(agg_name=agg_name,**kwargs)
        self.search_dict = dict(
            field  =date_agg_field,
            calendar_interval = calendar_interval,
            time_zone = time_zone,
            min_doc_count = min_doc_count
        )

    def query(self):
        query = super().query()
        search_obj = Search.from_dict(query)
        agg = A('date_histogram',**self.search_dict)
        search_obj.aggs.bucket(self.agg_name,agg)
        if self.no_docs:
            search_obj = search_obj[:0]
        return search_obj.to_dict()

    @staticmethod
    def transform_resp(doc_list:List[dict],metric_name='doc_count'):
        arr = []
        for doc in doc_list:
            arr.append(dict(
                key=dateparser.parse(doc['key_as_string']),
                metric=doc[metric_name]
            ))
        return arr

class TermsAggregation(Aggregation):
    def __init__(self, \
                field=CATEGORY_FIELD_NAME,\
                agg_name = 'terms_aggregation',\
                **kwargs):
        super().__init__(agg_name=agg_name,**kwargs)
        self.field = field

    def query(self):
        query = super().query()
        search_obj = Search.from_dict(query)
        agg = A('terms',field=self.field)
        search_obj.aggs.bucket(self.agg_name,agg)
        if self.no_docs:
            search_obj = search_obj[:0]
        return search_obj.to_dict()


class SearchResults:
    identity:ArxivIdentity = None
    result_locations:list = []
    def __init__(self,identity=None,result_locations=[],num_results=0,highlight_dict={}):
        self.identity =identity
        self.result_locations = result_locations
        self.num_results = num_results
        self.highlight_dict = highlight_dict


class ArxivElasticTextSearch(ArxivElasticSeachDatabaseClient):
    annotation_remove_keys = ['identity.','research_object.','.text']

    def __init__(self, index_name=None, host='localhost', port=9200):
        super().__init__(index_name=index_name, host=host, port=port)

    def text_search_scan(self,filter_obj:TextSearchFilter):
        search_query = filter_obj.query()
        
        search_generator = Search.from_dict(search_query)\
                            .using(self.es)\
                            .index(self.index_name)
        
        return search_generator
    
    def text_search(self,filter_obj:TextSearchFilter):
        # print(filter_obj.query())
        text_res = Search.from_dict(filter_obj.query())\
                .using(self.es)\
                .index(self.parsed_research_index_name)\
                .execute()
        response = []
        
        for hit in text_res:
            
            highlights = []
            highlight_dict = {

            }
            meta_dict =hit.meta.to_dict()
            if 'highlight' in meta_dict:
                for k in list(meta_dict['highlight'].keys()):
                    add_val = k 
                    for rm in self.annotation_remove_keys:
                        add_val = add_val.replace(rm,'')
                    highlights.append(add_val.title())
                    highlight_dict[add_val.title()] = meta_dict['highlight'][k]
            
            response.append(SearchResults(\
                    identity=ArxivIdentity(**hit.to_dict()['identity']),
                    result_locations = highlights,\
                    num_results=int(text_res.hits.total['value']),\
                    highlight_dict=highlight_dict
                    ))
        return response
            
    def text_aggregation(self,agg_obj:Aggregation):
        """
        Returns an aggregation of type : 
        ```json
            [{
                "key" : "",
                "metric" : ""
            }]
        ```
        """
        agg_query = agg_obj.query()
        text_agg_res = Search.from_dict(agg_query)\
                .using(self.es)\
                .index(self.parsed_research_index_name)\
                .execute()
        response = []
        # print(text_agg_res.agg)
        aggregation_buckets = text_agg_res.aggregations.to_dict()[agg_obj.agg_name]['buckets']
        return_buckets = agg_obj.transform_resp(aggregation_buckets)
        return return_buckets