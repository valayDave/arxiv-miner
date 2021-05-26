from .core import ArxivDatabase
from ..ontology_miner import Ontology
from ..record import \
        ArxivRecord,\
        ArxivIdentity,\
        ArxivPaperStatus,\
        ArxivSematicParsedResearch, Author, CoreOntology,\
        D2D
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


from typing import List
import random
import datetime
import dateparser
from typing import List
import json
from dataclasses import dataclass,field
import re
DEFAULT_TIME_RANGE = 30
from luqum.elasticsearch import ElasticsearchQueryBuilder
from luqum.parser import parser

import asyncio
from functools import wraps, partial

def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run 

class ArxivElasticSeachDatabaseClient(ArxivDatabase):
    def __init__(self,index_name=None,host='localhost',port=9200,auth=None):
        if index_name == None:
            raise ElasticsearchIndexMissingException()
        self.index_name = index_name
        self.status_index_name = index_name+'_status'
        self.parsed_research_index_name = index_name + '_parsed_research'
        if port is None:
            src_str = f'{host}'
        else:
            src_str = f'{host}:{port}'
        if auth is None:
            self.es = elasticsearch.Elasticsearch(src_str,timeout=30, max_retries=10)
        else:
            self.es = elasticsearch.Elasticsearch(src_str,http_auth=auth,timeout=30, max_retries=10)
        if not self.es.ping():
            if port is None:
                raise ArxivDatabaseConnectionException(src_str,80,'')
            else:
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
    
    def _save_many_parsed_research(self,records:List[ArxivSematicParsedResearch]):
        newrecords = []
        for r in records:
            newrecords.extend([{
                "index":{
                    "_index": self.parsed_research_index_name,
                    "_id": r.identity.identity,

                }
            },r.to_json()])            
        # print(newrecords)
        self.es.bulk(newrecords)

    
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

    def set_many_parsed_research(self,records:List[ArxivSematicParsedResearch]):
        self._save_many_parsed_research(records)
        
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

    def get_id_list(self,id_list,page_number=1,page_size=10,with_total=False):
        search_obj = Search(using=self.es, index=self.parsed_research_index_name)\
                        .query(Q('ids',**dict(values=id_list)))
        if page_size > 0:
            page_start = page_size*(page_number-1)
            page_end = page_size*(page_number)
            search_obj = search_obj[page_start:page_end]
        
        text_res = search_obj.execute()
        
        if not with_total:
            return [ArxivSematicParsedResearch.from_json(hit.to_dict()) for hit in text_res]
        else:
            return (text_res.hits.total.value, [ArxivSematicParsedResearch.from_json(hit.to_dict()) for hit in text_res])

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

REVERSE_FIELD_MAP = {v:k for k,v in zip(FIELD_MAPPING.keys(),FIELD_MAPPING.values())}

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
AUTHOR_FIELD_NAME = 'identity.authors.keyword'


@dataclass
class CategoryFilterItem:
    field_name:str = CATEGORY_FIELD_NAME
    match_type:str = 'AND'
    filter_values: list = field(default_factory=list)

class TextSearchFilter:
    """ 
    Used With the parsed Research index to quickly find and highlight data. 
    Lucene Text Query AAllowed 
    Eg. 
        string_match_query = '("brown fox" AND quick AND NOT dog)'
    """
    def __init__(self,\
                # Ids that act as addition filter layer
                id_vals=[],\
                # no_date : bool for not using date filter
                no_date = False,\
                # Text filter opt
                string_match_query="",\
                text_filter_fields = [],
                # Date filter opt
                start_date_key=None,\
                end_date_key = None,\
                date_filter_field = DATE_FIELD_NAME,\
                #Category filter :CategoryFilterItem  : use multi_category_filter or category_filter or category_filter_values,category_field,category_match_type
                category_filter = [],\
                category_filter_values =[],\
                category_field = CATEGORY_FIELD_NAME,\
                category_match_type= 'AND',\
                multi_category_filter=[], # multi_category_filter : [[CategoryFilterItem]]
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
        if multi_category_filter and len(multi_category_filter)>0:
            self.category_filter = self._multicategory_filter(multi_category_filter)
        elif category_filter and len(category_filter) > 0:
            self.category_filter = self._category_filter(category_filter)
        else:
            self.category_filter = self._subcategory_filter(category_filter_values,category_field,match_type=category_match_type)
        self.sort_key = sort_key
        self.sort_order = sort_order
        self.no_date = no_date
        # if self.sort_key is not None : 
        #     self.sort_key = '-'+self.sort_key if sort_order == 'descending' else self.sort_key
        self.highlights = highlights
        self.highlight_fragments =highlight_fragments
        self.page_number = page_number
        self.page_size = page_size
        self.source_fields = source_fields
        self.scan = scan
        self.id_vals = id_vals
        # self.sort_order = sort_order
    
    def __hash__(self): # For UI required Hashing to identify uniqueness of input. 
        return hash(''.join([str(v) for v in list(self.__dict__.values())]))

    def _multicategory_filter(self,category_filter_items:List[List[CategoryFilterItem]]):
        combined_query = None
        if len(category_filter_items) == 0:
            return combined_query
        for catblock in category_filter_items:
            query = self._dsl_category_filter(catblock)
            # print(query)
            if combined_query == None and query is not None:
                combined_query = query
            elif query is not None:
                combined_query = combined_query & query
        if combined_query is None:
            return combined_query
        return combined_query.to_dict()


    def _dsl_category_filter(self,category_filter_items:List[CategoryFilterItem]):
        combined_query = None
        if len(category_filter_items) == 0:
            return combined_query
        for cat in category_filter_items:
            if len(cat.filter_values) == 0:
                continue
            for fv in cat.filter_values:
                ph = dict()
                ph[cat.field_name] = fv
                if combined_query is None: 
                    combined_query = Q('match_phrase',**ph)
                else:
                    if cat.match_type == 'AND':
                        combined_query = combined_query & Q('match_phrase',**ph)
                    else:
                        combined_query = combined_query | Q('match_phrase',**ph)
        if combined_query is None:
            return combined_query
        return combined_query

    def _category_filter(self,category_filter_items:List[CategoryFilterItem]):
        combined_query = None
        if len(category_filter_items) == 0:
            return combined_query
        for cat in category_filter_items:
            for fv in cat.filter_values:
                ph = dict()
                ph[cat.field_name] = fv
                if combined_query is None: 
                    combined_query = Q('match_phrase',**ph)
                else:
                    if cat.match_type == 'AND':
                        combined_query = combined_query & Q('match_phrase',**ph)
                    else:
                        combined_query = combined_query | Q('match_phrase',**ph)                
        if combined_query is None:
            return combined_query
        return combined_query.to_dict()


    @staticmethod
    def _subcategory_filter(category_filter_values,category_field,match_type='AND'):
        combined_query = None
        if category_field is None or category_filter_values is None:
            return combined_query
        if len(category_filter_values) == 0:
            return combined_query
        # phrase_matches = []
        # combined_query
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
            
        ]
        # Filters wont apply any dates. 
        if not self.no_date:
            final_filter.append(
                Q('range',**self.date_filter)
            )
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

        if len(self.id_vals) > 0:
            # if id_vals are present then add filter to those too. 
            quer = quer & Q('ids',**{'values':self.id_vals})

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
            
        # print(json.dumps(search_obj.to_dict(),indent=4))
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
    def __init__(self,identity=None,result_locations=[],num_results=0,highlight_dict={},hightlight_frags=[],ontology=Ontology()):
        self.identity =identity
        self.result_locations = result_locations
        self.num_results = num_results
        self.highlight_dict = highlight_dict
        self.hightlight_frags = hightlight_frags
        self.ontology = ontology

    def to_json(self):
        self.identity
        return {
            'hightlight_frags' : self.hightlight_frags,
            'result_locations':self.result_locations,
            'num_results':self.num_results,
            'highlight_dict':self.highlight_dict,
            'identity' : self.identity.to_json(),
            'ontology': D2D(self.ontology)
        }
class ArxivElasticTextSearch(ArxivElasticSeachDatabaseClient):
    annotation_remove_keys = ['identity.','research_object.','.text']

    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def text_search_scan(self,filter_obj:TextSearchFilter):
        search_query = filter_obj.query()
        
        search_generator = Search.from_dict(search_query)\
                            .using(self.es)\
                            .index(self.index_name)
        
        return search_generator
    
    def text_search(self,filter_obj:TextSearchFilter) -> List[SearchResults]:
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
            searchfragments = [

            ]
            meta_dict =hit.meta.to_dict()
            if 'highlight' in meta_dict:
                
                for key in list(meta_dict['highlight'].keys()):
                    frag_obj = {}
                    add_val = key 
                    for rm in self.annotation_remove_keys:
                        add_val = add_val.replace(rm,'')
                    highlights.append(add_val.title())
                    highlight_dict[add_val.title()] = meta_dict['highlight'][key]
                    frag_obj['title'] = add_val.title()
                    frag_obj['highlight'] = self.fragment_from_hightlight(meta_dict['highlight'][key])
                    searchfragments.append(frag_obj)
            ontology = {}
            if 'ontology' in hit.to_dict():
                ontology = dict(ontology=Ontology(**hit.to_dict()['ontology']))
            
            response.append(SearchResults(\
                    identity=ArxivIdentity(**hit.to_dict()['identity']),
                    result_locations = highlights,\
                    num_results=int(text_res.hits.total['value']),\
                    highlight_dict=highlight_dict,
                    hightlight_frags=searchfragments,
                    **ontology
                    ))
        return response
    
    @staticmethod
    def fragment_from_hightlight(hightlights:List[str],tag='em'):
        reg_str = f"<{tag}>(.*?)</{tag}>"
        return_arr = []
        for frag in hightlights:
            res = re.split(reg_str, frag)
            new_word = []
            highlight_frag = dict(
                words = re.findall(reg_str, frag),
                text = " ".join(re.split(reg_str, frag))
            )
            # print(highlight_frag)
            return_arr.append(highlight_frag)
        return return_arr
            

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

class KeywordsTextSearch(ArxivElasticTextSearch):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.authors_index = self.index_name+"_authors"
        self.ontology_index = self.index_name+"_ontology"
        self.authors_autocomplete_field_name = "name.suggest"
        self.ontology_autocomplete_field_name = "value.suggest"
    
    def set_author(self,author_obj:Author):
        author_dict = D2D(author_obj)
        self.es.index(
            self.authors_index,author_dict,id=author_obj.name
        )

    def set_many_authors(self,authorslist:List[Author]):
        newrecords = []
        for r in authorslist:
            newrecords.extend([{
                "index":{
                    "_index": self.authors_index,
                    "_id": r.name,

                }
            },D2D(r)])
        # print(newrecords)
        self.es.bulk(newrecords)

    def set_many_ontology(self,ontologylist:List[str]):
        newrecords = []
        for ontstr in ontologylist:
            newrecords.extend([{
                "index":{
                    "_index": self.ontology_index,
                    "_id": ontstr,

                }
            },D2D(CoreOntology(value=ontstr))])
        # print(newrecords)
        self.es.bulk(newrecords)

    def set_ontology(self,ontlist:List[str]):
        for ont_str in ontlist:
            ont = CoreOntology(value=ont_str)
            ont_dict = D2D(ont)
            self.es.index(
                self.ontology_index,ont_dict,id=ont.value
            )

    def autocomplete_authors(self,fragment:str,max_frags=10) -> List[str]:
        sugget_dict =dict(
                suggest=dict(
                    authors_completion = dict(
                        prefix=fragment,
                        completion=dict(
                            field=self.authors_autocomplete_field_name,
                            size=max_frags
                        )
                    )
                )
            )
        search_results = self.es.search(sugget_dict,index=self.authors_index)
        return_text = []
        if len(search_results['suggest']['authors_completion']) > 0:
            for suggest in search_results['suggest']['authors_completion'][0]['options']:
                txt = suggest['text']
                return_text.append(txt)
        
        return return_text
    
    def autocomplete_ontology(self,fragment:str,max_frags=10) -> List[str]:
        sugget_dict =dict(
                suggest=dict(
                    ontology_completion = dict(
                        prefix=fragment,
                        completion=dict(
                            field=self.ontology_autocomplete_field_name,
                            size=max_frags
                        )
                    )
                )
            )
        search_results = self.es.search(sugget_dict,index=self.ontology_index)
        return_text = []
        if len(search_results['suggest']['ontology_completion']) > 0:
            for suggest in search_results['suggest']['ontology_completion'][0]['options']:
                txt = suggest['text']
                return_text.append(txt)
        
        return return_text