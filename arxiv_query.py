import arxiv
import dateparser
import pandas
import pickle
from utils import dir_exists
from constants import *

SORT_BY = ["relevance", "lastUpdatedDate", "submittedDate"]
SORT_ORDER =['descending','ascending']

def get_cs_labels():
    return list(COMPUTER_SCIENCE_TOPICS.values())

def get_cs_topics():
    return list(COMPUTER_SCIENCE_TOPICS.keys())

def wrap_brackets(string):
    RIGHT_BRACKET=")"
    LEFT_BRACKET="("
    return LEFT_BRACKET+string+RIGHT_BRACKET

def wrap_quotes(string):
    QUOTE='"'
    SPACE=' '
    return QUOTE+string.replace(' ',SPACE)+QUOTE


class ArxivRemoteObject:
    def __init__(self,arxiv_object):
        self.url = arxiv_object['id']
        self.title = arxiv_object['title']
        self.abstract = arxiv_object['summary']
        self.tags = ', '.join(list(map(lambda x : x['term'] if x['term'] not in ALL_TOPICS else ALL_TOPICS[x['term']],arxiv_object['tags'])))
        self.primary_category = arxiv_object['arxiv_primary_category']
        self.authors = ', '.join(arxiv_object['authors'])
        self.published = arxiv_object['published']
        
        # These are for search purposess. 
        self.unfiltered_tags = list(map(lambda x : x['term'],arxiv_object['tags']))

    def print_markdown_with_streamlit(self,st,only_meta=False):
        st.markdown('# %s'%self.title)
        st.markdown('## %s'%'Meta')
        human_readable_date = dateparser.parse(self.published).strftime("%d, %b %Y")
        st.markdown('''
            URL : {url}\n
            TAGS : {tags}\n
            Authors : {authors}\n
            Published : {human_readable_date}\n
            '''.format(**self.__dict__,human_readable_date=human_readable_date)
        )
        if not only_meta:
            st.markdown('## Abstract')
            st.markdown('%s'%self.abstract)


    def to_json(self):
        return dict(self.__dict__)
        

    def __str__(self):
        return """
        # {title}

        ## Meta
        URL : {url}
        TAGS : {tags}
        Authors : {authors}
        Published : {published}

        ## ABSTRACT
        {abstract}
        ---
        """.format(**self.__dict__)    


class ArxivLocalDatabase:
    def __init__(self,db_path):
        if not dir_exists(db_path):
            raise Exception("No Database File At : %s"%db_path)
        db = pickle.load(open(db_path, 'rb'))
        object_arr = db.values()
        for ob in object_arr:
            ob['authors']= list(map(lambda x : x['name'],ob['authors']))
        self.local_objects = [ArxivRemoteObject(i) for i in object_arr]
    
    def __getitem__(self,index):
        return self.local_objects[index]
    
    def to_dataframe(self):
        return pandas.DataFrame([i.to_json() for i in self.local_objects])

    def lookup_indices(self,indices):
        for i in indices: # Assumes indexes will be correct.
            yield self.local_objects[i]


def query_arxiv(categories,\
                search_text,\
                cat_concat_flag='AND',\
                sort_by=SORT_BY[0],\
                sort_order=SORT_ORDER[0],\
                max_chunk_results=10,\
                max_results=20\
                ):

    if len(categories) == 0 and search_text == '':
        return []

    search_query = build_arxiv_query(categories,search_text,cat_concat_flag=cat_concat_flag)
    query_result = arxiv.query(
        query=search_query,\
        max_chunk_results=max_chunk_results,\
        max_results=max_results,\
        iterative=True,
        sort_by=sort_by,
        sort_order=sort_order

    )
    return list(ArxivRemoteObject(paper) for paper in query_result()),search_query


def build_arxiv_query(categories,search_text,cat_concat_flag='AND'):
    SPACE=' '
    OR = SPACE+'OR'+SPACE
    AND = SPACE+'AND'+SPACE
    query = []
    if len(categories) > 0:
        categories = ['cat:'+cat for cat in  categories]
        cat_concat_op = OR if cat_concat_flag is 'OR' else AND
        query.append(cat_concat_op.join(categories))
    
    if search_text is not '':
        search_str = ["ti:"+wrap_quotes(search_text),"abs:"+wrap_quotes(search_text)]
        query.append(OR.join(search_str))
    
    if len(query) > 1:
        query = [wrap_brackets(q) for q in query]
        return AND.join(query)
        
    elif len(query) == 1:
        return query[0]
    
    return ''
    

# Todo Build Database From the already tooled methods that Are build here. 

class ArxivQueryInterface:
    """ 
    TODO: Scipt out queries which contain stuff the usually used. 
    """
    def __init__(self):
        pass
    # "cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML"