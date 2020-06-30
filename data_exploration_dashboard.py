import streamlit as st
from utils import Config 
from arxiv_miner import FSArxivLoadingFactory,ArxivLoader 
import pickle 
import pandas
import pandas
import dateparser
import arxiv_query

import numpy as np
import os
DETEX_PATH = os.path.abspath('./detex')
MAX_ARTICLES_PER_PAGE = 20
APP_MODES = [
    "Database Exploration",
    # "Instant URL Parsing", 
    "Dataset Exploration",
    "Source Code Lookup",
    "Arxiv Query Builder"
]
LOADER_FACTORY =  FSArxivLoadingFactory(detex_path=DETEX_PATH)

@st.cache(show_spinner=True,hash_funcs={ArxivLoader: id})
def get_paper_data(latex_papers = False):
    storage_path = Config.root_papers_path
    if not latex_papers :
        loader = ArxivLoader(storage_path,detex_path=DETEX_PATH)
    else:
        loader = LOADER_FACTORY.latex_parsed_loader(storage_path)
    return loader

@st.cache(show_spinner=True,hash_funcs={arxiv_query.ArxivLocalDatabase: id})
def get_local_paper_database():
    local_db = arxiv_query.ArxivLocalDatabase(Config.db_path)
    return local_db



def dataset_exploration():
    """source_lookup 
    This will hold all the needed functions for making the view. 
    """
    ltx_papers = st.checkbox('Show Latex Parsed Papers')
    loader = get_paper_data(latex_papers=ltx_papers)
    ids = list(range(len(loader)))
    id_value = st.selectbox("Select A Paper", ids, format_func=lambda x: loader[x].core_meta['title'])
    paper = loader[id_value]

    if paper.latex_parsed_document is None:
        st.markdown("# No Content Found")
        return 
    
    print_paper(paper.latex_parsed_document)

def print_paper(latex_parsed_document):
     
    content = latex_parsed_document.to_markdown() 
    human_readable_date =  dateparser.parse(latex_parsed_document.published).strftime("%d, %b %Y")
    title_str = '''
    # {title}\n
    '''.format(title=latex_parsed_document.title)
    st.title('%s'%latex_parsed_document.title)

    url_str = '''
    **URL : <{url}>**\n
    '''.format(url =latex_parsed_document.url)
    st.markdown('%s'%url_str)

    published_str = '''
    **Published ON : {published}**\n
    '''.format(published =human_readable_date)
    st.markdown(published_str)

    data_str = '''
    ## Latex Parsing Result
    '''
    st.markdown(data_str) 
    st.markdown(content)


def arxiv_query_builder():
    topics = arxiv_query.get_cs_topics()
    search_query = st.text_input("Key Words You Are Looking For ? Use | for OR Queries and & for AND Queries" )
    selected_topics = st.multiselect("Select Topics To Look For :",topics,None,lambda x:arxiv_query.COMPUTER_SCIENCE_TOPICS[x])
    topic_query_selection = st.radio("Topic AND OR Query ?",["AND","OR"])
    
    sort_by = st.radio("Sort By Options",arxiv_query.SORT_BY)
    sort_order= st.radio("Sort Order",arxiv_query.SORT_ORDER)
    btn_result = st.button('Run Query')
    if btn_result:
        parsed_objs,sq = arxiv_query.query_arxiv(selected_topics,search_query,topic_query_selection,sort_by=sort_by,sort_order=sort_order)
        # st.write("Found Reports %d"%len(parsed_objs))
        # st.write('%s'%sq)
        for obj in parsed_objs:
            obj.print_markdown_with_streamlit(st)
    

def set_match(search,query,match_all=False):
    search_set =set(search) 
    query_set = set(query)
    
    if match_all:
        if len(search_set - query_set) == 0:
            return True
        else:
            return False

    else:
        if len(search_set - query_set) < len(search_set):
            return True # Because the query contains something we are searching for. 

    return False

def database_exploration():
    paper_db = get_local_paper_database()
    df = paper_db.to_dataframe()
    topics = arxiv_query.get_cs_topics()
    df['published'] = pandas.to_datetime(df['published'])

    st.markdown("%s"%"# Local Database Exploration")
    selected_topics = st.multiselect("Select Topics To Look For :",topics,None,lambda x:arxiv_query.COMPUTER_SCIENCE_TOPICS[x])
    match_all = st.checkbox("Match All Topics ? ")
    search_mask = df['unfiltered_tags'].apply(lambda x : set_match(selected_topics,x,match_all=match_all))
    
    # todo : more search filters can come here. 
    search_df = df[search_mask]
    search_result = '## Results Found : %d'%len(search_df)
    st.markdown(search_result)
    if len(search_df) == 0:
        return
    
    # todo : Create Date histograms of publishing. 
    search_df = search_df.sample(min(len(search_df),MAX_ARTICLES_PER_PAGE))
    filtered_papers = paper_db.lookup_indices(search_df.index)
    for obj in list(filtered_papers):
        obj.print_markdown_with_streamlit(st,only_meta=True)


def init_app():
    # st.sidebar.title("What to do")
    app_mode = st.sidebar.selectbox("Choose the app mode",APP_MODES)
    
    if app_mode == "Dataset Exploration":
        dataset_exploration()
    elif app_mode == "Source Code Lookup":
        st.code(open('data_exploration_dashboard.py').read())
    elif app_mode == "Arxiv Query Builder":
        arxiv_query_builder()
    elif app_mode == 'Database Exploration':
        database_exploration()

if __name__=='__main__':
    init_app()

