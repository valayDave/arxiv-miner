import streamlit as st
from config import Config
import dateparser
from streamlit.cli import main
import datetime
from functools import wraps
import arxiv_miner
from arxiv_miner import \
        ArxivElasticTextSearch,\
        TextSearchFilter,\
        SearchResults

import click
import dateparser

from cli import database_choice,common_run_options
DEFAULT_APP_NAME = 'ArXiv-Search-Dashboard'

class DataView():
    def __init__(self,database:ArxivElasticTextSearch):
        super().__init__()
        if type(database) != ArxivElasticTextSearch:
            raise("Elastic Search Required For Text Based DB Search")
        st.markdown("# CS ArXiv Semantic Search")
        self.db = database
        self.search_text = st.text_input("What Are you Looking For ?")
        
        self.start_date = st.sidebar.date_input('Start Date',(datetime.datetime.now()-datetime.timedelta(days=30)))
        self.end_date = st.sidebar.date_input('End Date',datetime.datetime.now())
        topics = arxiv_miner.get_cs_topics()
        self.selected_topics = st.sidebar.multiselect("Select Topics To Look For :",topics,None,lambda x:arxiv_miner.COMPUTER_SCIENCE_TOPICS[x])
        self.page_number = st.sidebar.number_input('Page Number',
                                                    min_value=1,
                                                    value=1,
                                                    step=1,
                                                    )
        self.page_size = st.sidebar.number_input('Page Size',
                                                    min_value=0,
                                                    value=10,
                                                    step=10,
                                                    )
        self._run_search()

    # @st.cache(show_spinner=True,hash_funcs={list:id})
    def get_db_data(self):
        search_resp = self.db.text_search(TextSearchFilter(
                string_match_query=self.search_text,\
                start_date_key=str(self.start_date),\
                end_date_key=str(self.end_date),\
                category_filter_values=self.selected_topics,\
                page_number=self.page_number,\
                page_size=self.page_size,\
            )
        )
        return search_resp


    def _run_search(self):
        # str_txt = None if self.search_text == '' else self.search_text
        search_resp = self.get_db_data()
        if len(search_resp) > 0:
            st.markdown(DataView.badge_it("Found %d Articles"%search_resp[0].num_results,badge_type='badge-success'),unsafe_allow_html=True)
        for resp in search_resp:
            self.print_block(resp)
    
    @staticmethod
    def badge_it(tag,badge_structure='badge-pill',badge_type='badge-info'):
        return '<span class="badge {badge_structure} {badge_type}">'.format(badge_type=badge_type,badge_structure=badge_structure)+tag+'</span>'

    @staticmethod
    def print_block(block:SearchResults):
        human_readable_date = dateparser.parse(block.identity.published).strftime("%d, %b %Y")
        # st.markdown(head_string,unsafe_allow_html=True)
        cats = [arxiv_miner.COMPUTER_SCIENCE_TOPICS[cat] if cat in arxiv_miner.COMPUTER_SCIENCE_TOPICS else cat for cat in block.identity.categories]
        cats = [DataView.badge_it(cat) for cat in cats]
        cats = cats[:3]
        cats_html = '&nbsp;'.join(cats)
        date_html='<span class="badge badge-pill" style="text-align:left">'+human_readable_date+'</span>'
        details_html = '<p>'+block.identity.abstract+'</p></details>'
        findings_html = ''
        if len(block.result_locations) > 0:
            findings_html = '<i>Results Found in</i>&nbsp;&nbsp;'+'&nbsp;'.join([DataView.badge_it(cat,badge_type='badge-primary') for cat in block.result_locations])
        else:
            findings_html = '<span></span>'
        total ='''
        <summary>
        <h3>{title}</h3>
        <div>
        {cats_html}{date_html}
        </div>
        </summary>
        <details>
        <div>
        {findings_html}
        </div>
        <p>
        {abstract}
        </p>
        </details>
        <br/>
        '''.format(title=block.identity.title,\
                    cats_html=cats_html,\
                    date_html=date_html,\
                    findings_html=findings_html,\
                    abstract=block.identity.abstract)
        # total = cats_html +date_html + details_html
        st.markdown('%s'%total,unsafe_allow_html=True)


 



def get_db_obj(use_defaults,host,port,app_name=DEFAULT_APP_NAME):
    db_arg_obj = {}
    datastore = 'elasticsearch'
    args , client_class = database_choice(datastore,use_defaults,host,port)
    print_str = '\n %s Process Using %s Datastore'%(app_name,datastore)
    args_str = ''.join(['\n\t'+ i + ' : ' + str(args[i]) for i in args])
    click.secho(print_str,fg='green',bold=True)
    click.secho(args_str+'\n\n',fg='magenta')
    arxiv_database = client_class(**args)
    db_arg_obj['db_class'] = client_class
    db_arg_obj['db_args'] = args
    database_client = db_arg_obj['db_class'](**db_arg_obj['db_args'])
    return database_client


        
def text_search_dashboard(use_defaults,host,port,app_name=DEFAULT_APP_NAME):
    database_client = get_db_obj(use_defaults,host,port)
    DataView(database_client)
    # st.markdown("# CS ArXiv Semantic Search")
    # st.text_input("What Are you Looking For ?")
    # start_date = st.sidebar.date_input('Start Date',datetime.datetime.now())
    # end_date = st.sidebar.date_input('End Date',datetime.datetime.now())
    # topics = arxiv_miner.get_cs_topics()
    # selected_topics = st.sidebar.multiselect("Select Topics To Look For :",topics,None,lambda x:arxiv_miner.COMPUTER_SCIENCE_TOPICS[x])

if __name__=="__main__":
    # text_search_dashboard = wrap_db(text_search_dashboard,main)
    text_search_dashboard(True,None,None)