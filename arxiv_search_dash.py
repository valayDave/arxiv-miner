from typing import List
import streamlit as st
from config import Config
import dateparser
from streamlit.cli import main
import datetime
from functools import wraps
import arxiv_miner
from arxiv_miner import \
        ArxivElasticTextSearch,\
        TermsAggregation,\
        DateAggregation,\
        TextSearchFilter,\
        SearchResults,\
        FIELD_MAPPING,\
        COMPUTER_SCIENCE_TOPICS
import plotly.figure_factory as ff
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import click
import dateparser

from cli import database_choice,common_run_options
DEFAULT_APP_NAME = 'ArXiv-Search-Dashboard'
APP_HELP_STR = '''
<summary>
<i>Help</i>
</summary>
<details>
<p>
This is a search engine over the CS content in ArXiv. 
</p>
<p>
ArXiv Contains Over 400K+ Opensource Research papers in Computer Science
</p>
<p>
The purpose of this interface is to simplify search and give better Sematic context to the results of the search. 
</p>
<p>
'AND' ,'OR' , 'NOT', '()' are Reserved Keywords in the search text
</p>
<p>
Search Queries Can be of the free text form or of: 
<ul>
    <li> "deep learning" AND "data science" </li>
    <li> ("deep learning" AND "Transformer" ) OR ("Ensemble Learning")</li>
</ul>
</p>
<br/>
'''
class DataView():
    def __init__(self,database:ArxivElasticTextSearch):
        super().__init__()
        if type(database) != ArxivElasticTextSearch:
            raise("Elastic Search Required For Text Based DB Search")
        st.title("CS ArXiv Semantic Search")

        self.search_text = st.text_input("What Are you Looking For ?")
        self.view_opt = st.sidebar.selectbox("View Options",['Search','Analytics'])

        st.sidebar.title("Search Options")
        
        self.db = database
        
        self.start_date = st.sidebar.date_input('From Date ?',(datetime.datetime.now()-datetime.timedelta(days=30)))
        self.end_date = st.sidebar.date_input('To Date ?',datetime.datetime.now(),max_value=datetime.datetime.now())
        topics = arxiv_miner.get_cs_topics()
        self.selected_topics = st.sidebar.multiselect("Select CS Topics To Look For :",topics,None,lambda x:arxiv_miner.COMPUTER_SCIENCE_TOPICS[x])
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

        if len(self.search_text) > 0:
            self.text_filter_sections = st.sidebar.multiselect("Do you Want to Look Specifically in some section of the a Paper's Research ?",list(FIELD_MAPPING.keys()),None,lambda x: FIELD_MAPPING[x])
        else:
            self.text_filter_sections = []
        st.sidebar.markdown(APP_HELP_STR,unsafe_allow_html=True)

        if self.view_opt == 'Analytics':
            self._run_charts()
        else:
            self._run_search()
        
        # Make A Reading list. 
    
    def _run_charts(self):
        fig2 =  go.Figure(
            data=[self._run_date_distribution_chart()],
            layout=go.Layout(
                title=go.layout.Title(text="Date Distribution of Papers"),\
                xaxis=go.layout.XAxis(title='Date'),\
                yaxis=go.layout.YAxis(title='Articles Per %s'%self.choose_time_buckets(str(self.start_date),str(self.end_date)))\
            )
        )
        st.plotly_chart(fig2)
        fig = go.Figure(
            data=[self._run_category_distribution_chart()],
            layout=go.Layout(
                title=go.layout.Title(text="Category Distribution Of the Domains")
            )
        )
        st.plotly_chart(fig)

    def _run_category_distribution_chart(self):
        agg_resp = self.db.text_aggregation(
            TermsAggregation(
                string_match_query=self.search_text,\
                text_filter_fields=self.text_filter_sections,\
                start_date_key=str(self.start_date),\
                end_date_key=str(self.end_date),\
                category_filter_values=self.selected_topics,\
                page_number=self.page_number,\
                page_size=self.page_size,\
            )
        )
        plot_x,plot_y = self._2d_plot_point_from_agg_metrics(agg_resp)
        plot_x = [x if x not in COMPUTER_SCIENCE_TOPICS else COMPUTER_SCIENCE_TOPICS[x] for x in plot_x]
        return go.Pie(labels=plot_x,values=plot_y)


    def _run_date_distribution_chart(self):
        agg_resp = self.db.text_aggregation(
            DateAggregation(
                string_match_query=self.search_text,\
                text_filter_fields=self.text_filter_sections,\
                start_date_key=str(self.start_date),\
                end_date_key=str(self.end_date),\
                category_filter_values=self.selected_topics,\
                page_number=self.page_number,\
                page_size=self.page_size,\
                calendar_interval=self.choose_time_buckets(str(self.start_date),str(self.end_date)),\
            )
        )
        plot_x,plot_y = self._2d_plot_point_from_agg_metrics(agg_resp)
        return go.Bar(x=plot_x,y=plot_y)
        

    @staticmethod
    def _2d_plot_point_from_agg_metrics(metrics_docs:List[dict]):
        plot_x = []
        plot_y = []
        for metric in metrics_docs:
            plot_x.append(metric['key'])
            plot_y.append(metric['metric'])
        return plot_x,plot_y

    
    @staticmethod
    def choose_time_buckets(start_date,end_date):
        time_delta = dateparser.parse(end_date) - dateparser.parse(start_date)
        if time_delta.days > 180:
            return "month"
        if time_delta.days > 60:
            return "week"
        return "day"


    def get_db_data(self):
        search_resp = self.db.text_search(TextSearchFilter(
                string_match_query=self.search_text,\
                text_filter_fields=self.text_filter_sections,\
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
        if len(search_resp) ==0:
            st.header("No Articles Found :( ")    
            return 
        st.markdown(DataView.badge_it("Found %d Articles"%search_resp[0].num_results,badge_type='badge-success'),unsafe_allow_html=True)
        for resp in search_resp:
            self.print_block(resp)
    
    @staticmethod
    def badge_it(tag,badge_structure='badge-pill',badge_type='badge-info'):
        return '<span class="badge {badge_structure} {badge_type}">'.format(badge_type=badge_type,badge_structure=badge_structure)+tag+'</span>'

    @staticmethod
    def print_block(block:SearchResults):
        # TODO : Add The following : 
            # Show context Highlight. 
            # Show Graph of Papers in the Time Period. 
            # Show Graph of Search result data present distribution
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

        
        url_tag = '<a href={url} target="_blank" style="text-align:right"><svg width="2em" height="2em" viewBox="0 0 16 16" class="bi bi-link" fill="currentColor" xmlns="http://www.w3.org/2000/svg"><path d="M6.354 5.5H4a3 3 0 0 0 0 6h3a3 3 0 0 0 2.83-4H9c-.086 0-.17.01-.25.031A2 2 0 0 1 7 10.5H4a2 2 0 1 1 0-4h1.535c.218-.376.495-.714.82-1z"/><path d="M9 5.5a3 3 0 0 0-2.83 4h1.098A2 2 0 0 1 9 6.5h3a2 2 0 1 1 0 4h-1.535a4.02 4.02 0 0 1-.82 1H12a3 3 0 1 0 0-6H9z"/></svg></a>'\
            .format(url=block.identity.url)

        total ='''
        <summary>
        <a href={url} target="_blank"><h3>{title}</h3></a>
        <div>
        {cats_html}{date_html}<div style="display:inline-block">{url_tag}</div>
        </div>
        <div>
        {findings_html}
        </div>
        </summary>
        <details>
        <p>
        {abstract}
        </p>
        </details>
        <br/>
        '''.format(title=block.identity.title,\
                    url_tag=url_tag,\
                    cats_html=cats_html,\
                    date_html=date_html,\
                    url=block.identity.url,\
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
    
if __name__=="__main__":
    # text_search_dashboard = wrap_db(text_search_dashboard,main)
    text_search_dashboard(True,None,None)