# Core Components

## Scraping
[arxiv_miner/scraping_engine.py](https://github.com/valayDave/arxiv-miner/blob/master/arxiv_miner/scraping_engine.py) consists of the classes to tap into the feed from ArXiv and creates an [`ArxivRecord`](structures.md#ArxivRecord) in Elasticsearch. This is done so that records are only re-mined if necessary. Instructions to scrape data into Elasticsearch are provided [here](deployment_scripts.md#data-extraction).


## Mining and Parsing 
[arxiv_miner/mining_engine.py](https://github.com/valayDave/arxiv-miner/blob/master/arxiv_miner/mining_engine.py) consists of a process that mines papers which get scraped. [paper.py](https://github.com/valayDave/arxiv-miner/blob/master/arxiv_miner/paper.py) consists of the `ArxivPaper` class. This class extracts LaTeX source repository from remote source. Each LaTeX source repository is parsed to create a "Structure Tree" of the research document. The Structure tree is created using [tex2py](https://github.com/alvinwan/tex2py). The Structure tree helps correlate the structure of latex document. 

The structure tree is then used to create a `Section` object. More information about `Section` object can be found in [Core Structures](structures.md#Section) The `text` within each `Section` is populated by using the [opendetex library](https://github.com/pkubowicz/opendetex). The opendex library helps filter text information from individual tex files. A hacky algorithm based on number of tex files correlates the text with Structure Tree to create a single `Section`. 

Instructions to mine papers after scraping and index into Elasticsearch are provided [here](deployment_scripts.md#data-mining-and-storage).

### Standalone Paper Parsing 

```python
from arxiv_miner import ArxivPaper,ResearchPaperFactory
ROOT_DICTORY_TO_STORE_LATEX = './papers_root'
# This will extract LaTeX source from ArXiv parse the data to a `Section` Object
paper = ArxivPaper.from_arxiv_id('1706.03762',ROOT_DICTORY_TO_STORE_LATEX,detex_path='<PATH_TO_DETEX_BINARY>')
# The will create a `ResearchPaper`
paperdoc = ResearchPaperFactory.from_arxiv_record(paper) 
```


## Storage And Search
[arxiv_miner/database/elasticsearch.py](https://github.com/valayDave/arxiv-miner/blob/master/arxiv_miner/database/elasticsearch.py) consists of the core methods over **Elasticsearch** to search and aggregate data. Search and aggregation requires two classes : 
1.  *A wrapper class over Elasticsearch to execute the search and aggregate queries* : `KeywordsTextSearch` or `ArxivElasticTextSearch`
    - These classes contains methods that help retrieve information from the index containing the mined documents.
    ```python
    from arxiv_miner import KeywordsTextSearch 
    ELASTICARGS= dict(
        index_name=None,
        host='localhost',
        port=9200,
        auth=None
    )
    database = KeywordsTextSearch(**ELASTICARGS)

    ```
2. *A wrapper class to create the search and aggregation queries from input* : `TextSearchFilter`, `DateAggregation` and `TermsAggregation`
    - Search and aggregation wrappers are created using [elasticsearch_dsl](https://elasticsearch-dsl.readthedocs.io/en/latest/). 
    - `TextSearchFilter` is the core wrapper on what to search i.e. core filters over the indexed data after mining
    ```python
    from arxiv_miner import TextSearchFilter, DATE_FIELD_NAME, CATEGORY_FIELD_NAME, TEXT_HIGHLIGHT,CategoryFilterItem
    text_filter = TextSearchFilter(
        id_vals=[], # Explicitly filtering arxiv_id's in search/aggregation
        no_date = False, # no_date : bool for not using date filter
        string_match_query="", # Text filter opt
        text_filter_fields = [], # Specific fields in search index to filter. 
        start_date_key=None, # start date filter opt
        end_date_key = None, # end date filter opt
        date_filter_field = DATE_FIELD_NAME, # date field upon which date filter will be applied
        category_filter = [],# [CategoryFilterItem] Use `category_filter` or `multi_category_filter`
        category_filter_values =[], # if len(category_filter) > 0 the category_filter_values required
        category_field = CATEGORY_FIELD_NAME,
        category_match_type= 'AND',
        multi_category_filter=[], # multi_category_filter : [[CategoryFilterItem]]
        sort_key=DATE_FIELD_NAME, # Sort Key upon which search results will be ordered
        sort_order='descending',
        highlights = TEXT_HIGHLIGHT,# Search keys to highligh results fragments from
        highlight_fragments=60,
        source_fields=[],# Particular fields to restrict search on
        # Page settings 
        page_size=10,\
        page_number=1,
        scan=False# Full Dataset Traversal key # If scan==True, then no Pagination else paginate
    )
    ```
    - `DateAggregation` and `TermsAggregation` inherit `TextSearchFilter` to create aggregations for date and keywords for a particular search query. 

### Standalone Usage 

Paginated style results or iterator style retrieval of saved ArXiv Papers From Elasticsearch
```python
from arxiv_miner import KeywordsTextSearch,TextSearchFilter
ELASTICARGS= dict(
    index_name=None,
    host='localhost',
    port=9200,
    auth=None
)
database = KeywordsTextSearch(**ELASTICARGS)
# Pagination Style retrieval
text_filter = TextSearchFilter(
    string_match_query="out of distribution generalization",
    start_date_key='04/04/2015',
    end_date_key = '04/04/2021',
    page_size=100,
)
paginated_search_results = database.text_search(text_filter)
# Iterator style retrieval.
scan_text_filter = TextSearchFilter(
    string_match_query="out of distribution generalization",
    start_date_key='04/04/2015',
    end_date_key = '04/04/2021',
    scan=True
)
for doc in database.text_search_scan(scan_text_filter):
    handlestuff(doc)
```
