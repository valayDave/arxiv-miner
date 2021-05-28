# Running the Damn Thing. 

All scripts in the [scripts folder](https://github.com/valayDave/arxiv-miner/tree/master/scripts) consist of the scripts needed to scrape and parse content for storage in ArXiv. The `default_config.ini` file contains the `elasticsearch` configuration needed to run most scripts. General structure of the ini file is as follows:
```ini
[elasticsearch]
host = localhost
index = arxiv_papers
port = 9200
# auth = your_user_name your_super_secure_password
```

## Scraping / Data Extraction

`scripts/scrape_papers.py` will tap into feed provided by ArXiv from [this URL](http://export.arxiv.org/oai2?verb=ListRecords) to store records for further mining. It will start the data extraction according to arguments. This step is done to only scrape for new content or content which has changed.

`scripts/scrape_papers.py` provide two options:
- Extract new records which are published on the feed in the last 24 hours and store in DB.
```sh
python scripts/scrape_papers.py --with-config default_config.ini daily-harvest
```
- Extract records published in date range and store in DB.
```sh
python scripts/scrape_papers.py --with-config default_config.ini date-range --start_date '2020-05-29' --end_date '2020-06-30'
```

## Data Mining and Storage
`scripts/mine_papers.py` extracts the papers stored after scraping and extract LaTeX source and parses the data. 
```sh
python scripts/mine_papers.py --with-config default_config.ini start-miner
```
## Quick Streamlit Search Dashboard Over Stored Data
`scripts/arxiv_search_dash.py` runs a quick streamlit based dashboard to search and visualize search results stored after scraping and mining.  
```sh
streamlit run scripts/arxiv_search_dash.py -- --config default_config.ini
```
## Save LaTeX Source To S3 bucket
> This script needs some tweeks to make it more customizable

`scripts/mass_source_harvest.py` extracts the LaTeX sources from ArXiv and stores them in S3. 
```sh
python scripts/mass_source_harvest.py --max-chunks 200 > /home/ubuntu/arxiv-miner/mass_harvet.log &
```
