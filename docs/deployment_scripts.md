# Running the Damn Thing. 

All scripts in the [scripts folder](https://github.com/valayDave/arxiv-miner/tree/oss-release/scripts) consist of the scripts needed to scrape and parse content for storage in ArXiv. The `default_config.ini` file contains the `elasticsearch` configuration needed to run most scripts. 

## Data Extraction

`scripts/scrape_papers.py` will tap into feed provided by ArXiv from [this URL](http://export.arxiv.org/oai2?verb=ListRecords) to store records for futher mining. It will start the data extraction according to arguments. This step is done to only scrape for new content or content which has changed.

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

## Source Harvestation To S3
`scripts/mass_source_harvest.py` extracts the LaTeX sources from ArXiv and stores them in S3. 

```sh
python scripts/mass_source_harvest.py --max-chunks 200 > /home/ubuntu/arxiv-miner/mass_harvet.log &
```