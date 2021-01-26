# Arxiv Miner. 

Repository Helps Mine Arxiv Papers to quickly Scrape through new Papers and Mine data for Faster Readings. 

# BROADER GOAL
1. The goal of this project is to annotate and build faster search around research papers so that I can be quickly aware of what is happening in the domain. 
2. It is also ment to structure research papers in searialisable JSON so that I can start annotating research and fixing things around the same. 

# How can One get there ?

## ARXIV PAPER MINING

### GOAL OF PAPER MINING 
Parse the Arxiv Latex/PDF into A research Paper Object which can be serialised so that It is in readable format for some form of Machine learning/Annoation methods. But it all starts from cleaning the Dirt from Arxiv. 

### WAY TO DO IT 
1. Extract Papers from `Arxiv` using `scrape_papers.py` script. The `ArxivDatabase` will hold the `ArxivRecord`s.
2. `mine_papers.py` will download the Latex version of the Papers for Arxiv and create and `ArxivRecord` object.  
3. The `ArxivRecord` can is a base class to `ArxivPaper`. 
4. The `ArxivPaper` Object helps extract the Latex source from the Arxiv and parses it. 
    - Three things will help solve the Information mining Problem. 
        1. Extraction of Document Structure/hierarchy via Python-Latex Libraries like `tex2py`. 
        2. Extraction of Text from Latex Document Using `detex` : https://github.com/pkubowicz/opendetex
        3. Collate with the Tree with the text based on hierachical traversal of tree and text-splittig based search to collate the information. 
    - These things are Managed using the child classes of `LatexInformationParser`. These child classes will help for the Structured `Section` objects which contains the stored parsed structure of the Research paper. 
5. The Scaraped/Mined Papers are stored in a `fs` or `elasticsearch` based search engines. 


## Setup

```sh
sh setup.sh
```

### To Setup Ontology Miner: 

```sh
sh cso_setup.sh
```

## What is Done Yet : 

1. Arxiv PDF and LateX Extraction Pipeline
2. Arxiv Paper Parsing to JSON Objects using Latex and Python. --> Latex Based Symantically parsed Data Extraction :: READY 
3. Local Database Setup and Data Exploration. 

## What Needs to Be Done ?

1. Data Extraction And Pasing System Are pretty Well set from Database. 
    1. The Database Generation needs to move from Andrej's script to using the `arxivscraper` which uses the mass Metadata extraction.

2. Final System : 
    - Scraping Crons
    - Parsing Idempotent processes. 
        - TODO : Further parse
    - ArxivRecord Database with `fs` | `elasticsearch`
    - Search Interface
        - Daily Update of New Research
        - Search indexing for 


# How Does it Work ? 

## Overview 
- Parts of Current System : 
    - `ArxivDatabase` : Core class to expose base methods for interfacing with DB. It is an adapter that can work with an `filesystem` based database or `elasticsearch`. The purpose of the adapter is ment create an interopratable data layer that can switched according to requirement and need. 
    - Filesystem based DB uses `ArxivDatabaseService(rpyc.Service,ArxivFSDatabase)`. The `database_server.py` file helps create and FS based database server. 
    - `HarvestingProcess` : This uses a `ScrapingEngine` to extract `ArxivIdentity` from ArXiv API(`http://export.arxiv.org/oai2?verb=ListRecords`). 
        - The Data extracted is stored to the database as an `ArxivRecord`. 
        - `DailyHarvestationProcess` helps retrieve data daily papers. 
        - `MassHarvestationProcess` gets data based on DateRange. 
    - `MiningProcess`: Helps mine the papers for `LaTeX` information. The mined `ArxivRecord` is stored in the Database 
    
- The Database provides a Way to Create/Update `ArxivRecord`. The `ArxivRecord` contains an `ArxivIdentity` which is extracted using the `arxiv_miner.scraping_engine.ScrapingEngine`. `ArxivRecord` is the Fundamental Datastructure use to identify a research paper. `ArxivPaper` is a processing Object which can use a `ArxivRecord` to start the mining process. 

## Running the Damn Thing. 
- The `config.py` file contains the `Config` Object which is Singleton used for configuration across the project. 
- Start FS based Database Server with Below Command . The Database Server is responsible For Managing the data. Elasticsearch is also supported as a backend database. 
    ```sh
    python database_server.py
    ```
- Start the Data Harvester according to your requirements. Can perform a `daily-harvest` or a `date-range` harvest. 
    ```sh
    python scrape_papers.py --help
    ```
    - DB adapters can be switched. The `--use_defaults` will load the defaults of `--datastore` from `Config`.
        ```sh
        python scrape_papers.py --datastore elasticsearch --host localhost --port 18861 daily-harvest
        ```
- Start the Miner To parallely start mining the Extracted data. 
    ```sh
    python mine_papers.py --help
    ```
    - The Miner has the same database cli adapter as Scraper. 
    ```sh
    python mine_papers.py --datastore fs --use_defaults start-miner
    ```
- Source Harvest and Store to S3: 
    ```sh
    nohup /home/ubuntu/arxiv-miner/.env/bin/python /home/ubuntu/arxiv-miner/mass_source_harvest.py --max-chunks 200 > /home/ubuntu/arxiv-miner/mass_harvet.log &
    ```

-  Extract EC2 instance List from AWS
    ```
    aws ec2 describe-instances --region=us-east-1 --query 'Reservations[*].Instances[*].[InstanceId,Tags[?Key==`Name`].Value|[0],State.Name,PrivateIpAddress,PublicIpAddress]' --output table > instance_list.md
    ```
# TODO / VISION
1. Create a search interface for looking for research. 
2. Get daily analytics of the new research coming out 
3. Create reports and analytics for the new research
