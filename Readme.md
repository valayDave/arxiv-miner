# Arxiv Miner. 

Repository Helps Mine Arxiv Papers to quickly Scrape through new Papers and Mine data for Faster Readings. 

## Setup

```sh
sh setup.sh
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
    - Parsing/Analysing Event Handlers triggered on completion of Scraping Crons. 
    - ArxivRecord Database. 


2. Setup A Elastic search node On GCP after fixing the 

## BROADER GOAL
1. The goal of this project is to annotate and build faster search around research papers so that I can be quickly aware of what is happening in the domain. 
2. It is also ment to structure research papers in searialisable JSON so that I can start annotating research and fixing things around the same. 

# How can One get there ?

## ARXIV PAPER MINING

### GOAL OF PAPER MINING 
Parse the Arxiv Latex/PDF into A research Paper Object which can be serialised so that It is in readable format for some form of Machine learning/Annoation methods. But it all starts from cleaning the Dirt from Arxiv. 

### WAY TO DO IT 
1. Extract Papers from `Arxiv` using Andrej karapathy's `fetch_papers.py` script. It fills the `db.p` Pickle File.
2. `arxiv_multiprocess_download.py` will download the Latex version of the Papers for Arxiv and create and `ArxivPaper` object.  
3. The `ArxivPaper` Object created extracts the Latex source from the Arxiv and parses it too.
    - Three things will help solve the Information mining Problem. 
        1. Extraction of Document Structure/hierarchy via Python-Latex Libraries like `tex2py`. 
        2. Extraction of Text from Latex Document Using `detex` : https://github.com/pkubowicz/opendetex
        3. Collate with the Tree with the text based on hierachical traversal of tree and text-splittig based search to collate the information. 
    - These things are Managed using the child classes of `LatexInformationParser`. These child classes will help for the Structured `Section` objects which contains the stored parsed structure of the Research paper. 


# How Does it Work ? 

## Overview 
- Parts of Current System : 
    - `ArxivDatabase` : Core class to expose base methods for interfacing with DB. Current DB uses `ArxivDatabaseService(rpyc.Service,ArxivFSDatabase)`. This DB operates on FS. The DB adapter can be later switched with a production database. 
    - `HarvestingProcess` : This uses a `ScrapingEngine` to extract `ArxivIdentity` from ArXiv API(`http://export.arxiv.org/oai2?verb=ListRecords`). The Data extracted is stored to the database. `DailyHarvestationProcess` helps retrieve data daily papers. `MassHarvestationProcess` gets data based on DateRange. 
    - `MiningProcess`: Helps mine the papers for `LaTeX` information. The mined `ArxivRecord` is stored in the Database 
    
- The Database provides a Way to Create/Update `ArxivRecord`. The `ArxivRecord` contains an `ArxivIdentity` which is extracted using the `arxiv_miner.scraping_engine.ScrapingEngine`. `ArxivRecord` is the Fundamental Datastructure use to identify a research paper. `ArxivPaper` is a processing Object which can use a `ArxivRecord` to start the mining process. 

## Running the Damn Thing. 
- Start Database Server with Below Command . The Database Server is responsible For Managing the data. More Info can be found in `arxiv_miner.database`
    ```sh
    python database_server.py
    ```
- Start the Data Harvester according to your requirements. Can perform a `daily-harvest` or a `date-range` harvest
    ```sh
    python scrape_papers.py --help
    ```
- Start the Miner To parallely start mining the Extracted data. 
    ```sh
    python mine_papers.py --help
    ```

# TODO / VISION

- Currently Papers are downloaded,parsed and stored to files. 
    - This is fast for currently building a strong core datastructure and fast development, But the Data needs to transition to a Search Engine for faster retrieval.
