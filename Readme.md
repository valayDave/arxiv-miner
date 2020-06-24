# Arxiv Miner. 

Repository Helps Mine Arxiv Papers to quickly Scrape through new Papers and Mine data for Faster Readings. 

## What is Done Yet : 

1. Arxiv PDF and LateX Extraction Pipeline
2. Arxiv Paper Parsing to JSON Objects using Latex and Python. 


## BROADER GOAL
1. The goal of this project is to annotate and build faster search around research papers so that I can be quickly aware of what is happening in the domain. 
2. It is also ment to structure research papers in searialisable JSON so that I can start annotating research and fixing things around the same. 

# How can One get there ?

## ARXIV PAPER MINING

### GOAL OF PAPER MINING 
Parse the Arxiv Latex/PDF into A research Paper Object which can be serialised so that It is in readable format for some form of Machine learning/Annoation methods. But it all starts from cleaning the Dirt from Arxiv. 

### WAY TO DO IT 
1. Extract Papers from `Arxiv` using Adjrej karapathy's `fetch_papers.py` script. It fills the `db.p` Pickle File.
2. `arxiv_multiprocess_download.py` will download the Latex version of the Papers for Arxiv and create and `ArxivPaper` object.  
3. The `ArxivPaper` Object created extracts the Latex source from the Arxiv. **Integration is TODO**
    - Three things will help solve the Information mining Problem. 
        1. Extraction of Document Structure/hierarchy via Python-Latex Libraries like `tex2py`. 
        2. Extraction of Text from Latex Document Using `detex` : https://github.com/pkubowicz/opendetex
        3. Collate with the Tree with the text based on hierachical traversal of tree and text-splittig based search to collate the information. 
    - These things are Managed using the child classes of `LatexInformationParser`. These child classes will help for the Structured `Section` objects which contains the stored parsed structure of the Research paper. 
