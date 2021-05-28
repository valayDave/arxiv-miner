# Core Data Structures
[arxiv_miner/record.py](https://github.com/valayDave/arxiv-miner/blob/master/arxiv_miner/record.py) consists of all the core data structures needed to by the scraping/mining and data storage serialization/deserialization. 
## `ArxivRecord`
This is the core base class which is used by `ArxivPaper` and holds all relevant data/metadata post parsing of LaTeX sources. 
```python

class ArxivPaperProcessingMeta: 
    pdf_only:bool = False
    latex_files:int = 0
    mined:bool = False
    latex_parsed: bool=False

class ArxivRecord(object):
    # Core Identity : Information about arxivid, authors,categories etc.
    identity:ArxivIdentity = None
    # Processing Metadata
    # meta about processing results 
    paper_processing_meta : ArxivPaperProcessingMeta = None
    # Intermediate representation latex parsing results 
    latex_parsing_result : ArxivLatexParsingResult = None
    
    # Single `Section` Created after parsing LaTeX which can be converted to `ResearchPaper`
    latex_parsed_document : ArxivDocument = None

```

## `ResearchPaper`

The `ResearchPaper` is the data structure that holds the parsed text from the research document. The purpose of this object is to fit the research document and its hierarchy into predefined sections which are commonly occurring in research documents. The general pre-identified sections are given below:

- Introduction
- Related Works
- Methodology
- Experiments
- Dataset
- Conclusion
- Limitations
Any section that doesn't fit the predefined sections will be categorized as *Unknown*. 

The `ResearchPaper` consists of key value pairs which relate to the given predefined sections. The key is the name of the section eg. Introduction, Related Works etc. and the value is a `Section`. 
## `Section`

`Section` is a tree-like data structure that holds hierarchical information. `Section` is given by:
```python
class Section:
    title:str = "Introduction"
    text:str = "Text relating to the introduction of a paper"
    children:List[Section] 
```
The *children* in the `Section` hold the information about the child notes of that `Section`. The `Section` object helps capture a research paper’s hierarchy before it gets parsed into a key-value-based `ResearchPaper`. The `Section` object can also be serialized to JSON making it indexable in the Lucene search index.  

