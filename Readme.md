# ArXiv-Miner

> ArXiv Miner is a toolkit for mining research papers on CS ArXiv. 

## What is ArXiv-Miner

`arxiv-miner` is a quick handy library that helps power [Sci-Genie](https://sci-genie.com). Sci-Genie is a search engine for quickly searching through full text of papers on CS ArXiv. `arxiv-miner` helps extract and parse LaTeX documents from CS ArXiv. It also supports storage and search of those parsed documents using **Elasticsearch**. The library can be applicable for all other domains like Math, Physics, Biology etc. 

## Documentation 
All documentation on how to install and use `arxiv-miner` is provided in the [documentation website](https://arxiv-miner.turing-bot.com/) or inside the [docs folder](docs). Contribution guidelines are also provided there. 

## Why was ArXiv-Miner created ?
ArXiv Miner was created for easily scraping, parsing and searching research content on ArXiv. This library was created after stitching together solutions from the code of various tools like [arxiv-sanity](https://github.com/karpathy/arxiv-sanity-preserver), [arxiv-vanity/engrafo](https://github.com/arxiv-vanity/engrafo), [arxivscraper](https://github.com/Mahdisadjadi/arxivscraper), [tex2py](https://github.com/alvinwan/tex2py), [cso-classifier](https://github.com/angelosalatino/cso-classifier/) and [axcell](https://github.com/paperswithcode/axcell). Parsed structure of the content can be useful in search or any scientific research mining/AI applications as a heuristic baseline.

## Core Components of ArXiv-Miner
- Scraping 
- Parsing
- Indexing/Storage 

## Family Of Projects With ArXiv-Miner
- `arxiv-table-miner` : Coming Soon.
- `arxiv-table-ml-models` : Coming Soon.
- `semantic-scholar-data-pipeline` : https://github.com/valayDave/semantic-scholar-data-pipeline

## Disclaimer 
This project was developed like a [Cowboy coder](https://en.wikipedia.org/wiki/Cowboy_coding) over the [COVID-19 pandemic](https://en.wikipedia.org/wiki/COVID-19_pandemic). Hence, this **may have bugs and not the most well optimized code**. The primary reason for development was to aid CS and Machine Learning/AI research, but this tool can be extended to all 3M+ documents on ArXiv. 

## Call For Contributors
Any help with contributions to improve the project or fix bugs are completely welcome. Please read the contribution guide in the documentation.  

## Credits and Appreciation
This project like all others has been built on shoulders of giants. A big thanks to the creators of the following libraries/open source projects that aided the development of `arxiv-miner`, and it's family of projects:
- [arxiv-sanity](https://github.com/karpathy/arxiv-sanity-preserver)
- [arxiv-vanity/engrafo](https://github.com/arxiv-vanity/engrafo) 
- [arxivscraper](https://github.com/Mahdisadjadi/arxivscraper)
- [tex2py](https://github.com/alvinwan/tex2py)
- [cso-classifier](https://github.com/angelosalatino/cso-classifier/) 
- [axcell](https://github.com/paperswithcode/axcell)
- [elasticsearch](https://github.com/elastic/elasticsearch)
- [Semantic Scholar Open Research corpus](https://github.com/allenai/s2orc)
- [metaflow](https://metaflow.org)
- [docsify](https://docsify.js.org/#/)
## Licence
MIT