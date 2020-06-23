import multiprocessing
from utils import Config,dir_exists
import pickle
from arxiv_miner import ArxivPaper
import click

db = pickle.load(open(Config.db_path, 'rb'))
NUM_SUBPROCESSES = multiprocessing.cpu_count()

def scrape_articles(paper_id,root_path):
    paper = ArxivPaper(paper_id,root_path)    
    return (paper_id,paper.paper_meta.to_json())

def paper_callback(paper_tuple):
    print(paper_tuple)

@click.group()
def cli():
    pass

@cli.command(name='multiprocess_scraping',help='Scrapes Papers from Arxiv and Stores Latex to FS for Further Processing. ')
@click.argument('num_processes', type=int,default=4)
@click.option('--max_papers',default=10 ,type=int, help="Maximum Number of Papers To Scrape")
@click.option('--start_index',default=0,type=int,help='Start Index of the Scraping from the DB')
def multiprocess_scraping(num_processes,max_papers,start_index):
    pool = multiprocessing.Pool(num_processes)
    num_papers = 0
    paper_ids = list(db.keys())
    paper_ids = paper_ids[start_index:]
    print("Scraping Papers From Index ",start_index," To Index",start_index+max_papers)
    for paper_id in paper_ids:
        if num_papers > max_papers:
            break
        pool.apply_async(scrape_articles,(paper_id,Config.root_papers_path))
        num_papers+=1

    pool.close()
    pool.join()

if __name__=='__main__':
    cli()