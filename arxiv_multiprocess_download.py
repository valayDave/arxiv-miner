import multiprocessing
from utils import Config,dir_exists
import pickle
from arxiv_miner import ArxivPaper
import click
import sys
import traceback


db = pickle.load(open(Config.db_path, 'rb'))
NUM_SUBPROCESSES = multiprocessing.cpu_count()

def scrape_articles(paper_id,root_path):
    try:
        paper = ArxivPaper.from_fs(paper_id,root_path)   
        print("Paper Found From FS: ",paper_id) 
        return (paper_id)
    except:
        pass # This Means No paper was found
    paper = ArxivPaper(paper_id,root_path)
    print("Paper Built From Arxiv: ",paper_id) 
    return (paper_id)

def paper_callback(paper_tuple):
    print(paper_tuple)


# Shortcut to multiprocessing's logger
def error(msg, *args):
    return multiprocessing.get_logger().error('[ERROR]'+msg, *args)
class LogExceptions(object):
    # from https://stackoverflow.com/a/7678125
    def __init__(self, callable_v):
        self.__callable = callable_v

    def __call__(self, *args, **kwargs):
        try:
            result = self.__callable(*args, **kwargs)

        except Exception as e:
            # Here we add some debugging help. If multiprocessing's
            # debugging is on, it will arrange to log the traceback
            error(traceback.format_exc())
            # Re-raise the original exception so the Pool worker can
            # clean up
            raise

        # It was fine, give a normal answer
        return result

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
    res_arr = []
    for paper_id in paper_ids:
        if num_papers > max_papers:
            break
        res_arr.append(pool.apply_async(LogExceptions(scrape_articles),(paper_id,Config.root_papers_path)))
        num_papers+=1

    
    pool.close()
    pool.join()

if __name__=='__main__':
    cli()