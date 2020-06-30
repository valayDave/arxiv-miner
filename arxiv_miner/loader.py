import pandas
import random
import os
import tarfile
from typing import List
from .paper import ArxivPaper
from collections import Counter

class ArxivLoaderFilter:
    pdf_only : bool
    parsing_errors :bool
    min_latex_pages: int
    max_latex_pages: int
    sample_size:int
    
    def __init__(self,
                pdf_only = None,\
                parsing_errors = None,\
                min_latex_pages = None,\
                max_latex_pages = None,\
                sample_size = None,\
                scraped_only = None,
                ):
        self.pdf_only = pdf_only
        self.parsing_errors = parsing_errors
        self.min_latex_pages = min_latex_pages
        self.max_latex_pages = max_latex_pages
        self.sample_size = sample_size
        
        # For articles that have only been scraped.
        self.scraped_only = scraped_only
        

    @property
    def is_active(self): # Any of Its properties are set. 
        for k in self.__dict__:
            if getattr(self,k) is not None:
                return True
        return False

    @property
    def requires_mined_record(self):
        if self.pdf_only is not None:
            return True
        if self.parsing_errors is not None:
            return True
        if self.min_latex_pages is not None:
            return True
        if self.max_latex_pages is not None:
            return True
        return False

class FSArxivLoadingFactory:

    @staticmethod
    def only_scraped_loader(papers_root_path):
        ax_filter = ArxivLoaderFilter(scraped_only=True)
        loader_obj = ArxivLoader(papers_root_path,filter_object = ax_filter)
        return loader_obj
    
    @staticmethod
    def latex_failed_loader(papers_root_path):
        ax_filter = ArxivLoaderFilter(parsing_errors=True,pdf_only=False)
        loader_obj = ArxivLoader(papers_root_path,filter_object = ax_filter)
        return loader_obj
    
    @staticmethod
    def sampled_loader(papers_root_path,num_samples):
        ax_filter = ArxivLoaderFilter(sample_size = num_samples)
        loader_obj = ArxivLoader(papers_root_path,filter_object = ax_filter)
        return loader_obj
    
    @staticmethod
    def latex_parsed_loader(papers_root_path):
        ax_filter = ArxivLoaderFilter(pdf_only=False,parsing_errors=False)
        loader_obj = ArxivLoader(papers_root_path,filter_object = ax_filter)
        return loader_obj
    
    @staticmethod
    def latex_page_range_loader(papers_root_path,\
                                min_latex_pages,
                                max_latex_pages):
        ax_filter = ArxivLoaderFilter(min_latex_pages=min_latex_pages,max_latex_pages=max_latex_pages)
        loader_obj = ArxivLoader(papers_root_path,filter_object = ax_filter)
        return loader_obj                
  

class ArxivLoader():
    """ArxivLoader : 
    THIS CLASS's PURPOSE IS FOR FAST DATA MOVEMENT IIF NEEDED BETWEEEN SERVERS. 
    IT HELPS LOAD AND UNLOAD AN ENTIRE DATABASE WITH FILTERS. 

    ONE CAN USE THIS AS A BACKUP TOOL FOR DATA EXTRACTION
    Loads Arxiv Paper objects from Folder Root.

    Loader Features : 
        1. Filter Papers By : --> Via ArxivLoaderFilter
            1. pdf_only
            2. parsing_errors
            3. Latex Pages 
        2. Create Sampled Loader. --> Via ArxivLoaderFilter
        3. Act like an Indexable Array
        4. Get Papers using the axiv_id

    :param papers_root_path Folder where all Arxiv Papers are Stored by the `ArxivPaper` object.
    """
    papers = []
    loader_archieve_file_name = 'papers_loaded.tar.gz'
    
    def __init__(self,papers_root_path,filter_object=ArxivLoaderFilter(),detex_path=None):
        self.papers_root_path = papers_root_path
        self.papers = []
        list_of_subfolders_with_paths = [f.path for f in os.scandir(papers_root_path) if f.is_dir()]
        arxiv_ids = list(map(lambda x:x.split('/')[-1],list_of_subfolders_with_paths))
        
        self._build_papers_from_fs(arxiv_ids,filter_object,detex_path=detex_path)

    def __getitem__(self,index):
        return self.papers[index]

    def __len__(self):
        return len(self.papers)
    
    def get_sample(self):
        return self.papers[random.randint(0,len(self.papers)-1)]

    def to_metadata_dataframe(self):
        return pandas.DataFrame(self.get_meta_data_array())

    def _build_papers_from_fs(self,arxiv_ids:List[str],filter_object:ArxivLoaderFilter,detex_path=None):
        """_build_papers_from_fs 
        Build papers according to `ArxivLoaderFilter` filter object
        `ArxivLoaderFilter` : 
            - Filters Via Sampling 
            - Filters via Latex page counts
            - Filters via pdf_only papers 
            - Filters via latex_errored papers
        :param arxiv_ids: List[str] 
        :param filter_object: ArxivLoaderFilter
        """
        use_filter = False
        if filter_object.is_active:
            use_filter = True
        if filter_object.sample_size is not None:
            random.shuffle(arxiv_ids) # Ids are already sufflled so samples can be created.
            
        for paper_id in arxiv_ids:
            if len(self.papers) == filter_object.sample_size:
                break # post generating samples. 
            try:
                paper = ArxivPaper.from_fs(paper_id,self.papers_root_path,detex_path=detex_path)
            except Exception as e:# Ingnore Papers which are not Parsable. 
                print(e)
                continue
            if use_filter: 
                if not self.paper_filter(paper,filter_object):
                    continue
            self.papers.append(paper)

    @staticmethod
    def paper_filter(paper_obj:ArxivPaper,filter_obj:ArxivLoaderFilter):
        compiled_bool = True
        
        # paper_processing_meta is set only if Latex Information is Parsed. 
        # if paper_processing_meta none and ArxivLoaderFilter is active then ignore this record because we need parsed records. 
        if paper_obj.paper_processing_meta is None:
            if filter_obj.requires_mined_record:
                return False
            
            if filter_obj.scraped_only: 
                return True
        
        # As paper_processing_meta is not None and u only need unprocessed records
        if filter_obj.scraped_only: 
            return False
        
        if filter_obj.pdf_only is not None: # If Looking for PDFs_only and no paper_processing_meta then ignore
            cond_result = paper_obj.paper_processing_meta.pdf_only == filter_obj.pdf_only
            compiled_bool = cond_result and compiled_bool


        if filter_obj.parsing_errors is not None:
            cond_result = paper_obj.latex_parsing_result.parsing_error == filter_obj.parsing_errors
            compiled_bool = cond_result and compiled_bool


        if filter_obj.min_latex_pages is not None:
            cond_result = paper_obj.paper_processing_meta.latex_files >= filter_obj.min_latex_pages
            compiled_bool =  cond_result and compiled_bool


        if filter_obj.max_latex_pages is not None:
            cond_result = paper_obj.paper_processing_meta.latex_files < filter_obj.max_latex_pages
            compiled_bool = cond_result and compiled_bool

        return compiled_bool

    def get_meta_data_array(self):
        object_array = []
        for paper in self.papers:
            object_array.append(paper.core_meta)
        return object_array
    
    def __getitem__(self, index):
          return self.papers[index]

    def parsing_statistics(self):
        num_pdfs = sum([ 0 if paper.paper_processing_meta.pdf_only else 1 for paper in self.papers])
        latex_files_counts = dict(Counter([paper.paper_processing_meta.latex_files for paper in self.papers]))
        num_errored = sum([1 if paper.latex_processing_meta.parsing_error else 0 for paper in self.papers])
        fully_parsed = sum([ 1 if not paper.paper_processing_meta.pdf_only and not paper.latex_processing_meta.parsing_error else 0 for paper in self.papers])
        return {
            'num_pdfs':num_pdfs,
            'latex_files_counts':latex_files_counts,
            'num_errored':num_errored,
            'fully_parsed':fully_parsed
        }

    def from_archive(self):
        """from_archive 
        todo : Create a method that creates a loader from archive
        """
        pass

    def make_archive(self,archive_path='./',with_latex=False):
        """make_archive 
        create a Tar file with all the papers within the loader. 
        :param archive_path: [str], defaults to './' 
        :param with_latex: [bool], defaults to False : if False then it doesn't archieve the Latex Folder with raw latex source. 
        """
        archive_path = os.path.join(archive_path,self.loader_archieve_file_name)
        with tarfile.open(archive_path, "w:gz") as tar:
            for paper in self.papers:
                if with_latex:
                    tar.add(paper.paper_root_path, arcname=os.path.basename(paper.paper_root_path))
                else:
                    tar.add(paper.arxiv_meta_file_path,arcname=paper.arxiv_meta_file_path)
                    tar.add(paper.paper_meta_file_path,arcname=paper.paper_meta_file_path)
                    if paper.latex_parsed_document is not None:
                        tar.add(paper.tex_processing_file_path,arcname=paper.tex_processing_file_path)
        print("Finished Creating Tar File At Path : %s"%archive_path)
