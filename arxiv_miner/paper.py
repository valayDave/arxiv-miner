import os
import shutil
import glob
import arxiv
import tarfile
from typing import List,Tuple
import datetime
from .exception import *

from .utils import \
    dir_exists,\
    save_json_to_file,\
    load_json_from_file

from .semantic_parsing import \
    ArxivDocument,\
    Section,\
    ResearchPaper,\
    ResearchPaperSematicParser

from .latex_parser import \
    get_tex_tree,\
    split_match,\
    LatexInformationParser

from .record import \
    ArxivIdentity,\
    ArxivLatexParsingResult,\
    ArxivPaperProcessingMeta,\
    ArxivRecord

class ArxivPaper(ArxivRecord):
    """ArxivPaper
    This object helps download the Paper from Arxiv,
    Parse it from and help store information. 
    It is Based on the shell `ArxivRecord` so that all core data regarding 
    mining and identity can be certrailised and finally be driven towards a core 
    database solution.
    This is the Processing Object responsible for building the 
    arxiv Processed Information. 

    This class is a Child class of ArxivRecord. 

    :param `paper_id` : id of the Arxiv Paper. Eg. : 1904.03367
    :param `root_papers_path` : Path to directory of papers. 
    :param `build_paper` : Default=True. Ensures that the paper is scraped and data is built. 
    :raises ArxivFSLoadingError: Error when loading class from FS
    """

    def __init__(self,paper_id,root_papers_path,build_paper=True,detex_path=None):
        super().__init__()
        self.paper_root_path = os.path.join(root_papers_path,paper_id)
        self.latex_root_path = os.path.join(self.paper_root_path,'latex')
        self.detex_path = detex_path
        self.paper_id = paper_id
        if build_paper: # Builds and Saves to FS. 
            self._build_paper()
        # scan for the presence of the object in the FS.

    ############ FS Oriented Properties for Mining ############ ############ ############ ############
    @property
    def arxiv_meta_file_path(self):
        return os.path.join(self.paper_root_path,self.identity_file_name)
    
    @property
    def paper_meta_file_path(self):
        return os.path.join(self.paper_root_path,self.paper_meta_save_file_name)

    @property
    def tex_files(self):
        file_names = list(glob.glob(os.path.join(self.latex_root_path,"**/*.tex"),recursive=True))
        return file_names

    @property
    def tex_processing_file_path(self):
        return os.path.join(self.paper_root_path,self.latex_parsing_result_file_name)

    ############  ############ ############ ######################## ############ ############ ############
    ############ Fast Access Properties for the Processing Object ############
    @property
    def core_meta(self):
        final_dict = dict(**self.identity_meta)
        if self.paper_processing_meta:
            final_dict = {**final_dict,**self.paper_processing_meta.to_json()}
        if self.latex_parsing_result:
            final_dict['parsing_error'] = self.latex_parsing_result.parsing_error
        return final_dict
    
    @property
    def identity_meta(self):
        return dict(
            id_val = self.identity.identity,
            url = self.identity.url,
            title = self.identity.title,
            published = self.identity.published,
        )

    ############  ############ ############ ############ ############ ############ ############
    ############`ArxivRecord` core properties data loading methods: Can be astracted to further ############

    def _load_metadata_from_fs(self):
        # Load Identity
        self.identity = ArxivIdentity.from_file(self.arxiv_meta_file_path)
        # Load Processsing Metadata
        if not dir_exists(self.paper_meta_file_path):
            return # Don't Load Meta if it doesn't exist. 
        processing_meta_dict = load_json_from_file(self.paper_meta_file_path)
        if processing_meta_dict['latex_processing_meta']:
            self.latex_parsing_result = ArxivLatexParsingResult(**processing_meta_dict['latex_processing_meta'])
        if processing_meta_dict['paper_processing_meta']:
            self.paper_processing_meta = ArxivPaperProcessingMeta(**processing_meta_dict['paper_processing_meta'])
    
    def _save_metadata_to_fs(self):
        if not dir_exists(self.paper_root_path):
            os.makedirs(self.paper_root_path)
        # Save ArxivIdentity
        save_json_to_file(self.identity.to_json(),self.arxiv_meta_file_path)
        # Save ArxivProcessingPaperMeta and ArxivLatexParsingResult
        processing_result_dict = {
            'latex_processing_meta': None if self.latex_parsing_result is None else self.latex_parsing_result.to_json(),
            'paper_processing_meta': None if self.paper_processing_meta is None else self.paper_processing_meta.to_json(),
        }
        save_json_to_file(processing_result_dict,self.paper_meta_file_path)

    def _load_parsed_document_from_fs(self):
        # Load ArxivDocument from FS if Exists. 
        if dir_exists(self.tex_processing_file_path): # file doesn't exist
            json_obj = load_json_from_file(self.tex_processing_file_path)
            self.latex_parsed_document = ArxivDocument.from_json(json_obj)
    
    def _save_parsed_document_to_fs(self):
        if not dir_exists(self.paper_root_path):
            os.makedirs(self.paper_root_path)
        if self.latex_parsed_document is not None:
            save_json_to_file(self.latex_parsed_document.to_json(),self.tex_processing_file_path)
    ############  ############ ############ ############ ############ ############ ############

    def __str__(self):
        parsing_outline = None
        proc_meta_str = None
        latex_meta_str = None
        format_str = '''
        Properties
        ------------
        ID : {id_val}
        URL : {url}
        Title : {title}
        Published : {published}
        '''.format(**self.identity_meta)

        if self.paper_processing_meta is not None:
            proc_meta_str = str(self.paper_processing_meta)
            format_str = format_str+'\n'+proc_meta_str

        if self.latex_parsing_result is not None:
            latex_meta_str = str(self.latex_parsing_result)
            format_str = format_str+'\n'+latex_meta_str

        if self.latex_parsed_document is not None:
            parsing_outline = str(self.latex_parsed_document)
            latex_parsing_str = '''
            LATEX-PARSING
            ----------

            {parsing_outline}
            '''.format(parsing_outline=parsing_outline)
            format_str = format_str+'\n'+latex_parsing_str

        return format_str

    def _buid_from_fs(self,meta_only = False):
        self._load_metadata_from_fs()
        if not meta_only:
            self._load_parsed_document_from_fs()

    ############ Core Methods for Data Processing Methods for Latex ############

    def _extract_info_from_latex(self):
        # $ Set some Common Meta WRT the Paper. 
        self.paper_processing_meta = ArxivPaperProcessingMeta()
        self.paper_processing_meta.latex_files = len(self.tex_files)
        self.paper_processing_meta.pdf_only = True if len(self.tex_files) == 0 else False

        latex_processor = ArxivLatexParser(detex_path=self.detex_path)
        paper_procesesing_results,arxiv_parsed_doc = latex_processor(self) # Latex_processor on paper.
        self.paper_processing_meta.mined = True if not paper_procesesing_results.parsing_error and not self.paper_processing_meta.pdf_only else False
        
        # $ Set main ArxivDocument Property of this object
        self.latex_parsed_document = arxiv_parsed_doc
        self.latex_parsing_result = paper_procesesing_results

        self.paper_processing_meta.latex_parsed = False
        if self.paper_processing_meta.mined:
            if len(self.latex_parsing_result.section_list) > 0:
                self.paper_processing_meta.latex_parsed = True

    def _build_paper(self,save_data=True,store_latex=False):
        """_build_paper 
        Download's The Tex Version of the Paper and saves it to folder. 
        Also saves Metadata From Arxiv And Metadata About Tex Value of the paper.
        
        ASSUMPTIONS : 
            - No understanding regarding behaviour when paper_id is given with version_number
        1. Extract Identity : 
            Currently using Remote API. 
            Moving forward can directly be using `ArxivIdentity`. 
        """
        try:
            # $ Set the Arxiv Object to ensure Proper extraction
            identity,paper = self.extract_meta_from_remote(self.paper_id)
            self.identity = identity

            if not dir_exists(self.paper_root_path):
                os.makedirs(self.paper_root_path)
            # $ Download the paper. 
            downloaded_data = arxiv.download(paper,dirpath=self.paper_root_path,slugify=lambda paper: paper.get('id').split('/')[-1],prefer_source_tarfile=True)
        except Exception as e:
            raise ArxivAPIException(self.paper_id,str(e))
        # $ Extract Files in Folder.
        with tarfile.open(downloaded_data) as tar:
            tar.extractall(path=self.latex_root_path)

        # $ Remove the Tar File.
        if not store_latex:
            os.remove(downloaded_data)
        # $ Save the Metadata
        self._extract_info_from_latex()

        shutil.rmtree(self.latex_root_path) # remove Latex source data.
        # print("Extracted Latex Data")
        if save_data:
            self.to_fs()

    ############ Public Facing Core Methods for Data Processing Methods for Latex ############
    def download_latex(self):
        """download_latex 
        Downloads latex from arxiv as a tar file in the `self.paper_root_path`. 
        Ideally called seperately from the `mine_paper` method or from the ArxivPaper(build_paper=True)

        It will just download the latex tar source. 
        :raises ArxivAPIException: Arxiv showed the finger. 
        :return: [str] path of the downloaded tar file 
        """
        try:
            # $ Set the Arxiv Object to ensure Proper extraction
            identity,paper = self.extract_meta_from_remote(self.paper_id)
            self.identity = identity

            if not dir_exists(self.paper_root_path):
                os.makedirs(self.paper_root_path)
            # $ Download the paper. 
            downloaded_data = arxiv.download(paper,dirpath=self.paper_root_path,slugify=lambda paper: paper.get('id').split('/')[-1],prefer_source_tarfile=True)
            return downloaded_data
        except Exception as e:
            raise ArxivAPIException(self.paper_id,str(e))
        
    def mine_paper(self,store_latex=False):
        """mine_paper 
        This is an Exposed Method which will help mine LateX For the Paper
        It will `NOT STORE TO FS`
        """
        self._build_paper(save_data=False,store_latex=store_latex)

    
    ############  ############ ############ ############ ############ ############ ############
    ############  Methods to create/store Processing Object from/to FS for DB level Processes ############
    
    @classmethod
    def from_fs(cls,paper_id,root_papers_path,detex_path=None):
        paper = cls(paper_id,root_papers_path,build_paper=False,detex_path=detex_path)
        if not dir_exists(paper.paper_root_path):
            raise ArxivFSLoadingError(paper.paper_root_path)
        paper._buid_from_fs()
        return paper
    
    def to_fs(self):
        self._save_metadata_to_fs()
        self._save_parsed_document_to_fs()
    
    @classmethod
    def from_arxiv_id(cls,axid,root_papers_path,detex_path=None):
        axobj = cls(axid,root_papers_path,build_paper=True,detex_path=detex_path)
        return axobj

    ############ ############ ######################## ############ ############
    ############ Portability Methods To Make `ArxivPaper` a Processing Object that can reside anywhere.  ############
    @classmethod
    def from_arxiv_record(cls,root_papers_path,record:ArxivRecord,detex_path=None):
        """from_arxiv_record 
        classmethod that creates an `ArxivPaper` object from its Parent Record class. 
        This method
        """
        paper = cls(record.identity.identity,root_papers_path,build_paper=False,detex_path=detex_path)
        paper.identity = record.identity
        paper.latex_parsed_document = record.latex_parsed_document
        paper.paper_processing_meta = record.paper_processing_meta
        paper.latex_parsing_result = record.latex_parsing_result
        return paper        
    
    def to_arxiv_record(self) -> ArxivRecord:
        """to_arxiv_record 
        Extracts the base properties of the Object and Returns the Object. 
        """
        record = ArxivRecord(
            identity = self.identity,\
            latex_parsed_document = self.latex_parsed_document,\
            paper_processing_meta = self.paper_processing_meta,\
            latex_parsing_result = self.latex_parsing_result,\
        )
        return record
    
    @classmethod
    def from_json(cls,root_papers_path,json_object,detex_path=None):
        record = ArxivRecord.from_json(json_object)
        return cls.from_arxiv_record(root_papers_path,record,detex_path=detex_path)
    ############  ############ ############ ######################## ############ ############
    
    @staticmethod
    def extract_meta_from_remote(paper_id):
        """_extract_meta_from_remote 
        Make API call to Remote for Extraction of `ArxivIdentity` for paper_id
        :return `ArxivIdentity`,dict : dict hold arxiv API response. 
        """
        # $ Query Paper
        paper = arxiv.query(id_list=[paper_id])[0]
        # $ Set the Arxiv Object to ensure Proper extraction
        return ArxivIdentity.from_arxiv_response(paper),paper


class SingleDocumentLatexParser(LatexInformationParser):
    def __init__(self, max_section_limit=20,detex_path=None):
        super().__init__(max_section_limit=max_section_limit,detex_path=detex_path)
    
    def section_extraction(self,tex_file_path) -> List[Section]:
        tex_node = get_tex_tree(tex_file_path)
        if len(tex_node.branches) > self.max_section_limit:
            raise MaxSectionSizeException(len(tex_node.branches),self.max_section_limit)
        
        sequential_sections = []
        for node in tex_node:
            curr_section = Section(str(node))
            subsections = self.get_subsection_names(node)
            curr_section.subsections = [Section(ss) for ss in subsections]
            sequential_sections.append(curr_section)

        return sequential_sections
    
    def text_extraction(self,latex_path):
        return self.text_extractor(latex_path)

            
    @staticmethod
    def split_and_find_section(curr_text,curr_sec_name,prev_section,split_upto=0.2,split_bins=10):
        """split_and_find_section 
        Helps Recurrsively/iteratively split a Latex document for the Section list found from 
        `section_extraction`. Splits a text string based on `curr_sec_name` and then allocates the 
        split[0] to the `prev_section` object. 
        
        :type curr_text: [String]
        :type curr_sec_name: [String]
        :type prev_section: [Section]
        :type split_upto : refer `split_match`
        :type split_bins : refer `split_match`
        :returns curr_text,Find_status : After removal of redundant section post splitting. 
                                         status points to weather it could set text of the 
                                         `prev_section` object
        """
        current_text_split = split_match(curr_sec_name,curr_text,split_upto=split_upto,split_bins=split_bins)
        # print("Found Splits,",curr_sec_name,len(current_text_split))
        if len(current_text_split) == 0: 
            # This means no splits were found 
            return curr_text,False

        portion_before_section = current_text_split[0] 

        if prev_section is not None:
            prev_section.text = portion_before_section
            # print(ss.name,"added To Section ",prev_section.name,len(prev_section.text))
        portion_after_section = current_text_split[1:]
        curr_text = ''.join(portion_after_section)
        return curr_text,True

    
    def collate_sections(self,paper_text,section_list:List[Section],split_upto=0.2,split_bins=10):
        """collate_sections 
        Gets Latex compiled text string and 
        then uses the found section based hierarchy from Latex
        to fill Text content of the `Section` objects which were discovered in 
        `section_extraction`

        :param paper_text: text in string of from text_extraction]
        :type section_list: List[Section]
        :return: List[Section] : filled with text attributed
        """
        current_text_split = []
        prev_section = None
        curr_text = str(paper_text)
        unfound_sections = []
        some_section_not_found = False
        for index,s in enumerate(section_list):
            curr_text,section_status = self.split_and_find_section(curr_text,s.name,prev_section,split_upto=split_upto,split_bins=split_bins)
            if not section_status: # If couldn't match section add it here. 
                some_section_not_found = True
            # print('\n\t'+s.name)                
            prev_section = s 
            for ss in s.subsections:
                curr_text,section_status = self.split_and_find_section(curr_text,ss.name,prev_section,split_upto=split_upto,split_bins=split_bins)
                if not section_status:
                    some_section_not_found = True
                    # print("Cannot Match For :",ss.name)
                prev_section = ss
                # print('\n\t\t'+ss.name)
            if index == len(section_list)-1:
                s.text = curr_text
        return section_list,some_section_not_found
    
    def from_arxiv_paper(self,paper:ArxivPaper,lowest_section_match_percent=0.2,number_to_tries=10):
        """from_arxiv_paper 
        Extract Parsable section array from Arxiv Latex Paper which only have one paper will all the sections in it. 
        :param paper: [ArxivPaper]
        :param lowest_section_match_percent [float]: % of the section heading that mimimum matches to create the split in text for parsing. 
        :param number_to_tries: [float], number of different matches to make with mimimum match strings
        :return: Tuple (
            found_sections:List[Section],
            some_section_not_found:bool,
            latex_files_status_dict:dict
        ) 
        """
        largest_file = None
        max_size = 0
        file_results = {}
        for file_path in paper.tex_files:
            if os.path.getsize(file_path) > max_size:
                largest_file = file_path
        latex_path = largest_file
        sections = self.section_extraction(latex_path)
        tex_in_text = self.text_extraction(latex_path)
        sections,some_section_not_found = self.collate_sections(tex_in_text,sections,split_upto=lowest_section_match_percent,split_bins=number_to_tries)
        file_results[latex_path] = some_section_not_found
        return sections,some_section_not_found,file_results

class MultiDocumentLatexParser(SingleDocumentLatexParser):
    def __init__(self, max_section_limit=20, detex_path=None):
        super().__init__(max_section_limit=max_section_limit, detex_path=detex_path)
    

    def from_arxiv_paper(self,paper:ArxivPaper,lowest_section_match_percent=0.2,number_to_tries=10):
        """from_arxiv_paper 
        Extract Parsable section array from Arxiv Latex Paper which only have one paper will all the sections in it. 
        :param paper: [ArxivPaper]
        :param lowest_section_match_percent [float]: % of the section heading that mimimum matches to create the split in text for parsing. 
        :param number_to_tries: [float], number of different matches to make with mimimum match strings
        :return: Tuple (
            found_sections:List[Section],
            some_section_not_found:bool,
            latex_files_status_dict:dict
        ) 
        """
        collected_sections = []
        snf = False
        file_results = {}
        for latex_path in paper.tex_files:
            try:
                sections = self.section_extraction(latex_path)
                tex_in_text = self.text_extraction(latex_path)
                sections,some_section_not_found = self.collate_sections(tex_in_text,sections,split_upto=lowest_section_match_percent,split_bins=number_to_tries)
                file_results[latex_path] = True
                if some_section_not_found:
                    snf = some_section_not_found
                collected_sections+=sections
            except:
                file_results[latex_path] = False
        return collected_sections,snf,file_results

class ArxivLatexParser():
    """
    Parses Arxiv Latex Documents with `LatexInformationParser` according 
        - Based on Number of latex Pages (Chooses `SingleDocumentLatexParser` | `MultiDocumentLatexParser`)
            - Parsing is Ment to create Tree Like `Section` Datastructures.  
    
    :return: Tuple(`ArxivLatexParsingResult`,`ArxivDocument`)
    """
    parsing_result_name = 'Symantic Parsing Result'

    def __init__(self,max_section_limit=20, detex_path=None):
        self.single_doc_parser = SingleDocumentLatexParser(max_section_limit=max_section_limit, detex_path=detex_path)
        self.multi_doc_parser = MultiDocumentLatexParser(max_section_limit=max_section_limit, detex_path=detex_path)
    
    def __call__(self,paper:ArxivPaper,lowest_section_match_percent=0.2,number_to_tries=10):
        parsing_result = ArxivLatexParsingResult()
        selected_parser = None
        if paper.paper_processing_meta.latex_files == 0 : 
            parsing_result.error_message = "NO LATEX FILES PRESENT"
            parsing_result.parsing_error = True
            return parsing_result,None
        
        # Selected Parser for Latex based On Size.            
        if paper.paper_processing_meta.latex_files >=4:
            selected_parser = self.multi_doc_parser
            # parsing_result.latex_parsing_method = 
        elif paper.paper_processing_meta.latex_files >=1:
            selected_parser = self.single_doc_parser
        
        parsing_result.latex_parsing_method = selected_parser.__class__.__name__
        # Run the parser and see the results. 
        try:
            collected_sections,some_sections_failed,file_results = selected_parser.from_arxiv_paper(paper,lowest_section_match_percent=lowest_section_match_percent,number_to_tries=number_to_tries)
            parsing_result.section_list = collected_sections
            parsing_result.some_section_failed = some_sections_failed
            parsing_result.file_results = [{'name':k,'status':file_results[k]} for k in file_results]
        except Exception as e:
            parsing_result.error_message = str(e)
            parsing_result.parsing_error = True
        
        arxiv_parsed_doc = self._build_document_from_paper(paper,parsing_result)
        
        return parsing_result,arxiv_parsed_doc

    def _build_document_from_paper(self,paper:ArxivPaper,result:ArxivLatexParsingResult):
        if result.section_list is None : 
            return None

        sectionised_data = ArxivDocument(name=self.parsing_result_name,**paper.identity_meta)
        sectionised_data.subsections = result.section_list
        return sectionised_data


class ResearchPaperFactory:
    """ 
    Given Raw Unstructured `Section`ised documents, 
    Create a research paper Object from the document.
    """
    @staticmethod
    def from_arxiv_record(paper:ArxivRecord):
        if paper is None:
            raise Exception("`ArxivRecord` Cannot Be None")
        
        if paper.latex_parsed_document is None:
            return ResearchPaperSematicParser([])\
                .to_research_paper()

        found_subsections = paper.latex_parsed_document.subsections
        if len(found_subsections) == 0 :
            return ResearchPaperSematicParser([])\
                .to_research_paper()

        research_doc = \
                ResearchPaperSematicParser(found_subsections)\
                .to_research_paper()
        
        return research_doc
        
