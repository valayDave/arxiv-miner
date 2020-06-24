# Download the Arxiv Dataset with all Papers in 
import arxiv
from utils import \
    dir_exists,\
    save_json_to_file,\
    load_json_from_file

import os
import tarfile
import glob
import json
from parse_paper import \
    get_tex_tree,\
    LatexInformationParser,\
    LatexParserException,\
    MaxSectionSizeException,\
    Section,\
    split_match

import datetime
from typing import List

# from subprocess import Popen, PIPE

# def clean_latex_comments(document_path):
#     process = Popen(["./latex_expand", document_path], stdout=PIPE)
#     (output, err) = process.communicate()
#     exit_code = process.wait()
#     return output

class ArxivDocument(Section):
    def __init__(self, 
                name=None,
                id_val=None,
                title=None,
                url=None,
                published=None):

        super().__init__(name=name)
        self.id_val = id_val
        self.title = title
        self.published = published
        self.url=url
    
    @classmethod
    def from_json(cls, json_object):
        document_object = super().from_json(json_object)
        document_object.__dict__ = { **document_object.__dict__ ,**json_object['metadata'] }
        return document_object

    def to_json(self):
        serialized_object = {
            'metadata': {
                'id_val':self.id_val,
                'title':self.title,
                'published':self.published,
                'url':self.url
            }
        }
        document_object = super().to_json()
        return {**document_object,**serialized_object}

    def save_to_file(self,dir_path=os.path.abspath(os.path.dirname(__file__))):
        file_path = os.path.join(dir_path,self.id_val+'.json')
        super().save_to_file(file_path)
         
class ArxivPaperMeta():
    
    def __init__(self,\
                pdf_only = False,\
                latex_files = 0,\
                mined = True,\
                latex_parsable=True,\
                updated_on=datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"),
                some_section_failed=None,
                parsing_error=False,
                error_message=None):

        # Metadata about the Arxiv project.
        self.pdf_only = pdf_only
        self.latex_files = latex_files # number of files
        self.updated_on=updated_on
        
        # latex-processing status
        self.mined = mined
        self.latex_parsable=latex_parsable
        self.some_section_failed = some_section_failed
        self.parsing_error = parsing_error
        self.error_message = error_message

    def to_json(self):
        return dict(
            pdf_only =self.pdf_only,\
            latex_files = self.latex_files,\
            mined = self.mined,\
            latex_parsable = self.latex_parsable,\
            updated_on = self.updated_on,
            some_section_failed = self.some_section_failed,
            parsing_error = self.parsing_error,
            error_message = self.error_message,
        )

class ArxivLoader():
    """ArxivLoader
    Loads Arxiv Paper objects from Folder Root.

    :param papers_root_path Folder where all Arxiv Papers are Stored by the `ArxivPaper` object.
    """
    def __init__(self,papers_root_path):
        list_subfolders_with_paths = [f.path for f in os.scandir(papers_root_path) if f.is_dir()]
        arxiv_ids = list(map(lambda x:x.split('/')[-1],list_subfolders_with_paths))
        self.papers = list(map(lambda paper_id : ArxivPaper.from_fs(paper_id,papers_root_path),arxiv_ids))

    def get_meta_data_array(self):
        object_array = []
        for paper in self.papers:
            object_array.append(paper.core_meta)
        return object_array


class ArxivFSLoadingError(Exception):
    def __init__(self,path):
        msg = "FS Path To Arxiv Mined Data Doesn't Exist %s"%path
        super(ArxivFSLoadingError, self).__init__(msg)

class ArxivPaper(object):
    """ArxivPaper [summary]
    This object helps download the Paper from Arxiv,
    Parse it from and help store information. 

    :param `paper_id` : id of the Arxiv Paper. Eg. : 1904.03367
    :param `root_papers_path` : Path to directory of papers. 
    :param `build_paper` : Default=True. Ensures that the paper is scraped and data is built. 
    :raises ArxivFSLoadingError: Error when loading class from FS
    """
    # meta about the arxiv article
    arxiv_object:dict
    arxiv_save_file_name='arxiv.json'
    # meta about processing results
    paper_meta:ArxivPaperMeta
    paper_meta_save_file_name='processing_meta.json'
    # the actual parsed object document from latex. 
    latex_parsed_document:ArxivDocument
    latex_parsing_result_file_name = 'latex_processing_results.json'

    def __init__(self,paper_id,root_papers_path,build_paper=True):
        super().__init__()
        self.paper_root_path = os.path.join(root_papers_path,paper_id)
        self.latex_root_path = os.path.join(self.paper_root_path,'latex')
        self.latex_processor = ArxivLatexParser()
        self.paper_id = paper_id

        if build_paper:
            if not dir_exists(self.paper_root_path):
                os.makedirs(self.paper_root_path)
                self._build_paper()
        # scan for the presence of the object in the FS.

    @property
    def arxiv_meta_file_path(self):
        return os.path.join(self.paper_root_path,self.arxiv_save_file_name)
    
    @property
    def paper_meta_file_path(self):
        return os.path.join(self.paper_root_path,self.paper_meta_save_file_name)

    @property
    def tex_files(self):
        file_names = list(glob.glob(os.path.join(self.latex_root_path,"*.tex")))
        return file_names

    @property
    def tex_processing_file_path(self):
        return os.path.join(self.paper_root_path,self.latex_parsing_result_file_name)

    def _load_metadata_from_fs(self):
        self.arxiv_object = load_json_from_file(self.arxiv_meta_file_path)
        self.paper_meta = ArxivPaperMeta(**load_json_from_file(self.paper_meta_file_path))
        # return metadata_file
    
    def _save_metadata_to_fs(self):
        if not dir_exists(self.paper_root_path):
            os.makedirs(self.paper_root_path)
        save_json_to_file(self.arxiv_object,self.arxiv_meta_file_path)
        save_json_to_file(self.paper_meta.to_json(),self.paper_meta_file_path)

    def _load_parsed_document_from_fs(self):
        if dir_exists(self.tex_processing_file_path): # file doesn't exist
            json_obj = load_json_from_file(self.tex_processing_file_path)
            self.latex_parsed_document = ArxivDocument.from_json(json_obj)
    
    def _save_parsed_document_to_fs(self):
        if not dir_exists(self.paper_root_path):
            os.makedirs(self.paper_root_path)
        if self.latex_parsed_document is not None:
            save_json_to_file(self.latex_parsed_document.to_json(),self.tex_processing_file_path)

    @property
    def core_meta(self):
        return {
            **self.identity_meta,
            **self.paper_meta.to_json()
        }
    
    @property
    def identity_meta(self):
        return dict(
            id_val = self.paper_id,
            url = self.arxiv_object['id'],
            title = self.arxiv_object['title'],
            published = self.arxiv_object['published']
        )

    def __str__(self):
        format_str = '''
        Properties
        ------------
        ID : {id}
        Title : {title}
        Published : {published}
        

        MetaData
        ---------
        pdf_only = {pdf_only}
        latex_parsable = {latex_parsable}
        updated_on = {updated_on}
        latex_files = {latex_files}
        mined = {mined}
        '''.format(**self.core_meta)
        return format_str

    def _buid_from_fs(self):
        self._load_metadata_from_fs()
        self._load_parsed_document_from_fs()

    def _extract_info_from_latex(self):
        self.paper_meta = ArxivPaperMeta()
        self.paper_meta.latex_files = len(self.tex_files)
        self.paper_meta.pdf_only = True if len(self.tex_files) == 0 else False

        paper_procesesing_results,arxiv_parsed_doc = self.latex_processor(self) # Latex_processor on paper.
        # $ Set main ArxivDocument Property of this object
        self.latex_parsed_document = arxiv_parsed_doc

        # $ Set some Common Meta WRT the Paper. 
        
        self.paper_meta.mined = True if not paper_procesesing_results.parsing_error and not self.paper_meta.pdf_only else False
        
        self.paper_meta.error_message = paper_procesesing_results.error_message
        self.paper_meta.some_section_failed = paper_procesesing_results.some_section_failed
        self.paper_meta.parsing_error = paper_procesesing_results.parsing_error
        
        self.paper_meta.latex_parsable = True
        if paper_procesesing_results.section_list is None:
            if paper_procesesing_results.parsing_error:
                self.paper_meta.latex_parsable = False

    @classmethod
    def from_fs(cls,paper_id,root_papers_path):
        paper = cls(paper_id,root_papers_path,build_paper=False)
        if not dir_exists(paper.paper_root_path):
            raise ArxivFSLoadingError(paper.paper_root_path)
        paper._buid_from_fs()
        return paper
         
    
    def _build_paper(self):
        """_build_paper 
        Download's The Tex Version of the Paper and saves it to folder. 
        Also saves Metadata From Arxiv And Metadata About Tex Value of the paper.
        """
        # $ Query Paper
        paper = arxiv.query(id_list=[self.paper_id])[0]
        # $ Set the Arxiv Object to ensure Proper extraction
        self.arxiv_object = paper
        # $ Download the paper. 
        downloaded_data = arxiv.download(paper,dirpath=self.paper_root_path,slugify=lambda paper: paper.get('id').split('/')[-1],prefer_source_tarfile=True)
        # $ Extract Files in Folder.
        with tarfile.open(downloaded_data) as tar:
            tar.extractall(path=self.latex_root_path)
        
        # $ Remove the Tar File.
        os.remove(downloaded_data)
        # $ Save the Metadata

        self._extract_info_from_latex()
        # print("Extracted Latex Data")
        self._save_metadata_to_fs()
        self._save_parsed_document_to_fs()


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
        :return: List[Section],bool
        """
        largest_file = None
        max_size = 0
        for file_path in paper.tex_files:
            if os.path.getsize(file_path) > max_size:
                largest_file = file_path
        latex_path = largest_file
        sections = self.section_extraction(latex_path)
        tex_in_text = self.text_extraction(latex_path)
        sections,some_section_not_found = self.collate_sections(tex_in_text,sections,split_upto=lowest_section_match_percent,split_bins=number_to_tries)
        return sections,some_section_not_found

class MultiDocumentLatexParser(SingleDocumentLatexParser):
    def __init__(self, max_section_limit=20, detex_path=None):
        super().__init__(max_section_limit=max_section_limit, detex_path=detex_path)
    

    def from_arxiv_paper(self,paper:ArxivPaper,lowest_section_match_percent=0.2,number_to_tries=10):
        collected_sections = []
        snf = False
        for latex_path in paper.tex_files:
            try:
                sections = self.section_extraction(latex_path)
                tex_in_text = self.text_extraction(latex_path)
                sections,some_section_not_found = self.collate_sections(tex_in_text,sections,split_upto=lowest_section_match_percent,split_bins=number_to_tries)
                if some_section_not_found:
                    snf = some_section_not_found
                collected_sections+=sections
            except:
                pass
        return collected_sections,snf

class ArxivLatexParser():
    def __init__(self,max_section_limit=20, detex_path=None):
        self.single_doc_parser = SingleDocumentLatexParser(max_section_limit=max_section_limit, detex_path=detex_path)
        self.multi_doc_parser = MultiDocumentLatexParser(max_section_limit=max_section_limit, detex_path=detex_path)
    
    def __call__(self,paper:ArxivPaper,lowest_section_match_percent=0.2,number_to_tries=10):
        parsing_result = ArxivLatexParingResult()
        selected_parser = None
        if paper.paper_meta.latex_files == 0 : 
            parsing_result.error_message = "NO LATEX FILES PRESENT"
            parsing_result.parsing_error = True
            return parsing_result,None
        
        # Selected Parser for Latex based On Size.            
        if paper.paper_meta.latex_files >=4:
            selected_parser = self.multi_doc_parser.from_arxiv_paper
        elif paper.paper_meta.latex_files >=1:
            selected_parser = self.single_doc_parser.from_arxiv_paper
        
        # Run the parser and see the results. 
        try:
            collected_sections,some_sections_failed = selected_parser(paper,lowest_section_match_percent=lowest_section_match_percent,number_to_tries=number_to_tries)
            parsing_result.section_list = collected_sections
            parsing_result.some_section_failed = some_sections_failed
        except Exception as e:
            parsing_result.error_message = str(e)
            parsing_result.parsing_error = True
        
        arxiv_parsed_doc = parsing_result.build_document_from_paper(paper)
        
        return parsing_result,arxiv_parsed_doc

class ArxivLatexParingResult:
    parsing_result_name = 'Symantic Parsing Result'
    def __init__(self,\
                section_list=None,\
                some_section_failed=None,\
                parsing_error=False,\
                error_message=None):
        self.section_list = section_list
        self.some_section_failed = some_section_failed
        self.parsing_error = parsing_error
        self.error_message = error_message

    def to_json(self):
        return dict(
            some_section_failed=self.some_section_failed,
            parsing_error=self.parsing_error,
            error_message=self.error_message,
            section_list=str(0 if self.section_list is None else len(self.section_list))
        )

    def build_document_from_paper(self,paper:ArxivPaper):
        if self.section_list is None : 
            return None

        sectionised_data = ArxivDocument(name=self.parsing_result_name,**paper.identity_meta)
        sectionised_data.subsections = self.section_list
        return sectionised_data

    def __str__(self):
        return """
        some_section_failed : {some_section_failed}
        parsing_error : {parsing_error}
        error_message : {error_message}
        section_list_size: {section_list}
        """.format(
                **self.to_json()
            )