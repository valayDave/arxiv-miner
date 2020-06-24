# Download the Arxiv Dataset with all Papers in 
import arxiv
from utils import dir_exists,save_json_to_file,load_json_from_file
import os
import tarfile
import glob
import json
from parse_paper import get_tex_tree,LatexInformationParser,LatexParserException,MaxSectionSizeException,Section,split_match
import datetime
from typing import List

# from subprocess import Popen, PIPE

# def clean_latex_comments(document_path):
#     process = Popen(["./latex_expand", document_path], stdout=PIPE)
#     (output, err) = process.communicate()
#     exit_code = process.wait()
#     return output

class ArxivPaperMeta():
    
    def __init__(self,\
                pdf_only = False,\
                latex_files = 0,\
                mined = True,\
                latex_parsable=True,\
                updated_on=datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")):
        self.pdf_only = pdf_only
        self.latex_files = latex_files
        self.mined = mined
        self.latex_parsable=latex_parsable
        self.updated_on=updated_on

    def to_json(self):
        return dict(
            pdf_only =self.pdf_only,\
            latex_files = self.latex_files,\
            mined = self.mined,\
            latex_parsable = self.latex_parsable,\
            updated_on = self.updated_on
        )

class ArxivLoader():
    """ArxivLoader
    Loads Arxiv Paper objects from Folder Root.

    :param papers_root_path Folder where all Arxiv Papers are Stored by the `ArxivPaper` object.
    """
    def __init__(self,papers_root_path):
        list_subfolders_with_paths = [f.path for f in os.scandir(papers_root_path) if f.is_dir()]
        arxiv_ids = list(map(lambda x:x.split('/')[-1],list_subfolders_with_paths))
        self.papers = list(map(lambda paper_id : ArxivPaper(paper_id,papers_root_path),arxiv_ids))

    def get_meta_data_array(self):
        object_array = []
        for paper in self.papers:
            object_array.append(paper.core_meta)
        return object_array

class ArxivPaper(object):
    """ArxivPaper 
    This object helps download the Paper from Arxiv,
    Parse it from and help store information. 

    :param `paper_id` : id of the Arxiv Paper. Eg. : 1904.03367
    """
    arxiv_object:dict
    arxiv_save_file_name='arxiv.json'
    paper_meta:ArxivPaperMeta
    paper_meta_save_file_name='processing_meta.json'

    def __init__(self,paper_id,root_papers_path):
        super().__init__()
        self.paper_root_path = os.path.join(root_papers_path,paper_id)
        self.latex_root_path = os.path.join(self.paper_root_path,'latex')
        self.paper_id = paper_id
        if not dir_exists(self.paper_root_path):
            os.makedirs(self.paper_root_path)
            self._build_paper()
        else:
            self._buid_from_fs()
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

    def _load_metadata_from_fs(self):
        self.arxiv_object = load_json_from_file(self.arxiv_meta_file_path)
        self.paper_meta = ArxivPaperMeta(**load_json_from_file(self.paper_meta_file_path))
        # return metadata_file
    
    def _save_metadata_to_fs(self):
        if not dir_exists(self.paper_root_path):
            os.makedirs(self.paper_root_path)
        save_json_to_file(self.arxiv_object,self.arxiv_meta_file_path)
        save_json_to_file(self.paper_meta.to_json(),self.paper_meta_file_path)

    @property
    def core_meta(self):
        return dict(
            id = self.arxiv_object['id'],
            title = self.arxiv_object['title'],
            published = self.arxiv_object['published'],
            **self.paper_meta.to_json()
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
        
        # $ Set some Common Meta WRT the Paper. 
        self.paper_meta = ArxivPaperMeta()
        self.paper_meta.latex_files = len(self.tex_files)
        self.paper_meta.pdf_only = True if len(self.tex_files) == 0 else False
        # $ Remove the Tar File.
        os.remove(downloaded_data)
        # $ Save the Metadata
        self._save_metadata_to_fs()

class SingleDocumentLatexParser(LatexInformationParser):
    def __init__(self, max_section_limit=20,detex_path=None):
        super().__init__(max_section_limit=max_section_limit,detex_path=detex_path)
    
    def section_extraction(self,tex_file_path) -> List[Section]:
        tex_node = get_tex_tree(tex_file_path)
        if len(tex_node.branches) > self.max_section_limit:
            raise MaxSectionSizeException()
        
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
        latex_path = paper.tex_files[0]
        sections = self.section_extraction(latex_path)
        tex_in_text = self.text_extraction(latex_path)
        sections,some_section_not_found = self.collate_sections(tex_in_text,sections,split_upto=lowest_section_match_percent,split_bins=number_to_tries)
        return sections,some_section_not_found