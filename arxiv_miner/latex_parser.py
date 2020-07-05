from tex2py import tex2py 
from typing import List
import os 
from subprocess import Popen, PIPE
import json
from .exception import \
        LatexParserException,\
        LatexToTextException,\
        MaxSectionSizeException,\
        DetexBinaryAbsent

from .semantic_parsing import Section


def get_tex_tree(tex_path):
    with open(tex_path,'r') as f:
        data = f.read()
    tex_root_node = tex2py(data)
    return tex_root_node


def split_match(split_value:str,splitting_string:str,split_upto=0.5,split_bins=10):
    """split_match 
    Splits a Keep Splitting a `splitting_string` based on the value of `split_value`.
    It does so by removing `split_upto`% of the string until there is a match. or return no match. 
    
    `split_upto` specifies the size after which it will stop splitting. 

    :param split_value: [String]
    :param splitting_string: [description]
    :param split_upto: [float], defaults to 0.5 
    :param split_bins: [int] the number bins inside which each `split_value` will fall under. 
                                eg. 
                                    split_value = "Deep Learning Techniques for ASD Diagnosis and Rehabilitation'"
                                    split_bins=3,
                                    split_upto=0.5
                                    then the text will be checked for matches against : 
                                        - ['Deep Learning Techniques for','Deep Learning Techniques for ASD','Deep Learning Techniques for ASD Diagnosis','Deep Learning Techniques for ASD Diagnosis and' ....]
                                        - The purpose of doing this is to ensure a partial match of a string can help extract the split text 
    :returns splitted_text : List[String] : [s1,s2] or []
    """
    split_value = split_value.split(' ') # This make it remove words instead of the characters. 
    sb = [i for i in range(split_bins)]
    split_mul = (1-split_upto)/split_bins
    # Spread the `split_bins` according to the how much the split needs to happen. 
    split_range = [1-float((i)*split_mul) for i in sb]
    # index at which the new `split_value` will be determined. Order is descending to ensure largest match. 
    slice_indices = [int(len(split_value)*split_val) for split_val in split_range] 
    # creates the split strings.     
    split_values_to_checks = [' '.join(split_value[:index]) for index in slice_indices] 
    
    for split_val in split_values_to_checks:
        if split_val == '': # In case of empty seperator leaave it. 
            continue
        current_text_split = splitting_string.split(split_val)
        if len(current_text_split) > 1:
            return current_text_split
    
    return []


class LatexToText():
    """LatexToText 
    This class will manage the conversion of the latex document into text. 
    It uses `detex` to extract the text from tex Files. 
    """
    detex_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),'detex')

    def __init__(self,detex_path=None):
        # check binary existance. 
        if not self.binary_exists(self.detex_path):
            if detex_path is None:
                raise DetexBinaryAbsent()
            elif not self.binary_exists(detex_path):
                raise DetexBinaryAbsent()
            else:
                self.detex_path = detex_path
        
    @staticmethod
    def binary_exists(detex_path):
        try:
            os.stat(detex_path)
        except:
            return False
        return True
    
    def __call__(self,latex_document_path):
        try:
            process = Popen([self.detex_path, latex_document_path], stdout=PIPE)
            (output, err) = process.communicate()
            exit_code = process.wait()
            return output
        except Exception as e:
            print(e)
            raise LatexToTextException()


class LatexInformationParser(object):
    """LatexInformationParser 

    This is the parent class responsible for extraction of Processed information from 
    Latex Based Documents. Process follows the below steps:

    ## `section_extraction`
    Use the `section_extraction` method to extract the document information
    sections from the single/document Latex setup. This returns a Sequential Tree like structure with sub sequences. 
    This will use `tex2py` like functions to extract the document structure from the tex documents. 
    
    ## `text_extraction`:
    Will extract text from the Latex File. uses `opendetex` to extract the text from latex. 

    ## `collate_sections` : 
    This will collate the information text and sections extracted based on the strategy of the extraction. 

    """
    max_section_limit = 30 # Maximum number of sections to allow for extraction
    def __init__(self,max_section_limit=20,detex_path=None):
        self.max_section_limit = max_section_limit
        self.text_extractor = LatexToText(detex_path=detex_path)

    
    def section_extraction(self,tex_file_path) -> List[Section]:
        raise NotImplementedError()
    
    @staticmethod
    def get_subsection_names(tex_node):
        subsections = []
        try:
            subsections = list(tex_node.subsections)
            subsections = [i.string for i in subsections]
        except:
            pass
        return subsections            


    def text_extraction(self):
        raise NotImplementedError()

    def collate_sections(self):
        raise NotImplementedError()

    def from_arxiv_paper(self,paper):
        raise NotImplementedError()

