from tex2py import tex2py 
# from arxiv_miner import ArxivPaper
from typing import List
import os 
from subprocess import Popen, PIPE
import json

def get_tex_tree(tex_path):
    with open(tex_path,'r') as f:
        data = f.read()
    tex_root_node = tex2py(data)
    return tex_root_node

class LatexParserException(Exception):
    headline = 'Latex Parsing failed'
    def __init__(self, msg='', lineno=None):
        self.message = msg
        self.line_no = lineno
        super(LatexParserException, self).__init__()

    def __str__(self):
        prefix = 'line %d: ' % self.line_no if self.line_no else ''
        return '%s%s' % (prefix, self.message)


class MaxSectionSizeException(LatexParserException):
    def __init__(self,avail,limit):
        msg = "Number of Sections %d are Larger than Maximum allowed (%d) for a Parsed Document"%(avail,limit)
        super(MaxSectionSizeException, self).__init__(msg)


class LatexToTextException(LatexParserException):
    def __init__(self):
        msg = "Exception Raised From Text extraction at Detex"
        super(LatexToTextException, self).__init__(msg)


class DetexBinaryAbsent(LatexParserException):
    def __init__(self):
        msg = "Exception Raised Because Of No Detex Binary"
        super(DetexBinaryAbsent, self).__init__(msg)


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
        except:
            raise LatexToTextException()

class SectionSerialisationException(LatexParserException):
     def __init__(self,ms):
        msg = "Serialisation of Section Object Requeses %s"%ms
        super(SectionSerialisationException, self).__init__(msg)

class Section():
    """Section 
    Section will contain subsections which are of type Section
    """
    def __init__(self,name=None):
        # Core Attributes
        self.name = self.__class__.__name__ if name is None else name
        self.subsections = []
        self.text = ''
    
    def hierarchy_string(self,tab_counter=0):
        TAB = '\t'
        tab_counter+=1
        hierarchy = [TAB*tab_counter+self.name+"("+str(len(self.text))+")"]
        for subsection in self.subsections:
            hierarchy.append(subsection.hierarchy_string(tab_counter=tab_counter))
        return '\n'.join(hierarchy)
    
    def to_json(self):
        serialized_object = {
            'name' : self.name,
            'subsections': [],
            'text': self.text
        }
        for ss in self.subsections:
            serialized_object['subsections'].append(ss.to_json())
        return serialized_object

    @classmethod
    def from_json(cls,json_object):
        if 'name' not in json_object:
            raise SectionSerialisationException('"name" Attribute missing in object')
        generated_obj = cls(name=json_object['name'])
        generated_obj.text = json_object['text']
        for val in json_object['subsections']:
            generated_obj.subsections.append(cls(val))
        
        return generated_obj
    
    def save_to_file(self,file_path):
        with open(file_path,'w') as f:
            json.dump(self.to_json(),f)

    def _get_hierarchy(self):
        hierarchy = []
        for subsection in self.subsections:
            hierarchy.append(subsection._get_hierarchy())
        return {self.name:hierarchy}
        
    def __str__(self):
        return self.hierarchy_string()

class Introduction(Section):
    def __init__(self):
        super().__init__()

class RelatedWorks(Section):
    def __init__(self):
        super().__init__()
        
class Conclusion(Section):
    def __init__(self):
        super().__init__()

class Methodology(Section):
    def __init__(self):
        super().__init__()

class Citations(Section):
    def __init__(self):
        super().__init__()


class ResearchPaper(object):
    '''
    Build and Parse Downloaded Latex Papaer into this Object
    '''
    def __init__(self):
        super().__init__()
        self.introduction = Introduction()
        self.related_works = RelatedWorks()
        self.citations = Citations()
        self.methodology = Methodology()

def split_match(split_value:str,splitting_string:str,split_upto=0.5,split_bins=10):
    """split_match 
    Splits a Keep Splitting a `splitting_string` based on the value of `split_value`.
    It does so by removing `split_upto`% of the string until there is a match. or return no match. 
    
    `split_upto` specifies the size after which it will stop splitting. 

    :param split_value: [String]
    :param splitting_string: [description]
    :param split_upto: [float], defaults to 0.5 
    :param split_bins: [int] the number bins inside which each `split_value` will fall under. 
                                eg. if split_value = "abcde",split_bins=3,split_upto=0.5
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
        current_text_split = splitting_string.split(split_val)
        if len(current_text_split) > 1:
            return current_text_split
    
    return []


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


if __name__ == "__main__":
    tex_path = '1904.03367/Reinforcement_Learning_with_Attention_that_Works.tex'
    get_tex_tree(tex_path)