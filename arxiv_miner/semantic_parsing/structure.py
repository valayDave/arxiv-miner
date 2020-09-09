import json
from typing import List
from ..exception import SectionSerialisationException
import os 
from .constants import *

class Section():
    """Section 
    Section will contain subsections which are of type Section
    """
    def __init__(self,name=None):
        # Core Attributes
        self.name = self.__class__.__name__ if name is None else name
        self.subsections = [] # List[Sections]
        self.text = ''
    
    def to_markdown(self,tab_counter=0):
        SPACE= ' '
        HEADING='#'
        tab_counter+=1
        heading_val = HEADING*tab_counter if tab_counter <=3 else HEADING*3
        hierarchy = [heading_val+SPACE+self.name+"("+str(len(self.text))+")"]+[self._clean_text(self.text)] if len(self.text) > 0 else []
        for subsection in self.subsections:
            hierarchy.append(subsection.to_markdown(tab_counter=tab_counter))
        return '\n'.join(hierarchy)
    
    def to_quoted_tags(self):
        SPACE= ' '
        QUOTE_SECTION='<SECTION>'
        UNQUOTE_SECTION = '</SECTION>'
        QUOTE_CONTENT='\n<CONTENT>\n'
        UNQUOTE_CONTENT = '\n</CONTENT>\n'
        # tab_counter+=1
        # heading_val = HEADING*tab_counter if tab_counter <=3 else HEADING*3
        hierarchy = [
            QUOTE_SECTION+\
                self.name+UNQUOTE_SECTION]+[QUOTE_CONTENT+self._clean_text(self.text)+UNQUOTE_CONTENT] if len(self.text) > 0 else []
        for subsection in self.subsections:
            hierarchy.append(subsection.to_quoted_tags())
        return '\n'.join(hierarchy)

    @staticmethod
    def _clean_text(text):
        return text.replace('\\n','\n').replace('\t','').replace('\r','')
    
    def hierarchy_string(self,tab_counter=0):
        TAB = '\t'
        tab_counter+=1
        hierarchy = [TAB*tab_counter+self.name+"("+str(len(self.text.split(' ')))+")"]
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
            generated_obj.subsections.append(cls.from_json(val))
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
    
    def flattened_sections(self):
        flattened_arr = []
        curr_sec = Section(name=self.name)
        curr_sec.text=self.text
        flattened_arr.append(curr_sec)
        for subsection in self.subsections:
            flattened_arr.extend(subsection.flattened_sections())
        return flattened_arr


class SemanticParsedSection(Section):
    """SemanticParsedSection 
    Helps Parse and Match Sections. 
    """
    def __init__(self, name=None,text_match_tokens=[],required=False):
        super().__init__(name=name)
        self.text_match_tokens = text_match_tokens
        self.matched = False
        self.required = required
    
    def match_section(self,section:Section):
        for token in  self.text_match_tokens:
            if token in str(section.name).lower():
                if token not in STRICT_CHECK_CONSTS:
                    return True
                else:
                    if token == section.name:
                        return True
        return False
    
    def to_json(self):
        return {**super().to_json(),**{'matched':self.matched}}

    @classmethod
    def from_json(cls, json_object):
        sec = cls()
        sec.text = json_object['text']
        for subsec in json_object['subsections']:
            sec.subsections.append(Section.from_json(subsec))
        sec.matched = json_object['matched']
        return sec

# Core Requirement Sections : 
class Introduction(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=INTRODUCTION_SEARCH_CONSTS, required=True)

class RelatedWorks(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=RELATED_WORKS_SEARCH_CONSTS, required=True)

class Conclusion(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=CONCLUSION_SEARCH_CONSTS, required=True)

# Loose requirement sections
class Methodology(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=METHODOLOGY_SEARCH_CONST, required=False)

class Experiments(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=EXPERIMENTS_SEARCH_CONSTS, required=False)

class Limitations(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=LIMITATIONS_SEARCH_CONSTS, required=False)

class Results(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=RESULTS_SEARCH_CONSTS, required=False)

class Dataset(SemanticParsedSection):
    def __init__(self):
        super().__init__(text_match_tokens=DATA_SEARCH_CONST, required=False)


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
        # print(json_object['name'])
        object_metadata = json_object['metadata']
        document_object = cls(name=json_object['name'],**object_metadata)
        for subsec in json_object['subsections']:
            document_object.subsections.append(Section.from_json(subsec))
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
      

class MultiLatexResearchDocFactory:
    pass

# class PaperFactory:

#     @staticmethod
#     def from_arxiv_document(document,)