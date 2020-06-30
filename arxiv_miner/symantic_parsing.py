import json
from .exception import SectionSerialisationException
import os 

class Section():
    """Section 
    Section will contain subsections which are of type Section
    """
    def __init__(self,name=None):
        # Core Attributes
        self.name = self.__class__.__name__ if name is None else name
        self.subsections = []
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

    @staticmethod
    def _clean_text(text):
        return text.replace('\\n','\n').replace('\t','').replace('\r','')
    
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
      