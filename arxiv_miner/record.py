"""
The Purpose of this Module is to Hold all the Major Storage Attributes of the Project.
All the datastructures are used by Procssing Objects during the processing/results 
pipeline. 
"""
import datetime
import dateparser
from dataclasses import dataclass,field
from dataclasses import asdict as D2D
from typing import List

from .semantic_parsing import \
    ArxivDocument,\
    ResearchPaper

from .utils import load_json_from_file,dir_exists
from .exception import ArxivIdentityNotFoundException,CorruptArxivRecordException

# ISO Records Should be a Date Standard For all Record Docs. 
class ArxivLatexParsingResult:
    # Dict can create indexing issues. with file_results
    def __init__(self,\
                section_list=None,\
                some_section_failed=None,\
                file_results=[],\
                parsing_error=False,\
                error_message=None,\
                latex_parsing_method=None):
                
        self.section_list = section_list
        self.some_section_failed = some_section_failed
        self.parsing_error = parsing_error
        self.error_message = error_message
        self.latex_parsing_method  = latex_parsing_method
        self.file_results = file_results

    def to_json(self):
        return dict(
            some_section_failed=self.some_section_failed,
            parsing_error=self.parsing_error,
            error_message=self.error_message,
            latex_parsing_method = self.latex_parsing_method,
            file_results = self.file_results
        )


    def __str__(self):
        return """
        some_section_failed : {some_section_failed}
        parsing_error : {parsing_error}
        error_message : {error_message}
        latex_parsing_method : {latex_parsing_method}
        """.format(
                **self.to_json()
            )

class ArxivIdentity:
    """ ArxivIdentity:
    Holds the Individual Identity of the Arxiv Record For Faster Diff in Database.
    """
    def __init__(self,
                identity=None,
                url=None,
                title=None,
                abstract=None,
                categories=None,
                published=None,
                updated=None,
                authors=None,
                affiliation=None,
                journal_reference=None,
                version=None,
                created_on =None
                ):
        self.identity = identity
        self.url = url
        self.title = title
        self.abstract = abstract
        self.categories = categories
        self.published = published
        self.updated = published if updated is None or updated== '' else updated
        self.authors = authors
        self.affiliation = affiliation
        self.version = version
        self.journal_reference = journal_reference
        if created_on is None:
            created_on = datetime.datetime.now().isoformat()

        created_on = dateparser.parse(str(created_on))
        self.created_on = created_on

    @classmethod
    def from_oa2_response(cls,oa2Record):
        # raw_id,version = cls.parse_arxiv_url(oa2Record['url'])

        identity_dict = {
            "identity":oa2Record['id'],
            "url":oa2Record['url'], # Version changes come here. Should they be pruned ? TODO 
            "title":oa2Record['title'],
            "abstract":oa2Record['abstract'],
            "categories": oa2Record['categories'].split(' '),
            "journal_reference": oa2Record['journal_reference'],
            "published":oa2Record['created'],
            "updated":oa2Record['created'] if oa2Record['updated'] == '' else oa2Record['created'],
            "authors":oa2Record['authors'],
            "affiliation":oa2Record['affiliation'],
            "version":None # Need to check at Mining Time as Not Best Avaialable during scraping. 
        }
        return cls(**identity_dict)

    @classmethod
    def from_arxiv_response(cls,arxiv_response):
        """ 
        Creates the `ArxivIdentity` From the response from http://export.arxiv.org/api/query
        :param arxiv_response: [dict]
        """
        raw_id,version = cls.parse_arxiv_url(arxiv_response['id'])
        
        identity_dict = {
            "identity":raw_id,
            "url":arxiv_response['arxiv_url'], # Version changes come here. Should they be pruned ? TODO 
            "title":arxiv_response['title'],
            "abstract":arxiv_response['summary'],
            "categories":[ t['term'] for t in arxiv_response['tags'] ],
            "journal_reference":arxiv_response['journal_reference'],
            "published":arxiv_response['published'],
            "updated": arxiv_response['published'] if arxiv_response['updated'] == '' else arxiv_response['updated'],
            "authors":arxiv_response['authors'],
            "affiliation":[],
            "version":version
        }
        return cls(**identity_dict)

    @classmethod    
    def from_file(cls,file_path):
        if not dir_exists(file_path):
            raise ArxivIdentityNotFoundException('',file_path)
        return cls(**load_json_from_file(file_path))

    @staticmethod
    def parse_arxiv_url(url):
        """ 
        examples is http://arxiv.org/abs/1512.08756v2
        we want to extract the raw id and the version
        From Andrej Karpathy's Arxiv-Sanity 
        """
        ix = url.rfind('/')
        idversion = url[ix+1:] # extract just the id (and the version)
        parts = idversion.split('v')
        assert len(parts) == 2, 'error parsing url ' + url
        return parts[0], int(parts[1])

    def to_json(self):
        data_dict = {**self.__dict__}
        data_dict['created_on'] = data_dict['created_on'].isoformat()
        return data_dict

    def __str__(self):
        meta_str = ''.join(['\t'+str(t[0])+"  :  "+str(t[1])+'\n' for t in self.__dict__.items()])
        return 'ARXIV IDENTITY(%s)\n--------------\n%s'%(self.identity,meta_str)

class ArxivPaperProcessingMeta():
    """ArxivPaperProcessingMeta 
    Holds High Level summary of the Processing Status of the Paper.
    """
    def __init__(self,
                 pdf_only=False,\
                 latex_files=0,\
                 mined = True,\
                 latex_parsed=True,\
                 updated_on=None,
                 ):
        # Metadata about the Arxiv Latex project.
        self.pdf_only = pdf_only
        self.latex_files = latex_files  # number of files
        if updated_on is None:
            updated_on = datetime.datetime.now().isoformat()

        self.updated_on = dateparser.parse(str(updated_on))# .isoformat() # When was it Mined. 
        # latex-processing status
        self.mined = mined # if no error processing and not a PDF it is True
        
        # True if mined==True and section_list > len 0  else false. 
        self.latex_parsed = latex_parsed

    def __str__(self):
        return '''
        Processing MetaData
        -------------------
        pdf_only = {pdf_only}
        latex_parsed = {latex_parsed}
        updated_on = {updated_on}
        latex_files = {latex_files}
        mined = {mined}
        '''.format(**self.to_json())
        
    def to_json(self):
        data_dict = {**self.__dict__}
        data_dict['updated_on'] = data_dict['updated_on'].isoformat()
        return data_dict


class ArxivRecord(object):
    # Core Identity 
    identity:ArxivIdentity = None

    # Processing Metadata
    # meta about processing results 
    paper_processing_meta : ArxivPaperProcessingMeta = None
    # latex processing metadata 
    latex_parsing_result : ArxivLatexParsingResult = None

    # First Level Of Processing Generated Document 
    latex_parsed_document : ArxivDocument = None

    # ToDo : add Final Organisaed `RearchDocument` to This. 
    # meta about the arxiv article's Identity
    identity_file_name='arxiv.json'

    paper_meta_save_file_name='processing_meta.json'

    # the actual parsed object document from latex. 
    latex_parsing_result_file_name = 'latex_processing_results.json'

    def __init__(self,\
            identity=None,\
            paper_processing_meta=None,\
            latex_parsed_document=None,\
            latex_parsing_result=None,\
            created_on = None
            ):
        self.identity = identity
        self.paper_processing_meta = paper_processing_meta
        self.latex_parsed_document = latex_parsed_document
        self.latex_parsing_result = latex_parsing_result
        if created_on is None:
            created_on = datetime.datetime.now().isoformat()
        
        self.created_on =dateparser.parse(str(created_on))# .isoformat()


    def to_json(self):
        data_dict = {
            'identity' :self.identity.to_json(),
            'paper_processing_meta' : None if self.paper_processing_meta is None else self.paper_processing_meta.to_json(),
            'latex_parsing_result' : None if self.latex_parsing_result is None else self.latex_parsing_result.to_json(),
            'latex_parsed_document' : None if self.latex_parsed_document is None else self.latex_parsed_document.to_json(),
            'created_on' : self.created_on.isoformat()
        }
        return data_dict
    
    @classmethod
    def from_json(cls,json_object):
        identity = None
        paper_processing_meta = None
        latex_parsing_result = None
        latex_parsed_document = None
        if 'identity' not in json_object:
            raise CorruptArxivRecordException()
        
        identity = ArxivIdentity(**json_object['identity'])
        if 'paper_processing_meta' in json_object and json_object['paper_processing_meta']:
            paper_processing_meta = ArxivPaperProcessingMeta(**json_object['paper_processing_meta'])
        if 'latex_parsing_result' in json_object and json_object['latex_parsing_result']:
            latex_parsing_result = ArxivLatexParsingResult(**json_object['latex_parsing_result'])
        if 'latex_parsed_document' in json_object and json_object['latex_parsed_document']:
            latex_parsed_document = ArxivDocument.from_json(json_object['latex_parsed_document'])
        
        return cls(
            identity = identity,
            paper_processing_meta = paper_processing_meta,
            latex_parsing_result = latex_parsing_result,
            latex_parsed_document = latex_parsed_document,
            created_on= json_object['created_on']
        )


@dataclass
class Ontology:
    syntactic:List[str] =  field(default_factory=lambda : [])
    semantic:List[str]=  field(default_factory=lambda : [])
    union:List[str] =  field(default_factory=lambda : [])
    enhanced:List[str] = field(default_factory=lambda : [])
    mined:bool = False
@dataclass
class Author:
    name:str = None
    email:str = None

@dataclass
class CoreOntology:
    value:str = None

class ArxivSematicParsedResearch:

    research_object:ResearchPaper
    identity:ArxivIdentity
    ontology:Ontology

    def __init__(self,\
            identity=None,\
            research_object=None,\
            created_on = None,
            ontology=Ontology(),
            ):
        self.identity = identity
        self.research_object = research_object
        if created_on is None:
            created_on = datetime.datetime.now().isoformat()
        self.created_on =dateparser.parse(str(created_on))# .isoformat()
        self.ontology = ontology
    
    def to_json(self):

        return {
            'identity': self.identity.to_json(),
            'research_object': self.research_object.to_json(),
            'parsing_stats': self.research_object.parsing_results,
            'created_on' : self.created_on.isoformat(),
            'ontology':D2D(self.ontology)
        }
    
    @classmethod
    def from_json(cls,json_object):
        if 'identity' not in json_object:
            raise CorruptArxivRecordException()
        identity = ArxivIdentity(**json_object['identity'])
        research_object = ResearchPaper.from_json(json_object['research_object']) 
        ontology = Ontology()
        if 'ontology' in json_object:
            ontology = Ontology(**json_object['ontology'])
        return cls(\
                identity=identity,\
                research_object=research_object,\
                created_on= json_object['created_on'],
                ontology = ontology
            )


class ArxivPaperStatus:
    """ 
    This Record will help mark different stages of the 
    process From mining To Scraping Etc. For a Particular paper
    from Arxiv. 
    """

    def __init__(self,
                mined = False,
                created_on =None,
                scraped = False,
                mining = False,
                updated_on = None
                ):
        self.scraped = scraped
        
        # Latex Mining Status
        self.mined = mined
        self.mining = mining

        if updated_on is None:
            updated_on = datetime.datetime.now().isoformat()

        if created_on is None:
            created_on = datetime.datetime.now().isoformat()
        
        # Timestamps.
        self.created_on = dateparser.parse(str(created_on))# .isoformat()
        self.updated_on = dateparser.parse(str(updated_on))# .isoformat()
    
    def to_json(self):
        data_dict = {**self.__dict__}
        data_dict['created_on'] = data_dict['created_on'].isoformat()
        data_dict['updated_on'] = data_dict['updated_on'].isoformat()
        return data_dict

    def update(self):
        self.updated_on = datetime.datetime.now()# .isoformat()
    
    @classmethod
    def from_json(cls,json_object):
        return cls(**json_object)