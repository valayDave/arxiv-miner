"""
The Purpose of this Module is to Hold all the Major Storage Attributes of the Project.
All the datastructures are used by Procssing Objects during the processing/results 
pipeline. 
"""
import datetime
from arxivscraper import Record
from .symantic_parsing import ArxivDocument
from .utils import load_json_from_file,dir_exists
from .exception import ArxivIdentityNotFoundException

DATE_FORMAT = '%d-%b-%Y (%H:%M:%S.%f)'
class ArxivLatexParsingResult:

    def __init__(self,\
                section_list=None,\
                some_section_failed=None,\
                file_results={},\
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
        section_list_size: {section_list}
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
                created_on =None):
        self.identity = identity
        self.url = url
        self.title = title
        self.abstract = abstract
        self.categories = categories
        self.published = published
        self.updated = updated
        self.authors = authors
        self.affiliation = affiliation
        self.version = version
        self.journal_reference = journal_reference
        if created_on is None or type(created_on) == 'list': 
            create_on = datetime.datetime.now().strftime(DATE_FORMAT)
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
            "updated":oa2Record['updated'],
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
            "updated":arxiv_response['updated'],
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
        return {**self.__dict__}

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
                 updated_on=datetime.datetime.now().strftime(DATE_FORMAT),
                 ):

        # Metadata about the Arxiv Latex project.
        self.pdf_only = pdf_only
        self.latex_files = latex_files  # number of files
        self.updated_on = updated_on # When was it Mined. 

        # latex-processing status
        self.mined = mined # if no error processing and not a PDF it is True
        
        # True if mined==True and section_list > len 0  else false. 
        self.latex_parsed = latex_parsed
        
    def to_json(self):
        return {**self.__dict__}


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
            latex_parsing_result=None\
            ):
        self.identity = identity
        self.paper_processing_meta = paper_processing_meta
        self.latex_parsed_document = latex_parsed_document
        self.latex_parsing_result = latex_parsing_result



class ArxivPaperStatus:
    """ 
    This Record will help mark different stages of the 
    process From mining To Scraping Etc. For a Particular paper
    from Arxiv. 
    """

    def __init__(self,
                mined = False,
                created_on = datetime.datetime.now().strftime(DATE_FORMAT),
                scraped = False
                ):
        self.mined = mined
        self.scraped = scraped
        self.created_on = created_on
        self.updated_on = None
    
    def to_json(self):
        return {**self.__dict__}

    def update(self):
        self.updated_on = datetime.datetime.now().strftime(DATE_FORMAT)
    
    @classmethod
    def from_json(cls,json_object):
        return cls(**json_object)