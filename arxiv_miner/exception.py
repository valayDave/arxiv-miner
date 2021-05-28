
class LatexParserException(Exception):
    headline = 'Latex Parsing failed'
    def __init__(self, msg='', lineno=None):
        self.message = msg
        self.line_no = lineno
        super(LatexParserException, self).__init__()

    def __str__(self):
        prefix = 'line %d: ' % self.line_no if self.line_no else ''
        return '%s%s' % (prefix, self.message)

class ArxivFSLoadingError(Exception):
    def __init__(self,path):
        msg = "FS Path To Arxiv Mined Data Doesn't Exist %s"%path
        super(ArxivFSLoadingError, self).__init__(msg)

class ArxivMetaNotFoundException(Exception):
    def __init__(self,paper_id):
        msg = "Metadata About Arxiv PaperId %s Not Found on FS "%paper_id
        super(ArxivMetaNotFoundException, self).__init__(msg)

class ArxivIdentityNotFoundException(Exception):
    def __init__(self,paper_id,path):
        msg = "ArxivIdentity of PaperId %s Not Found on FS for Path : %s"%(paper_id,path)
        super(ArxivIdentityNotFoundException, self).__init__(msg)

class ArxivAPIException(Exception):
    def __init__(self,paper_id,message):
        msg = "Exception Reaching Arxiv API. PaperId %s Not Found on FS \n\n %s"%(paper_id,message)
        super(ArxivAPIException, self).__init__(msg)

class SectionSerialisationException(LatexParserException):
     def __init__(self,ms):
        msg = "Serialisation of Section Object Requires %s"%ms
        super(SectionSerialisationException, self).__init__(msg)



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

class CorruptArxivRecordException(Exception):
     def __init__(self):
        msg = "ArxivRecord Is Corrupt And Cannot Load From Dict"
        super(CorruptArxivRecordException, self).__init__(msg)

# DB RELATED EXCEPTIONS
class ArxivDatabaseConnectionException(Exception):
     def __init__(self,host,port,error_message):
        msg = "Exception Connecting To ArxivDatabase on %s : %d \n\n %s"%(host,port,error_message)
        super(ArxivDatabaseConnectionException, self).__init__(msg)

class DepsMissingException(Exception):
    """DepsMissingException 
    Used to With Soft deps so that install instructions can be cleanly printed 
    """
    headline = "[DEPENDENCY_MISSING_EXCEPTION]"
    def __init__(self,message,missing_deps=[],install_mode='pip install'):
        # msg = "Exception Connecting To ArxivDatabase on %s : %d \n\n %s"%(host,port,error_message)
        install_instructions = [ install_mode+' '+d for d in missing_deps]
        install_str = "\n Please perform To Install Dependencies: \n %s"%'\n\t'.join(install_instructions)
        msg = message + install_str
        super(DepsMissingException, self).__init__(msg)


class ElasticsearchMissingException(DepsMissingException):
    
     def __init__(self):
        es_deps = ['elasticsearch','elasticsearch_dsl']
        msg = "Elasticsearch Dependencies Not Installed in Python"
        super(ElasticsearchMissingException, self).__init__(msg,missing_deps=es_deps)

class ElasticsearchIndexMissingException(Exception):
    def __init__(self):
        msg = 'Index To Elasticsearch Cannot Be None'
        super(ElasticsearchIndexMissingException, self).__init__(msg)