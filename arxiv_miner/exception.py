
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

class ArxivDatabaseConnectionException(Exception):
     def __init__(self,host,port,error_message):
        msg = "Exception Connecting To ArxivDatabase on %s : %d \n\n %s"%(host,port,error_message)
        super(ArxivAPIException, self).__init__(msg)


class SectionSerialisationException(LatexParserException):
     def __init__(self,ms):
        msg = "Serialisation of Section Object Requeses %s"%ms
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