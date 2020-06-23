# Download the Arxiv Dataset with all Papers in 
import arxiv
from utils import dir_exists,save_json_to_file,load_json_from_file
import os
import tarfile
import glob
import json

class ArxivPaperMeta():
    
    def __init__(self,\
                pdf_only = False,\
                latex_files = 0,\
                mined = True):
        self.pdf_only = pdf_only
        self.latex_files = latex_files
        self.mined = mined

    def to_json(self):
        return dict(
            pdf_only =self.pdf_only,\
            latex_files = self.latex_files,\
            mined = self.mined,\
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
        

        Analytics
        ---------
        pdf_only = {pdf_only}
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
        
        file_names = list(glob.glob(os.path.join(self.latex_root_path,"*.tex")))
        # $ Set some Common Meta WRT the Paper. 
        self.paper_meta = ArxivPaperMeta()
        self.paper_meta.latex_files = len(file_names)
        self.paper_meta.pdf_only = True if len(file_names) == 0 else False
        # $ Remove the Tar File.
        os.remove(downloaded_data)
        # $ Save the Metadata
        self._save_metadata_to_fs()
