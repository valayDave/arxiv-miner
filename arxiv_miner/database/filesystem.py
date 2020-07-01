"""
This Module is responsible for Working as a Generalised FS based Database for Scraping/Mining etc. 
This uses the `ArxivDatabase` Adapter to create an FS driven DB. 
"""
import os
from ..record import ArxivRecord,ArxivIdentity,ArxivPaperStatus
from ..utils import dir_exists,save_json_to_file,load_json_from_file
from ..paper import ArxivPaper
from ..logger import create_logger
from .core import ArxivDatabase

class PaperMap:
    """
    This Datastructure is responsible for Storing the Metadata
    About the Scraping/Mining For the FS Database. 

    `ArxivDatabase` is an adapter class so that one can Quickly switch from an FS based database to 
    """
    filename = 'paper_map.json'
    paper_map = {}
    unmined_set = set()

    def __init__(self,data_root_path,build_new=False):
        self.papers_path = os.path.join(data_root_path,'papers')
        self.root_path = os.path.join(data_root_path,'map')
        self._init_paper_map(build_new=build_new)
    
    def save_map(self):
        if not dir_exists(self.root_path):
            os.makedirs(self.root_path)
        save_json_to_file(self.to_json(),os.path.join(self.root_path,self.filename))

    def _init_paper_map(self,build_new=False):
        """_init_paper_map 
        Load Map from a Directory Or Build one from The papers Path
        :param build_new [bool] : if True, Will initiate Proceess of Building from FS with paper_path Else it will use the map/paper_map.json
        """
        if build_new:
            self._load_map_from_fs()
            return 
        map_path = os.path.join(self.root_path,self.filename)
        if dir_exists(map_path):
            json_map = load_json_from_file(map_path)
            self.paper_map,self.unmined_set = self._from_json(json_map)
            return 
        # if PaperMap Json Doesn't Exist on Path and There Are a few Folders on the Papers path then
        self._load_map_from_fs()
        return 
    
    def _load_map_from_fs(self):
        """_load_map_from_fs 
        Method to Build the paper_map : this Helps with Mining and Scraping Process. 
        paper_map = {
            '0704.3931' : `ArxivPaperStatus`
        }
        """
        list_of_subfolders_with_paths = [f.path for f in os.scandir(self.papers_path) if f.is_dir()]
        for path in list_of_subfolders_with_paths:
            paper_id = path.split('/')[-1]
            try:
                paper = ArxivPaper.from_fs(paper_id,self.papers_path)
                # print("Adding Paper : ",paper_id)
                mined = True if paper.paper_processing_meta is not None else False
                # Create Paper Status in PaperMap.
                self.paper_map[paper_id] = ArxivPaperStatus(mined=mined,scraped=True)
                # Add to unmined_set if paper is Not Mined. 
                if not mined:
                    self.unmined_set.add(paper_id)
            except Exception as e:
                continue

    def _from_json(self,json_object):
        unmined_set = set()
        for key in json_object:
            json_object[key] = ArxivPaperStatus.from_json(json_object[key])
            if not json_object[key].mined:
                unmined_set.add(key)

        return json_object,unmined_set

    def __len__(self):
        return len(self.paper_map.keys())

    def __getitem__(self,paper_id):
        if paper_id not in self.paper_map:
            return None
        return self.paper_map[paper_id]

    def to_json(self):
        return dict((paper_id,self.paper_map[paper_id].to_json()) for paper_id in self.paper_map)

    def add(self,paper_id):
        if paper_id not in self.paper_map:
            self.paper_map[paper_id] = ArxivPaperStatus(scraped=True) # Create When An Identity is Scraped.
            self.unmined_set.add(paper_id)

    def get_unmined_paper(self) -> str:
        if len(self.unmined_set) == 0:
            return None        
        return self.unmined_set.pop()

    def add_unmined_id(self,paper_id):
        self.unmined_set.add(paper_id)
        self.update()


class ArxivFSDatabase(ArxivDatabase):
    # 4 M IDS IN MEMORY FOR 600 MB MEMORY : MAAAAX
    """ArxivFSDatabase 

    Works Similar to the `ArxivLoader`. 
    It will Be a centralised Database between Scraping Engine and Mining Engine.
    `ArxivFSDatabase` uses `PaperMap` to Help with Querying.  
    
    This Database Can Later GeT replaced with a more formal search Engine. 
    """
    def __init__(self,data_root_path,build_new_map=False):
        paper_path = os.path.join(data_root_path,'papers')
        if not dir_exists(paper_path):
            os.makedirs(paper_path) 
        self.papers_path = paper_path
        self.paper_map = PaperMap(data_root_path,build_new=build_new_map)
        self.logger = create_logger(self.__class__.__name__)
        self.logger.info("Database Has Started Currently With Papers : %d"%len(self.paper_map))

    
    def query(self, paper_id) -> ArxivRecord:
        # paper_path = os.path.join(self.papers_path,paper_id)
        # Ideally if It is not in Paper Map then there is no chance
        # That the paper is present in the 
        if self.paper_map[paper_id] is None: 
            return None
        paper = ArxivPaper.from_fs(paper_id,self.papers_path)
        record = paper.to_arxiv_record()
        return record
            
    def save_identity(self,identity:ArxivIdentity):
        paper_path = os.path.join(self.papers_path,identity.identity)
        paper_meta_path = os.path.join(paper_path,ArxivRecord.identity_file_name)
        # Update The Map if there is no Identity in the Map. 
        if self.paper_map[identity.identity] is None:
            self.paper_map.add(identity.identity) # Add paper to the map(It also sets scraped=True)
        # Save paper identity. 
        if not dir_exists(paper_path):
            os.makedirs(paper_path)
        save_json_to_file(identity.to_json(),paper_meta_path)
        
    def save_record(self,record:ArxivRecord):
        paper = ArxivPaper.from_arxiv_record(self.papers_path,record)
        paper.to_fs()

    def set_mined(self,identity:ArxivIdentity,mined_status:bool):
        self.paper_map[identity.identity].mined = mined_status
        if not mined_status: # If setting it as unmined then Re-add it to set.
            self.paper_map.add_unmined_id(identity.identity)

    def get_unmined_paper(self) -> ArxivRecord:
        paper_id = self.paper_map.get_unmined_paper()
        if paper_id is None:
            return None
        return self.query(paper_id)
        
