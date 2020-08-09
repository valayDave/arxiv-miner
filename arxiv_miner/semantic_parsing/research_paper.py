from .structure import *

class ResearchPaper(object):
    def __init__(self,build_new=True):
        super().__init__()
        self.introduction = Introduction() # --> type(Section)
        self.related_works = RelatedWorks() # --> type(Section)
        self.methodology = Methodology() # --> type(Section)
        self.experiments = Experiments() # --> type(Section)
        self.results = Results() # --> type(Section)
        self.conclusion = Conclusion() # --> type(Section)
        self.limitations = Limitations() # --> type(Section)
        self.dataset = Dataset() # --> type(Section)
        self.unknown_sections = []
        self.search_sections = []
        if build_new:
            self._build_search_secs()
    
    def build(self):
        self._build_search_secs()
        
    def _build_search_secs(self):
        self.search_sections = [
            self.introduction,
            self.related_works,
            self.methodology,
            self.dataset,
            self.experiments,
            self.limitations,
            self.conclusion
        ]

    def to_json(self):
        return {
            "introduction":self.introduction.to_json(),
            "related_works":self.related_works.to_json(),
            "methodology":self.methodology.to_json(),
            "experiments":self.experiments.to_json(),
            "results":self.results.to_json(),
            "dataset":self.dataset.to_json(),
            "conclusion":self.conclusion.to_json(),
            "limitations":self.limitations.to_json(),
            "unknown_sections":[sec.to_json() for sec in self.unknown_sections],
        }

    @classmethod
    def from_json(cls,json_object):
        paper = cls()
        paper.introduction = Introduction.from_json(json_object['introduction'])
        paper.related_works = RelatedWorks.from_json(json_object['related_works'])
        paper.methodology = Methodology.from_json(json_object['methodology'])
        paper.experiments = Experiments.from_json(json_object['experiments'])
        paper.results = Results.from_json(json_object['results'])
        paper.dataset = Dataset.from_json(json_object['dataset'])
        paper.conclusion = Conclusion.from_json(json_object['conclusion'])
        paper.limitations = Limitations.from_json(json_object['limitations'])
        paper.unknown_sections = [Section.from_json(sec_obj) for sec_obj in json_object['unknown_sections']]
        paper.build()
        return paper

    @property
    def parsing_results(self):
        num_active_sections = sum(list(map(lambda x : 1 if x.matched else 0 ,self.search_sections)))
        results = {
            'active' : True if (num_active_sections > 0 or len(self.unknown_sections) > 0)  else False,
            'section_matches' : [],
            'num_un_matched': len(self.unknown_sections)
        }
        for sec in self.search_sections:
            if sec.matched:
                results['section_matches'].append(sec.name)
        
        return results

class ResearchPaperSematicParser(ResearchPaper):
    '''
    Convert the `Section` List Document into Research paper based on Section 
    naming patterns.  
    '''
    def __init__(self,sections:List[Section]=None):
        super().__init__()
        if sections:
            self._injest_sections(sections)

    def _injest_sections(self,sections:List[Section]):
        """
        This is supposed to be called on Object creation. 
        The `Sections` are mapped in order. 
        The `Sections` will be checked according to parsing needs. 
        """
        for section in sections:
            self._build_search_secs()
            if not self._match_section(section):
                self.unknown_sections.append(section)
    
    def _match_section(self,section:Section):
        for index,matching_section in enumerate(self.search_sections):
            if not matching_section.matched and matching_section.match_section(section):
                md_text = section.to_markdown()
                matching_section.text = md_text
                matching_section.matched = True
                return True
        return False
        
    def num_marked(self):
        return sum([1 if i.matched else 0 for i in self.search_sections])

    def to_research_paper(self):
        paper = ResearchPaper(build_new=False)
        paper.introduction = self.introduction
        paper.related_works = self.related_works
        paper.methodology = self.methodology
        paper.experiments = self.experiments
        paper.dataset = self.dataset
        paper.results = self.results
        paper.conclusion = self.conclusion
        paper.limitations = self.limitations
        paper.unknown_sections = self.unknown_sections
        paper.build()
        return paper
