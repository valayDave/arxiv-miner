from tex2py import tex2py

def get_tex_tree(tex_path):
    with open(tex_path,'r') as f:
        data = f.read()
    tex_root_node = tex2py(data)
    return tex_root_node

class Section():
    def __init__(self,name=None):
        self.name = self.__class__.__name__ if name is None else name
        self.subsections = []

    def __str__(self):
        return self.name

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

class Citations(Sections):
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



if __name__ == "__main__":
    tex_path = '1904.03367/Reinforcement_Learning_with_Attention_that_Works.tex'
    get_tex_tree(tex_path)