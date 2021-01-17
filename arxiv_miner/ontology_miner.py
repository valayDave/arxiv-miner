ONTOLOGY_MINABLE = True
try:
    import classifier.classifier as CSO
except:
    ONTOLOGY_MINABLE = False
    print("No CSO Classifier Present")
from typing import List
from .record import ArxivIdentity,Ontology,ArxivSematicParsedResearch

class OntologyMiner:

    is_minable = ONTOLOGY_MINABLE
    
    @staticmethod
    def mine_paper(record:ArxivIdentity):
        ontology = None
        try:
            cso_ontology = CSO.run_cso_classifier({"title":record.title,"abstract":record.abstract})
            ontology = Ontology(
                mined=True, **cso_ontology
            )
        except Exception as e:
            ontology = Ontology()
        return ontology

    @staticmethod
    def mine_lots_of_papers(records:List[ArxivSematicParsedResearch]):
        ontologies = []
        try:
            mine_dict = {}
            id_dict = {}
            for r in records:
                mine_dict[r.identity.identity] = {"title":r.identity.title,"abstract":r.identity.abstract}
                id_dict[r.identity.identity] = r
            cso_ontology = CSO.run_cso_classifier_batch_model_single_worker(mine_dict)
            for ontid in cso_ontology:
                identity = id_dict[ontid]
                ontology = Ontology(
                    mined=True, **cso_ontology[ontid]
                )
                ontologies.append(
                    (identity,ontology)
                )
        except Exception as e:
            return []
        return ontologies