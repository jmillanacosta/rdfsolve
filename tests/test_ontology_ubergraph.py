"""UberGraph as an ontology source (rdfsolve.ontology.ubergraph), on a small copy of its graphs:
named parents, the closure, existential relations, and the most specific Biolink category."""

import pyoxigraph as ox

from rdfsolve.ontology import Ontologies
from rdfsolve.ontology.ubergraph import BIOLINK, BIOLINK_GRAPH, NONREDUNDANT, REDUNDANT, UberGraph

OBO = "http://purl.obolibrary.org/obo/"
SUB = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
L = "https://w3id.org/linkml/"
DATA = f"""
<{NONREDUNDANT}> {{ <{OBO}CL_2> <{SUB}> <{OBO}CL_1> . <{OBO}CL_1> <{SUB}> <{OBO}CL_0> .
  <{OBO}CL_2> <{OBO}BFO_0000050> <{OBO}UBERON_1> . }}
<http://reasoner.renci.org/ontology> {{ <{OBO}CL_2> a <http://www.w3.org/2002/07/owl#Class> .
  <{OBO}CHEBI_17115> a <http://www.w3.org/2002/07/owl#Class> . <{OBO}CHEBI_33384> a <http://www.w3.org/2002/07/owl#Class> .
  <{OBO}CHEBI_16523> a <http://www.w3.org/2002/07/owl#Class> .
  <{OBO}CL_1> a <http://www.w3.org/2002/07/owl#Class> . <{OBO}CL_0> a <http://www.w3.org/2002/07/owl#Class> . }}
<{REDUNDANT}> {{ <{OBO}CL_2> <{SUB}> <{OBO}CL_1>, <{OBO}CL_0>, <{OBO}CL_2>, <http://www.w3.org/2002/07/owl#Thing> .
  <{OBO}CL_1> <{SUB}> <{OBO}CL_0> .
  <{OBO}CHEBI_33384> <{OBO}RO_0018036> <{OBO}CHEBI_17115> . <{OBO}CHEBI_16523> <{OBO}RO_0018039> <{OBO}CHEBI_17115> . }}
<{BIOLINK_GRAPH}> {{ <{OBO}CL_2> <{BIOLINK}category> <{BIOLINK}Cell>, <{BIOLINK}AnatomicalEntity>,
    <{BIOLINK}NamedThing>, <{BIOLINK}PhysicalEssence> .
  <{BIOLINK}Cell> <{L}is_a> <{BIOLINK}AnatomicalEntity> .
  <{BIOLINK}AnatomicalEntity> <{L}is_a> <{BIOLINK}NamedThing> ; <{L}mixins> <{BIOLINK}PhysicalEssence> . }}
"""


def source():
    store = ox.Store()
    store.load(DATA.encode(), ox.RdfFormat.TRIG)

    def select(query):
        result = store.query(query)
        return [{v.value: {"value": row[v].value} for v in result.variables if row[v] is not None} for row in result]

    return UberGraph(select, batch_size=1)


def test_parents_closure_and_relations():
    u = source()
    assert u.parents([OBO + "CL_2"]) == {OBO + "CL_2": {OBO + "CL_1"}}
    assert u.ancestors([OBO + "CL_2", OBO + "CL_9"]) == {OBO + "CL_2": {OBO + "CL_1", OBO + "CL_0"}, OBO + "CL_9": set()}
    assert u.descendants(OBO + "CL_0") == {OBO + "CL_1", OBO + "CL_2"}
    assert u.relations([OBO + "CL_2"]) == [(OBO + "CL_2", OBO + "BFO_0000050", OBO + "UBERON_1")]


def test_the_most_specific_category_leaves_out_ancestors_and_mixins():
    u = source()
    assert u.categories([OBO + "CL_2"]) == {OBO + "CL_2": {BIOLINK + "Cell"}}
    assert len(u.categories([OBO + "CL_2"], most_specific=False)[OBO + "CL_2"]) == 4


def test_ontologies_cache_the_answers_with_their_source(tmp_path):
    ontologies = Ontologies(cache=tmp_path / "cache.json", ubergraph=source())
    assert ontologies.categories([OBO + "CL_2"]) == {OBO + "CL_2": [BIOLINK + "Cell"]}
    offline = Ontologies(cache=tmp_path / "cache.json", offline=True)
    assert offline.categories([OBO + "CL_2"]) == {OBO + "CL_2": [BIOLINK + "Cell"]}
    assert offline.events[-1]["provider"] == "ubergraph" and offline.events[-1]["cached"]
    assert offline.ancestors([OBO + "CL_2"]) == {OBO + "CL_2": None}, "Not asked before: unknown"


def test_a_term_ubergraph_does_not_hold_is_asked_of_ols_not_taken_as_parentless(monkeypatch):
    """UberGraph holds OBO PURL IRIs only: an EDAM term gets its ancestors from OLS, and a term
    that no source knows is None (unknown), never an empty list (no ancestors)."""
    edam = "http://edamontology.org/data_1025"
    ontologies = Ontologies(ubergraph=source())
    terms = {edam: {"iri": edam, "ontology": "edam", "label": "Gene identifier"}}
    monkeypatch.setattr(ontologies, "lookup", lambda iri, hierarchy=True: terms.get(iri))
    pages = {0: ["http://edamontology.org/data_0976"], 1: ["http://edamontology.org/data_0842"]}

    def answer(path, **params):
        assert path.endswith("/ancestors") and "/ontologies/edam/" in path
        return {"_embedded": {"terms": [{"iri": i} for i in pages[params["page"]]]}, "page": {"totalPages": 2}}

    monkeypatch.setattr(ontologies, "_json", answer)
    found = ontologies.ancestors([OBO + "CL_2", edam, "http://example.org/unknown"])
    assert found[OBO + "CL_2"] == [OBO + "CL_0", OBO + "CL_1"]
    assert found[edam] == ["http://edamontology.org/data_0842", "http://edamontology.org/data_0976"]
    assert found["http://example.org/unknown"] is None
    assert ontologies.categories([edam]) == {edam: None}, "Biolink kinds come from UberGraph only"


def test_variants_are_tautomers_and_conjugates_not_enantiomers():
    ontologies = Ontologies(ubergraph=source())
    terms = [OBO + "CHEBI_17115", OBO + "CHEBI_33384", OBO + "CHEBI_16523"]
    assert ontologies.variants(terms) == [(OBO + "CHEBI_33384", OBO + "CHEBI_17115")]
