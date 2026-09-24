"""Keep list order across data graphs with companion owner and member types."""
from rdflib import Dataset, RDF, URIRef
from rdfsolve import SchemaMiner
from rdfsolve.client.api import Client

def test_scoped_list_mining_and_extraction():
    data=Dataset().parse(data='''@prefix e: <urn:example:> .
      @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
      e:links { e:article e:author e:head . e:head rdf:first e:alice; rdf:rest e:tail . }
      e:tailGraph { e:tail rdf:first e:bob; rdf:rest rdf:nil . }
      e:types { e:article a e:Article . e:alice a e:Person . e:bob a e:Person .
                e:head rdf:first e:noise . }
    ''',format="trig")
    with SchemaMiner.from_graph(data,graph_uris=["urn:example:links","urn:example:tailGraph"],
            type_context_graph_uris=["urn:example:types"],delay=0) as miner:
        schema=miner.mine("ordered-authors")
    assert len(schema.collections)==1, "Discover list owners typed in companion context"
    profile=schema.collections[0]
    assert profile.list_count==1 and profile.invalid_count==0
    assert profile.min_length==profile.max_length==2
    assert profile.member_types==["urn:example:Person"]
    with Client(schema,data) as client:
        result=client.extract(schema.select(fields=[("urn:example:Article","urn:example:author")]),
                              root_class="urn:example:Article")
    saved=result.to_dataset()
    first=saved.graph(URIRef("urn:example:links")).value(URIRef("urn:example:head"),RDF.first)
    second=saved.graph(URIRef("urn:example:tailGraph")).value(URIRef("urn:example:tail"),RDF.first)
    assert (str(first),str(second))==("urn:example:alice","urn:example:bob")
    assert not any(q.object.value=="urn:example:noise" for q in result.quads)
