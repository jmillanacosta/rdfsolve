"""Extract a connected view with original terms and graph evidence."""
import pytest
from rdflib import Dataset, URIRef
from rdfsolve import SchemaMiner
from rdfsolve.client.api import Client
from rdfsolve.client.hydration import HydrationLimitError

def test_extract_selected_connected_records(tmp_path):
    data = Dataset().parse(data='''@prefix e: <urn:example:> .
      e:data { e:c a e:Chemical; e:group e:g, e:h . e:missing a e:Chemical . }
      e:labels { e:g e:label "PFAS"@en . e:h e:label "Other"@en . }
      e:types { e:g a e:Group . e:h a e:Group; e:noise "excluded" . }
    ''',format="trig")
    with SchemaMiner.from_graph(data, graph_uris=["urn:example:data","urn:example:labels"],
            type_context_graph_uris=["urn:example:types"],delay=0) as miner:
        schema=miner.mine("chemical-groups")
        nav=schema.discover_paths(max_hops=2)
    path=next(p for p in nav.paths if p.steps[-1].property_uri=="urn:example:label")
    selection=schema.select(paths=[path])
    with Client(schema,data) as client:
        result=client.extract(selection, root_class="urn:example:Chemical")
        assert len(result.roots)==2, "Retain chemicals without the selected relationship"
        assert len(result.quads)==8, "Two roots, two links, two labels and two group types"
        assert {q.graph for q in result.quads}=={"urn:example:data","urn:example:labels","urn:example:types"}
        assert all(q.predicate!="urn:example:noise" for q in result.quads)
        labels=[q.object for q in result.quads if q.predicate=="urn:example:label"]
        assert {t.value for t in labels}=={"PFAS","Other"} and all(t.language=="en" for t in labels)
        assert result.selection.source.about.snapshot_id==schema.about.snapshot_id
        restored=type(result).model_validate_json(result.model_dump_json())
        target=tmp_path/"selected.trig"
        restored.save(target)
        saved=Dataset().parse(target,format="trig")
        assert len(saved.graph(URIRef("urn:example:labels")))==2
        only=client.extract(selection,root_class="urn:example:Chemical",roots=["urn:example:missing"])
        assert len(only.roots)==1 and len(only.quads)==1
    with Client(schema,data,max_rows=1) as client:
        with pytest.raises(HydrationLimitError):
            client.extract(selection,root_class="urn:example:Chemical")
