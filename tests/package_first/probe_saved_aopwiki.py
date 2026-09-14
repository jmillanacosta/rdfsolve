"""Exercise the actual package against the saved AOPWiki schema and RDF excerpt."""
from pathlib import Path
import json
from rdflib import Graph,RDF,URIRef
from rdfsolve.client_api import Client

ROOT=Path(__file__).resolve().parents[2]
AOP='http://aopkb.org/aop_ontology#AdverseOutcomePathway'
CHEMICAL='http://semanticscience.org/resource/CHEMINF_000000'
STRESSOR='http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C54571'
source=Graph().parse(ROOT/'tests/test_data/aopwikirdf_phenobarbital_excerpt.ttl')
c=Client.open(ROOT/'notebooks/data/aopwikirdf.schema.json',source=source,graph_uris=[])
w=c.workspace(source_id='saved-aopwiki')
found=w.find('Phenobarbital',kind=CHEMICAL)
assert len(found['records'])==1, found
chemical=found['records'][0]['ref']
assert w.records[chemical].uri=='https://identifiers.org/cas/50-06-6'
paths=w.paths(AOP,chemical,max_hops=2)
# Select only the evidenced stressor -> chemical route, not lexical similarity.
routes=[p for p in paths['paths'] if p.get('steps')==2]
assert routes, paths
route=next(p for p in routes if any(step[2]==STRESSOR for step in w.fragments[p['ref']].steps))
sparql='SELECT DISTINCT ?aop ?stressor ?chemical WHERE { {{'+route['ref']+' ?aop ?stressor ?chemical}} }'
prepared=w.prepare(sparql)
probe=w.probe(prepared['query_ref'],limit=1)
assert not w.executions
finished=w.finish(prepared['query_ref'])
assert finished['state']=='complete', finished
rows=w.resource(finished['result_ref'])['bindings']
actual={tuple(r[k]['value'] for k in ['aop','stressor','chemical']) for r in rows}
expected={(str(a),str(s),str(o)) for a,p,s in source if p==URIRef(STRESSOR) and (a,RDF.type,URIRef(AOP)) in source
          for o in source.objects(s,URIRef('http://aopkb.org/aop_ontology#has_chemical_entity'))
          if str(o)==w.records[chemical].uri}
assert actual==expected and len(actual)==2, (actual,expected)
output={'source':'supplied AOPWiki RDF excerpt, not live endpoint',
 'schema':str(ROOT/'notebooks/data/aopwikirdf.schema.json'),
 'typed_model':type(w.records[chemical]).__name__,
 'found':found,'route':route,'probe':probe,
 'query':w.prepared[prepared['query_ref']].sparql,
 'bindings':rows,'exact_oracle':'independent traversal of source RDF, not generated query text',
 'package':w.diagnostics()}
print(json.dumps(output,indent=2,default=str))
