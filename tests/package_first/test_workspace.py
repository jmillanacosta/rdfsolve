"""Behavioral oracles run the real Client, generated models, helper and RDFLib.

No model-generated gold queries. Fixtures contain deliberate false positives,
missing optional data, multiple values, and conflicting named-graph evidence.
"""
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path

import pytest
from pydantic import BaseModel
from rdflib import BNode, Dataset, Graph, Literal, Namespace, RDF, RDFS, XSD

from rdfsolve.client_api import Client
from rdfsolve.mcp.server import CONTRACTS, dispatch
from rdfsolve.query import QueryResult
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation
from rdfsolve.schema_models.pattern import SchemaPattern

E=Namespace('https://workspace-test.invalid/')


def insert(ref,*variables):
    return '{{'+ref+(' '+' '.join('?'+v for v in variables) if variables else '')+'}}'


def fixture(graph=None,scope=None):
    g=graph if graph is not None else Graph()
    triples=[
        (E.humanAOP,RDF.type,E.AOP),(E.mouseAOP,RDF.type,E.AOP),
        (E.noChemicalAOP,RDF.type,E.AOP),
        (E.Human,RDF.type,E.Taxon),(E.Mouse,RDF.type,E.Taxon),
        (E.Human,RDFS.label,Literal('Human')),(E.Mouse,RDFS.label,Literal('Mouse')),
        (E.humanAOP,E.taxon,E.Human),(E.mouseAOP,E.taxon,E.Mouse),
        (E.noChemicalAOP,E.taxon,E.Human),
        (E.humanAOP,E.context,Literal('Thyroid response in this species')),
        (E.mouseAOP,E.context,Literal('Thyroid in mice; relevance to human health unknown')),
        (E.noChemicalAOP,E.context,Literal('Thyroid response without chemical metadata')),
        (E.ke1,RDF.type,E.Event),(E.ke2,RDF.type,E.Event),(E.ke3,RDF.type,E.Event),
        (E.humanAOP,E.event,E.ke1),(E.humanAOP,E.event,E.ke3),(E.mouseAOP,E.event,E.ke2),
        (E.ke1,E.taxon,E.Mouse),(E.ke2,E.taxon,E.Human),
        (E.ke1,E.method,Literal('Assay A',lang='en')),(E.ke1,E.method,Literal('Assay B',lang='en')),
        (E.ke2,E.method,Literal('Unrelated assay')),
        (E.chemical1,RDF.type,E.Chemical),(E.humanAOP,E.chemical,E.chemical1),
        (E.chemical1,RDFS.label,Literal('Name one',lang='en')),
        (E.chemical1,RDFS.label,Literal('Naam twee',lang='nl')),
        (E.chemical1,E.identifier,Literal('001',datatype=XSD.string)),
        (E.chemical1,E.identifier,Literal('123',datatype=XSD.string)),
    ]
    for t in triples:g.add(t)
    patterns=[(E.AOP,E.taxon,E.Taxon),(E.AOP,E.context,'Literal'),(E.AOP,E.event,E.Event),
              (E.AOP,E.chemical,E.Chemical),(E.Event,E.taxon,E.Taxon),(E.Event,E.method,'Literal'),
              (E.Taxon,RDFS.label,'Literal'),(E.Chemical,RDFS.label,'Literal'),(E.Chemical,E.identifier,'Literal')]
    schema=MinedSchema(about={'dataset_name':'test'},patterns=[SchemaPattern(subject_class=str(a),property_uri=str(p),object_class=str(b)) for a,p,b in patterns])
    labels=[(E.AOP,'Adverse Outcome Pathway'),(E.Event,'Key Event'),(E.Taxon,'Taxon'),(E.Chemical,'Chemical'),
            (E.event,'has Key Event'),(E.taxon,'applicable taxon'),(E.method,'measurement method')]
    for iri,label in labels:
        schema.enrichment.labels.append(TermAnnotation(term_iri=str(iri),predicate=str(RDFS.label),text=RdfTerm(kind='literal',value=label)))
    c=Client(schema,g,graph_uris=scope or [])
    return c.workspace()


def fr(w,owner,p):
    model=w.client.model(str(owner))
    return w.field_refs[(str(owner),w.client.field_name(model,str(p))) ]


def typ(w,cls,var):
    return insert(w.type_refs[str(cls)],var)


def run(w,query):
    p=w.prepare(query)
    assert p['state']=='prepared' and p['variables']
    done=w.finish(p['query_ref'])
    assert done['state']=='complete'
    data=w.resource(done['result_ref'])
    assert data['query']==w.prepared[p['query_ref']].sparql==w.client.queries[-1]
    return data['bindings']


def key(rows,*columns):
    return Counter(tuple((r[c]['type'],r[c]['value'],r[c].get('datatype'),r[c].get('xml:lang')) if c in r else None for c in columns) for r in rows)


def test_actual_typed_grounding_drives_taxon_path_not_prose():
    w=fixture()
    found=w.find('Human',kind=str(E.Taxon))
    human=found['records'][0]['ref']
    assert isinstance(w.records[human],BaseModel)
    assert w.records[human].uri==str(E.Human)
    before=len(w.client.queries)
    assert w.find('Human',kind=str(E.Taxon))==found
    assert len(w.client.queries)==before  # Actual record/evidence cache.
    paths=w.paths(str(E.AOP),human,max_hops=1)
    p=paths['paths'][0]['ref']
    rows=run(w,'SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' '+insert(p,'a','tax')+' '+insert(fr(w,E.AOP,E.context),'a','text')+' FILTER(CONTAINS(LCASE(STR(?text)),"thyroid")) }')
    assert {r['a']['value'] for r in rows}=={str(E.humanAOP),str(E.noChemicalAOP)}
    assert str(E.mouseAOP) not in {r['a']['value'] for r in rows}
    assert w.diagnostics()['package_calls']['Client.find']==1
    assert w.diagnostics()['package_calls']['Client.paths_between']==1
    assert w.diagnostics()['package_calls']['Client.select']==1
    assert str(E.Human) in w.client.queries[-1]


def test_key_event_outputs_species_and_methods_have_correct_owners():
    w=fixture()
    human=w.find('Human',kind=str(E.Taxon))['records'][0]['ref']
    p=w.paths(str(E.AOP),human,max_hops=1)['paths'][0]['ref']
    q=('SELECT ?a ?event ?species ?method WHERE { '+typ(w,E.AOP,'a')+' '+insert(p,'a','aopTaxon')+' '+
       insert(fr(w,E.AOP,E.event),'a','event')+' OPTIONAL { '+insert(fr(w,E.Event,E.taxon),'event','species')+' } OPTIONAL { '+
       insert(fr(w,E.Event,E.method),'event','method')+' } }')
    rows=run(w,q)
    actual={(r['a']['value'],r['event']['value'],r.get('species',{}).get('value'),r.get('method',{}).get('value')) for r in rows}
    assert actual=={(str(E.humanAOP),str(E.ke1),str(E.Mouse),'Assay A'),
                    (str(E.humanAOP),str(E.ke1),str(E.Mouse),'Assay B'),
                    (str(E.humanAOP),str(E.ke3),None,None)}
    assert all(r.get('method',{}).get('xml:lang')=='en' for r in rows if 'method' in r)


def test_optional_dependencies_do_not_rebind_and_multivalues_preserve_terms():
    w=fixture()
    q=('SELECT ?a ?c ?label ?id WHERE { '+typ(w,E.AOP,'a')+' OPTIONAL { '+insert(fr(w,E.AOP,E.chemical),'a','c')+
       ' OPTIONAL { '+insert(fr(w,E.Chemical,RDFS.label),'c','label')+' } OPTIONAL { '+insert(fr(w,E.Chemical,E.identifier),'c','id')+' } } }')
    rows=run(w,q)
    assert len(rows)==6
    assert {r['a']['value'] for r in rows if 'c' not in r}=={str(E.mouseAOP),str(E.noChemicalAOP)}
    assert len([r for r in rows if 'c' in r])==4
    assert {(r['label']['xml:lang'],r['id']['value'],r['id']['datatype']) for r in rows if 'c' in r}=={
        (lang,iden,str(XSD.string)) for lang in ['en','nl'] for iden in ['001','123']}


def test_prepare_and_probe_never_execute_final_early_and_finish_replays():
    w=fixture();p=w.prepare('SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' }')
    assert w.client.queries==[] and not w.results
    probe=w.probe(p['query_ref'],limit=1)
    assert probe['state']=='probed' and probe['limited'] and not w.executions
    assert len(w.client.queries)==1
    assert w.probe(p['query_ref'],limit=1)==probe and len(w.client.queries)==1
    finish=w.finish(p['query_ref'])
    assert finish['rows']==3 and len(w.client.queries)==2
    assert w.finish(p['query_ref'])==finish and len(w.client.queries)==2
    assert isinstance(w.results[finish['result_ref']],QueryResult)


def test_empty_result_is_real_execution_not_an_error():
    w=fixture()
    rows=run(w,'SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' FILTER(?a != ?a) }')
    assert rows==[] and len(w.client.queries)==1


def test_explicit_limit_and_bag_semantics_are_not_rewritten_by_probe():
    w=fixture()
    q='SELECT ?a WHERE { { '+typ(w,E.AOP,'a')+' } UNION { '+typ(w,E.AOP,'a')+' } } ORDER BY ?a LIMIT 3 OFFSET 1'
    p=w.prepare(q);sample=w.probe(p['query_ref'],limit=10)
    rows=run(w,q)
    assert len(rows)==3
    assert key(sample['rows'],'a')==key(rows,'a')


def test_same_class_roles_supported_without_implicit_inequality():
    w=fixture()
    paths=w.paths(str(E.AOP),str(E.AOP),max_hops=2)
    route=next(p['ref'] for p in paths['paths'] if 'taxon' in p['label'])
    q='SELECT ?a ?b WHERE { '+insert(route,'a','b')+' FILTER(?a != ?b) }'
    rows=run(w,q)
    assert {(r['a']['value'],r['b']['value']) for r in rows}=={(str(E.humanAOP),str(E.noChemicalAOP)),(str(E.noChemicalAOP),str(E.humanAOP))}


def test_partial_paths_are_candidates_not_false_absence():
    w=fixture();w.max_paths=1
    p=w.paths(str(E.AOP),str(E.AOP),max_hops=2)
    assert len(p['paths'])==1 and p['search_limited'] is True
    assert not w.client.queries


def test_independent_shared_class_intermediates_do_not_collide():
    w=fixture()
    # Reach taxon through a Key Event, not through the root AOP taxon.
    p=w.paths(str(E.AOP),str(E.Taxon),max_hops=2)
    refs=[r['ref'] for r in p['paths'] if r.get('steps')==2]
    assert refs
    q='SELECT ?a ?one ?two WHERE { '+insert(refs[0],'a','one')+' '+insert(refs[0],'a','two')+' }'
    prepared=w.prepare(q);text=w.prepared[prepared['query_ref']].sparql
    assert '?__rdfsolve1' in text and '?__rdfsolve2' in text
    # Explicit step variables are shared only when the caller requests it.
    explicit=w.prepare('SELECT ?a ?event ?tax WHERE { '+insert(refs[0],'a','event','tax')+' }')
    assert '?__rdfsolve' not in w.prepared[explicit['query_ref']].sparql


def test_shape_only_sequence_inverse_path_is_discovered_hydrated_and_executed():
    schema=MinedSchema.from_shacl('''@prefix sh:<http://www.w3.org/ns/shacl#> .
      <urn:S> a sh:NodeShape; sh:targetClass <urn:A>; sh:property [sh:name "organization";
      sh:path (<urn:p> [sh:inversePath <urn:q>]); sh:qualifiedValueShape [sh:class <urn:B>]; sh:qualifiedMinCount 0] .
      <urn:T> a sh:NodeShape; sh:targetClass <urn:B> .''')
    assert schema.patterns==[]
    g=Graph().parse(data='<urn:a> a <urn:A>; <urn:p> <urn:mid> . <urn:b> a <urn:B>; <urn:q> <urn:mid> .',format='turtle')
    w=Client(schema,g,graph_uris=[]).workspace()
    paths=w.paths('urn:A','urn:B',max_hops=2)
    assert len(paths['paths'])==1
    route=paths['paths'][0];assert route['complexity']['max_hops']==2
    rows=run(w,'SELECT ?a ?b WHERE { '+typ(w,'urn:A','a')+' '+insert(route['ref'],'a','b')+' }')
    assert [(r['a']['value'],r['b']['value']) for r in rows]==[('urn:a','urn:b')]
    field=next(ref for (owner,_),ref in w.field_refs.items() if owner=='urn:A')
    sampled=w.inspect(field)
    assert sampled['values'][0]['term']['value']=='urn:b'
    model=w.client.model('urn:A')
    hydrated=w.client.get_many(model,['urn:a'],fields=[w.fragments[field].field_name])[0]
    assert hydrated.rdf_terms[w.fragments[field].field_name][0]['value']=='urn:b'
    assert w.paths('urn:A','urn:B',max_hops=1)['paths']==[]


def test_shape_direct_predicate_without_pairwise_pattern_is_a_candidate():
    schema=MinedSchema.from_shacl('''@prefix sh:<http://www.w3.org/ns/shacl#> .
      <urn:S> a sh:NodeShape; sh:targetClass <urn:A>; sh:property [sh:path <urn:p>;sh:class <urn:B>] .
      <urn:T> a sh:NodeShape;sh:targetClass <urn:B>.''')
    # Import may provide a lossless pairwise projection; the shape path must work either way.
    g=Graph().parse(data='<urn:a> a <urn:A>; <urn:p> <urn:b> . <urn:b> a <urn:B>.',format='turtle')
    w=Client(schema,g,graph_uris=[]).workspace()
    p=w.paths('urn:A','urn:B',max_hops=1)
    assert p['paths']
    assert run(w,'SELECT ?a ?b WHERE { '+insert(p['paths'][0]['ref'],'a','b')+' }')[0]['b']['value']=='urn:b'


@pytest.mark.parametrize('query',[
    'SELECT ?s WHERE { SERVICE <urn:remote> {?s ?p ?o} }',
    'SELECT ?s WHERE { { SELECT ?s WHERE { SERVICE SILENT ?x {?s ?p ?o} } } }',
    'SELECT ?s FROM <urn:graph> WHERE {?s ?p ?o}',
    'SELECT ?s WHERE {GRAPH ?g {?s ?p ?o}}',
    'DELETE WHERE {?s ?p ?o}',
    'ASK {?s ?p ?o}',
])
def test_external_scope_and_nonselect_operations_rejected_before_io(query):
    w=fixture()
    assert 'error' in dispatch(w,'rdf_prepare',{'sparql':query})
    assert not w.client.queries


def test_unknown_vocabulary_inside_path_is_rejected_not_only_direct_iris():
    w=fixture()
    query='SELECT ?a ?b WHERE {?a (<https://workspace-test.invalid/taxon>/<urn:invented>) ?b}'
    r=dispatch(w,'rdf_prepare',{'sparql':query})
    assert 'Ungrounded' in r['error']['message'] and not w.client.queries


def test_markers_inside_literals_and_comments_are_not_interpolated():
    w=fixture()
    query='SELECT ?text WHERE { BIND("{{invented ?x}} SERVICE" AS ?text) } # {{no_such_ref ?x}}'
    assert run(w,query)[0]['text']['value']=='{{invented ?x}} SERVICE'


def test_prefix_and_select_word_in_comment_survive_probe():
    w=fixture()
    q='# SELECT in comment\nPREFIX ex: <https://workspace-test.invalid/>\nSELECT ?a WHERE {?a a ex:AOP} LIMIT 1'
    p=w.prepare(q)
    assert len(w.probe(p['query_ref'],limit=1)['rows'])==1


def test_graph_scope_keeps_connected_patterns_in_one_graph():
    d=Dataset(default_union=True)
    g1=d.graph(E.g1);g2=d.graph(E.g2)
    w=fixture(g1,scope=[str(E.g1),str(E.g2)])
    # Connect event in one graph, method only in another: no cross-graph join.
    for t in list(g1.triples((None,E.method,None))):g1.remove(t);g2.add(t)
    q='SELECT ?a ?event ?method WHERE { '+insert(fr(w,E.AOP,E.event),'a','event')+' '+insert(fr(w,E.Event,E.method),'event','method')+' }'
    w.client.source=d
    assert run(w,q)==[]
    assert 'GRAPH ?_graph' in w.client.queries[-1]


def test_schema_batching_field_scope_and_bounded_observations():
    w=fixture();r=w.schema(['Key Event','Taxon','measurement'])
    assert all(x['items'] for x in r['matches'])
    assert any(x['kind']=='field' for x in r['matches'][2]['items'])
    field=fr(w,E.AOP,E.context)
    w.client.source.add((E.humanAOP,E.context,Literal('x'*100000)))
    r=w.inspect(field)
    assert len(json.dumps(r).encode())<w.observation_bytes
    long=next(x for x in r['values'] if isinstance(x['term']['value'],dict))
    assert long['term']['value']['truncated']
    assert w.fragments[long['ref']].term.value=='x'*100000
    big=w.schema(['aop','event','taxon','method','context','chemical','identifier','label'])
    assert len(json.dumps(big).encode())<w.observation_bytes


def test_field_values_can_be_literals_not_fabricated_entity_ids():
    w=fixture();w.client.source.add((E.noChemicalAOP,E.taxon,Literal('Human',lang='en')))
    r=w.inspect(fr(w,E.AOP,E.taxon),text='Human')
    literal=next(x for x in r['values'] if x['term']['kind']=='literal')
    q='SELECT ?a WHERE { '+insert(fr(w,E.AOP,E.taxon),'a','tax')+' VALUES ?tax { '+insert(literal['ref'])+' } }'
    rows=run(w,q)
    assert [r['a']['value'] for r in rows]==[str(E.noChemicalAOP)]


def test_record_hydration_and_follow_use_actual_package_models():
    w=fixture()
    rec=w.find('Human',kind=str(E.Taxon))['records'][0]['ref']
    w.inspect(rec,fields=['label'])
    assert w.diagnostics()['package_calls']['Client.get_many']==1
    # Inspect actual AOP labels so Client.find can supply typed source records.
    w.client.source.add((E.humanAOP,RDFS.label,Literal('Chosen pathway')))
    aop=w.find('Chosen pathway',kind=str(E.AOP))['records'][0]['ref']
    followed=w.follow(aop,fr(w,E.AOP,E.event),str(E.Event))
    assert {w.records[r['ref']].uri for r in followed['records']}=={str(E.ke1),str(E.ke3)}
    assert w.diagnostics()['package_calls']['Client.follow']==1


def test_observed_value_paths_really_use_core_value_search():
    w=fixture()
    r=w.paths(str(E.AOP),target_text='Human',max_hops=1)
    assert r['paths'] and all(x['basis']=='observed instance path' for x in r['paths'])
    assert w.diagnostics()['package_calls']['Client.paths_between']==1
    assert len(w.client.queries)>=2
    rows=run(w,'SELECT ?a ?tax WHERE { '+insert(r['paths'][0]['ref'],'a','tax')+' }')
    assert {row['tax']['value'] for row in rows}=={str(E.Human)}


def test_server_only_registers_new_package_tools_and_rejects_bad_arguments():
    w=fixture()
    assert set(CONTRACTS)=={'rdf_schema','rdf_find','rdf_paths','rdf_inspect','rdf_follow','rdf_prepare','rdf_probe','rdf_finish'}
    assert dispatch(w,'query_start',{})['error']['code']=='unknown_tool'
    assert dispatch(w,'rdf_finish',{'query_ref':'x','operation_id':'1'})['error']['code']=='invalid_arguments'
    assert not w.client.queries


def test_failed_execution_never_becomes_a_success_artifact():
    w=fixture()
    p=w.prepare('SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' }')
    # Genuine backend failure: a graph whose query execution raises, not an empty result.
    class BrokenGraph(Graph):
        def query(self,*a,**kw):raise RuntimeError('deliberate backend outage')
    w.client.source=BrokenGraph()
    r=dispatch(w,'rdf_finish',{'query_ref':p['query_ref']})
    assert 'error' in r and r['state']=='failed' and not w.results
    count=len(w.client.queries)
    assert dispatch(w,'rdf_finish',{'query_ref':p['query_ref']})==r
    assert len(w.client.queries)==count


def test_compound_field_search_retains_path_evidence_and_typed_records():
    schema=MinedSchema.from_shacl('''@prefix sh:<http://www.w3.org/ns/shacl#> .
      <urn:S> a sh:NodeShape; sh:targetClass <urn:A>; sh:property [sh:name "display";
      sh:path (<urn:p> <urn:label>);sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .''')
    g=Graph().parse(data='<urn:a> a <urn:A>; <urn:p> <urn:mid> . <urn:mid> <urn:label> "Human" .',format='turtle')
    w=Client(schema,g,graph_uris=[]).workspace()
    field=next(iter(w.field_refs.values()))
    found=w.find('Human',kind='urn:A',fields=[field])
    assert len(found['records'])==1
    record=w.records[found['records'][0]['ref']]
    assert isinstance(record,BaseModel) and record.uri=='urn:a'
    evidence=w.groups[found['result_ref']].evidence[0]
    assert evidence['path']['operator']=='sequence' and evidence['predicate'] is None
    assert w.diagnostics()['package_calls']['Client.search']==1


def test_shape_only_grounded_path_keeps_selected_entity_anchor():
    schema=MinedSchema.from_shacl('''@prefix sh:<http://www.w3.org/ns/shacl#> .
      <urn:S> a sh:NodeShape; sh:targetClass <urn:A>; sh:property [sh:name "applicable taxon";
      sh:path (<urn:applicability> <urn:taxon>); sh:class <urn:Taxon>] .
      <urn:T> a sh:NodeShape; sh:targetClass <urn:Taxon>; sh:property [
      sh:path <http://www.w3.org/2000/01/rdf-schema#label>; sh:datatype <http://www.w3.org/2001/XMLSchema#string>] .''')
    g=Graph().parse(data='''<urn:a1> a <urn:A>; <urn:applicability> [<urn:taxon> <urn:human>].
      <urn:a2> a <urn:A>; <urn:applicability> [<urn:taxon> <urn:mouse>].
      <urn:human> a <urn:Taxon>; <http://www.w3.org/2000/01/rdf-schema#label> "Human".
      <urn:mouse> a <urn:Taxon>; <http://www.w3.org/2000/01/rdf-schema#label> "Mouse".''',format='turtle')
    w=Client(schema,g,graph_uris=[]).workspace()
    entity=w.find('Human',kind='urn:Taxon')['records'][0]['ref']
    path=w.paths('urn:A',entity,max_hops=2)['paths'][0]['ref']
    rows=run(w,'SELECT ?a WHERE { '+insert(path,'a','taxon')+' }')
    assert [r['a']['value'] for r in rows]==['urn:a1']
    assert 'VALUES ?taxon { <urn:human> }' in w.client.queries[-1]


def test_field_reference_scope_is_preserved_even_without_explicit_type_insert():
    w=fixture()
    w.client.source.add((E.notChemical,RDFS.label,Literal('not a chemical')))
    rows=run(w,'SELECT ?s ?label WHERE { '+insert(fr(w,E.Chemical,RDFS.label),'s','label')+' }')
    assert {r['s']['value'] for r in rows}=={str(E.chemical1)}
    assert len(rows)==2


def test_probe_discovers_actual_terms_for_followup_queries():
    w=fixture()
    new_pred=E.unknownButPresent
    w.client.source.add((E.humanAOP,new_pred,E.actualValue))
    p=w.prepare('SELECT ?p ?o WHERE { '+typ(w,E.AOP,'s')+' ?s ?p ?o } ORDER BY ?p ?o')
    obs=w.probe(p['query_ref'],limit=20)
    j=next(i for i,row in enumerate(obs['rows']) if row['p']['value']==str(new_pred))
    term=obs['term_refs'][j]['o']
    rows=run(w,'SELECT ?s WHERE { ?s <'+str(new_pred)+'> '+insert(term)+' }')
    assert rows==[{'s':{'type':'uri','value':str(E.humanAOP)}}]
    assert w.fragments[term].basis.startswith('observed probe')


def test_foreign_owner_field_handles_fail_before_fetch():
    w=fixture();record=w.find('Human',kind='Taxon')['records'][0]['ref']
    wrong=fr(w,E.Chemical,RDFS.label);before=len(w.client.queries)
    with pytest.raises(ValueError,match='different subject'):
        w.inspect(record,fields=[wrong])
    with pytest.raises(ValueError,match='different subject'):
        w.follow(record,wrong,str(E.Chemical))
    assert len(w.client.queries)==before


def test_actual_saved_schema_discovers_ncbitaxon_without_fabricated_species_synonyms():
    root=Path(__file__).resolve().parents[2]
    client=Client.open(root/'notebooks/data/aopwikirdf.schema.json',source=Graph(),graph_uris=[])
    w=client.workspace()
    groups=w.schema(['Taxon','Key Event','chemical entity'])['matches']
    assert any(item['kind']=='type' and 'NCBITAXON' in item['iri'] for item in groups[0]['items'])
    assert groups[1]['items'][0]['label']=='Key Event'
    assert groups[2]['items'][0]['label']=='chemical entity'
    assert client.queries==[]


def test_typed_select_preserves_noncanonical_literal_lexical_forms():
    w=fixture();w.client.source.add((E.chemical1,E.identifier,Literal('0007',datatype=XSD.integer,normalize=False)))
    rows=run(w,'SELECT ?id WHERE { '+insert(fr(w,E.Chemical,E.identifier),'c','id')+' }')
    assert any(r['id']['value']=='0007' and r['id']['datatype']==str(XSD.integer) for r in rows)


def test_oversized_result_page_advances_without_altering_retained_data():
    w=fixture();p=w.prepare('SELECT ?a WHERE { '+typ(w,E.AOP,'a')+' }');done=w.finish(p['query_ref'])
    from rdfsolve.query import ResultCell
    row={str(i):ResultCell(value='x'*1000,type='literal') for i in range(100)}
    w.results[done['result_ref']].rows=[row,row]
    page=w.inspect(done['result_ref'])
    assert page['rows'][0]['omitted'] and page['next_offset']==1
    assert len(json.dumps(page))<w.observation_bytes
    assert w.resource(done['result_ref'])['bindings'][0]['0']['value']=='x'*1000
