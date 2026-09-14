"""Package-owned RDF exploration and composition, usable without MCP or an LLM.

The workspace retains generated models, full RDF values, paths, query artifacts,
and evidence. Observations are bounded views of that state, not its serialization.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from dataclasses import asdict
import json
import re
from threading import RLock
from time import perf_counter
from typing import Any

from pydantic import BaseModel
from rdflib import URIRef

from rdfsolve.client_api import Client, Results
from rdfsolve.hydration import HydrationLimitError, _term
from rdfsolve.query import QueryResult
from rdfsolve.query_fragments import Fragment, PreparedQuery, PROTECTED, compile_query, identifier, path_size, walk
from rdfsolve.schema_models.enrichment import RdfTerm
from rdfsolve.schema_models.exporters.paths import path_to_sparql
from rdfsolve.schema_models.paths import PropertyPath


def excerpt(value: str | None, length: int = 320):
    if value is None or len(value) <= length:
        return value
    return {'excerpt': value[:length], 'length': len(value), 'truncated': True}


def words(text: str) -> set[str]:
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    return {w.lower() for w in re.findall(r'[A-Za-z0-9]+', text)}


def _score(text: str, query: str) -> float:
    needle = words(query)
    if not needle:
        return 1
    # Retrieval ranking only. No assertion of semantic equivalence.
    got = words(text)
    hits = sum(w in got or any((len(w) >= 3 and w in x) or (len(x) > 3 and w.startswith(x)) for x in got) for w in needle)
    return hits / len(needle) + (2 if query.lower() in text.lower() else 0) if hits else 0


def bindings(result: QueryResult):
    return [{name: {'type': cell.type, 'value': cell.value,
                    **({'datatype': cell.datatype} if cell.datatype else {}),
                    **({'xml:lang': cell.lang} if cell.lang else {})}
             for name, cell in row.items()} for row in result.rows]


class Workspace:
    """One source and one investigation. Blocking operations run sequentially."""

    def __init__(self, client: Client, *, source_id='rdf', page_size=5,
                 observation_bytes=12000, max_paths=200):
        if page_size < 1 or max_paths < 1 or observation_bytes < 2000:
            raise ValueError('Invalid workspace budget')
        self.client, self.source_id = client, source_id
        self.page_size, self.observation_bytes, self.max_paths = page_size, observation_bytes, max_paths
        self.lock = RLock()
        self.registry = client.registry(source_id=source_id)
        self.fragments: dict[str, Fragment] = {}
        self.metadata: dict[str, dict] = {}
        self.records: dict[str, BaseModel] = {}
        self.groups: dict[str, Results] = {}
        self.prepared: dict[str, PreparedQuery] = {}
        self.results: dict[str, QueryResult] = {}
        self.executions: dict[str, dict] = {}
        self.cache: dict[str, Any] = {}
        self.events: list[dict] = []
        self.known_iris: set[str] = set()
        self.type_refs, self.field_refs = {}, {}
        for item in self.registry.types:
            ref = self._put(Fragment('type', item.label, iri=item.id, description=item.description), item.id)
            self.type_refs[item.id] = ref
            self.metadata[ref] = {'description': item.description, 'fields': []}
            for f in item.fields:
                path = PropertyPath.model_validate(f.binding['path'])
                fref = self._put(Fragment('field', f.label, owner=item.id, field_name=f.name,
                    path=path, description=f.description), [item.id, f.name, f.binding])
                self.field_refs[(item.id, f.name)] = fref
                self.metadata[ref]['fields'].append(fref)
                self.metadata[fref] = f.model_dump(mode='json')
            # Qualified constraints remain hints, never unconditional type filters.
            for shape in getattr(client.model(item.id), 'rdf_shapes', []):
                for prop in shape.get('property_shapes', []):
                    if prop.get('deactivated'):
                        continue
                    p = prop['path']
                    pp = PropertyPath(operator='predicate', iri=p) if isinstance(p, str) else PropertyPath.model_validate(p)
                    for fr in self.metadata[ref]['fields']:
                        if self.fragments[fr].path == pp:
                            m = self.metadata[fr]
                            target = prop.get('class_constraint') or (prop.get('qualified_shape') or {}).get('class_constraint')
                            if target:
                                m['targets'] = sorted(set(m['targets'] + [target]))
                                m['target_basis'] = 'SHACL hint; qualified constraints do not restrict every value'
                            if prop.get('name'):
                                self.fragments[fr].label = prop['name']
                            if prop.get('description'):
                                self.fragments[fr].description = prop['description']

    def _put(self, fragment: Fragment, key):
        prefix = {'type': 't', 'field': 'f', 'path': 'p', 'term': 'e'}[fragment.kind]
        ref = identifier(prefix, key)
        self.fragments.setdefault(ref, fragment)
        if fragment.iri:
            self.known_iris.add(fragment.iri)
        if fragment.path:
            self.known_iris.update(x.iri for x in self._paths(fragment.path) if x.iri)
        self.known_iris.update(fragment.endpoint_types.values())
        self.known_iris.update(t.value for t in fragment.anchors.values() if t.kind == 'uri')
        if fragment.term and fragment.term.kind == 'uri':
            self.known_iris.add(fragment.term.value)
        return ref

    @staticmethod
    def _paths(path):
        yield path
        for child in path.items:
            yield from Workspace._paths(child)

    def _call(self, method, *args, **kwargs):
        start, before = perf_counter(), len(self.client.queries)
        event = {'package_method': f'Client.{method}', 'status': 'failed'}
        try:
            value = getattr(self.client, method)(*args, **kwargs)
            event['status'] = 'complete'
            return value
        except Exception as exc:
            event['error'] = f'{type(exc).__name__}: {exc}'
            raise
        finally:
            event.update(seconds=perf_counter()-start, source_queries=len(self.client.queries)-before)
            self.events.append(event)

    def _type(self, value):
        if value in self.fragments and self.fragments[value].kind == 'type':
            return self.fragments[value].iri
        if value in self.records:
            return self.records[value].rdf_class_iri
        matches=[t.id for t in self.registry.types if t.label.casefold()==str(value).casefold() or t.id==value]
        if len(matches)==1:
            return matches[0]
        if len(matches)>1:
            raise ValueError('Ambiguous class label; use the retained type reference')
        return str(self.client.model(value).rdf_class_iri)

    def _card(self, ref):
        f = self.fragments[ref]
        card = {'ref': ref, 'kind': f.kind, 'label': excerpt(f.label, 160), 'basis': f.basis}
        if f.description:
            card['description'] = excerpt(f.description)
        if f.kind == 'type':
            card.update(iri=f.iri, fields=len(self.metadata[ref]['fields']), insert='{{' + ref + ' ?resource}}')
        if f.path:
            card.update(owner=f.owner, sparql_path=path_to_sparql(f.path), complexity=path_size(f.path),
                        insert='{{' + ref + ' ?subject ?value}}')
            if f.steps:
                card['steps'] = len(f.steps)
            m = self.metadata.get(ref, {})
            card.update({k: m[k] for k in ('node_kinds', 'targets', 'target_basis') if m.get(k)})
            if f.anchors:
                card['anchors'] = {str(k): term.model_dump(mode='json') for k, term in f.anchors.items()}
            if f.endpoint_types:
                card['endpoint_types'] = {str(k): cls for k, cls in f.endpoint_types.items()}
        if f.term:
            term = f.term.model_dump(mode='json', exclude_none=True)
            term['value'] = excerpt(term['value'], 240) if f.term.kind == 'literal' else term['value']
            card.update(term=term, insert='{{' + ref + '}}', reusable=f.term.kind != 'bnode')
        if ref in self.records:
            r = self.records[ref]
            card.update(model=type(r).__name__, type=r.rdf_class_iri,
                fields={k: [excerpt(str(t.get('value', '')), 200) for t in v[:2]]
                        for k, v in getattr(r, 'rdf_terms', {}).items() if k != '@type'})
        if self.metadata.get(ref, {}).get('matches'):
            card['matches'] = self.metadata[ref]['matches'][:2]
        return card

    def _page(self, items, offset=0, *, kind='items', budget=None):
        if offset < 0:
            raise ValueError('Offset must be nonnegative')
        budget = budget or (self.observation_bytes-1500)
        page, size = [], 0
        for item in items[offset:offset+self.page_size]:
            n = len(json.dumps(item, ensure_ascii=False).encode())
            if size + n > budget:
                if not page:
                    item = ({k:item[k] for k in ('ref','kind','label') if k in item} if 'ref' in item
                            else {'row_offset':offset,'omitted':True})
                    item['detail'] = 'Retained detail exceeds this page budget; read the full artifact resource.'
                    page.append(item)
                break
            page.append(item); size += n
        end = offset + len(page)
        return {kind: page, 'more': end < len(items), 'next_offset': end if end < len(items) else None,
                'retained': len(items)}

    def schema(self, concepts: list[str] | None = None, *, owner: str | None = None, offset=0):
        """Batched lexical discovery over actual generated classes AND field paths."""
        if owner:
            refs = self.metadata[self.type_refs[self._type(owner)]]['fields']
        else:
            refs = [*self.type_refs.values(), *self.field_refs.values()]
        concepts = concepts or ['']
        if len(concepts) > 8:
            raise ValueError('Use at most eight separate concepts')
        groups = []
        for concept in concepts:
            ranked = []
            for ref in refs:
                f = self.fragments[ref]
                text = f'{f.label} {f.iri or ""} {f.description or ""} {f.field_name or ""}'
                if f.path:
                    text += ' ' + path_to_sparql(f.path)
                score = _score(text, concept)
                if score:
                    score += 2 * _score(f'{f.label} {f.field_name or ""}', concept)
                    if concept and concept.casefold() == f.label.casefold():
                        score += 4
                if score:
                    ranked.append((score, ref))
            ranked.sort(key=lambda pair: (-pair[0], self.fragments[pair[1]].kind != 'type', pair[1]))
            groups.append({'concept': concept, **self._page([self._card(ref) for _, ref in ranked], offset, budget=(self.observation_bytes-1200)//len(concepts))})
        # Per-concept groups are bounded; batch size is explicit and small.
        return {'source': self.source_id, 'scope': list(self.client.graph_uris), 'matches': groups,
                'note': 'Schema hints, not instances. Empty keyword matches do not establish absence. Inspect owner fields and values when terminology is opaque.'}

    def _retain_records(self, result):
        refs = []
        for record in result:
            term = RdfTerm(kind='uri', value=str(record.uri))
            ref = self._put(Fragment('term', str(record.uri), term=term, basis='Client typed retrieval'),
                            [record.rdf_class_iri, str(record.uri)])
            self.records[ref] = record
            refs.append(ref)
        return refs

    def find(self, text: str, *, kind: str | None = None, fields: list[str] | None = None,
             descriptions=False, offset=0):
        """Use the client, not an MCP-local entity search query."""
        cls = self._type(kind) if kind else None
        for ref in fields or []:
            if ref in self.fragments and (self.fragments[ref].kind!='field' or self.fragments[ref].owner != cls):
                raise ValueError('Field references must belong to the selected type')
        names = [self.fragments[f].field_name if f in self.fragments else f for f in fields or []]
        key = identifier('find', [text, cls, names, descriptions])
        if key not in self.cache:
            result = (self._call('search', [text], kind=cls, fields=names) if descriptions or names
                      else self._call('find', text, kind=cls))
            self.groups[key] = result
            self.cache[key] = self._retain_records(result)
            for ref in self.cache[key]:
                matches = [e for e in result.evidence if e.get('id') == str(self.records[ref].uri)]
                self.metadata.setdefault(ref, {})['matches'] = [
                    {'field': e.get('field'), 'text': excerpt(e.get('text', {}).get('value', ''), 240)} for e in matches[:2]]
        result = self.groups[key]
        return {**self._page([self._card(ref) for ref in self.cache[key]], offset, kind='records'),
                'result_ref': key, 'coverage': {k:v for k,v in result.coverage.items() if k not in {'searched_fields','terms'}},
                'searched_fields':len(result.coverage.get('searched_fields',[])),
                'note': 'Candidate names/values, not a semantic filter. Use a selected RDF term on its actual relationship, not a prose substring.'}

    def inspect(self, ref: str, *, fields: list[str] | None = None, text='', offset=0):
        """Read source definitions, sample real values, or hydrate a retained record."""
        if ref in self.prepared:
            p = self.prepared[ref]
            return {'query_ref': ref, 'query': p.sparql, 'variables': p.variables, 'uses': p.uses}
        if ref in self.results:
            return {**self._page([self._row_preview(r) for r in bindings(self.results[ref])], offset, kind='rows'),
                    'result_ref': ref, 'preview': True}
        if ref in self.records:
            record = self.records[ref]
            if fields:
                names = self._field_names(record.rdf_class_iri, fields)
                record = self._call('get_many', type(record), [str(record.uri)], fields=names)[0]
                self.records[ref] = record
            return self._card(ref)
        if ref not in self.fragments:
            raise ValueError('Unknown retained reference')
        f = self.fragments[ref]
        if f.kind == 'type':
            return self.schema(owner=ref, offset=offset)
        if f.kind == 'field':
            key = identifier('values', [ref, text, offset])
            if key not in self.cache:
                self.cache[key] = self._call('field_values', f.owner, f.field_name, text=text,
                                           offset=offset, limit=self.page_size+1)
            rows = self.cache[key]
            values = []
            for row in rows[:self.page_size]:
                subject = _term(row['subject'])
                sr = self._put(Fragment('term', subject.value, term=subject, basis=f'field sample {ref}'), subject.model_dump())
                term = _term(row['value'])
                er = self._put(Fragment('term', term.value, term=term, basis=f'observed value of {ref}'),
                               [key if term.kind == 'bnode' else '', term.model_dump()])
                values.append({'subject': row['subject'], 'subject_ref':sr, **self._card(er)})
            return {'field': self._card(ref), 'values': values, 'sample': True,
                    'more': len(rows)>self.page_size, 'next_offset': offset+self.page_size if len(rows)>self.page_size else None}
        card = self._card(ref)
        if f.kind == 'path' and f.steps:
            card['steps'] = self._page([{'from':a, 'predicate':p, 'label':self._predicate_label(p),
                'to':b, 'reverse':rev} for a,p,b,rev in f.steps], offset)['items']
        return card

    def _field_names(self, owner, fields):
        names = []
        for field in fields:
            f = self.fragments.get(field)
            if f and (f.kind != 'field' or f.owner != owner):
                raise ValueError('Field reference belongs to a different subject type')
            names.append(f.field_name if f else field)
        return names

    def follow(self, record: str, field: str, target: str, *, fields=None):
        if record not in self.records:
            raise ValueError('Follow requires a retained typed record')
        subject = self.records[record]
        name = self._field_names(subject.rdf_class_iri, [field])[0]
        cls = self._type(target)
        names = self._field_names(cls, fields or [])
        result = self._call('follow', [subject], name, self.client.model(cls), fields=names)
        refs = self._retain_records(result)
        return self._page([self._card(ref) for ref in refs], kind='records')

    def paths(self, source: str, target: str | None = None, *, target_text: str | None = None,
              max_hops=2, offset=0):
        """Use core schema/value/resource path discovery, retaining all returned evidence."""
        if (target is None) == (target_text is None):
            raise ValueError('Supply a target type/record or target_text, not both')
        key = identifier('paths', [source, target, target_text, max_hops])
        if key not in self.cache:
            src = self.records.get(source) or self._type(source)
            if target_text is not None:
                table = self._call('paths_between', src, target_value=target_text, max_hops=max_hops, max_paths=self.max_paths)
            elif source in self.records and target in self.records:
                table = self._call('connections', self.records[source], self.records[target], max_hops=max_hops, max_paths=self.max_paths)
            else:
                table = self._call('paths_between', src, self._type(target), max_hops=max_hops, max_paths=self.max_paths,
                    **({} if source in self.records else {'allow_partial': True, 'allow_repeated_classes': True}))
            refs = []
            for route in table.attrs.get('routes', []):
                observed = isinstance(route, dict)
                anchors = {}
                if observed:
                    b, n = route['bindings'], route['hops']
                    paths = [PropertyPath(operator='predicate', iri=b[f'p{i}']['value']) for i in range(n)]
                    paths = [PropertyPath(operator='inverse', items=[p]) if b[f'back{i}']['value'] in ('true','1') else p for i,p in enumerate(paths)]
                    anchors[-1] = _term(b[f'n{n}'])
                    if source in self.records:
                        anchors[0] = _term(b['n0'])
                    steps = []
                else:
                    paths = [PropertyPath(operator='predicate', iri=p) for _,p,_,_ in route]
                    paths = [PropertyPath(operator='inverse', items=[p]) if edge[3] else p for p,edge in zip(paths,route)]
                    steps = route
                    if target in self.records:
                        anchors[-1] = RdfTerm(kind='uri', value=str(self.records[target].uri))
                path = paths[0] if len(paths)==1 else PropertyPath(operator='sequence', items=paths)
                labels = []
                for p in paths:
                    iris = [x.iri for x in self._paths(p) if x.iri]
                    labels.append(' / '.join(self._predicate_label(iri) for iri in iris))
                fragment = Fragment('path', ' → '.join(labels), path=path, steps=steps, anchors=anchors,
                                    basis='observed instance path' if observed else 'schema-composed candidate; instance support not checked')
                ref = self._put(fragment, [key, route])
                self.metadata[ref] = {'route': deepcopy(route), 'query_ids': [route.get('query_id')] if observed else []}
                refs.append(ref)
            # The existing class enumerator is pairwise. Full SHACL field paths
            # supplement it without flattening compound paths into fake predicates.
            if target is not None and source not in self.records:
                s,t = self._type(source),self._type(target)
                for fr in self.field_refs.values():
                    f,m = self.fragments[fr],self.metadata[fr]
                    if any(self.fragments[r].path == f.path and f.owner == s and t in m.get('targets', []) for r in refs):
                        continue
                    size=path_size(f.path)
                    if size['max_hops'] is not None and size['max_hops'] > max_hops:
                        continue
                    reverse = f.owner == t and s in m.get('targets', [])
                    if (f.owner == s and t in m.get('targets', [])) or reverse:
                        path = PropertyPath(operator='inverse',items=[f.path]) if reverse else f.path
                        fragment = Fragment('path', ('inverse of ' if reverse else '') + f.label,
                            path=path, description=f.description, endpoint_types={0:s, -1:t},
                            anchors={-1:RdfTerm(kind='uri', value=str(self.records[target].uri))} if target in self.records else {},
                            basis='retained SHACL path; endpoint types are explicitly requested, not inferred for all values')
                        refs.insert(0,self._put(fragment,[key,fr,reverse]))
            self.cache[key] = (list(dict.fromkeys(refs)), bool(table.attrs.get('truncated')), deepcopy(table.attrs))
        refs, truncated, _ = self.cache[key]
        return {**self._page([self._card(ref) for ref in refs], offset, kind='paths'), 'search_limited': truncated,
                'max_hops': max_hops, 'note': 'Length is not semantic relevance. No path is auto-selected. Increase max_hops explicitly to widen search.'}

    def _predicate_label(self, iri):
        return next((f.label for f in self.fragments.values() if f.path and f.path.operator=='predicate' and f.path.iri==iri),iri)

    def prepare(self, sparql: str):
        p = compile_query(sparql, self.fragments, self.client._scope, self.known_iris)
        self.prepared[p.ref] = p
        return {'state': 'prepared', 'query_ref': p.ref, 'variables': p.variables,
                'grounded_refs': list(dict.fromkeys(u['ref'] for u in p.uses)),
                'warnings': p.warnings, 'next': 'Probe this artifact or explicitly finish. Preparing never executes.'}

    @staticmethod
    def _row_preview(row):
        return {name: {**term, 'value': excerpt(term['value'],240) if term['type']=='literal' else term['value']}
                for name,term in row.items()}

    def probe(self, query_ref: str, *, limit=5):
        if not 1 <= limit <= 20:
            raise ValueError('Probe limit must be 1..20')
        p = self.prepared[query_ref]
        key = identifier('probe', [query_ref,limit])
        if key not in self.cache:
            # A subselect preserves LIMIT/OFFSET, aggregates and bag semantics.
            # PREFIX/BASE declarations must stay outside the enclosing query.
            match = re.search(r'\bSELECT\b', PROTECTED.sub(lambda m:' '*len(m.group()),p.sparql), re.I)
            head, body = p.sparql[:match.start()], p.sparql[match.start():]
            query = head + f'SELECT * WHERE {{ {{ {body} }} }} LIMIT {limit+1}'
            result = self._call('select', query)
            self.cache[key] = result
        result = self.cache[key]
        self.results[key] = result
        term_refs = []
        for row in bindings(result)[:limit]:
            refs = {}
            for name, value in row.items():
                term = _term(value)
                refs[name] = self._put(Fragment('term', term.value, term=term, basis=f'observed probe {key}'),
                    [key if term.kind == 'bnode' else '', term.model_dump()])
            term_refs.append(refs)
        return {'state':'probed','query_ref':query_ref,'result_ref':key,
                'rows': [self._row_preview(row) for row in bindings(result)[:limit]], 'term_refs':term_refs,
                'at_least':result.row_count,'limited':result.row_count>limit,
                'note':'A probe is not the final answer. Zero rows do not authorize removing a requirement.'}

    def finish(self, query_ref: str):
        if query_ref in self.executions:
            return deepcopy(self.executions[query_ref])
        p = self.prepared[query_ref]
        try:
            result = self._call('select', p.sparql)
        except Exception as exc:
            value = {'state':'failed','query_ref':query_ref,
                     'error':{'code':'execution_failed','type':type(exc).__name__,'message':str(exc)},
                     'execution':deepcopy(self.client.last_query_execution)}
            self.executions[query_ref] = value
            return deepcopy(value)
        ref = identifier('r', query_ref)
        self.results[ref] = result
        value = {'state':'complete','query_ref':query_ref,'result_ref':ref,'rows':result.row_count,
                 'variables':result.variables,'execution':deepcopy(self.client.last_query_execution),
                 'preview':[self._row_preview(r) for r in bindings(result)[:2]],
                 'preview_only':True,'semantic_correctness':'not independently verified'}
        self.executions[query_ref] = value
        return deepcopy(value)

    def resource(self, ref):
        if ref in self.results:
            r=self.results[ref]
            return {'ref':ref,'query':r.query,'variables':r.variables,'bindings':bindings(r),
                    'query_result':r.model_dump(mode='json')}
        if ref in self.prepared:
            return asdict(self.prepared[ref])
        if ref in self.groups:
            return {'coverage':self.groups[ref].coverage, 'evidence':self.groups[ref].evidence,
                    'records':[r.model_dump(mode='json') for r in self.groups[ref]]}
        if ref in self.records:
            return {'model':type(self.records[ref]).__name__, 'record':self.records[ref].model_dump(mode='json')}
        if ref in self.fragments:
            return {'fragment':asdict(self.fragments[ref]),'metadata':self.metadata.get(ref,{})}
        raise KeyError(ref)

    def diagnostics(self):
        return {'package_calls': dict(Counter(e['package_method'] for e in self.events)),
                'source_queries':len(self.client.queries),'events':deepcopy(self.events)}
