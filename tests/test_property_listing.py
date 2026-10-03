"""The properties of a class are listed from the subjects typed by any member of a group of
ontology terms, as the pattern queries read them; the listing read only the subjects typed by
the IRI of the group. On QLever the listing reads the property sets of the subjects
(ql:has-predicate), which hold every property of a subject: the listed properties are then
queried one by one in the scope. RDFLib stands in for QLever here."""

import re
from itertools import count

from rdflib import Graph

from rdfsolve.mining import query_fallbacks
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.query_builders import Representative

DATA = """
<urn:s1> a <urn:T1> ; <urn:p1> "a" .
<urn:s2> a <urn:T2> ; <urn:p2> "b" .
<urn:s3> a <urn:Group> ; <urn:p3> "c" .
"""
GROUP = Representative("urn:Group", ["urn:Group", "urn:T1", "urn:T2"])


def listing(monkeypatch, engine):
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", engine)
        select, sent = miner.helper.select, []

        def translate(query, *args, **kwargs):
            sent.append(query)
            numbers = count()
            query = query.replace("PREFIX ql: <http://qlever.cs.uni-freiburg.de/builtin-functions/>\n", "")
            query = re.sub(r"(\?\w+) ql:has-predicate (\?\w+)", lambda m: f"{m[1]} {m[2]} ?_hp{next(numbers)}", query)
            return select(query, *args, **kwargs)

        monkeypatch.setattr(miner.helper, "select", translate)
        found = query_fallbacks.enumerate_properties_for_class(GROUP, None, "test", miner.helper)
    return {r["p"]["value"] for r in found.rows} - {"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}, found.state, sent


def test_the_properties_of_every_member_are_listed(monkeypatch):
    for engine in ("virtuoso", "qlever"):
        found, state, sent = listing(monkeypatch, engine)
        assert found == {"urn:p1", "urn:p2", "urn:p3"} and state == "complete", engine
        assert any("ql:has-predicate" in q for q in sent) == (engine == "qlever")
