"""Small RDF oracles for the public investigation workflow."""

import json
import multiprocessing
import signal
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from time import perf_counter
from urllib.parse import parse_qs, urlsplit

import pytest
from rdflib import RDF, RDFS, XSD, BNode, Dataset, Graph, Literal, Namespace

from rdfsolve.client.api import Client
from rdfsolve.mcp.session import Session
from rdfsolve.schema_models.core import MinedSchema
from rdfsolve.schema_models.enrichment import RdfTerm, TermAnnotation
from rdfsolve.schema_models.pattern import SchemaPattern

E = Namespace("https://workspace-test.invalid/")


def insert(ref, *variables):
    return "{{" + ref + (" " + " ".join("?" + v for v in variables) if variables else "") + "}}"


def make_session(graph=None, scope=None):
    g = graph if graph is not None else Graph()
    triples = [
        (E.humanAOP, RDF.type, E.AOP),
        (E.mouseAOP, RDF.type, E.AOP),
        (E.noChemicalAOP, RDF.type, E.AOP),
        (E.Human, RDF.type, E.Taxon),
        (E.Mouse, RDF.type, E.Taxon),
        (E.Human, RDFS.label, Literal("Human")),
        (E.Mouse, RDFS.label, Literal("Mouse")),
        (E.humanAOP, E.taxon, E.Human),
        (E.mouseAOP, E.taxon, E.Mouse),
        (E.noChemicalAOP, E.taxon, E.Human),
        (E.humanAOP, E.context, Literal("Thyroid response in this species")),
        (E.mouseAOP, E.context, Literal("Thyroid in mice; relevance to human health unknown")),
        (E.noChemicalAOP, E.context, Literal("Thyroid response without chemical metadata")),
        (E.ke1, RDF.type, E.Event),
        (E.ke2, RDF.type, E.Event),
        (E.ke3, RDF.type, E.Event),
        (E.humanAOP, E.event, E.ke1),
        (E.humanAOP, E.event, E.ke3),
        (E.mouseAOP, E.event, E.ke2),
        (E.ke1, E.taxon, E.Mouse),
        (E.ke2, E.taxon, E.Human),
        (E.ke1, E.method, Literal("Assay A", lang="en")),
        (E.ke1, E.method, Literal("Assay B", lang="en")),
        (E.ke2, E.method, Literal("Unrelated assay")),
        (E.chemical1, RDF.type, E.Chemical),
        (E.humanAOP, E.chemical, E.chemical1),
        (E.chemical1, RDFS.label, Literal("Name one", lang="en")),
        (E.chemical1, RDFS.label, Literal("Naam twee", lang="nl")),
        (E.chemical1, E.identifier, Literal("001", datatype=XSD.string)),
        (E.chemical1, E.identifier, Literal("123", datatype=XSD.string)),
    ]
    for t in triples:
        g.add(t)
    patterns = [
        (E.AOP, E.taxon, E.Taxon),
        (E.AOP, E.context, "Literal"),
        (E.AOP, E.event, E.Event),
        (E.AOP, E.chemical, E.Chemical),
        (E.Event, E.taxon, E.Taxon),
        (E.Event, E.method, "Literal"),
        (E.Taxon, RDFS.label, "Literal"),
        (E.Chemical, RDFS.label, "Literal"),
        (E.Chemical, E.identifier, "Literal"),
    ]
    schema = MinedSchema(
        about={"dataset_name": "test"},
        patterns=[
            SchemaPattern(subject_class=str(a), property_uri=str(p), object_class=str(b))
            for a, p, b in patterns
        ],
    )
    labels = [
        (E.AOP, "Adverse Outcome Pathway"),
        (E.Event, "Key Event"),
        (E.Taxon, "Taxon"),
        (E.Chemical, "Chemical"),
        (E.event, "has Key Event"),
        (E.taxon, "applicable taxon"),
        (E.method, "measurement method"),
    ]
    for iri, label in labels:
        schema.enrichment.labels.append(
            TermAnnotation(
                term_iri=str(iri),
                predicate=str(RDFS.label),
                text=RdfTerm(kind="literal", value=label),
            )
        )
    c = Client(schema, g, graph_uris=scope or [])
    return Session(c)


@pytest.fixture
def session():
    s = make_session()
    yield s
    s.client.close()


def field(s, owner, predicate, left, right):
    c = s.catalogue
    name = s.client.field_name(s.client.model(str(owner)), str(predicate))
    return insert(c.field_refs[(str(owner), name)], left, right)


def declare(s, clause, *, concept):
    s.schema(question=clause, goals=[{"clause": clause, "kind": "output", "concept": concept}])


def event_goals(s):
    s.schema(
        question="Return Key Events and available metadata for Human AOPs",
        goals=[
            {"clause": "Return Key Events", "kind": "output", "concept": "Key Event"},
            {
                "clause": "AOP applicability is Human",
                "kind": "entity_filter",
                "concept": "Taxon",
                "owner": "Adverse Outcome Pathway",
                "value": "Human",
            },
            {
                "clause": "Return available applicability values",
                "kind": "output",
                "concept": "applicable taxon",
                "required": False,
            },
            {
                "clause": "Return available methods",
                "kind": "output",
                "concept": "measurement method",
                "required": False,
            },
        ],
    )
    human = s.find("Human", str(E.Taxon))["items"][0]["ref"]
    return {
        "g1": {
            "pattern": field(s, E.AOP, E.event, "aop", "event")
            + " "
            + insert(s.catalogue.type_refs[str(E.Event)], "event"),
            "evidence": [s.catalogue.type_refs[str(E.Event)]],
            "project": ["event"],
        },
        "g2": {
            "evidence": [
                s.catalogue.field_refs[
                    (str(E.AOP), s.client.field_name(s.client.model(str(E.AOP)), str(E.taxon)))
                ]
            ],
            "entity": human,
            "pattern": field(s, E.AOP, E.taxon, "aop", "tax")
            + " VALUES ?tax { "
            + insert(human)
            + " }",
        },
        **{
            key: {
                "evidence": [
                    s.catalogue.field_refs[
                        (
                            str(E.Event),
                            s.client.field_name(s.client.model(str(E.Event)), str(predicate)),
                        )
                    ]
                ],
                "pattern": "OPTIONAL { " + field(s, E.Event, predicate, "event", variable) + " }",
                "project": [variable],
            }
            for key, predicate, variable in [("g3", E.taxon, "species"), ("g4", E.method, "method")]
        },
    }


def values(s, prepared):
    done = s.finish(prepared["query_ref"])
    assert done["state"] == "complete", done
    return s.export(done["result_ref"])["bindings"]


def prepare(session, clauses, *, sparql=None):
    """Build fixture SELECTs explicitly while using the public query boundary."""
    projection = list(
        dict.fromkeys(v.lstrip("?$") for g in clauses.values() for v in g.get("project", []))
    )
    patterns = [g["pattern"].rstrip() for g in clauses.values()]
    query = (
        "SELECT DISTINCT "
        + " ".join("?" + v for v in projection)
        + " WHERE {\n"
        + "\n".join(patterns)
        + "\n}"
    )
    evidence = {key: {k: v for k, v in g.items() if k != "pattern"} for key, g in clauses.items()}
    return session.prepare(sparql or query, evidence)


def _serve(data, journal, connection):
    graph = Graph().parse(data, format="turtle")

    def expired(*_):
        raise TimeoutError("Snapshot query exceeded 60 seconds")

    signal.signal(signal.SIGALRM, expired)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            self.respond(parse_qs(urlsplit(self.path).query).get("query", [""])[0])

        def do_POST(self):
            body = self.rfile.read(
                min(int(self.headers.get("Content-Length", 0)), 200_000)
            ).decode()
            query = (
                body
                if "application/sparql-query" in self.headers.get("Content-Type", "")
                else parse_qs(body).get("query", [""])[0]
            )
            self.respond(query)

        def respond(self, query):
            event = {"query": query, "status": "running"}
            started = perf_counter()
            status = 200
            try:
                signal.alarm(60)
                from rdflib.plugins.sparql import prepareQuery
                from rdflib.plugins.sparql.parserutils import CompValue

                from rdfsolve.client.query_fragments import walk

                parsed = prepareQuery(query)
                if any(
                    isinstance(n, CompValue) and n.name in {"ServiceGraphPattern", "DatasetClause"}
                    for n in walk(parsed.algebra)
                ):
                    raise ValueError("Query leaves the local test endpoint")
                result = graph.query(parsed)
                payload = result.serialize(format="json")
                if len(payload) > 64 * 1024 * 1024:
                    raise ValueError("Snapshot response exceeded 64 MiB")
                event.update(status="complete", rows=len(result))
            except Exception as exc:
                status = 400
                event.update(status="failed", error_type=type(exc).__name__, error=str(exc))
                payload = json.dumps({"error": str(exc)[:1500]}).encode()
            finally:
                signal.alarm(0)
                event["seconds"] = perf_counter() - started
                with Path(journal).open("a") as log:
                    log.write(json.dumps(event) + "\n")
            self.send_response(status)
            self.send_header(
                "Content-Type",
                "application/sparql-results+json" if status == 200 else "application/json",
            )
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    connection.send(f"http://127.0.0.1:{server.server_port}/sparql")
    connection.close()
    server.serve_forever()


@contextmanager
def snapshot_endpoint(data, journal):
    """Serve the identical frozen RDF to each model condition."""
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_serve, args=(str(data), str(journal), sender))
    process.start()
    sender.close()
    try:
        if not receiver.poll(60):
            raise TimeoutError("Snapshot endpoint did not start")
        yield receiver.recv()
    finally:
        receiver.close()
        process.terminate()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join()
