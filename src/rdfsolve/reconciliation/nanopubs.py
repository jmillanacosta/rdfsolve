"""Nanopublications on disk.

Each artifact of a reconciliation (rule, policy, mapping claim, decision) is a nanopublication
(Groth et al. 2010): an assertion graph, its provenance and its publication information. Its
trusty URI (Kuhn and Dumontier 2014) is computed from the content, so that the same content has
the same URI and a changed file is detected; blank nodes become IRIs of the nanopublication. The nanopublications are not signed: they are kept
on disk until the approach is validated, and signing gives a signed one a URI of its own. A
nanopublication index (Kuhn et al. 2015) groups them. The ``nanopub`` library (extra
``reconciliation``) builds and verifies them.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from nanopub.nanopub import Nanopub
from nanopub.nanopub_conf import NanopubConf
from nanopub.sign_utils import replace_trusty_in_graph
from nanopub.trustyuri.rdf import RdfHasher, RdfUtils
from rdflib import DCTERMS, PROV, BNode, Dataset, Graph, Literal, Namespace, URIRef
from rdflib.compare import to_canonical_graph

__all__ = ["index", "load", "nanopublication", "save"]

NPX = Namespace("http://purl.org/nanopub/x/")
_CONF = NanopubConf(
    add_prov_generated_time=False,
    add_pubinfo_generated_time=False,
    attribute_assertion_to_profile=False,
    attribute_publication_to_profile=False,
)


def nanopublication(
    assertion: Graph,
    *,
    kinds: Sequence[str],
    attributed_to: str,
    created: str,
    provenance: Graph | None = None,
) -> Nanopub:
    """Return an unsigned nanopublication of *assertion*, identified by its trusty URI.

    *kinds* are the classes of what it asserts (npx:hasNanopubType); *attributed_to* is the agent
    the assertion is attributed to, and *provenance* adds statements about the assertion.
    """
    draft = Nanopub(assertion=Graph(), conf=_CONF)
    meta = draft.metadata
    for triple in _skolemized(assertion, meta.namespace):
        draft.assertion.add(triple)
    draft.provenance.add((meta.assertion, PROV.wasAttributedTo, URIRef(attributed_to)))
    for triple in _skolemized(provenance or Graph(), meta.namespace):
        draft.provenance.add(triple)
    draft.pubinfo.add((meta.np_uri, DCTERMS.created, Literal(created)))
    for kind in kinds:
        draft.pubinfo.add((meta.np_uri, NPX.hasNanopubType, URIRef(kind)))
    namespace = str(meta.namespace)
    quads = RdfUtils.get_quads(draft.rdf)  # type: ignore[no-untyped-call]
    artefact = RdfHasher.make_hash(quads, baseuri=namespace, hashstr=" ")
    return Nanopub(rdf=replace_trusty_in_graph(artefact, namespace, draft.rdf))


def _skolemized(graph: Graph, namespace: Namespace) -> Graph:
    """Return *graph* with its blank nodes as IRIs of the nanopublication.

    Blank nodes are labelled canonically first, so that the same content gives the same trusty
    URI in every run.
    """
    found = Graph()
    for triple in to_canonical_graph(graph):
        found.add(tuple(namespace[f"_{n}"] if isinstance(n, BNode) else n for n in triple))  # type: ignore[arg-type]
    return found


def save(nanopub: Nanopub, directory: Path) -> Path:
    """Write *nanopub* as TriG named by its trusty artefact, and return the path."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{nanopub.source_uri.rsplit('/', 1)[1]}.trig"
    path.write_text(nanopub.rdf.serialize(format="trig"), encoding="utf-8")
    return path


def load(path: Path) -> Nanopub:
    """Read a nanopublication from *path*; a content that does not match its URI is refused."""
    try:
        nanopub = Nanopub(rdf=Dataset().parse(path, format="trig"))
        nanopub.has_valid_trusty  # noqa: B018 - the property verifies and raises
    except Exception as error:  # the library raises its own error types
        raise ValueError(f"{path}: the content does not match its trusty URI") from error
    return nanopub


def index(members: Sequence[Nanopub], *, title: str, attributed_to: str, created: str) -> Nanopub:
    """Return a nanopublication index that lists *members* (npx:includesElement)."""
    assertion = Graph()
    this = URIRef("http://purl.org/nanopub/temp/np/")
    assertion.add((this, DCTERMS.title, Literal(title)))
    for member in members:
        assertion.add((this, NPX.includesElement, URIRef(member.source_uri)))
    return nanopublication(
        assertion, kinds=[NPX.NanopubIndex], attributed_to=attributed_to, created=created
    )
