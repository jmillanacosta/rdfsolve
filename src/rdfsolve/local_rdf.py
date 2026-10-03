"""Run local SPARQL with explicit RDFLib or Oxigraph execution."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any, cast
from typing import Literal as Choice

import pyoxigraph as ox
from rdflib import BNode, Dataset, Graph, Literal, URIRef, Variable
from rdflib.query import Result
from rdflib.term import Identifier, Node

LocalBackend = Choice["oxigraph", "rdflib"]
_XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
logger = logging.getLogger(__name__)


def _node(value: Node) -> ox.NamedNode | ox.BlankNode:
    if isinstance(value, URIRef):
        return ox.NamedNode(str(value))
    if isinstance(value, BNode):
        return ox.BlankNode(str(value))
    raise ValueError(f"Unsupported RDF node: {value!r}")


def _term(value: Node) -> ox.NamedNode | ox.BlankNode | ox.Literal:
    if isinstance(value, Literal):
        return ox.Literal(
            str(value),
            language=value.language,
            datatype=ox.NamedNode(str(value.datatype))
            if value.datatype and not value.language
            else None,
        )
    return _node(value)


def to_oxigraph(graph: Graph) -> ox.Dataset:
    """Return the statements of an RDFLib graph or dataset as an Oxigraph dataset.

    Named graphs stay named. Literals keep their lexical forms.
    """
    if isinstance(graph, Dataset):
        return ox.Dataset(
            ox.Quad(
                _node(s),
                ox.NamedNode(str(p)),
                _term(o),
                ox.DefaultGraph() if g is None or g == graph.default_graph.identifier else _node(g),
            )
            for s, p, o, g in graph.quads((None, None, None, None))
        )
    return ox.Dataset(ox.Quad(_node(s), ox.NamedNode(str(p)), _term(o)) for s, p, o in graph)


def load_store(path: str | Path | Iterable[str | Path]) -> ox.Store:
    """Load an RDF file (gzip allowed), or every RDF file in a zip archive (a source's dump),
    into an Oxigraph store, with Oxigraph's bulk loader. Several paths (the dumps of one
    release) load into the same store.

    The format comes from the extension; a format Oxigraph does not read is parsed by RDFLib.
    """
    store = ox.Store()
    for one in [path] if isinstance(path, (str, Path)) else path:
        _load_file(Path(one), store)
    return store


def _load_file(path: Path, store: ox.Store) -> None:
    """Load one RDF file or zip archive into *store*."""
    import gzip

    if path.suffix == ".zip":
        _load_zip(path, store)
        return
    suffixes = [s.lstrip(".") for s in path.suffixes]
    zipped = suffixes[-1:] == ["gz"]
    extension = suffixes[-2] if zipped and len(suffixes) > 1 else suffixes[-1] if suffixes else ""
    rdf_format = ox.RdfFormat.from_extension(extension)
    if rdf_format is None:
        store.extend(to_oxigraph(Dataset().parse(path)))
    elif zipped:
        with gzip.open(path, "rb") as handle:
            store.bulk_load(cast("IO[bytes]", handle), format=rdf_format)
    else:
        store.bulk_load(path=str(path), format=rdf_format)


_RDF_EXTENSIONS = frozenset({"ttl", "nt", "nq", "trig", "n3", "rdf", "owl"})


def registry_files(name: str) -> list[Path]:
    """Return the RDF downloads of a registry entry, fetched once into $RDFSOLVE_DOWNLOADS/<name>.

    The registry lists them (its download_* fields); the files that load_store reads are kept
    (RDF, gzip, zip).
    """
    import os
    import urllib.request

    from rdfsolve.sources import load_sources

    entry = next((s for s in load_sources() if s.name == name), None)
    if entry is None:
        raise FileNotFoundError(f"{name!r} is neither a file nor a registry entry")
    folder = Path(os.environ.get("RDFSOLVE_DOWNLOADS", "~/.cache/rdfsolve")).expanduser() / name
    folder.mkdir(parents=True, exist_ok=True)
    fields = {**(entry.model_extra or {}), "download_ttl": entry.download_ttl}
    urls = [
        url
        for key, value in fields.items()
        if key.startswith("download_")
        for url in (value if isinstance(value, list) else [value])
        if url
    ]
    files = []
    for url in urls:
        path = folder / Path(url.split("?")[0]).name
        kinds = [s.lstrip(".") for s in path.suffixes if s != ".gz"]
        if not kinds or kinds[-1] not in _RDF_EXTENSIONS | {"zip"}:
            continue
        if not path.exists():
            partial = path.with_name(path.name + ".part")
            urllib.request.urlretrieve(url, partial)  # noqa: S310 (registry URLs)
            partial.replace(path)
        files.append(path)
    return files


def _load_zip(path: Path, store: ox.Store) -> None:
    """Load every member of a zip archive whose extension is an RDF format Oxigraph reads."""
    import zipfile

    with zipfile.ZipFile(path) as archive:
        for member in archive.namelist():
            extension = Path(member).suffix.lstrip(".").lower()
            if extension not in _RDF_EXTENSIONS:
                continue
            rdf_format = ox.RdfFormat.from_extension(extension)
            if rdf_format is None:
                continue
            with archive.open(member) as handle:
                store.bulk_load(handle, format=rdf_format)


def to_rdflib(quads: Iterable[ox.Quad]) -> Dataset:
    """Return Oxigraph statements as an RDFLib dataset. Named graphs stay named."""
    dataset = Dataset()
    for q in quads:
        target = (
            dataset.default_graph
            if isinstance(q.graph_name, ox.DefaultGraph)
            else dataset.graph(_rdf(q.graph_name))
        )
        target.add((_rdf(q.subject), _rdf(q.predicate), _rdf(q.object)))
    return dataset


def _rdf(value: ox.NamedNode | ox.BlankNode | ox.Literal | ox.Triple) -> Identifier:
    if isinstance(value, ox.NamedNode):
        return URIRef(value.value)
    if isinstance(value, ox.BlankNode):
        return BNode(value.value)
    if isinstance(value, ox.Literal):
        # RDF 1.1 gives plain literals xsd:string. RDFLib parsers leave the datatype out.
        plain = value.language or value.datatype.value == _XSD_STRING
        return Literal(
            value.value,
            lang=value.language,
            datatype=None if plain else URIRef(value.datatype.value),
            normalize=False,
        )
    raise ValueError("RDF triple terms are not supported")


class LocalRdf:
    """Query a local snapshot while retaining RDFLib terms at the API boundary."""

    def __init__(
        self, graph: Graph | ox.Dataset | ox.Store, *, backend: LocalBackend = "oxigraph"
    ) -> None:
        """Select an engine; retain RDFLib when Oxigraph changes stored RDF terms.

        Oxigraph data is queried without an RDFLib copy. It is copied to RDFLib only for the
        rdflib backend, or when the Oxigraph store changes literal forms.
        """
        if backend not in {"oxigraph", "rdflib"}:
            raise ValueError("local backend must be oxigraph or rdflib")
        self.requested = backend
        self.backend = backend
        self.fallback_reason: str | None = None
        self._store: ox.Store | None = None
        self._literals: dict[ox.Literal, Literal] = {}
        self.graph: Graph | None = None
        if isinstance(graph, ox.Store) and backend == "oxigraph":
            # Already stored by Oxigraph: queried as it is, without a copy.
            self._store = graph
            return
        if isinstance(graph, (ox.Dataset, ox.Store)):
            self._from_oxigraph(set(graph))
            return
        self.graph = graph
        if backend == "rdflib":
            return
        try:
            quads = set()
            if isinstance(graph, Dataset):
                for s, p, o, g in graph.quads((None, None, None, None)):
                    name = (
                        ox.DefaultGraph()
                        if g is None or g == graph.default_graph.identifier
                        else _node(g)
                    )
                    quads.add(ox.Quad(_node(s), ox.NamedNode(str(p)), self._object(o), name))
                named_edges = [
                    (q.subject, q.predicate, q.object)
                    for q in quads
                    if not isinstance(q.graph_name, ox.DefaultGraph)
                ]
                if len(set(named_edges)) != len(named_edges):
                    raise ValueError(
                        "Overlapping named graphs require RDFLib's RDF merge semantics"
                    )
                for context in graph.graphs():
                    if context.identifier != graph.default_graph.identifier:
                        if self._store is None:
                            self._store = ox.Store()
                        self._store.add_graph(_node(context.identifier))
                if graph.default_union:
                    quads.update(ox.Quad(q.subject, q.predicate, q.object) for q in list(quads))
            else:
                quads = {
                    ox.Quad(_node(s), ox.NamedNode(str(p)), self._object(o)) for s, p, o in graph
                }
            store = self._store if self._store is not None else ox.Store()
            store.extend(quads)
            if set(store) != quads:
                raise ValueError("Oxigraph changes RDF literal forms during storage")
            self._store = store
        except (ValueError, SyntaxError) as error:
            self._store = None
            self._literals.clear()
            self.backend = "rdflib"
            self.fallback_reason = str(error)
            logger.warning("Using RDFLib to preserve the local RDF: %s", error)

    def _from_oxigraph(self, quads: set[ox.Quad]) -> None:
        """Query Oxigraph data in a store, or in RDFLib when the store would change it."""
        self.graph = None
        if self.backend == "oxigraph":
            store = ox.Store()
            store.extend(quads)
            if set(store) == quads:
                self._store = store
                return
            self.backend = "rdflib"
            self.fallback_reason = "Oxigraph changes RDF literal forms during storage"
            logger.warning("Using RDFLib to preserve the local RDF: %s", self.fallback_reason)
        self.graph = to_rdflib(quads)

    def _object(self, value: Node) -> ox.NamedNode | ox.BlankNode | ox.Literal:
        term = _term(value)
        if isinstance(term, ox.Literal) and isinstance(value, Literal):
            previous = self._literals.setdefault(term, value)
            if previous != value:
                raise ValueError("Oxigraph merges distinct RDFLib literal terms")
        return term

    def _result_term(
        self, value: ox.NamedNode | ox.BlankNode | ox.Literal | ox.Triple
    ) -> Identifier:
        if isinstance(value, ox.Literal) and value in self._literals:
            return self._literals[value]
        return _rdf(value)

    def metadata(self) -> dict[str, str | None]:
        """Describe requested and actual execution."""
        from importlib.metadata import version

        return {
            "requested": self.requested,
            "engine": self.backend,
            "version": version("pyoxigraph" if self.backend == "oxigraph" else "rdflib"),
            "fallback_reason": self.fallback_reason,
        }

    def select_json(self, query: str) -> dict[str, Any]:
        """Return SELECT results in the SPARQL 1.1 JSON format.

        Oxigraph writes the results itself when no RDFLib literal forms must be restored.
        """
        if self._store is not None and not self._literals:
            namespaces = self.graph.namespaces() if self.graph is not None else []
            raw = self._store.query(query, prefixes={p: str(iri) for p, iri in namespaces})
            if isinstance(raw, ox.QuerySolutions):
                written = raw.serialize(format=ox.QueryResultsFormat.JSON)
                if written is None:
                    raise RuntimeError("Oxigraph returned no serialized query result")
                value: dict[str, Any] = json.loads(written)
                return value
        data = self.query(query).serialize(format="json")
        if data is None:
            raise RuntimeError("RDFLib returned no serialized query result")
        result: dict[str, Any] = json.loads(data)
        return result

    def query(self, query: str) -> Result:
        """Execute SELECT, ASK or CONSTRUCT with the query's graph clauses."""
        if self._store is None:
            if self.graph is None:
                raise RuntimeError("Local RDF backend is not initialized")
            return self.graph.query(query)
        namespaces = self.graph.namespaces() if self.graph is not None else []
        raw = self._store.query(query, prefixes={prefix: str(iri) for prefix, iri in namespaces})
        if isinstance(raw, ox.QueryBoolean):
            result = Result("ASK")
            result.askAnswer = bool(raw)
        elif isinstance(raw, ox.QuerySolutions):
            result = Result("SELECT")
            variables = [Variable(v.value) for v in raw.variables]
            result.vars = variables
            result.bindings = [
                {
                    name: self._result_term(value)
                    for name, value in zip(variables, row, strict=True)
                    if value is not None
                }
                for row in raw
            ]
        else:
            result = Result("CONSTRUCT")
            result.graph = Graph()
            for triple in raw:
                result.graph.add(
                    (_rdf(triple.subject), _rdf(triple.predicate), self._result_term(triple.object))
                )
        return result
