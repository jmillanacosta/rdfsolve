"""VoID catalogs: sources whose data are VoID descriptions of other datasets.

A catalog (registry ``dataset_kind: catalog``) such as okn-void holds, for each dataset it
describes, the class partitions, property partitions, object class and datatype partitions and
counts of that dataset. Mining a catalog as data gives the schema of VoID itself, which says
nothing about the datasets. Instead its VoID is read once, split by described dataset, and each
described dataset is matched to the registry entries that are that dataset. A matched entry gets
the dataset's VoID as its published VoID: the input of VoID-first mining
(rdfsolve.mining.void_strategy), with the light mining and the drift check against the entry's
own endpoint, and the input of the agreement between a published VoID and a mined schema
(rdfsolve.analysis.void_comparison).

Matching is by what the catalog and the registry both state, strongest first:

- ``void_iri``: the entry names the dataset's IRI (registry ``void_iri``);
- ``graph``: the dataset's IRI is one of the entry's graphs, or a service description in the
  catalog maps one of the entry's graphs to the dataset;
- ``endpoint``: the dataset's void:sparqlEndpoint is the entry's endpoint and the entry has no
  graphs (it is the whole endpoint).

A dataset whose void:uriSpace is an entry's URI prefix, or whose title is an entry's name, is a
candidate only: several datasets share a namespace or a title, and a candidate is reported, not
given its VoID.
"""

from __future__ import annotations

import gzip
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

from pydantic import BaseModel, Field
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import DCTERMS

from rdfsolve.schema_models._rdf import optional_count
from rdfsolve.schema_models.readers.void import (
    VOID,
    described_datasets,
    scope_void_graph,
    void_datasets_of_graphs,
    void_issued,
)

if TYPE_CHECKING:
    from rdfsolve.models.source_model import SourceModel
    from rdfsolve.sparql_helper import SparqlHelper

logger = logging.getLogger(__name__)

PAV = Namespace("http://purl.org/pav/")
# Bases that give a matched entry the dataset's VoID; the others make candidates only.
STRONG_BASES = ("void_iri", "graph", "endpoint")
_FORMATS = {
    "download_nt": "nt",
    "download_ttl": "turtle",
    "download_rdf": "xml",
    "download_rdfxml": "xml",
    "download_owl": "xml",
    "download_nq": "nquads",
    "download_trig": "trig",
}


class CatalogMatch(BaseModel):
    """A registry entry that a described dataset is, and on what basis."""

    name: str
    basis: list[str]


class CatalogDataset(BaseModel):
    """One dataset that a catalog describes: what the catalog says of it, and its entries."""

    iri: str
    title: str | None = None
    version: str | None = None
    updated: str | None = None
    endpoints: list[str] = Field(default_factory=list)
    uri_spaces: list[str] = Field(default_factory=list)
    triples: int | None = None
    distinct_subjects: int | None = None
    distinct_objects: int | None = None
    class_partitions: int = 0
    property_partitions: int = 0
    # The catalog's description of itself (its own data graph), not of another dataset.
    describes_catalog: bool = False
    entries: list[CatalogMatch] = Field(default_factory=list)
    candidates: list[CatalogMatch] = Field(default_factory=list)


def _normal(url: str) -> str:
    """Return an endpoint or namespace URL without its trailing slash, for comparison."""
    return url.strip().rstrip("/")


def describe_dataset(void: Graph, dataset: URIRef) -> CatalogDataset:
    """Return what a catalog states of one dataset: identity, version, size and partitions."""

    def one(predicate: Any) -> str | None:
        """Return the largest value the dataset states for a predicate (one, mostly)."""
        values = sorted(str(o) for o in void.objects(dataset, predicate))
        return values[-1] if values else None

    return CatalogDataset(
        iri=str(dataset),
        title=one(DCTERMS.title),
        version=one(PAV.version) or one(URIRef("http://www.w3.org/2002/07/owl#versionInfo")),
        updated=void_issued(void, dataset),
        endpoints=sorted(str(o) for o in void.objects(dataset, VOID.sparqlEndpoint)),
        uri_spaces=sorted(str(o) for o in void.objects(dataset, VOID.uriSpace)),
        triples=optional_count(void.value(dataset, VOID.triples)),
        distinct_subjects=optional_count(void.value(dataset, VOID.distinctSubjects)),
        distinct_objects=optional_count(void.value(dataset, VOID.distinctObjects)),
        class_partitions=len(set(void.objects(dataset, VOID.classPartition))),
        property_partitions=len(set(void.objects(dataset, VOID.propertyPartition))),
    )


def match_dataset(
    void: Graph, described: CatalogDataset, registry: Iterable[SourceModel]
) -> tuple[list[CatalogMatch], list[CatalogMatch]]:
    """Return the registry entries that a described dataset is, and the candidates."""
    iri = URIRef(described.iri)
    endpoints = {_normal(e) for e in described.endpoints}
    matches, candidates = [], []
    for entry in registry:
        if entry.dataset_kind == "catalog":
            continue
        basis = []
        if entry.void_iri and entry.void_iri == described.iri:
            basis.append("void_iri")
        if entry.graph_uris and (
            described.iri in entry.graph_uris
            or iri in void_datasets_of_graphs(void, list(entry.graph_uris))
        ):
            basis.append("graph")
        if not entry.graph_uris and entry.endpoint and _normal(entry.endpoint) in endpoints:
            basis.append("endpoint")
        if basis:
            matches.append(CatalogMatch(name=entry.name, basis=basis))
            continue
        weak = []
        prefixes = {_normal(p) for p in [entry.bioregistry_uri_prefix] if p}
        if prefixes & {_normal(u) for u in described.uri_spaces}:
            weak.append("uri_space")
        if described.title and described.title.casefold() in {
            n.casefold() for n in (entry.name, entry.bioregistry_name) if n
        }:
            weak.append("title")
        if weak:
            candidates.append(CatalogMatch(name=entry.name, basis=weak))
    return matches, candidates


@dataclass
class CatalogVoid:
    """The VoID of one catalog, the datasets it describes and the entries they match."""

    name: str
    void: Graph
    datasets: list[CatalogDataset]
    read_from: str
    _scoped: dict[tuple[str, ...], Graph] = field(default_factory=dict, repr=False)

    def matched(self, name: str) -> list[CatalogDataset]:
        """Return the described datasets that the entry NAME is (strong bases only)."""
        return [d for d in self.datasets if any(m.name == name for m in d.entries)]

    def scoped(self, datasets: Iterable[str]) -> Graph:
        """Return the part of the catalog that describes DATASETS (kept once read)."""
        key = tuple(sorted(datasets))
        if key not in self._scoped:
            self._scoped[key] = scope_void_graph(self.void, [URIRef(d) for d in key])
        return self._scoped[key]

    def for_source(
        self,
        source: SourceModel,
        graph_uris: list[str] | None = None,
        *,
        require_all: bool = True,
    ) -> tuple[Graph, list[CatalogDataset]] | None:
        """Return the catalog's VoID of a registry entry, or None when it does not describe it.

        An entry matched by its void_iri or by its endpoint is the dataset as a whole. An entry
        matched by its graphs takes the datasets of its graphs (GRAPH_URIS, else its own), each
        of which must be described: a graph the catalog does not describe leaves the entry
        without a VoID from the catalog (it is then mined), as for an endpoint's own VoID.
        With REQUIRE_ALL false, the described graphs are taken and the others left out: the
        scope of the agreement with a mined schema cut to the same graphs (an OKN entry's own
        VoID graph, <kg>#void, beside its data graph), never of VoID-first mining.
        """
        matched = self.matched(source.name)
        if not matched:
            return None
        whole = [
            d
            for d in matched
            if any(
                m.name == source.name and {"void_iri", "endpoint"} & set(m.basis) for m in d.entries
            )
        ]
        if whole:
            chosen = whole
        else:
            graphs = list(graph_uris or source.graph_uris)
            per_graph = [void_datasets_of_graphs(self.void, [g]) for g in graphs]
            if not graphs or not any(per_graph) or (require_all and not all(per_graph)):
                return None
            iris = {str(d) for found in per_graph for d in found}
            chosen = [d for d in self.datasets if d.iri in iris]
        scoped = self.scoped(d.iri for d in chosen)
        if not any(scoped.objects(None, VOID.classPartition)):
            return None
        return scoped, chosen

    def record(self) -> dict[str, Any]:
        """Return the catalog's datasets and matches, for the run's record."""
        own = [d for d in self.datasets if not d.describes_catalog]
        return {
            "catalog": self.name,
            "read_from": self.read_from,
            "triples": len(self.void),
            "datasets_described": len(own),
            "datasets_matched": sum(bool(d.entries) for d in own),
            "entries_matched": sorted({m.name for d in own for m in d.entries}),
            "datasets": [d.model_dump(exclude_none=True) for d in self.datasets],
        }


def describe_catalog(
    name: str,
    void: Graph,
    registry: Iterable[SourceModel],
    *,
    own_graphs: Iterable[str] = (),
    read_from: str = "",
) -> CatalogVoid:
    """Return the datasets that a catalog's VoID describes, matched to the registry.

    OWN_GRAPHS are the catalog's own graphs: a dataset named by one of them is the catalog's
    description of itself, recorded apart and matched to no entry.
    """
    entries = list(registry)
    own = set(own_graphs)
    datasets = []
    for iri in described_datasets(void):
        described = describe_dataset(void, iri)
        if str(iri) in own:
            described.describes_catalog = True
        else:
            described.entries, described.candidates = match_dataset(void, described, entries)
        datasets.append(described)
    catalog = CatalogVoid(name, void, datasets, read_from)
    record = catalog.record()
    logger.info(
        "VoID catalog %s: %d datasets described, %d matched to %d registry entries",
        name,
        record["datasets_described"],
        record["datasets_matched"],
        len(record["entries_matched"]),
    )
    return catalog


def _local_path(location: str) -> Path | None:
    """Return the local file of a download location (file:// or a path), if it exists."""
    if location.startswith("file://"):
        path = Path(unquote(urlparse(location).path))
    elif "://" in location:
        return None
    else:
        path = Path(location)
    return path if path.is_file() else None


def catalog_files(source: SourceModel) -> list[tuple[Path, str]]:
    """Return the local files of a catalog's data, with their RDF formats."""
    fields: dict[str, list[str]] = {}
    for inputs in source.graph_sources.values():
        for key, urls in inputs.items():
            fields.setdefault(key, []).extend(urls)
    for key, value in (source.model_extra or {}).items():
        if key in _FORMATS and value:
            fields.setdefault(key, []).extend([value] if isinstance(value, str) else value)
    if source.download_ttl:
        fields.setdefault("download_ttl", []).extend(source.download_ttl)
    found = []
    for key, urls in fields.items():
        for url in urls:
            path = _local_path(url)
            if path is not None and key in _FORMATS:
                found.append((path, _FORMATS[key]))
    return found


def read_catalog_void(
    source: SourceModel,
    *,
    helper_factory: Callable[[SourceModel], Any] | None = None,
    max_bytes: int = 4 * 1024**3,
) -> tuple[Graph, str]:
    """Read a catalog's VoID: from its local files when it has them, else from its endpoint.

    Local files are the registry's graph_sources or downloads that are on this machine (an
    export of the endpoint's graphs). From the endpoint, each of the catalog's graphs (or, when
    it names none, the graph that holds the most class partitions) is read whole, as the
    VoID-first reader reads an endpoint's own VoID, with a larger response allowed: a catalog
    is metadata, and okn-void's is 1.15 GB of N-Triples.
    """
    files = catalog_files(source)
    void = Graph()
    if files and all(path.is_file() for path, _ in files):
        for path, fmt in files:
            if path.suffix == ".gz":
                with gzip.open(path, "rb") as stream:
                    void.parse(data=stream.read(), format=fmt)
            else:
                void.parse(source=str(path), format=fmt)
        return void, "local files: " + ", ".join(str(p) for p, _ in files)
    if not source.endpoint:
        raise ValueError(f"The catalog {source.name} has neither local files nor an endpoint")
    from rdfsolve.mining.void_strategy import _read_void_graph, find_published_void
    from rdfsolve.sparql_helper import SparqlHelper

    def open_helper(entry: SourceModel) -> SparqlHelper:
        """Open a helper on the catalog's endpoint."""
        return SparqlHelper.from_source_entry(entry.model_dump(), timeout=600)

    with (helper_factory or open_helper)(source) as helper:
        helper.max_response_bytes = max(helper.max_response_bytes, max_bytes)
        if not source.graph_uris:
            published = find_published_void(helper, max_bytes=max_bytes)
            if published is None:
                raise ValueError(f"The catalog {source.name} publishes no VoID")
            return published.void, f"endpoint graph {published.graph} ({published.read_by})"
        for graph in source.graph_uris:
            part, read_by = _read_void_graph(helper, graph)
            if part is None:
                raise ValueError(f"The catalog graph {graph} could not be read ({read_by})")
            void += part
    return void, f"endpoint {source.endpoint}: " + ", ".join(source.graph_uris)


def load_catalogs(
    registry: Iterable[SourceModel],
    *,
    read: Callable[[SourceModel], tuple[Graph, str]] = read_catalog_void,
) -> list[CatalogVoid]:
    """Read every catalog of the registry and match its datasets to the other entries.

    A catalog that cannot be read is logged and left out: its entries are mined as before.
    """
    entries = list(registry)
    catalogs = []
    for source in entries:
        if source.dataset_kind != "catalog":
            continue
        try:
            void, read_from = read(source)
        except Exception as error:  # a catalog that cannot be read is skipped
            logger.warning("The VoID catalog %s was not read: %s", source.name, str(error)[:300])
            continue
        catalogs.append(
            describe_catalog(
                source.name, void, entries, own_graphs=source.graph_uris, read_from=read_from
            )
        )
    return catalogs


def write_catalog(
    catalog: CatalogVoid, registry: Iterable[SourceModel], folder: Path
) -> dict[str, Any]:
    """Write a catalog's record and, for each matched entry, the VoID it publishes of it.

    FOLDER/<catalog>.json lists the datasets described, their matches and, per entry, the file
    FOLDER/<catalog>/<entry>.void.nt that holds the catalog's VoID of the entry (scoped to the
    entry's datasets), or why it has none. This is what the remote stage reads with
    --void-catalogs (catalog_void_of).
    """
    import json

    by_name = {source.name: source for source in registry}
    record = catalog.record()
    entries: dict[str, Any] = {}
    for name in record["entries_matched"]:
        source = by_name.get(name)
        found = catalog.for_source(source) if source is not None else None
        void_first = found is not None
        if found is None and source is not None:
            found = catalog.for_source(source, require_all=False)
        if found is None:
            entries[name] = {
                "void": None,
                "reason": "described without class partitions"
                if source is not None
                and catalog.matched(name)
                and not any(d.class_partitions for d in catalog.matched(name))
                else "no class partitions for the entry's graphs",
            }
            continue
        scoped, datasets = found
        path = folder / catalog.name / f"{name}.void.nt"
        path.parent.mkdir(parents=True, exist_ok=True)
        scoped.serialize(destination=path, format="nt", encoding="utf-8")
        described = {d.iri for d in datasets}
        entries[name] = {
            "void": f"{catalog.name}/{name}.void.nt",
            # False when some of the entry's graphs are not described: the file is the scope
            # of the agreement with the mined schema (cut to the same graphs), not VoID-first.
            "void_first": void_first,
            "graphs_not_described": [
                g
                for g in (source.graph_uris if source is not None else [])
                if g not in described
                and not {str(x) for x in void_datasets_of_graphs(catalog.void, [g])} & described
            ],
            "datasets": [d.iri for d in datasets],
            "versions": [d.version for d in datasets],
            "updated": max((d.updated for d in datasets if d.updated), default=None),
            "triples": len(scoped),
        }
    record["entries"] = entries
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{catalog.name}.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    return record


def catalog_void_of(folder: Path, name: str) -> Any:
    """Return the VoID that a catalog publishes of the entry NAME, as a published VoID, or None.

    FOLDER is the output of write_catalog. The result is a
    rdfsolve.mining.void_strategy.PublishedVoid whose graph names the catalog and the described
    datasets, issued when the catalog says the dataset was last updated.
    """
    import json

    from rdfsolve.mining.void_strategy import PublishedVoid

    for index in sorted(Path(folder).glob("*.json")):
        record = json.loads(index.read_text(encoding="utf-8"))
        entry = (record.get("entries") or {}).get(name)
        if not entry or not entry.get("void") or entry.get("void_first") is False:
            continue
        void = Graph().parse(Path(folder) / entry["void"], format="nt")
        return PublishedVoid(
            graph=f"{record['catalog']}: {', '.join(entry['datasets'])}",
            void=void,
            issued=entry.get("updated"),
            read_by=f"catalog {record['catalog']}",
        )
    return None
