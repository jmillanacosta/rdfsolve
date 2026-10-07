"""Describe the source registry and its identity decisions as RDF.

The registry stays a YAML file (``data/sources.yaml``) read through
:class:`~rdfsolve.models.source_model.SourceModel`; this module only writes an
RDF view of it for a release. Each entry becomes one node: a DCAT/VoID dataset,
or a SPARQL Service Description service for ``source_role: service``. Dataset
nodes use the IRIs that linksets and SSSOM files already use
(``<base>/dataset/<name>``), so the registry joins them.

Standard terms are used only where their meaning matches the registry field.
The few registry notions without such a term are declared in the output under
:data:`RDFSOLVE` with a label and a comment. :data:`FIELDS` lists every
registry field with the terms it is written with, or why it is left out.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from urllib.parse import quote

from rdflib import OWL, RDF, RDFS, SH, XSD, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, FOAF, PROV

from rdfsolve.config import DEFAULT_BASE_URI, get_base_uri, mint_from_base
from rdfsolve.dataset_identity import (
    IdentityRelation,
    IdentityResolution,
    read_overrides,
    read_registry,
    resolve_identity,
)
from rdfsolve.models.source_model import PublicationRef, SourceModel
from rdfsolve.source_metadata import with_source_metadata

logger = logging.getLogger(__name__)

__all__ = [
    "FIELDS",
    "RDFSOLVE",
    "load_registry",
    "registry_rdf_from_files",
    "registry_to_rdf",
    "write_registry_rdf",
]

DCAT = Namespace("http://www.w3.org/ns/dcat#")
VOID = Namespace("http://rdfs.org/ns/void#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
SPDX = Namespace("http://spdx.org/rdf/terms#")
IANA = Namespace("https://www.iana.org/assignments/media-types/")
BIOREGISTRY = Namespace("https://bioregistry.io/registry/")
# Fixed, unlike the instance base: a term must not change with RDFSOLVE_BASE_URI.
RDFSOLVE = Namespace(DEFAULT_BASE_URI + "vocab/")
KG_REGISTRY_CATALOG = "kg-registry"

# rdfsolve terms: (kind, label, comment). Only registry notions that no standard term states.
TERMS: dict[str, tuple[URIRef, str, str]] = {
    "sameDataset": (
        RDF.Property,
        "same dataset",
        (
            "The two registry entries are one published RDF dataset under two registry names. Each "
            "entry keeps its own access settings; owl:sameAs is not used because it would merge them."
        ),
    ),
    "distributionOf": (
        RDF.Property,
        "distribution of",
        (
            "One entry is another endpoint, mirror or dump of the same published RDF dataset as the "
            "other. The registry does not record which of the two is the original, so the relation "
            "is read in both directions."
        ),
    ),
    "basis": (
        RDF.Property,
        "basis",
        (
            "What an identity relation between registry entries rests on, such as a structural "
            "rule over endpoints and graphs or a curated override."
        ),
    ),
    "decidedBy": (
        RDF.Property,
        "decided by",
        (
            "How an identity relation was decided: 'rule' (structural rule), 'override' (curated) "
            "or 'candidate' (proposed, not decided, so the relation itself is not asserted)."
        ),
    ),
    "identityReviewComplete": (
        RDF.Property,
        "identity review complete",
        "Whether every candidate identity relation of the registry has been decided.",
    ),
    "identityCandidateCount": (
        RDF.Property,
        "identity candidate count",
        "The number of candidate identity relations of the registry that are not yet decided.",
    ),
    "datasetKind": (
        RDF.Property,
        "dataset kind",
        (
            "Curated kind of a registry dataset: 'instance' data, an 'ontology', or 'unknown' "
            "until reviewed."
        ),
    ),
    "bioregistryRecord": (
        RDF.Property,
        "Bioregistry record",
        "The Bioregistry resource whose prefix the registry assigns to this entry.",
    ),
    "alias": (
        RDF.Property,
        "alias",
        "Another registry name of this entry, under which earlier outputs name it.",
    ),
    "typeContextGraph": (
        RDF.Property,
        "type context graph",
        (
            "A named graph read for the types of subjects and objects of the entry's data. It is "
            "not part of the entry's data."
        ),
    ),
    "ontologyGraph": (
        RDF.Property,
        "ontology graph",
        (
            "A named graph read to interpret the entry's data with its ontology. It is not part of "
            "the entry's data."
        ),
    ),
    "sampledInputs": (
        RDF.Property,
        "sampled inputs",
        (
            "How the dumps given for this graph sample it: counts made from them are counts of the "
            "sample, not of the graph."
        ),
    ),
    "queryExamples": (
        RDF.Property,
        "query examples",
        (
            "A named graph, document or repository with published SPARQL query examples for this "
            "entry."
        ),
    ),
}

# Every registry field and how it is written; "left out: ..." gives the reason it is not.
_RUNTIME = "left out: mining or access setting of the pipeline, not a description of the data"
_HEALTH = "left out: volatile endpoint observation (registry check A6)"
_CACHED = "left out: cached derived data (registry check A6)"
_BIOREGISTRY_OWN = (
    "left out: Bioregistry's own record, reachable through rdfsolve:bioregistryRecord"
)
FIELDS: dict[str, str] = {
    "name": "dcterms:identifier; also the last segment of the entry IRI",
    "aliases": "rdfsolve:alias",
    "catalogs": "dcat:Catalog <base/catalog/NAME> dcat:dataset/dcat:service and dcat:record",
    "catalog_local_name": "dcterms:identifier of the entry's dcat:CatalogRecord in each catalog",
    "source_role": "rdf:type: dcat:Dataset, void:Dataset (dataset) or sd:Service, dcat:DataService",
    "dataset_kind": "rdfsolve:datasetKind (datasets only)",
    "skip_mining": "left out: corpus selection of the pipeline; release.json lists mined datasets",
    "skip_remote": _RUNTIME,
    "endpoint": "void:sparqlEndpoint (dataset); sd:endpoint and dcat:endpointURL (service)",
    "sparql_examples": "rdfsolve:queryExamples (graphs, absolute document IRIs, repository)",
    "dataset_metadata": _CACHED,
    "metadata_graph_uris": _CACHED,
    "enrichment": _CACHED,
    "void_graphs": _CACHED,
    "void_schema": _CACHED,
    "void_default_graph": _CACHED,
    "has_void": _CACHED,
    "has_void_partitions": _CACHED,
    "has_void_patterns": _CACHED,
    "void_iri": "left out: the IRI of the dataset in a published VoID, for matching (void_catalog)",
    "graph_uris": "sd:namedGraph [a sd:NamedGraph; sd:name G] (service: under sd:defaultDataset)",
    "type_context_graph_uris": "rdfsolve:typeContextGraph [a sd:NamedGraph; sd:name G]",
    "ontology_graph_uris": "rdfsolve:ontologyGraph [a sd:NamedGraph; sd:name G]",
    "graph_sources": "sd:graph [a sd:Graph, void:Dataset; void:dataDump URL] of that graph",
    "sampled_graphs": "rdfsolve:sampledInputs on the sd:Graph of that graph",
    "graph_settings": _RUNTIME,
    "chunk_size": _RUNTIME,
    "class_batch_size": _RUNTIME,
    "class_chunk_size": _RUNTIME,
    "timeout": _RUNTIME,
    "delay": _RUNTIME,
    "counts": _RUNTIME,
    "unsafe_paging": _RUNTIME,
    "classes_as_data": _RUNTIME,
    "membership_properties": _RUNTIME,
    "uri_formats": "void:uriRegexPattern (datasets only)",
    "notes": "rdfs:comment",
    "local_provider": _RUNTIME,
    "download_*": (
        "dcat:distribution [dcat:downloadURL; dcat:mediaType; dcat:compressFormat; "
        "dcat:packageFormat]; void:dataDump for RDF serializations (not archives or "
        "database files); rdfs:seeAlso on services"
    ),
    "local_tar_url": "as download_*: a dcat:Distribution archive, not a void:dataDump",
    "archive_members_left_out": _RUNTIME,
    "checksum_files": "left out: where checksums are published; the hashes are in the release",
    "checksums": "spdx:checksum [spdx:algorithm; spdx:checksumValue] on the distribution",
    "endpoint_export": "left out: local export record with local file paths",
    "sparql_engine": _HEALTH,
    "sparql_strategy": _RUNTIME,
    "supports_graph": _HEALTH,
    "endpoint_down": _HEALTH,
    "endpoint_status": _HEALTH,
    "last_checked": _HEALTH,
    "last_success": _HEALTH,
    "last_error": _HEALTH,
    "failure_count": _HEALTH,
    "avg_response_time": _HEALTH,
    "bioregistry_prefix": "rdfsolve:bioregistryRecord <https://bioregistry.io/registry/P> sh:prefix",
    "bioregistry_name": "dcterms:title",
    "bioregistry_description": "dcterms:description",
    "bioregistry_homepage": "dcat:landingPage (not foaf:homepage, which is inverse functional)",
    "bioregistry_license": "dcterms:license (SPDX IRI, IRI, or a labelled dcterms:LicenseDocument)",
    "bioregistry_domain": "left out: free-text topic; dcat:keyword carries the topics",
    "keywords": "dcat:keyword",
    "bioregistry_publications": "dcterms:isReferencedBy (DOI, else PubMed, else PMC IRI)",
    "bioregistry_uri_prefix": "void:uriSpace (datasets only)",
    "bioregistry_uri_prefixes": _BIOREGISTRY_OWN,
    "bioregistry_synonyms": _BIOREGISTRY_OWN,
    "bioregistry_mappings": _BIOREGISTRY_OWN,
    "bioregistry_logo": _BIOREGISTRY_OWN,
    "bioregistry_extra_providers": _BIOREGISTRY_OWN,
    "bioregistry_repository": _BIOREGISTRY_OWN,
    "bioregistry_owl_download": _BIOREGISTRY_OWN + "; the registry's downloads are download_*",
    "bioregistry_rdf_download": _BIOREGISTRY_OWN + "; the registry's downloads are download_*",
    "bioregistry_obo_download": _BIOREGISTRY_OWN + "; the registry's downloads are download_*",
    "bioregistry_enriched_at": "left out: provenance of the metadata sidecar, kept in that file",
    "bioregistry_package_version": "left out: provenance of the metadata sidecar, kept there",
    "kg_registry_id": "dcterms:identifier of the entry's dcat:CatalogRecord in KG-Registry",
    "kg_registry_category": "left out: KG-Registry's own record",
    "kg_registry_domains": "left out: KG-Registry's own record",
    "kg_registry_products": "left out: KG-Registry's own record",
    "science_area": "left out: analysis selection derived from KG-Registry domains",
    "in_kamdar": "left out: membership in one published analysis, used for comparison only",
    "terminology_nomenclature": "left out: analysis tags",
}

# RDF serialization of each download field, as an IANA media type; None for files that hold
# RDF without a registered type (HDT, OWL in an unstated syntax).
_RDF_DUMPS: dict[str, str | None] = {
    "download_ttl": "text/turtle",
    "download_nt": "application/n-triples",
    "download_nq": "application/n-quads",
    "download_n3": "text/n3",
    "download_trig": "application/trig",
    "download_rdf": "application/rdf+xml",
    "download_rdfxml": "application/rdf+xml",
    "download_owl": None,
    "download_hdt": None,
}
# Archives and database files: distributions, but not RDF dumps (they may hold other files).
_PACKAGES: dict[str, tuple[str | None, str | None]] = {
    "download_zip": ("application/zip", None),
    "download_tgz": ("application/x-tar", "application/gzip"),
    "download_tar_gz": ("application/x-tar", "application/gzip"),
    "local_tar_url": ("application/x-tar", None),
}
_COMPRESSION = {
    ".gz": "application/gzip",
    ".bz2": "application/x-bzip2",
    ".xz": "application/x-xz",
    ".zst": "application/zstd",
}
_CHECKSUM_ALGORITHMS = {"md5": "md5", "sha1": "sha1", "sha256": "sha256"}
_IRI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:[^\x00-\x20<>\"{}|\\^`]+$")
_SPDX_ID = re.compile(r"^(?=.*[A-Z0-9])[A-Za-z0-9][A-Za-z0-9.+-]*$")
_PREDICATES: dict[str, URIRef] = {
    "same_dataset": RDFSOLVE.sameDataset,
    "distribution_of": RDFSOLVE.distributionOf,
    # Two RDF datasets built from one upstream resource: alternates of the same thing. The
    # registry has no node for the upstream resource, so dcterms:source cannot name it.
    "same_upstream": PROV.alternateOf,
    # Stored older (left) to newer (right); written as newer dcat:previousVersion older.
    "version_of": DCAT.previousVersion,
    # Left is a graph subset of right on the same endpoint: logically included in it.
    "graph_scope_of": DCTERMS.isPartOf,
    "distinct": OWL.differentFrom,
}

# Relations stored in the opposite direction of their predicate: the subject is the right entry.
_INVERSE = frozenset({"version_of"})


def _iri(value: str, where: str) -> URIRef:
    """Return *value* as an IRI, or raise ValueError naming the registry field."""
    if not isinstance(value, str) or not _IRI.match(value):
        raise ValueError(f"{where}: {value!r} is not an absolute IRI")
    return URIRef(value)


def _metadata_iri(value: str, where: str) -> URIRef | None:
    """Return a Bioregistry-derived IRI, or None with a warning when it is not one.

    Curated access fields must be IRIs and raise; metadata copied from Bioregistry
    is skipped instead, so one malformed upstream value does not stop a release.
    """
    try:
        return _iri(value, where)
    except ValueError as error:
        logger.warning("%s; left out", error)
        return None


def _digest(value: str) -> str:
    """Return a short stable digest for minting a node IRI from a long value."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _downloads(source: SourceModel) -> dict[str, list[str]]:
    """Return the entry-level download fields, each a list of URLs, by field name."""
    fields: dict[str, list[str]] = {}
    extra = source.model_extra or {}
    for key, value in [("download_ttl", source.download_ttl), *sorted(extra.items())]:
        if not (key.startswith("download_") or key == "local_tar_url"):
            continue
        urls = [value] if isinstance(value, str) else list(value or [])
        if urls:
            fields.setdefault(key, []).extend(url for url in urls if url)
    return fields


def _license(graph: Graph, base: str, node: URIRef, value: str) -> None:
    """Add dcterms:license as an SPDX IRI, an IRI, or a labelled license document."""
    if value.startswith(("http://", "https://")):
        iri = _metadata_iri(value, "bioregistry_license")
        if iri is not None:
            graph.add((node, DCTERMS.license, iri))
    elif _SPDX_ID.match(value):
        graph.add((node, DCTERMS.license, URIRef(f"https://spdx.org/licenses/{value}")))
    else:
        document = URIRef(mint_from_base(base, "license", value))
        graph.add((node, DCTERMS.license, document))
        graph.add((document, RDF.type, DCTERMS.LicenseDocument))
        graph.add((document, RDFS.label, Literal(value)))


def _publication(item: PublicationRef) -> str | None:
    """Return the IRI of a Bioregistry publication: DOI, else PubMed, else PMC."""
    if item.doi:
        # Old SICI DOIs hold characters that IRIs exclude; the DOI resolver decodes them.
        return f"https://doi.org/{quote(item.doi, safe='/:;()._-')}"
    if item.pubmed:
        return f"https://pubmed.ncbi.nlm.nih.gov/{item.pubmed}"
    if item.pmc:
        return f"https://www.ncbi.nlm.nih.gov/pmc/articles/{item.pmc}"
    return None


def _uri_pattern(uri_format: str) -> str:
    """Turn a Bioregistry-style IRI format with ``$1`` into a VoID IRI regex."""
    head, _, tail = uri_format.partition("$1")
    return "^" + re.escape(head) + ".+" + re.escape(tail) + "$"


class _Writer:
    """Add the registry's entries to one graph; one instance per serialization."""

    def __init__(self, graph: Graph, base: str) -> None:
        """Keep the target graph and the base of minted IRIs."""
        self.graph = graph
        self.base = base

    def node(self, source: SourceModel) -> URIRef:
        """Return the IRI of a registry entry: a dataset IRI, or a service IRI."""
        kind = "service" if source.source_role == "service" else "dataset"
        return URIRef(mint_from_base(self.base, kind, source.name))

    def add(self, source: SourceModel, catalog: URIRef) -> URIRef:
        """Describe one registry entry and list it in the registry catalog."""
        graph, node = self.graph, self.node(source)
        unknown = set(source.model_extra or {}) - set(FIELDS)
        unknown = {key for key in unknown if not key.startswith("download_")}
        if unknown:
            logger.warning("%s: fields without an RDF mapping: %s", source.name, sorted(unknown))
        service = source.source_role == "service"
        if service:
            graph.add((node, RDF.type, SD.Service))
            graph.add((node, RDF.type, DCAT.DataService))
            graph.add((catalog, DCAT.service, node))
        else:
            graph.add((node, RDF.type, DCAT.Dataset))
            graph.add((node, RDF.type, VOID.Dataset))
            graph.add((catalog, DCAT.dataset, node))
            graph.add((node, RDFSOLVE.datasetKind, Literal(source.dataset_kind)))
        graph.add((node, DCTERMS.identifier, Literal(source.name)))
        for alias in source.aliases:
            graph.add((node, RDFSOLVE.alias, Literal(alias)))
        self._describe(source, node)
        self._catalogs(source, node, service)
        if source.endpoint:
            endpoint = _iri(source.endpoint, f"{source.name}.endpoint")
            if service:
                graph.add((node, SD.endpoint, endpoint))
                graph.add((node, DCAT.endpointURL, endpoint))
            else:
                graph.add((node, VOID.sparqlEndpoint, endpoint))
        self._graphs(source, node, service)
        self._files(source, node, service)
        if not service:
            if source.bioregistry_uri_prefix:
                graph.add((node, VOID.uriSpace, Literal(source.bioregistry_uri_prefix)))
            for uri_format in source.uri_formats:
                graph.add((node, VOID.uriRegexPattern, Literal(_uri_pattern(uri_format))))
        if source.sparql_examples is not None:
            examples = source.sparql_examples
            locations = [
                *examples.shacl_graph_in_endpoint,
                *(d for d in examples.shacl_dumps if _IRI.match(d)),
                *([examples.link_to_repository] if examples.link_to_repository else []),
            ]
            for location in locations:
                graph.add((node, RDFSOLVE.queryExamples, _iri(location, f"{source.name}.examples")))
        return node

    def _describe(self, source: SourceModel, node: URIRef) -> None:
        """Add the title, description, topics, license, landing page and Bioregistry record."""
        graph = self.graph
        if source.bioregistry_name:
            graph.add((node, DCTERMS.title, Literal(source.bioregistry_name)))
        if source.bioregistry_description:
            graph.add((node, DCTERMS.description, Literal(source.bioregistry_description)))
        for keyword in source.keywords:
            graph.add((node, DCAT.keyword, Literal(keyword)))
        if source.notes:
            graph.add((node, RDFS.comment, Literal(source.notes)))
        if source.bioregistry_homepage:
            homepage = _metadata_iri(
                source.bioregistry_homepage, f"{source.name}.bioregistry_homepage"
            )
            if homepage is not None:
                graph.add((node, DCAT.landingPage, homepage))
        if source.bioregistry_license:
            _license(graph, self.base, node, source.bioregistry_license)
        for item in source.bioregistry_publications:
            publication = _publication(item)
            if publication is not None:
                iri = _metadata_iri(publication, f"{source.name}.bioregistry_publications")
                if iri is not None:
                    graph.add((node, DCTERMS.isReferencedBy, iri))
        if source.bioregistry_prefix:
            record = BIOREGISTRY[source.bioregistry_prefix]
            graph.add((node, RDFSOLVE.bioregistryRecord, record))
            graph.add((record, SH.prefix, Literal(source.bioregistry_prefix)))

    def _catalogs(self, source: SourceModel, node: URIRef, service: bool) -> None:
        """List the entry in each external catalog, with its name there as a catalog record."""
        records = [(name, source.catalog_local_name) for name in source.catalogs]
        if source.kg_registry_id:
            records.append((KG_REGISTRY_CATALOG, source.kg_registry_id))
        for name, local_name in records:
            catalog = URIRef(mint_from_base(self.base, "catalog", name))
            record = URIRef(mint_from_base(self.base, "catalog", name, "record", source.name))
            self.graph.add((catalog, RDF.type, DCAT.Catalog))
            self.graph.add((catalog, DCTERMS.identifier, Literal(name)))
            self.graph.add((catalog, DCAT.service if service else DCAT.dataset, node))
            self.graph.add((catalog, DCAT.record, record))
            self.graph.add((record, RDF.type, DCAT.CatalogRecord))
            self.graph.add((record, FOAF.primaryTopic, node))
            if local_name:
                self.graph.add((record, DCTERMS.identifier, Literal(local_name)))

    def _graphs(self, source: SourceModel, node: URIRef, service: bool) -> None:
        """Describe the data graphs, the type and ontology context graphs and their dumps."""
        graph = self.graph
        holder = node
        if service and source.graph_uris:
            # A service's named graphs belong to the dataset it serves by default.
            holder = URIRef(f"{node}/default-dataset")
            graph.add((node, SD.defaultDataset, holder))
            graph.add((holder, RDF.type, SD.Dataset))
        elif source.graph_uris:
            graph.add((node, RDF.type, SD.Dataset))
        named: dict[str, URIRef] = {}

        def named_graph(iri: str) -> URIRef:
            """Return the one sd:NamedGraph node of graph *iri* in this entry."""
            if iri not in named:
                ng = URIRef(f"{node}/graph/{_digest(iri)}")
                graph.add((ng, RDF.type, SD.NamedGraph))
                graph.add((ng, SD.name, _iri(iri, f"{source.name} graph")))
                named[iri] = ng
            return named[iri]

        for iri in source.graph_uris:
            graph.add((holder, SD.namedGraph, named_graph(iri)))
        for iri in source.type_context_graph_uris:
            graph.add((node, RDFSOLVE.typeContextGraph, named_graph(iri)))
        for iri in source.ontology_graph_uris:
            graph.add((node, RDFSOLVE.ontologyGraph, named_graph(iri)))
        for iri, fields in sorted(source.graph_sources.items()):
            content = URIRef(f"{named_graph(iri)}/content")
            graph.add((named_graph(iri), SD.graph, content))
            graph.add((content, RDF.type, SD.Graph))
            graph.add((content, RDF.type, VOID.Dataset))
            for key, urls in sorted(fields.items()):
                for url in urls:
                    graph.add((content, VOID.dataDump, _iri(url, f"{source.name}.{key}")))
                    if iri in source.graph_uris and not service:
                        self._distribution(source, node, key, url)
            if iri in source.sampled_graphs:
                graph.add((content, RDFSOLVE.sampledInputs, Literal(source.sampled_graphs[iri])))
            if iri in source.graph_uris and not service:
                for urls in fields.values():
                    for url in urls:
                        graph.add((node, VOID.dataDump, URIRef(url)))

    def _files(self, source: SourceModel, node: URIRef, service: bool) -> None:
        """Describe the entry-level downloads as distributions and, for RDF, as dumps."""
        for key, urls in sorted(_downloads(source).items()):
            for url in urls:
                iri = _iri(url, f"{source.name}.{key}")
                if service:
                    # A service is not a dataset: its files are documents about what it serves.
                    self.graph.add((node, RDFS.seeAlso, iri))
                    continue
                self._distribution(source, node, key, url)
                if key in _RDF_DUMPS:
                    self.graph.add((node, VOID.dataDump, iri))
        if not service:
            for url in sorted(set(source.checksums) - self._distributed(source)):
                # A checksum for a URL that is not a download of the entry: keep its file.
                self._distribution(source, node, "", url)

    def _distributed(self, source: SourceModel) -> set[str]:
        """Return every download URL of an entry, from its download fields and graph sources."""
        urls = {url for values in _downloads(source).values() for url in values}
        for iri in source.graph_uris:
            for values in source.graph_sources.get(iri, {}).values():
                urls.update(values)
        return urls

    def _distribution(self, source: SourceModel, node: URIRef, key: str, url: str) -> None:
        """Add one dcat:Distribution of the entry with its formats and published checksums."""
        graph = self.graph
        distribution = URIRef(f"{node}/distribution/{_digest(url)}")
        graph.add((node, DCAT.distribution, distribution))
        graph.add((distribution, RDF.type, DCAT.Distribution))
        graph.add((distribution, DCAT.downloadURL, _iri(url, f"{source.name}.{key}")))
        media = _RDF_DUMPS.get(key)
        if media:
            graph.add((distribution, DCAT.mediaType, IANA[media]))
        package, compression = _PACKAGES.get(key, (None, None))
        if package:
            graph.add((distribution, DCAT.packageFormat, IANA[package]))
        suffix = next((s for s in _COMPRESSION if url.lower().endswith(s)), None)
        compression = compression or (_COMPRESSION[suffix] if suffix else None)
        if compression and package != compression:
            graph.add((distribution, DCAT.compressFormat, IANA[compression]))
        for kind, value in sorted(source.checksums.get(url, {}).items()):
            checksum = URIRef(f"{distribution}/checksum/{kind}")
            graph.add((distribution, SPDX.checksum, checksum))
            graph.add((checksum, RDF.type, SPDX.Checksum))
            algorithm = SPDX[f"checksumAlgorithm_{_CHECKSUM_ALGORITHMS[kind]}"]
            graph.add((checksum, SPDX.algorithm, algorithm))
            graph.add((checksum, SPDX.checksumValue, Literal(value, datatype=XSD.hexBinary)))


def _relation(graph: Graph, base: str, nodes: Mapping[str, URIRef], item: IdentityRelation) -> None:
    """Add one identity relation: asserted when decided, and described with its basis."""
    left, right = nodes[item.left], nodes[item.right]
    predicate = _PREDICATES[item.relation]
    if item.relation in _INVERSE:
        left, right = right, left
    if item.decided_by != "candidate":
        graph.add((left, predicate, right))
    statement = URIRef(mint_from_base(base, "identity", item.left, item.right))
    graph.add((statement, RDF.type, RDF.Statement))
    graph.add((statement, RDF.subject, left))
    graph.add((statement, RDF.predicate, predicate))
    graph.add((statement, RDF.object, right))
    graph.add((statement, RDFSOLVE.basis, Literal(item.basis)))
    graph.add((statement, RDFSOLVE.decidedBy, Literal(item.decided_by)))
    if item.note:
        graph.add((statement, RDFS.comment, Literal(item.note)))


def _declare_terms(graph: Graph) -> None:
    """Declare the rdfsolve terms used, so the file states what each one means."""
    for name, (kind, label, comment) in TERMS.items():
        term = RDFSOLVE[name]
        graph.add((term, RDF.type, kind))
        graph.add((term, RDFS.label, Literal(label, lang="en")))
        graph.add((term, RDFS.comment, Literal(comment, lang="en")))


def registry_to_rdf(
    sources: Sequence[SourceModel],
    resolution: IdentityResolution,
    *,
    base_uri: str | None = None,
    registry_id: str | None = None,
) -> Graph:
    """Describe registry entries and their identity resolution as one RDF graph.

    Parameters
    ----------
    sources:
        Validated registry entries, with their Bioregistry and KG-Registry metadata.
    resolution:
        The identity resolution of the same entries
        (:func:`rdfsolve.dataset_identity.resolve_identity`).
    base_uri:
        Base of minted IRIs; defaults to :func:`rdfsolve.config.get_base_uri`.
    registry_id:
        Identifier of this registry version; defaults to a digest of the entries and
        relations, so the same registry always gets the same catalog IRI.

    Returns
    -------
    Graph
        The registry catalog, one node per entry, the external catalogs, the identity
        relations with their basis, and the declarations of the rdfsolve terms used.

    Raises
    ------
    ValueError
        If two entries share a name, the resolution names an entry that is not in
        *sources*, or a field that must be an IRI is not one.
    """
    base = (base_uri or get_base_uri()).rstrip("/") + "/"
    names = [source.name for source in sources]
    if len(set(names)) != len(names):
        raise ValueError("Registry names must be unique")
    if registry_id is None:
        content = json.dumps(
            [
                [source.model_dump(mode="json") for source in sources],
                [item.model_dump(mode="json") for item in resolution.relations],
                [item.model_dump(mode="json") for item in resolution.candidates],
            ],
            sort_keys=True,
        )
        registry_id = "sha256:" + hashlib.sha256(content.encode()).hexdigest()
    graph = Graph()
    for prefix, namespace in {
        "dcat": DCAT,
        "dcterms": DCTERMS,
        "void": VOID,
        "sd": SD,
        "spdx": SPDX,
        "foaf": FOAF,
        "owl": OWL,
        "prov": PROV,
        "sh": SH,
        "iana": IANA,
        "bioregistry": BIOREGISTRY,
        "rdfsolve": RDFSOLVE,
        "dataset": Namespace(base + "dataset/"),
        "service": Namespace(base + "service/"),
    }.items():
        graph.bind(prefix, namespace)
    catalog = URIRef(
        mint_from_base(base, "registry", hashlib.sha256(registry_id.encode()).hexdigest()[:24])
    )
    graph.add((catalog, RDF.type, DCAT.Catalog))
    graph.add((catalog, DCTERMS.identifier, Literal(registry_id)))
    graph.add((catalog, DCTERMS.title, Literal("rdfsolve source registry", lang="en")))
    graph.add((catalog, RDFSOLVE.identityReviewComplete, Literal(resolution.review_complete)))
    graph.add(
        (
            catalog,
            RDFSOLVE.identityCandidateCount,
            Literal(len(resolution.candidates), datatype=XSD.nonNegativeInteger),
        )
    )
    writer = _Writer(graph, base)
    nodes = {source.name: writer.add(source, catalog) for source in sources}
    unknown = {
        name
        for item in (*resolution.relations, *resolution.candidates)
        for name in (item.left, item.right)
    } - set(nodes)
    if unknown:
        raise ValueError(f"Identity relations name entries not in the registry: {sorted(unknown)}")
    for item in (*resolution.relations, *resolution.candidates):
        _relation(graph, base, nodes, item)
    _declare_terms(graph)
    return graph


def load_registry(
    sources: str | Path, overrides: str | Path | None = None
) -> tuple[list[SourceModel], IdentityResolution]:
    """Read a registry YAML with its metadata sidecar, and resolve its identity.

    The registry is read as the pipeline reads it: the YAML list (or the wrapped
    ``sources:`` form of run directories) merged with the adjacent
    ``.metadata.json`` sidecar, each entry validated as a :class:`SourceModel`.
    *overrides* defaults to ``identity_overrides.yaml`` next to the registry; a
    missing file means no overrides.
    """
    path = Path(sources)
    raw = read_registry(path)
    entries = [SourceModel.model_validate(item) for item in with_source_metadata(raw, path)]
    overrides_path = (
        overrides if overrides is not None else path.with_name("identity_overrides.yaml")
    )
    resolution = resolve_identity(raw, read_overrides(overrides_path))
    return entries, resolution


def registry_rdf_from_files(
    sources: str | Path,
    overrides: str | Path | None = None,
    *,
    base_uri: str | None = None,
) -> Graph:
    """Return the RDF description of a registry file and its identity overrides."""
    entries, resolution = load_registry(sources, overrides)
    return registry_to_rdf(entries, resolution, base_uri=base_uri)


_FORMATS = {".ttl": "turtle", ".jsonld": "json-ld", ".nt": "nt"}


def write_registry_rdf(
    sources: str | Path,
    outputs: Iterable[str | Path],
    overrides: str | Path | None = None,
    *,
    base_uri: str | None = None,
) -> Graph:
    """Write the registry's RDF description to each output, in the format of its suffix.

    Supported suffixes are ``.ttl`` (Turtle), ``.jsonld`` (JSON-LD) and ``.nt``.
    """
    paths = [Path(item) for item in outputs]
    unsupported = [str(p) for p in paths if p.suffix not in _FORMATS]
    if unsupported:
        raise ValueError(f"Use .ttl, .jsonld or .nt: {unsupported}")
    graph = registry_rdf_from_files(sources, overrides, base_uri=base_uri)
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(graph.serialize(format=_FORMATS[path.suffix]), encoding="utf-8")
    return graph
