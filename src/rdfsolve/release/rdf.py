"""Standards-based RDF projection of a canonical rdfsolve release manifest."""

from __future__ import annotations

import hashlib

from rdflib import RDF, XSD, BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS

from rdfsolve.config import mint_from_base

from .model import ReleaseManifest

DCAT = Namespace("http://www.w3.org/ns/dcat#")
PROV = Namespace("http://www.w3.org/ns/prov#")
VOID = Namespace("http://rdfs.org/ns/void#")
SPDX = Namespace("http://spdx.org/rdf/terms#")


def _artifact_uri(base: str, artifact_id: str) -> URIRef:
    digest = hashlib.sha256(artifact_id.encode()).hexdigest()[:24]
    return URIRef(mint_from_base(base, "distribution", digest))


def release_to_rdf(
    manifest: ReleaseManifest,
    *,
    base_uri: str | None = None,
) -> Graph:
    """Project the canonical manifest into DCAT/VoID/PROV/SPDX RDF.

    Completion states and internal pipeline details remain authoritative in JSON;
    RDF is a standards-oriented catalog/provenance view rather than a lossy
    replacement for the canonical manifest.
    """
    base = (base_uri or manifest.base_uri).rstrip("/") + "/"
    graph = Graph()
    for prefix, ns in {
        "dcat": DCAT,
        "dcterms": DCTERMS,
        "prov": PROV,
        "void": VOID,
        "spdx": SPDX,
    }.items():
        graph.bind(prefix, ns)

    release_hash = hashlib.sha256(manifest.release_id.encode()).hexdigest()[:24]
    catalog = URIRef(mint_from_base(base, "release", release_hash))
    run = URIRef(mint_from_base(base, "activity", release_hash))
    graph.add((catalog, RDF.type, DCAT.Catalog))
    graph.add((catalog, RDF.type, VOID.Dataset))
    graph.add((catalog, DCTERMS.identifier, Literal(manifest.release_id)))
    graph.add(
        (catalog, DCTERMS.issued, Literal(manifest.issued.isoformat(), datatype=XSD.dateTime))
    )
    graph.add((catalog, PROV.wasGeneratedBy, run))
    graph.add((run, RDF.type, PROV.Activity))
    if manifest.code_commit:
        graph.add(
            (
                run,
                PROV.used,
                URIRef(f"https://github.com/jmillanacosta/rdfsolve/commit/{manifest.code_commit}"),
            )
        )

    artifact_by_id = manifest.artifact_by_id()
    for dataset in manifest.datasets:
        snapshot = URIRef(
            dataset.snapshot_id or mint_from_base(base, "dataset", dataset.dataset_id)
        )
        graph.add((snapshot, RDF.type, DCAT.Dataset))
        graph.add((snapshot, RDF.type, VOID.Dataset))
        graph.add((snapshot, DCTERMS.identifier, Literal(dataset.dataset_id)))
        graph.add((catalog, DCAT.dataset, snapshot))
        graph.add((catalog, VOID.subset, snapshot))
        graph.add((snapshot, PROV.wasGeneratedBy, run))
        if dataset.endpoint:
            graph.add((snapshot, VOID.sparqlEndpoint, URIRef(dataset.endpoint)))
        for url in dataset.distributions:
            # Preserve upstream access location without claiming that every
            # registry field has identical semantics.
            graph.add((snapshot, DCTERMS.source, URIRef(url)))
        if dataset.source_version:
            graph.add((snapshot, DCAT.version, Literal(dataset.source_version)))
        if dataset.source_version_iri:
            graph.add((snapshot, PROV.wasDerivedFrom, URIRef(dataset.source_version_iri)))

        for extraction in dataset.extractions:
            if extraction.snapshot_id:
                observed = URIRef(extraction.snapshot_id)
                graph.add((observed, RDF.type, DCAT.Dataset))
                graph.add(
                    (
                        observed,
                        PROV.specializationOf,
                        URIRef(mint_from_base(base, "dataset", dataset.dataset_id)),
                    )
                )
                graph.add((observed, PROV.wasGeneratedBy, run))
                if extraction.schema_artifact_id:
                    graph.add(
                        (
                            observed,
                            DCAT.distribution,
                            _artifact_uri(base, extraction.schema_artifact_id),
                        )
                    )

        for artifact_id in dataset.artifacts:
            artifact = artifact_by_id.get(artifact_id)
            if artifact is None:
                continue
            distribution = _artifact_uri(base, artifact_id)
            graph.add((distribution, RDF.type, DCAT.Distribution))
            graph.add((snapshot, DCAT.distribution, distribution))
            graph.add((distribution, DCTERMS.identifier, Literal(artifact.artifact_id)))
            graph.add((distribution, DCAT.downloadURL, URIRef(artifact.path)))
            graph.add(
                (
                    distribution,
                    DCAT.byteSize,
                    Literal(artifact.byte_size, datatype=XSD.nonNegativeInteger),
                )
            )
            if artifact.media_type:
                graph.add((distribution, DCAT.mediaType, Literal(artifact.media_type)))
            checksum = BNode()
            graph.add((distribution, SPDX.checksum, checksum))
            graph.add((checksum, RDF.type, SPDX.Checksum))
            graph.add((checksum, SPDX.algorithm, SPDX.checksumAlgorithm_sha256))
            graph.add(
                (checksum, SPDX.checksumValue, Literal(artifact.sha256, datatype=XSD.hexBinary))
            )
            if artifact.generated_by:
                graph.add((distribution, PROV.wasGeneratedBy, URIRef(artifact.generated_by)))

    return graph


__all__ = ["release_to_rdf"]
