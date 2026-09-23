"""Resolve which registry entries describe the same dataset.

Rules decide only structural cases. A graph set that is a strict subset of
another graph set on the same endpoint is a graph scope of it. Disjoint graph
sets on the same endpoint are distinct datasets. Every other shared endpoint,
host, identifier space or catalog name gives a candidate for review. Curated
overrides decide candidates and take precedence over rules.

same_dataset and distribution_of join entries into one canonical dataset:
the same published RDF dataset under two names, or reached through another
endpoint or dump. same_upstream relates different RDF datasets built from one
upstream resource, such as independent RDF conversions. version_of,
graph_scope_of and distinct keep entries apart. Candidates never join entries.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping, Sequence
from itertools import combinations
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, Field, computed_field

from rdfsolve.models.source_model import SourceModel

Relation = Literal[
    "same_dataset", "distribution_of", "same_upstream", "version_of", "graph_scope_of", "distinct"
]
# Decided relations that make two registry entries one canonical dataset.
JOINING: frozenset[str] = frozenset({"same_dataset", "distribution_of"})

# Catalog membership from the registry name prefix or the endpoint host.
CATALOG_NAME_PREFIXES = {
    "rdfportal.": "rdfportal",
    "bio2rdf.": "bio2rdf",
    "pubchem.ftp.": "pubchem_ftp",
}
CATALOG_HOSTS = {
    "rdfportal.org": "rdfportal",
    "bio2rdf.org": "bio2rdf",
    "idsm.elixir-czech.cz": "idsm",
}


class DatasetIdentity(BaseModel):
    """One registry entry with its access routes and catalog provenance."""

    dataset_id: str
    source_role: Literal["dataset", "service"] = "dataset"
    bioregistry_prefix: str = ""
    homepage: str = ""
    kg_registry_id: str = ""
    endpoint: str = ""
    graph_uris: list[str] = Field(default_factory=list)
    distributions: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    catalogs: list[str] = Field(default_factory=list)

    @classmethod
    def from_entry(cls, entry: Mapping[str, Any]) -> DatasetIdentity:
        """Read identity and access fields from one registry mapping."""
        source = SourceModel.model_validate(dict(entry))
        downloads = sorted(
            {
                url
                for key, value in entry.items()
                if key.startswith("download_") or key == "local_tar_url"
                for url in ([value] if isinstance(value, str) else value or [])
                if isinstance(url, str) and url
            }
            | set(source.download_ttl)
            | {
                url
                for graph in source.graph_uris
                for urls in source.graph_sources.get(graph, {}).values()
                for url in urls
            }
        )
        return cls(
            dataset_id=source.name,
            source_role=source.source_role,
            bioregistry_prefix=source.bioregistry_prefix,
            homepage=source.bioregistry_homepage,
            kg_registry_id=source.kg_registry_id,
            endpoint=_endpoint(source.endpoint),
            graph_uris=sorted(set(source.graph_uris)),
            distributions=downloads,
            aliases=source.aliases,
            catalogs=catalogs(source.name, source.endpoint),
        )

    @property
    def local_name(self) -> str:
        """Return the registry name without its catalog prefix."""
        for prefix in CATALOG_NAME_PREFIXES:
            if self.dataset_id.startswith(prefix):
                return self.dataset_id[len(prefix) :]
        return self.dataset_id


class IdentityRelation(BaseModel):
    """A relation between two registry entries and how it was decided.

    graph_scope_of reads as left is a graph scope of right. Other relations are
    stored with their names in sorted order.
    """

    left: str
    right: str
    relation: Relation
    basis: str
    decided_by: Literal["rule", "override", "candidate"]
    note: str = ""


class IdentityResolution(BaseModel):
    """Registry entries, decided relations, and unresolved candidates.

    ``datasets`` contains provisional groups formed only from decided
    ``same_dataset`` relations.  It must not be reported as a canonical dataset
    denominator while candidate relations remain unresolved.
    """

    entries: list[DatasetIdentity]
    relations: list[IdentityRelation]
    candidates: list[IdentityRelation]
    datasets: dict[str, list[str]]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def review_complete(self) -> bool:
        """Return whether every generated identity candidate has been adjudicated."""
        return not self.candidates

    @computed_field  # type: ignore[prop-decorator]
    @property
    def canonical_dataset_count(self) -> int | None:
        """Return a canonical denominator only after identity review is complete."""
        return len(self.datasets) if self.review_complete else None

    def write(self, directory: str | Path) -> None:
        """Write identity.json and identity_review.tsv."""
        folder = Path(directory)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "identity.json").write_text(self.model_dump_json(indent=2), encoding="utf-8")
        with (folder / "identity_review.tsv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
            writer.writerow(["left", "right", "candidate_relation", "basis"])
            for item in self.candidates:
                writer.writerow([item.left, item.right, item.relation, item.basis])


def catalogs(name: str, endpoint: str) -> list[str]:
    """Derive catalog memberships from the registry name and endpoint host."""
    found = {value for prefix, value in CATALOG_NAME_PREFIXES.items() if name.startswith(prefix)}
    host = urlsplit(endpoint).hostname or ""
    found.update(
        value
        for suffix, value in CATALOG_HOSTS.items()
        if host == suffix or host.endswith("." + suffix)
    )
    return sorted(found)


def _endpoint(url: str) -> str:
    return url.rstrip("/")


GENERIC_PATH_PARTS = {"sparql", "lode", "query", "endpoint"}


def _service(url: str) -> tuple[str, tuple[str, ...]]:
    """Return the host and the dataset-specific path parts of an endpoint URL."""
    parts = urlsplit(url)
    path = tuple(p for p in parts.path.split("/") if p and p.lower() not in GENERIC_PATH_PARTS)
    return parts.hostname or "", path


def read_overrides(path: str | Path | None) -> list[IdentityRelation]:
    """Read curated relations; a missing file means no overrides."""
    if path is None or not Path(path).exists():
        return []
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or []
    if not isinstance(raw, list):
        raise ValueError(f"Expected a YAML list of relations in {path}")
    return [
        IdentityRelation(
            left=item["left"],
            right=item["right"],
            relation=item["relation"],
            basis="curated override",
            decided_by="override",
            note=str(item.get("note", "")),
        )
        for item in raw
    ]


def relate(left: DatasetIdentity, right: DatasetIdentity) -> IdentityRelation | None:
    """Apply the structural rules to one pair, or return None if nothing relates them."""
    if left.source_role == "service" or right.source_role == "service":
        return None
    first, second = sorted((left, right), key=lambda item: item.dataset_id)
    a, b = set(first.graph_uris), set(second.graph_uris)

    def found(
        relation: Relation,
        basis: str,
        decided_by: Literal["rule", "candidate"],
        pair: tuple[DatasetIdentity, DatasetIdentity] = (first, second),
    ) -> IdentityRelation:
        """Build the relation for a pair in the given order."""
        return IdentityRelation(
            left=pair[0].dataset_id,
            right=pair[1].dataset_id,
            relation=relation,
            basis=basis,
            decided_by=decided_by,
        )

    if first.endpoint and first.endpoint == second.endpoint:
        if a == b:
            return found("same_dataset", "same endpoint and graph set", "candidate")
        if a and b and a < b:
            return found("graph_scope_of", "graph subset on the same endpoint", "rule")
        if a and b and b < a:
            return found(
                "graph_scope_of", "graph subset on the same endpoint", "rule", (second, first)
            )
        if a and b and not a & b:
            return found("distinct", "disjoint graphs on the same endpoint", "rule")
        if not (a and b):
            narrower, wider = (first, second) if a else (second, first)
            return found(
                "graph_scope_of",
                "named graphs and default graph on the same endpoint",
                "candidate",
                (narrower, wider),
            )
        return found("distinct", "overlapping graphs on the same endpoint", "candidate")
    if first.endpoint and _service(first.endpoint) == _service(second.endpoint) and a == b:
        return found("same_dataset", "same endpoint service and graph set", "candidate")
    if first.local_name == second.local_name:
        return found("same_upstream", "same name in different catalogs", "candidate")
    if first.kg_registry_id and first.kg_registry_id == second.kg_registry_id:
        return found("same_upstream", "same KG-Registry identifier", "candidate")
    prefix = first.bioregistry_prefix
    if (
        prefix
        and prefix == second.bioregistry_prefix
        and prefix
        in {
            first.local_name,
            second.local_name,
        }
    ):
        return found("same_upstream", "same Bioregistry prefix", "candidate")
    return None


def read_registry(path: str | Path) -> list[dict[str, Any]]:
    """Read raw registry mappings, including download fields.

    The project registry is a YAML list; the wrapped ``sources:`` form is also
    accepted for frozen/test fixtures and older run directories.
    """
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "sources" in raw:
        raw = raw["sources"]
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise ValueError(f"Expected a YAML list of source mappings in {path}")
    return raw


def resolve_identity(
    registry: Sequence[Mapping[str, Any]], overrides: Iterable[IdentityRelation] = ()
) -> IdentityResolution:
    """Relate every pair of registry entries and group joining decisions.

    Raise ValueError for overrides that name unknown entries, decide one pair
    twice, or place a distinct pair in one canonical dataset.
    """
    entries = [DatasetIdentity.from_entry(item) for item in registry]
    names = [entry.dataset_id for entry in entries]
    if len(set(names)) != len(names):
        raise ValueError("Registry names must be unique")
    curated: dict[frozenset[str], IdentityRelation] = {}
    for item in overrides:
        key = frozenset((item.left, item.right))
        if len(key) != 2:
            raise ValueError(f"An override relates two different entries: {item.left}")
        if key in curated:
            raise ValueError(f"Overrides decide {sorted(key)} more than once")
        curated[key] = item
    unknown = {name for pair in curated for name in pair} - set(names)
    if unknown:
        raise ValueError(f"Overrides name unknown registry entries: {sorted(unknown)}")
    services = {entry.dataset_id for entry in entries if entry.source_role == "service"}
    if any(services.intersection(pair) for pair in curated):
        raise ValueError("Service records cannot have dataset identity overrides")
    relations: list[IdentityRelation] = []
    candidates: list[IdentityRelation] = []
    for left, right in combinations(entries, 2):
        key = frozenset((left.dataset_id, right.dataset_id))
        if key in curated:
            relations.append(curated[key])
            continue
        relation = relate(left, right)
        if relation is None:
            continue
        (candidates if relation.decided_by == "candidate" else relations).append(relation)
    parent = {name: name for name in names}

    def root(name: str) -> str:
        """Return the representative name of a canonical dataset group."""
        while parent[name] != name:
            name = parent[name]
        return name

    for item in relations:
        if item.relation in JOINING:
            first, second = sorted((root(item.left), root(item.right)))
            parent[second] = first
    for item in relations:
        if item.relation == "distinct" and root(item.left) == root(item.right):
            raise ValueError(
                f"{item.left} and {item.right} are distinct but join one dataset "
                f"through {JOINING} decisions"
            )
    groups: dict[str, list[str]] = {}
    for name in sorted(set(names) - services):
        groups.setdefault(root(name), []).append(name)
    return IdentityResolution(
        entries=entries,
        relations=relations,
        candidates=candidates,
        datasets=groups,
    )
