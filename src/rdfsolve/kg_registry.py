"""Synchronise the source registry with KG-Registry (https://kghub.org/kg-registry).

KG-Registry describes knowledge graphs, data sources and ontologies with a category, domains and
products (files, endpoints, pages). Each registry entry is matched to a KG-Registry resource by
its KG-Registry id, its name, or (for an entry that is not a provider's copy) its Bioregistry
prefix. A matched entry gets:

- in the metadata sidecar (generated observations): KG-Registry's category, domains and the
  resource's own products;
- in the registry (curated access): its KG-Registry id, a science area from the domains, and the
  RDF locations it lacks, from the resource's own products: a SPARQL endpoint, and RDF files
  (direct file URLs, not pages). A curated endpoint or download is never replaced; a differing
  one is reported.

A KG-Registry resource with RDF files or a SPARQL endpoint that matches no entry is added. A
product belongs to the resource whose id prefixes the product id (KG-Registry lists a product
under each source it draws from). The sidecar records the KG-Registry commit read.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from rdfsolve.qlever.utils import _file_name

__all__ = [
    "LIFE_KEYWORDS",
    "LIFE_SCIENCES",
    "keyword_area",
    "patch_registry",
    "science_area",
    "sync",
]

URL = "https://kghub.org/kg-registry/registry/kgs.yml"

# KG-Registry domains that make a resource a life-science one; any of them suffices. The other
# domains (environment, information technology, general, literature, public health, agriculture,
# geographic information systems, ...) are "other".
LIFE_SCIENCES = frozenset(
    {
        "anatomy and development",
        "biodiversity",
        "biological systems",
        "biomedical",
        "cancer",
        "cell biology",
        "chemistry and biochemistry",
        "clinical",
        "clinical coding",
        "drug discovery",
        "drug repositioning",
        "electronic health records",
        "gene expression profiling",
        "gene regulation",
        "genetic variation",
        "genome-wide association studies",
        "genomics",
        "high-throughput screening",
        "host-pathogen interactions",
        "immunology",
        "infectious disease",
        "insects",
        "metabolism",
        "metabolomics",
        "microbiology",
        "microbiome",
        "model organisms",
        "molecular evolution",
        "neuroscience",
        "non-coding RNA",
        "nutrition",
        "organisms",
        "pathways",
        "pharmacology",
        "pharmacovigilance",
        "phenotype",
        "plants",
        "post-translational modification",
        "precision medicine",
        "protein domains",
        "protein interactions",
        "protein structure",
        "proteomics",
        "rare disease",
        "single-cell analysis",
        "systems biology",
        "taxonomy",
        "toxicology",
    }
)
# Registry keywords (from RDF Portal, Bioregistry and earlier curation) that decide the science
# area of an entry KG-Registry does not list; an entry with a Bioregistry prefix is a life-science
# one (Bioregistry registers life-science identifier spaces). An entry with neither is reviewed.
LIFE_KEYWORDS = frozenset(
    {
        "3d structure",
        "3d structure databases",
        "ageing",
        "archaea",
        "bacteria",
        "bioactivities",
        "biocatalysis",
        "biochemistry",
        "bioinformatics",
        "biological regulation",
        "biology",
        "biomedical science",
        "biopax",
        "bioresource",
        "botany",
        "carbohydrate",
        "cdna",
        "cell",
        "cell line",
        "cell lines",
        "chemical",
        "chemical compound",
        "chemical structure",
        "chemistry",
        "chemistry databases",
        "co-expression",
        "compound",
        "disease",
        "disease search",
        "dna",
        "drug",
        "drug interaction",
        "drug/chemical",
        "enzyme",
        "enzyme and pathway databases",
        "enzyme kinetics",
        "epidemiology",
        "evolutionary biology",
        "expression",
        "family and domain databases",
        "fungal biology",
        "gene",
        "gene expression",
        "genetic disease",
        "genetic variation",
        "genetic variation databases",
        "genome",
        "genome/gene",
        "genomics",
        "glycomics",
        "health",
        "health/disease",
        "human",
        "human phenotype ontology",
        "immunology",
        "interaction/pathway",
        "kinase",
        "life science",
        "lipid",
        "lipidomics",
        "mass spectrometry",
        "metabolite",
        "metabolomics",
        "metagenome",
        "microbe",
        "microbes",
        "microbiology",
        "molecular entity",
        "molecular interaction",
        "molecules",
        "mouse",
        "natural product",
        "nutritional science",
        "omics",
        "organic chemistry",
        "organism",
        "orthology",
        "other biomolecule",
        "other dna",
        "pathogen surveillance",
        "pathway",
        "pharmacology",
        "phenotype",
        "phylogenetics",
        "phylogeny/classification",
        "plant biology",
        "polymorphism",
        "protein",
        "protein interactions",
        "proteomics",
        "psi-mi",
        "rare disease",
        "reaction",
        "reaction data",
        "regulation",
        "rna",
        "sbml",
        "sequence",
        "signaling",
        "small molecule",
        "structure",
        "systems biology",
        "tag sequence (nucleic acid)",
        "taxonomic classification",
        "taxonomy",
        "toxicology",
        "variant",
        "variation",
        "virology",
        "virus",
    }
)
OTHER_KEYWORDS = frozenset(
    {
        "agricultural products",
        "agriculture",
        "agronomy",
        "aquaculture",
        "climate",
        "earth sciences",
        "environmental science",
        "fisheries science",
        "food security",
        "forest management",
        "water management",
    }
)
# KG-Registry product formats that are RDF, and the registry field of each.
DOWNLOAD_FIELDS = {
    "ttl": "download_ttl",
    "ntriples": "download_nt",
    "nt": "download_nt",
    "nquads": "download_nq",
    "nq": "download_nq",
    "trig": "download_trig",
    "rdfxml": "download_rdf",
    "rdf": "download_rdf",
    "jsonld": "download_jsonld",
}
ACCESS = ("endpoint", *sorted(set(DOWNLOAD_FIELDS.values())))


def science_area(domains: Iterable[str]) -> str:
    """Return ``life sciences`` when any domain is a life-science one, else ``other``."""
    return "life sciences" if set(domains) & LIFE_SCIENCES else "other"


def keyword_area(entry: Mapping[str, Any]) -> str:
    """Return the science area of an entry from its keywords and Bioregistry prefix, or ""."""
    words = {str(k).lower() for k in entry.get("keywords") or []}
    if words & LIFE_KEYWORDS or entry.get("bioregistry_prefix"):
        return "life sciences"
    return "other" if words & OTHER_KEYWORDS else ""


def _key(text: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(text).lower())


def _own(resource: Mapping[str, Any], ids: set[str]) -> list[dict[str, Any]]:
    """Return the products the resource itself produces (its id prefixes theirs)."""
    found = []
    for product in resource.get("products") or []:
        pid = str(product.get("id", ""))
        producer = max((i for i in ids if pid.startswith(i + ".")), key=len, default="")
        if producer == resource["id"]:
            found.append(product)
    return found


def _locations(products: Sequence[Mapping[str, Any]]) -> dict[str, list[str]]:
    """Return the SPARQL endpoint and the RDF files of these products, by registry field."""
    found: dict[str, list[str]] = {}
    for product in products:
        url = str(product.get("product_url") or "")
        if not url.startswith(("http://", "https://")):
            continue
        if str(product.get("id", "")).endswith(".sparql") or (
            product.get("category") == "ProgrammingInterface" and "sparql" in url.lower()
        ):
            found.setdefault("endpoint", []).append(url)
        field = DOWNLOAD_FIELDS.get(str(product.get("format", "")).lower())
        if field and product.get("category") in ("GraphProduct", "Product") and _file_name(url):
            found.setdefault(field, []).append(url)
    return found


def _match(
    entries: Sequence[Mapping[str, Any]], resources: Sequence[Mapping[str, Any]]
) -> dict[str, str]:
    """Return the KG-Registry id of each matched entry name."""
    by_key: dict[str, str] = {}
    for resource in resources:
        for name in (resource["id"], resource.get("name"), *(resource.get("synonyms") or [])):
            by_key.setdefault(_key(name), resource["id"])
    ids = {r["id"] for r in resources}
    matched = {}
    for entry in entries:
        name = str(entry["name"])
        if entry.get("kg_registry_id") in ids:
            matched[name] = str(entry["kg_registry_id"])
        elif _key(name) in by_key:
            matched[name] = by_key[_key(name)]
        elif "." not in name and _key(entry.get("bioregistry_prefix", "")) in by_key:
            matched[name] = by_key[_key(entry["bioregistry_prefix"])]
    return matched


def sync(
    entries: Sequence[Mapping[str, Any]],
    sidecar: Mapping[str, Any],
    resources: Sequence[Mapping[str, Any]],
    *,
    commit: str,
    retrieved_at: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, str]]]:
    """Return the registry entries, the sidecar and a review report after the synchronisation.

    The report has one row for each change or difference: matched, added, kept curated FIELD.
    """
    ids = {r["id"] for r in resources}
    by_id = {r["id"]: r for r in resources}
    matched = _match(entries, resources)
    records = {name: dict(record) for name, record in (sidecar.get("sources") or {}).items()}
    report: list[dict[str, str]] = []
    result: list[dict[str, Any]] = []

    def observe(name: str, resource: Mapping[str, Any], own: list[dict[str, Any]]) -> None:
        """Record KG-Registry's description of the resource in the sidecar."""
        records.setdefault(name, {}).update(
            kg_registry_category=str(resource.get("category", "")),
            kg_registry_domains=list(resource.get("domains") or []),
            kg_registry_products=[
                {k: str(p[k]) for k in ("id", "category", "format", "product_url") if p.get(k)}
                for p in own
            ],
        )

    for entry in entries:
        entry = dict(entry)
        kg_id = matched.get(str(entry["name"]))
        if kg_id is None:
            area = entry.get("science_area") or keyword_area(entry)
            entry["science_area"] = area
            action = "science area from keywords" if area else "science area to review"
            report.append(
                {"name": entry["name"], "kg_registry_id": "", "action": action, "detail": area}
            )
            result.append(entry)
            continue
        resource = by_id[kg_id]
        own = _own(resource, ids)
        observe(str(entry["name"]), resource, own)
        entry["kg_registry_id"] = kg_id
        entry["science_area"] = science_area(resource.get("domains") or [])
        report.append(
            {"name": entry["name"], "kg_registry_id": kg_id, "action": "matched", "detail": ""}
        )
        for field, urls in _locations(own).items():
            value = urls[0] if field == "endpoint" else urls
            if not entry.get(field):
                entry[field] = value
                report.append(
                    {
                        "name": entry["name"],
                        "kg_registry_id": kg_id,
                        "action": f"added {field}",
                        "detail": " ".join(urls),
                    }
                )
            elif entry[field] != value:
                report.append(
                    {
                        "name": entry["name"],
                        "kg_registry_id": kg_id,
                        "action": f"kept curated {field}",
                        "detail": " ".join(urls),
                    }
                )
        result.append(entry)

    taken = set(matched.values())
    names = {str(e["name"]) for e in result}
    endpoints = {
        str(e["endpoint"]).rstrip("/"): str(e["name"]) for e in result if e.get("endpoint")
    }
    for resource in resources:
        if resource["id"] in taken or resource.get("activity_status") not in (None, "active"):
            continue
        own = _own(resource, ids)
        locations = _locations(own)
        if not locations:
            pages = [
                str(p.get("product_url"))
                for p in own
                if str(p.get("format", "")).lower() in DOWNLOAD_FIELDS and p.get("product_url")
            ]
            if pages:
                report.append(
                    {
                        "name": resource["id"],
                        "kg_registry_id": resource["id"],
                        "action": "not added: RDF only as pages",
                        "detail": " ".join(pages),
                    }
                )
            continue
        name = resource["id"].lower()
        if name in names:
            continue
        used = endpoints.get(locations.get("endpoint", [""])[0].rstrip("/"))
        if used:
            report.append(
                {
                    "name": name,
                    "kg_registry_id": resource["id"],
                    "action": "endpoint not taken",
                    "detail": f"{locations.pop('endpoint')[0]} is the endpoint of {used}",
                }
            )
            if not locations:
                continue
        entry = {
            "name": name,
            "dataset_kind": "ontology" if resource.get("category") == "Ontology" else "instance",
            **({"source_role": "service"} if resource.get("category") == "Aggregator" else {}),
            "kg_registry_id": resource["id"],
            "science_area": science_area(resource.get("domains") or []),
            "notes": f"KG Registry: {resource.get('name', resource['id'])}",
            **{f: (u[0] if f == "endpoint" else u) for f, u in locations.items()},
        }
        observe(name, resource, own)
        result.append(entry)
        report.append(
            {
                "name": name,
                "kg_registry_id": resource["id"],
                "action": "added",
                "detail": ", ".join(sorted(locations)),
            }
        )

    document = dict(sidecar)
    document["kg_registry"] = {"url": URL, "commit": commit, "retrieved_at": retrieved_at}
    document["sources"] = records
    return result, document, report


def patch_registry(text: str, entries: Sequence[Mapping[str, Any]]) -> str:
    """Return the registry *text* with the synchronised *entries*, keeping every existing line.

    The fields an entry gains are written at the end of its block, and new entries at the end,
    so that the comments and the order of the curated registry stay as they are.
    """
    import yaml

    old = {str(e["name"]): e for e in yaml.safe_load(text) or []}
    lines = text.splitlines(keepends=True)
    starts = [i for i, line in enumerate(lines) if line.startswith("- name: ")]
    ends = dict(zip(starts, [*starts[1:], len(lines)], strict=True))
    inserts: dict[int, str] = {}
    for start in starts:
        name = str(yaml.safe_load(lines[start][2:])["name"])
        entry = next((e for e in entries if str(e["name"]) == name), None)
        if entry is None:
            continue
        added = {k: v for k, v in entry.items() if k not in old.get(name, {})}
        if added:
            block = yaml.safe_dump(added, sort_keys=False, allow_unicode=True, width=1000)
            end = ends[start]
            while end > start + 1 and not lines[end - 1].strip():
                end -= 1
            inserts[end] = "".join(f"  {line}\n" for line in block.splitlines())
    for at in sorted(inserts, reverse=True):
        lines.insert(at, inserts[at])
    new = [e for e in entries if str(e["name"]) not in old]
    body = "".join(lines)
    if new:
        body = (
            body.rstrip("\n")
            + "\n"
            + yaml.safe_dump(
                [dict(e) for e in new], sort_keys=False, allow_unicode=True, width=1000
            )
        )
    return body
