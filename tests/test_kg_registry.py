"""rdfsolve.kg_registry: KG-Registry resources are matched to registry entries (by KG-Registry id,
Bioregistry prefix or name); each entry gets KG-Registry's category, domains and products as
generated metadata, a science area from the domains, and the RDF locations it lacks (its own
products only, files only); a resource with RDF files or a SPARQL endpoint that the registry
lacks becomes a new entry; curated values are never replaced."""

from rdfsolve.kg_registry import science_area, sync

KG = [
    {
        "id": "aopwiki-rdf",
        "name": "AOP-Wiki RDF",
        "category": "KnowledgeGraph",
        "domains": ["toxicology", "biomedical"],
        "activity_status": "active",
        "products": [
            {
                "id": "aopwiki-rdf.sparql",
                "category": "ProgrammingInterface",
                "format": "http",
                "product_url": "https://aopwiki.example.org/sparql",
            },
            {
                "id": "aopwiki-rdf.ttl",
                "category": "GraphProduct",
                "format": "ttl",
                "product_url": "https://example.org/AOPWikiRDF.ttl.gz",
            },
            {
                "id": "aopwiki-rdf.tree",
                "category": "GraphProduct",
                "format": "ttl",
                "product_url": "https://github.com/x/AOPWikiRDF/tree/master/data",
            },
            {
                "id": "kg-monarch.graph",
                "category": "GraphProduct",
                "format": "ntriples",
                "product_url": "https://example.org/monarch-kg.nt.gz",
            },
        ],
    },
    {
        "id": "water-kg",
        "name": "Water KG",
        "category": "KnowledgeGraph",
        "domains": ["environment", "water resources"],
        "activity_status": "active",
        "products": [
            {
                "id": "water-kg.sparql",
                "category": "ProgrammingInterface",
                "format": "http",
                "product_url": "https://water.example.org/sparql",
            },
        ],
    },
    {
        "id": "csv-only",
        "name": "CSV only",
        "category": "DataSource",
        "domains": ["biomedical"],
        "activity_status": "active",
        "products": [
            {
                "id": "csv-only.csv",
                "category": "GraphProduct",
                "format": "csv",
                "product_url": "https://example.org/a.csv",
            }
        ],
    },
]


def test_science_area_follows_the_domains():
    assert science_area(["toxicology", "biomedical"]) == "life sciences"
    assert science_area(["environment", "water resources"]) == "other"
    assert science_area(["environment", "genomics"]) == "life sciences", "Any life-science domain"


def test_entries_are_matched_completed_and_added():
    entries = [
        {"name": "aopwikirdf", "endpoint": "https://curated.example.org/sparql"},
        {"name": "unrelated", "bioregistry_prefix": "go"},
        {"name": "climate", "keywords": ["Climate"]},
        {"name": "lake", "endpoint": "https://water.example.org/sparql/"},
        {"name": "unknown"},
    ]
    synced, sidecar, report = sync(
        entries, {"sources": {}}, KG, commit="abc123", retrieved_at="2026-10-05"
    )
    by_name = {e["name"]: e for e in synced}
    aop = by_name["aopwikirdf"]
    assert aop["kg_registry_id"] == "aopwiki-rdf" and aop["science_area"] == "life sciences"
    assert aop["endpoint"] == "https://curated.example.org/sparql", "A curated value is kept"
    assert aop["download_ttl"] == ["https://example.org/AOPWikiRDF.ttl.gz"], (
        "Its own file is added; a page and another resource's product are not"
    )
    assert "water-kg" not in by_name, "Its only location is the endpoint of another entry"
    assert "csv-only" not in by_name, "A resource without RDF or SPARQL is not added"
    assert "kg_registry_id" not in by_name["unrelated"]
    assert [by_name[n]["science_area"] for n in ("unrelated", "climate", "unknown")] == [
        "life sciences",
        "other",
        "",
    ], "Without KG-Registry: the keywords or a Bioregistry prefix, else to review"
    meta = sidecar["sources"]["aopwikirdf"]
    assert meta["kg_registry_category"] == "KnowledgeGraph"
    assert meta["kg_registry_domains"] == ["toxicology", "biomedical"]
    assert {p["id"] for p in meta["kg_registry_products"]} == {
        "aopwiki-rdf.sparql",
        "aopwiki-rdf.ttl",
        "aopwiki-rdf.tree",
    }
    assert sidecar["kg_registry"] == {
        "url": "https://kghub.org/kg-registry/registry/kgs.yml",
        "commit": "abc123",
        "retrieved_at": "2026-10-05",
    }
    actions = {(r["name"], r["action"]) for r in report}
    assert ("aopwikirdf", "matched") in actions and ("water-kg", "endpoint not taken") in actions
    assert ("aopwikirdf", "kept curated endpoint") in actions
    assert ("unknown", "science area to review") in actions


def test_the_registry_text_keeps_its_comments_and_gains_the_synchronised_fields():
    import yaml

    from rdfsolve.kg_registry import patch_registry

    text = (
        "- name: aopwikirdf\n  endpoint: https://curated.example.org/sparql  # curated\n"
        "- name: unrelated\n  bioregistry_prefix: go\n"
    )
    synced, _, _ = sync(yaml.safe_load(text), {}, KG, commit="c", retrieved_at="d")
    patched = patch_registry(text, synced)
    assert "# curated" in patched and patched.startswith("- name: aopwikirdf\n  endpoint:")
    assert yaml.safe_load(patched) == synced
