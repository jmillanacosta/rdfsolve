"""rdfsolve.mining.scan_terms: ontology terms used as types, grouped after counting by rewriting
the type table of a row store and counting again (exact counts), minimal types, and the folding
of ``p some F`` class expressions into edge rows."""

from rdflib import BNode, Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF, RDFS

from rdfsolve.mining import mine_with_ontology
from rdfsolve.mining.miner import SchemaMiner
from rdfsolve.mining.scan import count_patterns, name_class_expressions, store_from_graph
from rdfsolve.mining.scan_terms import (
    RetypedStore,
    count_aware_cut,
    fold_class_expressions,
    foldable_expressions,
    group_terms,
    minimal_types,
    type_table,
    without_classes,
)
from tests.mining.data import EX, FIXTURE, T

E = Namespace(EX)
N = Namespace(T)


def _rows(patterns):
    return {
        (p.subject_class, p.property_uri, p.object_class, p.datatype): (
            p.count,
            p.distinct_subjects,
            p.distinct_objects,
        )
        for p in patterns
    }


def _mined(graph, **options):
    with SchemaMiner.from_graph(graph, delay=0) as miner:
        result = mine_with_ontology(miner, dataset_name="fixture", ontology_as_data=True, **options)
        return result.data_schema, miner.last_report.config


def test_the_representatives_are_the_miners_and_the_counts_exact(tmp_path):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    schema, config = _mined(graph, ontology_term_budget=6)
    store = store_from_graph(graph, tmp_path / "store")
    grouping = group_terms(store, budget=6)
    mined = config["ontology_term_subsumption"]
    assert grouping.summary["representative_members"] == mined["representative_members"]
    assert grouping.members[T + "alcohol"] == [T + "ethanol", T + "methanol"]
    ours = _rows(grouping.patterns)
    for pattern in schema.patterns:
        key = (pattern.subject_class, pattern.property_uri, pattern.object_class, pattern.datatype)
        if pattern.count_semantics != "upper_bound" and key in ours:
            assert ours[key][0] == pattern.count, key
    row = ours[(T + "alcohol", EX + "mass", "Literal", "http://www.w3.org/2001/XMLSchema#decimal")]
    assert row == (2, 2, 2), "The merged row has exact distinct counts"
    assert all(p.count_semantics != "upper_bound" for p in grouping.patterns)
    assert _rows(grouping.raw_patterns) == _rows(count_patterns(store)), "The per-term layer"


def test_grouping_before_mining_is_the_miners(tmp_path):
    graph = Graph().parse(data=FIXTURE, format="turtle")
    _, config = _mined(graph, ontology_term_budget=6, ontology_group_before_mining=1)
    store = store_from_graph(graph, tmp_path / "store")
    grouping = group_terms(store, budget=6, group_before_mining=1)
    mined = config["ontology_term_grouping"]
    assert grouping.before_mining is not None
    assert grouping.before_mining["representative_members"] == mined["representative_members"]
    assert grouping.before_mining["classes_after"] == mined["classes_after"]


def _double_typed() -> Graph:
    """x is typed with two members of one representative (TERA's taxon term and division)."""
    graph = Graph()
    for leaf in ("a", "b", "c", "d"):
        graph.add((N[leaf], RDFS.subClassOf, N.root))
        graph.add((N[leaf], RDF.type, OWL.Class))
    graph.add((E.x, RDF.type, N.a))
    graph.add((E.x, RDF.type, N.b))
    graph.add((E.x, E.name, Literal("x")))
    graph.add((E.y, RDF.type, N.c))
    graph.add((E.y, E.name, Literal("y")))
    graph.add((E.z, RDF.type, N.d))
    graph.add((E.z, E.link, E.x))
    return graph


def test_a_record_typed_with_two_members_is_counted_once(tmp_path):
    store = store_from_graph(_double_typed(), tmp_path / "store")
    grouping = group_terms(store, budget=2)
    assert set(grouping.members[str(N.root)]) >= {str(N.a), str(N.b), str(N.c), str(N.d)}
    rows = _rows(grouping.patterns)
    string = "http://www.w3.org/2001/XMLSchema#string"
    assert rows[(str(N.root), str(E.name), "Literal", string)] == (2, 2, 2)
    assert rows[(str(N.root), str(E.link), str(N.root), None)] == (1, 1, 1), "Objects too"
    summed = sum(
        p.count
        for p in grouping.raw_patterns
        if p.property_uri == str(E.name) and p.subject_class in (str(N.a), str(N.b), str(N.c))
    )
    assert summed == 3, "Summing the members counts x twice"


def test_minimal_types_drop_the_ancestors_a_record_also_has(tmp_path):
    graph = _double_typed()
    graph.add((E.x, RDF.type, N.root))
    store = store_from_graph(graph, tmp_path / "store")
    table = minimal_types(store).collect()
    x = sorted(table.filter(table["s"] == f"<{E.x}>")["c"])
    assert x == [f"<{N.a}>", f"<{N.b}>"]
    assert table.height == store.types().collect().height - 1
    retyped = count_patterns(RetypedStore(store, minimal_types(store)))
    assert not [p for p in retyped if p.subject_class == str(N.root)]


def test_count_aware_cut_meets_the_budget_and_keeps_the_frequent_term():
    parents = {"a": {"r"}, "b": {"r"}, "c": {"r"}, "r": set()}
    chosen = count_aware_cut(["a", "b", "c"], parents, 2, {"a": 100, "b": 1, "c": 1})
    assert chosen.classes_after == 2
    assert chosen.representative["a"] == "a" and chosen.representative["b"] == "r"


def _expressions() -> Graph:
    graph = Graph()
    for n, organ in enumerate(("Heart", "Lung", "Heart")):
        restriction = BNode()
        graph.add((restriction, RDF.type, OWL.Restriction))
        graph.add((restriction, OWL.onProperty, E.partOf))
        graph.add((restriction, OWL.someValuesFrom, E[organ]))
        graph.add((E[f"v{n}"], RDF.type, restriction))
        graph.add((E[f"v{n}"], RDF.type, E.Vessel))
        graph.add((E[f"v{n}"], E.size, Literal(n)))
    graph.add((E.Heart, RDFS.subClassOf, E.Organ))
    graph.add((E.Lung, RDFS.subClassOf, E.Organ))
    return graph


def test_class_expressions_fold_into_edges_with_grouped_fillers(tmp_path):
    store = store_from_graph(_expressions(), tmp_path / "store")
    name_class_expressions(store)
    expressions = foldable_expressions(store)
    assert sorted(expressions.values()) == [
        (str(E.partOf), str(E.Heart)),
        (str(E.partOf), str(E.Lung)),
    ]
    types = without_classes(type_table(store), expressions)
    grouping = group_terms(store, types=types, budget=300)
    assert not [p for p in grouping.patterns if p.subject_class in expressions]
    per_term = fold_class_expressions(store, expressions, types=grouping.types)
    assert _rows(per_term.patterns) == {
        (str(E.Vessel), str(E.partOf), str(E.Heart), None): (2, 2, 1),
        (str(E.Vessel), str(E.partOf), str(E.Lung), None): (1, 1, 1),
    }
    assert all(p.evidence_source == "inferred" for p in per_term.patterns)
    grouped = fold_class_expressions(store, expressions, types=grouping.types, slot_budget=1)
    assert _rows(grouped.patterns) == {
        (str(E.Vessel), str(E.partOf), str(E.Organ), None): (3, 3, 2)
    }
