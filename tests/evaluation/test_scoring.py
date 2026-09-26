"""Answers are scored by RDF term and by the resource that each cell shows."""

from conftest import E, text, uri
from rdfsolve.evaluation.scoring import maximum_matching, score, score_levels

INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
REFERENCE = [{"ke": uri(E[f"ke{n}"]), "name": text(f"Event title {n}")} for n in (1, 2)]


def test_other_views_of_the_same_resources_match_at_resource_level(select):
    answer = [{"ke": uri(E.page1), "name": text("KE 1")}, {"ke": uri(E.ke2), "name": text("KE 2")},
              {"ke": uri(E.ke2), "name": text("Event title 2")}]
    scores = score_levels(REFERENCE, answer, ["ke", "name"], select)
    assert (scores["term"].matched, scores["term"].f1) == (1, 0.4), "Only the third row is the same"
    resource = scores["resource"]
    assert (resource.expected, resource.actual, resource.matched, resource.exact) == (2, 2, 2, True)
    assert resource.substitutions == {"ke: self -> page": 1, "name: title -> label": 2}


def test_wrong_resources_other_fields_and_counts_do_not_match(select):
    swapped = [{"ke": uri(E.ke1), "name": text("KE 2")}, {"ke": uri(E.ke2), "name": text("Shared text")}]
    assert score_levels(REFERENCE, swapped, ["ke", "name"], select)["resource"].matched == 0
    count = [{"n": text("2", datatype=INTEGER)}]
    decimal = [{"n": text("2.0", datatype="http://www.w3.org/2001/XMLSchema#decimal")}]
    levels = score_levels(count, decimal, ["n"], select)
    assert (levels["term"].matched, levels["resource"].matched) == (0, 1), "Numbers match by value"
    assert score_levels(count, [{"n": text("3", datatype=INTEGER)}], ["n"], select)["resource"].f1 == 0


def test_unbound_cells_duplicate_rows_and_empty_answers():
    reference = [{"a": uri(E.x)}, {}]
    answer = [{"a": uri(E.x)}, {"a": uri(E.x)}, {}]
    result = score(reference, answer, ["a"])
    assert (result.expected, result.actual, result.matched, result.exact) == (2, 2, 2, True)
    empty = score([], [], ["a"])
    assert (empty.precision, empty.recall, empty.f1, empty.exact) == (1.0, 1.0, 1.0, True)
    assert score(reference, [], ["a"]).f1 == 0.0


def test_maximum_matching_finds_augmenting_paths():
    assert len(maximum_matching([[0, 1], [0], [1, 2]], 3)) == 3
    assert maximum_matching([[0], [0]], 1) in ({0: 0}, {1: 0})
