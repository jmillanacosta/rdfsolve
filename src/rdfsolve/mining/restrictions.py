"""Mine the relations that an ontology states between its terms with OWL restrictions.

A class pattern shows only that classes are subclasses of restrictions, because a restriction
is a blank node. Here each form is read with one grouped query that joins through the blank
node and does not return it: a class that is a subclass of a restriction, and a class that is
equivalent to an intersection with a restriction. Terms are grouped by the namespace of their
IRI (the IRI without its last identifier part). A label in Manchester syntax is made for each
pattern, with the labels that the source gives to the properties.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from rdfsolve.mining.query_builders import _graph_scope
from rdfsolve.schema_models.restrictions import (
    CLASS_EXPRESSION,
    RestrictionPattern,
    RestrictionPatterns,
)

logger = logging.getLogger(__name__)

OWL = "http://www.w3.org/2002/07/owl#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
FORMS = {"some": OWL + "someValuesFrom", "only": OWL + "allValuesFrom", "value": OWL + "hasValue"}
# How a class reaches its restriction: directly, or through an intersection.
AXIOMS = {
    "SubClassOf": f"?c <{RDFS}subClassOf> ?r .",
    "EquivalentTo": f"?c <{OWL}equivalentClass>/<{OWL}intersectionOf>/<{RDF}rest>*/<{RDF}first> ?r .",
}
# The namespace of a term: its IRI without the last identifier part.
NAMESPACE = r"[^/#_:]*$"


def _query(axiom: str, form: str, dataset: str) -> str:
    """Return the grouped query of one axiom and one restriction form."""
    return f"""SELECT ?p ?sns ?fns (COUNT(*) AS ?n) (COUNT(DISTINCT ?c) AS ?classes)
       (SAMPLE(?c) AS ?c1) (SAMPLE(?v) AS ?v1) {dataset}
WHERE {{
  {AXIOMS[axiom]}
  ?r <{OWL}onProperty> ?p ; <{FORMS[form]}> ?v .
  FILTER(isIRI(?c) && isIRI(?p))
  BIND(REPLACE(STR(?c), "{NAMESPACE}", "") AS ?sns)
  BIND(IF(isIRI(?v), REPLACE(STR(?v), "{NAMESPACE}", ""), "{CLASS_EXPRESSION}") AS ?fns)
}}
GROUP BY ?p ?sns ?fns"""


def _name(namespace: str) -> str:
    """Return a short name of a namespace for a label: its last part without separators."""
    if namespace == CLASS_EXPRESSION:
        return namespace
    parts = [part for part in re.split(r"[/#]", namespace.rstrip("/#_:")) if part]
    return parts[-1] if parts else namespace


def manchester(pattern_fields: dict[str, Any], property_label: str | None) -> str:
    """Return a restriction pattern in Manchester syntax.

    The property has the label of the source when there is one, else the last part of its IRI.
    """
    prop = pattern_fields["property_uri"]
    name = f"'{property_label}'" if property_label else _name(prop + "/")
    part = f"{name} {pattern_fields['form']} {_name(pattern_fields['filler_namespace'])}"
    subject = _name(pattern_fields["subject_namespace"])
    if pattern_fields["axiom"] == "EquivalentTo":
        return f"{subject} EquivalentTo (… and {part})"
    return f"{subject} SubClassOf {part}"


def _labels(helper: Any, properties: list[str], dataset: str) -> dict[str, str]:
    """Return one rdfs:label of each property, the English or untagged one first."""
    labels: dict[str, str] = {}
    for start in range(0, len(properties), 200):
        values = " ".join(f"<{p}>" for p in properties[start : start + 200])
        query = (
            f"SELECT ?p ?l {dataset} WHERE {{ VALUES ?p {{ {values} }} ?p <{RDFS}label> ?l . "
            'FILTER(LANG(?l) = "" || LANGMATCHES(LANG(?l), "en")) }'
        )
        answer = helper.select(query, purpose="restrictions/labels")
        for row in answer.get("results", {}).get("bindings", []):
            found = row["l"]["value"]
            kept = labels.get(row["p"]["value"])
            if kept is None or found < kept:
                labels[row["p"]["value"]] = found
    return labels


def mine_restriction_patterns(
    helper: Any, *, graph_uris: list[str] | None = None
) -> RestrictionPatterns:
    """Mine the restriction patterns of the data that *helper* reads.

    A form whose query fails is recorded and the state is partial; when every query fails the
    state is failed. A dataset without restrictions gives no pattern and the state complete.
    """
    dataset, _, _ = _graph_scope(graph_uris)
    result = RestrictionPatterns()
    rows: list[dict[str, Any]] = []
    for axiom in AXIOMS:
        for form in FORMS:
            result.query_count += 1
            try:
                answer = helper.select(_query(axiom, form, dataset), purpose="restrictions")
            except Exception as error:
                logger.warning("Restrictions: %s %s not read: %s", axiom, form, str(error)[:200])
                result.failures.append(f"{axiom} {form}: {str(error)[:300]}")
                continue
            for row in answer.get("results", {}).get("bindings", []):
                if "p" not in row:
                    continue  # an empty group
                rows.append(
                    {
                        "subject_namespace": row["sns"]["value"],
                        "axiom": axiom,
                        "property_uri": row["p"]["value"],
                        "form": form,
                        "filler_namespace": row["fns"]["value"],
                        "count": int(row["n"]["value"]),
                        "classes": int(row["classes"]["value"]),
                        "example_subject": row.get("c1", {}).get("value"),
                        "example_filler": row["v1"]["value"]
                        if row.get("v1", {}).get("type") == "uri"
                        else None,
                    }
                )
    labels: dict[str, str] = {}
    if rows:
        result.query_count += 1
        try:
            labels = _labels(helper, sorted({row["property_uri"] for row in rows}), dataset)
        except Exception as error:
            result.failures.append(f"labels: {str(error)[:300]}")
    result.patterns = sorted(
        (
            RestrictionPattern(**row, label=manchester(row, labels.get(row["property_uri"])))
            for row in rows
        ),
        key=lambda p: (-p.count, p.label),
    )
    failed_forms = sum(not failure.startswith("labels") for failure in result.failures)
    if failed_forms == len(AXIOMS) * len(FORMS):
        result.state = "failed"
    elif result.failures:
        result.state = "partial"
    return result
