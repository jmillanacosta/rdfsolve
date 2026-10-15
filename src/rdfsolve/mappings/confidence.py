"""How much a mapping is trusted, what kind of edge it makes, and how it was made.

A mapping set gives a confidence for some rows and a justification (a SEMAPV method) for each.
ConfidencePolicy turns that into one effective confidence per row: the row's own value, unless
its set's values are not informative (ignore_confidence), else the default of its set, else the
default of its justification. The defaults can be set when the policy is made; DEFAULTS holds the
values used when none are given.

The predicate of a mapping decides the kind of edge it makes in the network (edge_type); its
justification decides Biolink's knowledge level and agent type (knowledge_level, agent_type), so
that a curated equivalence and a lexical guess stay apart. Confidences of one edge from several
independent sets combine as a noisy-OR (combined).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

__all__ = [
    "DEFAULTS",
    "UNSPECIFIED",
    "ConfidencePolicy",
    "agent_type",
    "combined",
    "edge_type",
    "knowledge_level",
]

# The default confidence of a mapping by its justification (the local name of its SEMAPV term),
# used where the row and its set give none.
DEFAULTS: dict[str, float] = {
    "ManualMappingCuration": 0.95,
    "MappingReview": 0.95,
    "LogicalReasoning": 0.9,
    "CompositeMatching": 0.85,
    "MappingChaining": 0.85,
    "MappingInversion": 0.85,
    "BackgroundKnowledgeBasedMatching": 0.85,
    "LexicalMatching": 0.7,
    "LexicalSimilarityThresholdMatching": 0.7,
    "SemanticSimilarityThresholdMatching": 0.7,
}
# The default of a justification that is not in the defaults (UnspecifiedMatching among them).
UNSPECIFIED = 0.6

# The kind of edge a mapping predicate makes (by the predicate's local name).
_EDGE_TYPES = {
    "exactMatch": "EQUIVALENT",
    "equivalentClass": "EQUIVALENT",
    "equivalentProperty": "EQUIVALENT",
    "sameAs": "EQUIVALENT",
    "crossSpeciesExactMatch": "CROSS_SPECIES",
    "crossSpeciesBroadMatch": "CROSS_SPECIES",
    "crossSpeciesNarrowMatch": "CROSS_SPECIES",
    "broadMatch": "BROADER",
    "narrowMatch": "NARROWER",
    "closeMatch": "CLOSE",
    "relatedMatch": "CLOSE",
    "hasDbXref": "XREF",
    "database_cross_reference": "XREF",
}
# Predicates that state a hierarchy within a vocabulary, not a mapping between vocabularies.
_HIERARCHY = {"subClassOf", "subPropertyOf"}

# Biolink knowledge level and agent type by justification (the local name of its SEMAPV term).
_KNOWLEDGE = {
    "ManualMappingCuration": ("knowledge_assertion", "manual_agent"),
    "MappingReview": ("knowledge_assertion", "manual_agent"),
    "LogicalReasoning": ("logical_entailment", "automated_agent"),
    "MappingChaining": ("logical_entailment", "automated_agent"),
    "MappingInversion": ("logical_entailment", "automated_agent"),
    "CompositeMatching": ("logical_entailment", "automated_agent"),
    "BackgroundKnowledgeBasedMatching": ("knowledge_assertion", "automated_agent"),
    "LexicalMatching": ("prediction", "automated_agent"),
    "LexicalSimilarityThresholdMatching": ("prediction", "automated_agent"),
    "SemanticSimilarityThresholdMatching": ("prediction", "automated_agent"),
    "InstanceBasedMatching": ("observation", "data_analysis_pipeline"),
}


def _local(term: str) -> str:
    """Return the local name of a CURIE or IRI."""
    for separator in ("#", "/", ":"):
        term = term.rsplit(separator, 1)[-1]
    return term


def edge_type(predicate: str) -> str | None:
    """Return the kind of edge a mapping predicate makes; None for a hierarchy predicate (not a
    mapping); RELATED for any other predicate.
    """
    name = _local(predicate)
    if name in _HIERARCHY:
        return None
    return _EDGE_TYPES.get(name, "RELATED")


def knowledge_level(justification: str) -> str:
    """Return the Biolink knowledge level of a mapping made by this justification."""
    return _KNOWLEDGE.get(_local(justification), ("not_provided", "not_provided"))[0]


def agent_type(justification: str) -> str:
    """Return the Biolink agent type of a mapping made by this justification."""
    return _KNOWLEDGE.get(_local(justification), ("not_provided", "not_provided"))[1]


def combined(confidences: Iterable[float]) -> float:
    """Return the confidence of an edge that independent sets state with these confidences
    (noisy-OR: the edge is wrong only when every set is wrong).
    """
    wrong = 1.0
    for value in confidences:
        wrong *= 1.0 - value
    return 1.0 - wrong


@dataclass(frozen=True)
class ConfidencePolicy:
    """The confidence a mapping is given where its row and its set give none.

    *by_justification* maps a justification (the local name of its SEMAPV term) to its default;
    *unspecified* is the default of any other justification.
    """

    by_justification: Mapping[str, float] = field(default_factory=lambda: dict(DEFAULTS))
    unspecified: float = UNSPECIFIED

    def __post_init__(self) -> None:
        """Refuse a confidence outside 0..1."""
        for value in [*self.by_justification.values(), self.unspecified]:
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"A confidence is between 0 and 1, not {value}")

    def default(self, justification: str) -> float:
        """Return the default confidence of a mapping made by this justification."""
        return self.by_justification.get(_local(justification), self.unspecified)

    def of(
        self,
        row_confidence: float | None,
        justification: str,
        set_confidence: float | None = None,
        ignore_confidence: bool = False,
    ) -> float:
        """Return the effective confidence of one mapping row.

        The row's own value counts unless its set's values are not informative
        (*ignore_confidence*); without one, the set's default (*set_confidence*, for the rows
        whose justification is unspecified) and then the justification's default apply.
        """
        if row_confidence is not None and not ignore_confidence:
            return float(row_confidence)
        if set_confidence is not None and _local(justification) not in self.by_justification:
            return float(set_confidence)
        return self.default(justification)
