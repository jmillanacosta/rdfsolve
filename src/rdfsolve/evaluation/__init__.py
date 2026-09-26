"""Score answers of question-answering systems over RDF, and compare and plan evaluations.

``views`` finds the resources that the values of an answer show. ``scoring`` compares the
rows of an answer with reference rows by RDF term and by resource. ``statistics`` gives
question-level estimates, paired tests and simulated power.
"""

from rdfsolve.evaluation.scoring import LEVELS, Score, score, score_levels
from rdfsolve.evaluation.statistics import (
    Components,
    bootstrap,
    compare,
    fit_components,
    power_table,
    simulate_power,
)
from rdfsolve.evaluation.views import View, resolve

__all__ = [
    "LEVELS",
    "Components",
    "Score",
    "View",
    "bootstrap",
    "compare",
    "fit_components",
    "power_table",
    "resolve",
    "score",
    "score_levels",
    "simulate_power",
]
