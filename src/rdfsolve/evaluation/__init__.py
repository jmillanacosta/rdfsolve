"""Score answers of question-answering systems over RDF.

``views`` finds the resources that the values of an answer show. ``scoring`` compares the
rows of an answer with reference rows by RDF term and by resource.
"""

from rdfsolve.evaluation.scoring import LEVELS, Score, score, score_levels
from rdfsolve.evaluation.views import View, resolve

__all__ = ["LEVELS", "Score", "View", "resolve", "score", "score_levels"]
