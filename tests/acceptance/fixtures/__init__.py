"""Test fixtures for acceptance tests.

Fixtures F01-F12 as specified in the acceptance contract.
All application IRIs use https://fixture.invalid/ (abbreviated ex:).
"""

from rdflib import Graph, Namespace, Literal, URIRef, Dataset
from rdflib.namespace import RDF, RDFS, XSD, SH

EX = Namespace("https://fixture.invalid/")

# Import all fixtures for convenient access
from . import f01_unrestricted
from . import f02_ambiguous
from . import f03_conjunction
from . import f04_employment
from . import f05_shape_inverse
from . import f06_repeated_class
from . import f07_candidate_search
from . import f08_required_via
from . import f09_date_integer
from . import f10_named_graph
from . import f11_complementary
from . import f12_exact_terms

__all__ = [
    "EX",
    "f01_unrestricted",
    "f02_ambiguous",
    "f03_conjunction",
    "f04_employment",
    "f05_shape_inverse",
    "f06_repeated_class",
    "f07_candidate_search",
    "f08_required_via",
    "f09_date_integer",
    "f10_named_graph",
    "f11_complementary",
    "f12_exact_terms",
]
