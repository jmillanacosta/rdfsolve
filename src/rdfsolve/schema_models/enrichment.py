"""Store observed examples and source text without changing RDF terms."""

from __future__ import annotations

import math
from typing import Any
from typing import Literal as Kind

from pydantic import BaseModel, Field, model_validator
from rdflib import RDF, RDFS, BNode, Graph, Literal, URIRef
from rdflib.term import Identifier, Node

from rdfsolve._outcomes import QueryFailure

DEFINITION_PREDICATES = (
    "http://www.w3.org/2004/02/skos/core#definition",
    "http://purl.obolibrary.org/obo/IAO_0000115",
    "http://purl.org/dc/terms/description",
    "http://purl.org/dc/terms/abstract",
    "http://www.w3.org/2000/01/rdf-schema#comment",
    "http://schema.org/description",
    "https://schema.org/description",
    "http://purl.org/dc/elements/1.1/description",
)


LABEL_PREDICATES = (
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
    "http://purl.org/dc/terms/title",
    "http://purl.org/dc/elements/1.1/title",
)


class RdfTerm(BaseModel):
    """An RDF term with its lexical form, datatype, and language."""

    kind: Kind["uri", "literal", "bnode"]
    value: str
    datatype: str | None = None
    language: str | None = None

    @model_validator(mode="after")
    def check_literal_metadata(self) -> RdfTerm:
        """Reject datatype and language metadata on non-literals."""
        if self.kind != "literal" and (self.datatype or self.language):
            raise ValueError("Only literals have a datatype or language")
        if self.language and self.datatype not in (None, str(RDF.langString)):
            raise ValueError("Language literals must use rdf:langString")
        return self

    def to_rdf(self) -> Identifier:
        """Restore an RDF term without normalizing its lexical form."""
        if self.kind == "uri":
            return URIRef(self.value)
        if self.kind == "bnode":
            return BNode(self.value)
        return Literal(
            self.value,
            lang=self.language,
            datatype=URIRef(self.datatype) if self.datatype and not self.language else None,
            normalize=False,
        )

    @classmethod
    def from_rdf(cls, term: Node) -> RdfTerm:
        """Keep the RDF node kind and literal metadata."""
        if isinstance(term, Literal):
            return cls(
                kind="literal",
                value=str(term),
                language=term.language,
                datatype=str(term.datatype) if term.datatype else None,
            )
        return cls(kind="bnode" if isinstance(term, BNode) else "uri", value=str(term))

    def json_value(self) -> Any:
        """Use JSON primitives where they preserve the observed value."""
        if self.kind != "literal":
            return ("_:" if self.kind == "bnode" else "") + self.value
        literal = self.to_rdf()
        if not isinstance(literal, Literal):
            raise TypeError("Expected an RDF literal")
        value = literal.toPython()
        if isinstance(value, float) and not math.isfinite(value):
            return self.value
        if type(value) in (int, bool, float):
            return value
        return self.value


class TermAnnotation(BaseModel):
    """A source text statement. Keep its predicate and language."""

    term_iri: str
    predicate: str
    text: RdfTerm

    @model_validator(mode="after")
    def check_text(self) -> TermAnnotation:
        """Keep only literal definition text."""
        if self.text.kind != "literal":
            raise ValueError("Definition text must be an RDF literal")
        return self


class PatternExample(BaseModel):
    """One observed triple and the class used to select its subject."""

    subject_class: str
    property_uri: str
    subject: RdfTerm
    value: RdfTerm

    @model_validator(mode="after")
    def check_subject(self) -> PatternExample:
        """Reject literal subjects, which RDF does not allow."""
        if self.subject.kind == "literal":
            raise ValueError("An example subject must be an IRI or blank node")
        return self


class SchemaEnrichment(BaseModel):
    """Bounded convenience samples; these do not estimate coverage or frequency."""

    state: Kind["complete", "partial", "failed", "skipped"] = "skipped"
    examples_per_pattern: int = Field(default=0, ge=0)
    query_count: int = Field(default=0, ge=0)
    graph_uris: list[str] | None = None
    endpoint: str | None = None
    definitions: list[TermAnnotation] = Field(default_factory=list)
    labels: list[TermAnnotation] = Field(default_factory=list)
    class_examples: dict[str, list[RdfTerm]] = Field(default_factory=dict)
    examples: list[PatternExample] = Field(default_factory=list)
    failures: list[QueryFailure] = Field(default_factory=list)

    def description(self, iri: str) -> str | None:
        """Choose an English or untagged source definition when available."""
        candidates = [d for d in self.definitions if d.term_iri == iri]
        if not candidates:
            return None
        candidates.sort(
            key=lambda d: (
                d.text.language not in ("en", None, ""),
                DEFINITION_PREDICATES.index(d.predicate)
                if d.predicate in DEFINITION_PREDICATES
                else len(DEFINITION_PREDICATES),
                d.text.value,
            )
        )
        return candidates[0].text.value

    def to_rdf_graph(self) -> Graph:
        """Copy source definitions and observed triples; do not add constraints."""
        graph = Graph()
        for definition in self.definitions + self.labels:
            graph.add(
                (
                    URIRef(definition.term_iri),
                    URIRef(definition.predicate),
                    definition.text.to_rdf(),
                )
            )
        for iri, examples in self.class_examples.items():
            for term in examples:
                graph.add((term.to_rdf(), RDF.type, URIRef(iri)))
                graph.add((URIRef(iri), RDFS.seeAlso, term.to_rdf()))
        for example in self.examples:
            graph.add((URIRef(example.subject_class), RDFS.seeAlso, example.subject.to_rdf()))
            graph.add((example.subject.to_rdf(), RDF.type, URIRef(example.subject_class)))
            graph.add(
                (example.subject.to_rdf(), URIRef(example.property_uri), example.value.to_rdf())
            )
        return graph

    @classmethod
    def from_rdf_graph(
        cls, graph: Graph, classes: list[str], properties: list[str]
    ) -> SchemaEnrichment:
        """Read annotations and linked examples, not query completion claims."""
        result = cls()
        for iri in set(classes) | set(properties):
            for predicate in DEFINITION_PREDICATES + LABEL_PREDICATES:
                for text in graph.objects(URIRef(iri), URIRef(predicate)):
                    if isinstance(text, Literal):
                        destination = (
                            result.labels if predicate in LABEL_PREDICATES else result.definitions
                        )
                        destination.append(
                            TermAnnotation(
                                term_iri=iri, predicate=predicate, text=RdfTerm.from_rdf(text)
                            )
                        )
        for iri in classes:
            for subject in graph.objects(URIRef(iri), RDFS.seeAlso):
                if (
                    not isinstance(subject, (URIRef, BNode))
                    or (subject, RDF.type, URIRef(iri)) not in graph
                ):
                    continue
                term = RdfTerm.from_rdf(subject)
                result.class_examples.setdefault(iri, []).append(term)
                for prop in properties:
                    for value in graph.objects(subject, URIRef(prop)):
                        result.examples.append(
                            PatternExample(
                                subject_class=iri,
                                property_uri=prop,
                                subject=term,
                                value=RdfTerm.from_rdf(value),
                            )
                        )
        return result
