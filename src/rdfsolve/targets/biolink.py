"""The Biolink Model's conventions that its LinkML schema does not state as data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from rdfsolve.target_model import Model, register

__all__ = ["Biolink"]


@register("https://w3id.org/biolink/vocab/")
@dataclass
class Biolink(Model):
    """The Biolink Model: a target model with its process-to-association convention.

    The model's chemical-to-chemical derivation association states it: a molecular activity
    (or a class below it) that has input C1, has output C2 and is catalyzed by P implies C1
    derives_into C2, with P as its catalyst_qualifier. A process of no more specific class
    implies only related_to, the root predicate, from each input to each output, so nothing is
    claimed that the source does not state.
    """

    PREFIX: ClassVar[str] = "biolink"
    NAMESPACE: ClassVar[str] = "https://w3id.org/biolink/vocab/"

    def __post_init__(self) -> None:
        """Name the model's terms in its vocabulary namespace (its prefix), not the schema id."""
        self.prefix = self.PREFIX
        self.base = self.prefixes.get(self.PREFIX) or self.NAMESPACE

    def statement_classes(self) -> list[str]:
        """Return the classes that reify a statement: association and the classes below it."""
        return self.below(self.name_of("Association"))

    def process_kinds(self) -> list[str]:
        """Return the classes of processes: biological process or activity and those below it."""
        return self.below(self.name_of("BiologicalProcessOrActivity"))

    def root(self) -> str | None:
        """Return the class a node without a more specific class takes: named thing."""
        return "named thing"

    def implied(self) -> list[str]:
        """Return the CONSTRUCT of the associations that process nodes imply."""
        from rdfsolve.config import mint

        reactions = " ".join(
            f"<{self.class_iri(n)}>" for n in self.below(self.name_of("MolecularActivity"))
        )
        derivation = self.class_iri(self.name_of("ChemicalEntityToChemicalDerivationAssociation"))
        literal = '"' + (mint("association") + "/").replace('"', '\\"') + '"'
        return [
            f"""PREFIX biolink: <{self.base}>
CONSTRUCT {{
  ?association a ?kind ; biolink:subject ?input ; biolink:predicate ?predicate ;
    biolink:object ?output ; biolink:catalyst_qualifier ?catalyst .
}} WHERE {{
  ?process biolink:has_input ?input ; biolink:has_output ?output .
  OPTIONAL {{ ?catalyst biolink:catalyzes ?process }}
  BIND(EXISTS {{ VALUES ?reaction {{ {reactions} }} ?process a ?reaction }} AS ?is_reaction)
  FILTER(!?is_reaction || ?input != ?output)
  BIND(IF(?is_reaction, <{derivation}>, biolink:Association) AS ?kind)
  BIND(IF(?is_reaction, biolink:derives_into, biolink:related_to) AS ?predicate)
  BIND(IRI(CONCAT({literal},
    MD5(CONCAT(STR(?process), " ", STR(?input), " ", STR(?output))))) AS ?association)
}}"""
        ]
