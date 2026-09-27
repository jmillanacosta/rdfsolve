"""Identifiers are resolved with an SSSOM mapping set (secondary to primary) before a link is verified."""

from rdflib import Dataset

from rdfsolve.api import Client
from rdfsolve.mappings.signatures import Link, read_replacements, verify
from tests.test_link_signatures import GENES, PROTEINS, UP

# The form of pysec2pri: the subject is the secondary identifier, the object the primary one.
MAPPING_SET = """# curie_map:
#   IAO: http://purl.obolibrary.org/obo/IAO_
#   UniProtKB: http://purl.uniprot.org/uniprot/
#   semapv: https://w3id.org/semapv/vocab/
#   sssom: https://w3id.org/sssom/
# license: https://creativecommons.org/publicdomain/zero/1.0/
# mapping_set_id: https://example.org/sec2pri
subject_id	predicate_id	object_id	mapping_justification
UniProtKB:P99999	IAO:0100001	UniProtKB:P04637	semapv:BackgroundKnowledgeBasedMatching
UniProtKB:P00001	IAO:0100001	sssom:NoTermFound	semapv:BackgroundKnowledgeBasedMatching
UniProtKB:P00002	IAO:0100001	UniProtKB:P11111	semapv:BackgroundKnowledgeBasedMatching
UniProtKB:P00002	IAO:0100001	UniProtKB:P22222	semapv:BackgroundKnowledgeBasedMatching
"""


def test_replacements_keep_one_to_one_rows_with_bioregistry_prefixes(tmp_path):
    path = tmp_path / "sec2pri.sssom.tsv"
    path.write_text(MAPPING_SET)
    assert read_replacements(path) == {"uniprot:P99999": "uniprot:P04637"}, (
        "A withdrawn identifier and a split have no single replacement"
    )


def test_a_secondary_identifier_is_found_through_its_primary_identifier():
    genes = Dataset().parse(
        format="turtle",
        data="""
        <urn:gene/1> a <urn:Gene> ; <urn:xref> <https://identifiers.org/uniprot:P04637> .
        <urn:gene/2> a <urn:Gene> ; <urn:xref> <https://identifiers.org/uniprot:P99999> .""",
    )
    proteins = Dataset().parse(format="turtle", data=f"<{UP}P04637> a <urn:Protein> .")
    link = Link("join", "genes", "urn:Gene", "urn:xref", "uniprot", "proteins", "urn:Protein")
    with Client(GENES, genes) as source, Client(PROTEINS, proteins) as target:
        plain = verify(link, source, target)
        resolved = verify(link, source, target, replacements={"uniprot:P99999": "uniprot:P04637"})
    assert (plain.found, plain.replaced) == (1, 0)
    assert (resolved.found, resolved.replaced) == (2, 1), "The replaced identifier is counted"
    assert resolved.target_forms == {UP + "{id}": 2}
    assert ("https://identifiers.org/uniprot:P99999", UP + "P04637") in resolved.examples
