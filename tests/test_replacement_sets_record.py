"""The identifier replacement sets (pysec2pri) used by link verification are recorded with their
checksum, mapping set identifier and version, and a citation that stays empty until the sets are
deposited with a persistent identifier (placeholder, the owner, 2026-09-30)."""

import hashlib

from rdfsolve.mappings.signatures import describe_replacement_sets

SSSOM = """#curie_map:
#  IAO: http://purl.obolibrary.org/obo/IAO_
#  HGNC: https://identifiers.org/hgnc:
#mapping_set_id: https://example.org/pysec2pri/hgnc.sssom.tsv
#mapping_set_version: "2026-09-01"
#license: https://creativecommons.org/publicdomain/zero/1.0/
subject_id\tpredicate_id\tobject_id\tmapping_justification
HGNC:1\tIAO:0100001\tHGNC:2\tsemapv:ManualMappingCuration
"""


def test_replacement_sets_are_described(tmp_path):
    path = tmp_path / "hgnc.sssom.tsv"
    path.write_text(SSSOM)
    (entry,) = describe_replacement_sets([path])
    assert entry == {
        "file": "hgnc.sssom.tsv",
        "sha256": hashlib.sha256(SSSOM.encode()).hexdigest(),
        "mapping_set_id": "https://example.org/pysec2pri/hgnc.sssom.tsv",
        "mapping_set_version": "2026-09-01",
        "citation": None,
    }
