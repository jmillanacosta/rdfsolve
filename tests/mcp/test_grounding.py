from conftest import E, declare, insert, prepare, values
from pydantic import BaseModel


def test_typed_entity_and_package_paths_anchor_the_answer(session):
    bare = False
    found = session.find("Human", str(E.Taxon))
    ref = found["items"][0]["ref"]
    assert isinstance(session.records[ref], BaseModel)
    assert session.records[ref].uri == str(E.Human)
    before = len(session.client.queries)
    assert session.find("Human", str(E.Taxon)) == found
    assert len(session.client.queries) == before
    path = session.paths(str(E.AOP), ref, max_hops=1)["items"][0]
    declare(session, "Pathways applicable to Human", concept="Adverse Outcome Pathway")
    rows = values(
        session,
        prepare(
            session,
            {
                "g1": {
                    "pattern": f"?a {path['ref']} ?tax"
                    if bare
                    else insert(path["ref"], "a", "tax"),
                    "project": ["a"],
                }
            },
        ),
    )
    assert {r["a"]["value"] for r in rows} == {str(E.humanAOP), str(E.noChemicalAOP)}
