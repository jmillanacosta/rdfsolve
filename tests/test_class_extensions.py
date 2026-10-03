"""The member sets of the schema's classes are compared exactly in the data: classes with the same
members form one group, and a class all of whose members are members of another is recorded
with its nearest such classes. Bgee: 26 classes in 15 groups (orth:Gene, orth:SequenceUnit,
orth:GeneTreeNode, SO_0000704 and CDAO_0000140: 1,559,014 members each); the anatomical group
is within BFO_0000004, within BFO_0000001. RDFLib stands in for QLever here."""

from rdflib import Graph

from rdfsolve import SchemaMiner

DATA = """
<urn:a1> a <urn:A>, <urn:B>, <urn:C>, <urn:F> ; <urn:p> "1" .
<urn:a2> a <urn:A>, <urn:B>, <urn:C>, <urn:F> ; <urn:p> "2" .
<urn:c1> a <urn:C>, <urn:D>, <urn:F> ; <urn:p> "3" .
<urn:f1> a <urn:F> ; <urn:p> "4" .
<urn:e1> a <urn:E> ; <urn:p> "5" .
"""


def mine(monkeypatch, engine):
    with SchemaMiner.from_graph(Graph().parse(data=DATA, format="turtle"), delay=0) as miner:
        monkeypatch.setattr(miner.helper, "sparql_engine", engine)
        return miner.mine("extensions")


def test_classes_with_the_same_members_and_their_nearest_containers(monkeypatch):
    found = mine(monkeypatch, "qlever").class_extensions
    assert found.members == {"urn:A": 2, "urn:B": 2, "urn:C": 3, "urn:D": 1, "urn:E": 1, "urn:F": 4}
    assert found.same_members == [["urn:A", "urn:B"]]
    assert found.contained_in == {
        "urn:A": ["urn:C"], "urn:B": ["urn:C"], "urn:C": ["urn:F"], "urn:D": ["urn:C"]
    }, "Only the nearest containing classes"
    assert found.not_checked == {}
    assert mine(monkeypatch, "virtuoso").class_extensions is None, "Measured on QLever only"
