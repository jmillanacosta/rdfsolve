"""Class discovery stops at the run's class listing limit (BioGateway, job 115328: 10.8 M type
values, one instance each, listed for hours): a sample says what the listed classes are, a
spread of them is mined, and the source ends partial with its rows marked sampled."""

from rdflib import Dataset

from rdfsolve.mining import two_phase_strategy
from rdfsolve.mining.miner import SchemaMiner

RECORDS = "\n".join(
    f"<urn:crm{i}> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:SO_0000727> .\n"
    f"<urn:crm{i}#ev> a <urn:crm{i}> ; <urn:origin> <urn:source> ."
    for i in range(40)
)


def _mine(limit, chunk=None):
    data = Dataset().parse(data=RECORDS, format="turtle")
    with SchemaMiner.from_graph(data, delay=0, class_chunk_size=chunk) as miner:
        miner.class_listing_limit = limit
        result = miner.mine("records")
        return result, miner.last_report


def test_a_listing_over_the_limit_stops_and_leaves_the_source_partial(monkeypatch):
    monkeypatch.setattr(two_phase_strategy, "CLASS_SAMPLE", 8)
    result, report = _mine(10)
    evidence = report.config["class_discovery"]
    assert evidence["state"] == "stopped" and evidence["limit"] == 10
    assert evidence["median_instances"] == 1 and evidence["classes_with_one_instance"] == 40
    assert evidence["parents"] == {"urn:SO_0000727": 40}
    assert evidence["mined_classes"] == 8
    assert report.completion_state == "partial"
    (failure,) = [f for f in report.query_failures if f.purpose == "two-phase/classes"]
    assert failure.category == "sampled"
    assert (
        len({p.subject_class for p in result.patterns if p.subject_class.startswith("urn:crm")})
        == 8
    )


def test_a_paged_listing_stops_at_the_limit():
    _, report = _mine(10, chunk=5)
    evidence = report.config["class_discovery"]
    assert evidence["state"] == "stopped" and evidence["listed"] == 15, (
        "Stopped at the page past it"
    )


def test_a_listing_within_the_limit_is_mined():
    result, report = _mine(1000)
    assert "class_discovery" not in report.config and result.patterns
