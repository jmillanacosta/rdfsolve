import configparser

from rdfsolve.qlever import QleverConfig, build_qleverfile


def test_qleverfile_preserves_parser_buffer(tmp_path):
    source = {"name": "fixture", "download_ttl": "https://example.org/data.ttl"}
    for settings, expected in [(None, "10M"), (QleverConfig(parser_buffer_size="20M"), "20M")]:
        output = build_qleverfile(source, tmp_path, 7000, "docker", cfg=settings)
        parsed = configparser.ConfigParser(interpolation=None)
        parsed.read_string(output)
        assert parsed["index"]["PARSER_BUFFER_SIZE"] == expected, "Wrong parser buffer in Qleverfile"

    import json
    import subprocess

    from rdflib import Graph, Namespace

    source = {"name": "fixture", "download_owl": ["https://example.org/a.owl", "https://example.org/b.owl"]}
    rdf = tmp_path / "rdf"
    rdf.mkdir()
    (rdf / "a.nq").write_text('_:same <urn:test:kind> <urn:test:Restriction> .\n')
    (rdf / "b.nq").write_text('_:same <urn:test:kind> <urn:test:Axiom> .\n')
    parsed.read_string(build_qleverfile(source, tmp_path, 7000, "docker"))
    index = parsed["index"]
    graph = Graph()
    if index.get("MULTI_INPUT_JSON"):
        assert not index["CAT_INPUT_FILES"], "Two input modes are configured"
        for spec in json.loads(index["MULTI_INPUT_JSON"]):
            for path in sorted(tmp_path.glob(spec["for-each"])):
                raw = subprocess.check_output(["bash", "-c", spec["cmd"].format(path)])
                graph.parse(data=raw.decode(), format="nt")
    else:
        raw = subprocess.check_output(
            ["bash", "-c", 'INPUT_FILES="rdf/*.nq"; ' + index["CAT_INPUT_FILES"]], cwd=tmp_path
        )
        graph.parse(data=raw.decode(), format="nt")
    ns = Namespace("urn:test:")
    restrictions = set(graph.subjects(ns.kind, ns.Restriction))
    axioms = set(graph.subjects(ns.kind, ns.Axiom))
    assert len(restrictions) == len(axioms) == 1, "A document lost its record"
    assert restrictions.isdisjoint(axioms), "Blank nodes from different documents were merged"
