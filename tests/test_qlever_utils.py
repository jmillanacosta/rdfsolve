import configparser

from rdfsolve.qlever import QleverConfig, build_qleverfile


def test_qleverfile_preserves_parser_buffer(tmp_path):
    source = {"name": "fixture", "download_ttl": "https://example.org/data.ttl"}
    for settings, expected in [(None, "10M"), (QleverConfig(parser_buffer_size="20M"), "20M")]:
        output = build_qleverfile(source, tmp_path, 7000, "docker", cfg=settings)
        parsed = configparser.ConfigParser(interpolation=None)
        parsed.read_string(output)
        assert parsed["index"]["PARSER_BUFFER_SIZE"] == expected, "Wrong parser buffer in Qleverfile"
