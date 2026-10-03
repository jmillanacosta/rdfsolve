import json
from unittest.mock import Mock

from rdfsolve.endpoint_health import EndpointHealthCheck
from scripts import check_endpoints


def test_endpoint_check_writes_status_without_changing_sources(tmp_path, monkeypatch):
    path = tmp_path / "sources.yaml"
    original = "- name: demo\n  endpoint: https://example.org/sparql\n"
    path.write_text(original)
    output = tmp_path / "status.json"
    monkeypatch.setattr(
        "sys.argv", ["check_endpoints", "--sources", str(path), "--output", str(output)]
    )
    health = Mock(
        return_value=EndpointHealthCheck(
            "https://example.org/sparql", "rate_limited", 0.125, "HTTP 429", "2026-09-22"
        )
    )
    monkeypatch.setattr(check_endpoints, "check_endpoint_health", health)
    check_endpoints.main()
    assert json.loads(output.read_text())["endpoints"] == {
        "demo": {
            "endpoint": "https://example.org/sparql",
            "hostname": "example.org",
            "status": "rate_limited",
            "response_time_ms": 125,
            "error": "HTTP 429",
        }
    }, "Pipeline status report"
    assert path.read_text() == original, "Source specification"
    path.write_text("[]\n")
    check_endpoints.main()
    assert json.loads(output.read_text())["endpoints"] == {}, "Empty source set"
    health.assert_called_once_with("https://example.org/sparql")
