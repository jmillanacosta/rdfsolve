import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from rdfsolve.cli import main


def _run(root: Path):
    (root / "sources.yaml").write_text(yaml.safe_dump([{"name":"demo","endpoint":"https://example.org/sparql"}]))
    d=root/"demo"; d.mkdir()
    (d/"demo_remote_report.json").write_text(json.dumps({"completion_state":"complete"}))
    (d/"demo_remote_schema.json").write_text(json.dumps({"schema":{"patterns":[]}}))


def test_release_cli_build_summarize_validate(tmp_path: Path):
    _run(tmp_path)
    runner=CliRunner()
    built=runner.invoke(main,["release","build",str(tmp_path),"--release-id","test"])
    assert built.exit_code==0, built.output
    assert (tmp_path/"release.json").exists()
    assert (tmp_path/"release.ttl").exists()
    summary=runner.invoke(main,["release","summarize",str(tmp_path)])
    assert summary.exit_code==0
    assert '"datasets": 1' in summary.output
    validated=runner.invoke(main,["release","validate",str(tmp_path)])
    assert validated.exit_code==0, validated.output
