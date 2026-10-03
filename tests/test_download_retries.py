"""A download is tried again after a fault of the network or of the server that passes (a
refused connection, HTTP 429 or 5xx): one file of 19 failed once and stopped the rebuild of
MetaNetX (job 114421, 2026-09-30). A file that is not there (404) is not tried again."""

from rdfsolve.qlever.utils import _wget_cmd


def test_each_download_command_retries_passing_faults():
    for url in ("https://example.org/a/data.ttl.gz", "https://example.org/download?id=7"):
        command = _wget_cmd(url)
        assert "--tries=5" in command and "--waitretry=20" in command
        assert "--retry-connrefused" in command
        assert "--retry-on-http-error=429,500,502,503,504" in command
        assert "404" not in command and command.startswith("wget -c -q ")


def test_a_failed_connection_is_tried_again_and_a_missing_file_is_not(tmp_path):
    """wget does not try again after "Unable to establish SSL connection" (exit 5): the rebuild
    of MetaNetX failed four times on it (2026-09-30). The download step tries such a file
    again; a missing file (exit 8) fails at once."""
    import os
    import subprocess

    from rdfsolve.qlever.utils import _build_get_data_steps, analyse_source

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "wget").write_text(
        "#!/bin/sh\n"
        f'echo x >> "{tmp_path}/calls"\n'
        'for a in "$@"; do case "$a" in *missing*) exit 8;; esac; done\n'
        f'[ "$(wc -l < "{tmp_path}/calls")" -lt 3 ] && exit 5\n'
        'for a in "$@"; do case "$a" in http*) touch "$(basename "$a")";; esac; done\n'
    )
    (bin_dir / "wget").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "RDFSOLVE_DOWNLOAD_WAIT": "0"}

    def run(url):
        steps = _build_get_data_steps(analyse_source({"name": "fixture", "download_ttl": [url]}), str(tmp_path / "rdf"))
        return subprocess.run(["bash"], input=" && ".join(steps), text=True, env=env, capture_output=True)

    assert run("https://example.org/data.ttl").returncode == 0
    assert (tmp_path / "rdf" / "data.ttl").exists()
    assert len((tmp_path / "calls").read_text().splitlines()) == 3, "Two failed connections, then the file"
    (tmp_path / "calls").write_text("x\nx\nx\n")
    assert run("https://example.org/missing.ttl").returncode != 0
    assert len((tmp_path / "calls").read_text().splitlines()) == 4, "A missing file is asked for once"
