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
