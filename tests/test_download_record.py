"""The local stage keeps a record of the downloads of a source (downloads.json: the URLs and
what the server said about each file). Files are downloaded when there is none, when an earlier
download did not end, or when the URLs of the registry entry changed. With --update-downloads
(the owner, 2026-09-30) an index is kept only when the server has no update: a file that the
server changed after the download, or after the index was built when there is no record, makes
the source be downloaded and indexed again. Without the option nothing is asked of the server,
so a frozen corpus stays as it is."""

import sys

import pytest

from rdfsolve.qlever.downloads import (
    MARKER,
    needs_download,
    read_record,
    updated_urls,
    write_record,
)

URLS = ["https://example.org/a.ttl.gz", "https://example.org/b.ttl.gz"]
STATE = {"last_modified": "Mon, 01 Sep 2026 10:00:00 GMT", "content_length": "10"}


def test_files_are_downloaded_when_missing_unfinished_or_of_another_entry(tmp_path):
    assert needs_download(tmp_path, URLS, has_inputs=False)
    assert not needs_download(tmp_path, URLS, has_inputs=True), "An old folder without a record"
    (tmp_path / MARKER).touch()
    assert needs_download(tmp_path, URLS, has_inputs=True), "An earlier download did not end"
    write_record(tmp_path, URLS, lambda url: STATE)
    assert not (tmp_path / MARKER).exists() and read_record(tmp_path)["urls"] == URLS
    assert not needs_download(tmp_path, URLS, has_inputs=True)
    assert needs_download(tmp_path, URLS[:1], has_inputs=True), "The registry entry changed"


def test_an_update_on_the_server_is_seen_from_the_record(tmp_path):
    write_record(tmp_path, URLS, lambda url: STATE)
    record = read_record(tmp_path)
    assert updated_urls(URLS, record, built_at=0, head=lambda url: STATE) == []
    newer = {**STATE, "content_length": "11"}
    changed = updated_urls(URLS, record, built_at=0, head=lambda url: newer if "b." in url else STATE)
    assert changed == [URLS[1]]
    assert updated_urls(URLS, record, built_at=0, head=lambda url: None) == [], (
        "A server that does not answer is not an update"
    )


def test_without_a_record_the_date_of_the_index_is_compared():
    september = 1788256800.0  # 2026-09-01 10:00:00 GMT
    assert updated_urls(URLS, None, built_at=september + 60, head=lambda url: STATE) == []
    assert updated_urls(URLS, None, built_at=september - 60, head=lambda url: STATE) == URLS


def test_the_option_is_read_from_the_command_line(monkeypatch):
    from scripts.pipeline_stages import cli

    seen = {}

    def stop(config, **_):
        seen["config"] = config
        raise SystemExit(0)

    monkeypatch.setattr(cli, "preflight", stop)
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--preflight", "--update-downloads"])
    with pytest.raises(SystemExit):
        cli.main()
    assert seen["config"].update_downloads is True
