import gzip
import hashlib
from pathlib import Path

import pytest
from rdfsolve.qlever.inputs import check_cached_input


def test_cached_gzip_checks_real_content_and_rejects_truncation(tmp_path):
    raw = (Path(__file__).parent / "test_data/aopwikirdf_generated_void.ttl").read_bytes()
    path = tmp_path / "aop.ttl.gz"
    compressed = gzip.compress(raw)
    path.write_bytes(compressed)
    result = check_cached_input(path)
    assert result.decoded_bytes == len(raw)
    assert result.decoded_sha256 == hashlib.sha256(raw).hexdigest()
    path.write_bytes(compressed[:-4])
    with pytest.raises(EOFError):
        check_cached_input(path)
