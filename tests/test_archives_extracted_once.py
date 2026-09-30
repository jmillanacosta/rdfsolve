"""An archive is extracted once: the second pass is for archives found inside archives, and it
skips those of the first pass (WikiPathways: the three zip files were extracted twice, and every
file was an index input twice, 2026-09-30)."""

import subprocess
import zipfile

from rdfsolve.qlever.utils import _extract_archives_steps


def test_the_second_pass_skips_extracted_archives(tmp_path):
    inner = tmp_path / "inner.zip"
    with zipfile.ZipFile(inner, "w") as z:
        z.writestr("deep/b.ttl", "<urn:b> <urn:p> <urn:o> .\n")
    with zipfile.ZipFile(tmp_path / "outer.zip", "w") as z:
        z.writestr("wp/a.ttl", "<urn:a> <urn:p> <urn:o> .\n")
        z.write(inner, "nested/inner.zip")
    inner.unlink()
    done = subprocess.run(
        ["bash"], input=" && ".join(_extract_archives_steps()), text=True, cwd=tmp_path,
        capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    turtle = sorted(p.name for p in tmp_path.glob("*.ttl"))
    assert turtle == ["a.ttl", "b.ttl"], "Each file once, the nested archive extracted too"
