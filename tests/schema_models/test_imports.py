"""Each module imports on its own, in a fresh interpreter (no import cycle)."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module",
    ["rdfsolve.ontology.structure", "rdfsolve.schema_models", "rdfsolve.schema_models.core"],
)
def test_a_module_imports_on_its_own(module):
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
