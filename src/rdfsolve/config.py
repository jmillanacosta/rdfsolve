"""Base IRI for resources that rdfsolve identifies."""

import os
from pathlib import Path
from urllib.parse import quote

DEFAULT_BASE_URI = "https://w3id.org/rdfsolve/"


def get_base_uri() -> str:
    """Return the base IRI, always ending with a slash.

    Priority:
    1. RDFSOLVE_BASE_URI environment variable
    2. base_uri in scripts/config/config.yaml
    3. DEFAULT_BASE_URI
    """
    env_uri = os.environ.get("RDFSOLVE_BASE_URI")
    if env_uri:
        return env_uri.rstrip("/") + "/"

    try:
        import yaml

        possible_paths = [
            Path.cwd() / "scripts" / "config" / "config.yaml",
            Path.cwd() / "config" / "config.yaml",
            Path(__file__).parent.parent.parent / "scripts" / "config" / "config.yaml",
        ]

        for config_path in possible_paths:
            if config_path.exists():
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                    if config and "base_uri" in config:
                        base_uri_value: str = config["base_uri"]
                        return base_uri_value.rstrip("/") + "/"
    except Exception:
        pass

    return DEFAULT_BASE_URI


def mint(kind: str, *parts: str) -> str:
    """Return the IRI of an rdfsolve resource of one kind, such as dataset or graph.

    Each part is percent-encoded as one path segment.
    """
    if not kind or "/" in kind:
        raise ValueError("Use one path segment as the resource kind")
    segments = [kind, *(quote(part, safe="") for part in parts)]
    if not all(segments):
        raise ValueError("IRI path segments must be nonempty")
    return get_base_uri() + "/".join(segments)
