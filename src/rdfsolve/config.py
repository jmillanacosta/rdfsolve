"""Configuration management for VoID URI namespaces."""

import os
from pathlib import Path

DEFAULT_BASE_URI = "http://example.com/void"


def get_base_uri() -> str:
    """Get base URI for VoID documents from config or environment.

    Priority:
    1. RDFSOLVE_BASE_URI environment variable
    2. scripts/config/config.yaml if exists
    3. Default: http://example.com/void
    """
    # Check environment variable first
    env_uri = os.environ.get("RDFSOLVE_BASE_URI")
    if env_uri:
        return env_uri.rstrip("/")

    # Try to read from config.yaml
    try:
        import yaml

        # Look for config in multiple locations
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
                        return base_uri_value.rstrip("/")
    except Exception:
        pass

    return DEFAULT_BASE_URI
