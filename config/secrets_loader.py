"""
Load config/secrets.yaml into environment variables.

Called at the top of runner scripts. Values already set in the environment
(e.g. from an export) take precedence over the file, so CI/CD overrides work
without touching the file.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml

_SECRETS_PATH = Path(__file__).parent / "secrets.yaml"

_KEY_MAP = {
    "openalex_email":      "OPENALEX_EMAIL",
    "unpaywall_email":     "UNPAYWALL_EMAIL",
    "scopus_api_key":      "SCOPUS_API_KEY",
    "scopus_inst_token":   "SCOPUS_INST_TOKEN",
    "wos_api_key":         "WOS_API_KEY",
    "dimensions_api_key":  "DIMENSIONS_API_KEY",
    "dimensions_username": "DIMENSIONS_USERNAME",
    "dimensions_password": "DIMENSIONS_PASSWORD",
}


def load_secrets(path: Path = _SECRETS_PATH) -> None:
    """
    Read secrets.yaml and populate os.environ for any key not already set.
    Silently skips if the file doesn't exist.
    """
    if not path.exists():
        return
    with path.open() as f:
        secrets = yaml.safe_load(f) or {}
    for yaml_key, env_key in _KEY_MAP.items():
        value = secrets.get(yaml_key, "")
        if value and not os.environ.get(env_key):
            os.environ[env_key] = str(value)
