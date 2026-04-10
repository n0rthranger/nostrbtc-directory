"""Read secrets from Podman secrets (/run/secrets/) with env var fallback."""

import os


def get_secret(name: str, default: str = "") -> str:
    """Read a secret from /run/secrets/<name>, falling back to env var."""
    if "/" in name or "\\" in name or ".." in name:
        raise ValueError(f"Invalid secret name: {name}")
    secret_path = f"/run/secrets/{name}"
    try:
        with open(secret_path) as f:
            value = f.read().strip()
            if value:
                return value
    except FileNotFoundError:
        pass
    return os.environ.get(name, default)
