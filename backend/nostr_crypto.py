"""Nostr cryptographic primitives — re-exported from shared module."""
# All crypto logic lives in shared/nostr_crypto.py (single canonical copy).
# This file re-exports everything so existing imports continue to work.
from nostr_crypto_shared import *  # noqa: F401,F403
