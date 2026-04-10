"""Centralized relay URL constants for the directory backend.

All public relay URLs used for broadcasting, fetching, and syncing
should be defined here to avoid duplication across files.
"""

# Core public relays — highest coverage, used for broadcasting and fetching
PUBLIC_RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://nos.lol",
]

# Extended relays — additional relays for deeper coverage (interactions, sync)
EXTENDED_RELAYS = [
    "wss://nostr.wine",
    "wss://nostr.bitcoiner.social",
    "wss://relay.noswhere.com",
]

# All public relays combined
ALL_PUBLIC_RELAYS = PUBLIC_RELAYS + EXTENDED_RELAYS

# Interaction relays — used for fetching reactions, zaps, replies
INTERACTION_RELAYS = PUBLIC_RELAYS + EXTENDED_RELAYS
