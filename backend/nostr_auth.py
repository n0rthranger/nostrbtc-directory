"""Nostr DM-based authentication: login code generation and DM delivery."""

import asyncio
import json
import logging
import hmac
import secrets
import time

import websockets

import os

import discovery
import nostr_crypto
import secrets_util

logger = logging.getLogger("nostrbtc.auth")

RELAY_DOMAIN = os.environ.get("RELAY_DOMAIN", "your-relay.example.com")

RELAY_PRIVATE_KEY = secrets_util.get_secret("RELAY_PRIVATE_KEY")
try:
    RELAY_PUBLIC_KEY = nostr_crypto.privkey_to_pubkey(RELAY_PRIVATE_KEY) if RELAY_PRIVATE_KEY else ""
except Exception as _e:
    logger.error(f"Invalid RELAY_PRIVATE_KEY, DM auth disabled: {type(_e).__name__}")
    RELAY_PRIVATE_KEY = ""
    RELAY_PUBLIC_KEY = ""

# In-memory login code store: {pubkey: {"code": str, "expires": float, "attempts": int}}
_login_codes: dict[str, dict] = {}
_login_lock = asyncio.Lock()
CODE_TTL = 300  # 5 minutes
MAX_ATTEMPTS = 5
MAX_CODES = 5000  # prevent unbounded growth
LOGIN_DELIVERY_MIN_SECONDS = 1.2
LOGIN_DELIVERY_JITTER_MS = 400


async def login_delivery_delay(started_at: float) -> None:
    """Normalize login response timing so DM-sent vs not-sent is less observable."""
    target = LOGIN_DELIVERY_MIN_SECONDS + (secrets.randbelow(LOGIN_DELIVERY_JITTER_MS) / 1000)
    remaining = target - (time.time() - started_at)
    if remaining > 0:
        await asyncio.sleep(remaining)


async def generate_code(pubkey: str) -> str:
    """Generate a 6-digit login code for a pubkey. Overwrites any existing code."""
    async with _login_lock:
        code = f"{secrets.randbelow(900000) + 100000}"
        # Evict stale entries if at capacity
        if len(_login_codes) >= MAX_CODES:
            now = time.time()
            stale = [k for k, v in _login_codes.items() if v["expires"] < now]
            for k in stale:
                del _login_codes[k]
        # Still at capacity — evict oldest entries (LRU) to prevent DoS
        if len(_login_codes) >= MAX_CODES:
            oldest = sorted(_login_codes, key=lambda k: _login_codes[k]["expires"])
            for k in oldest[:len(_login_codes) // 2]:
                del _login_codes[k]
        if len(_login_codes) >= MAX_CODES:
            raise ValueError("Too many active login codes, try again later")
        _login_codes[pubkey] = {
            "code": code,
            "expires": time.time() + CODE_TTL,
            "attempts": 0,
        }
        return code


async def verify_code(pubkey: str, code: str) -> bool:
    """Verify a login code. Returns True on success, False on failure.
    Deletes the code after successful verification or too many attempts."""
    async with _login_lock:
        entry = _login_codes.get(pubkey)
        if not entry:
            return False
        if time.time() > entry["expires"]:
            del _login_codes[pubkey]
            return False
        entry["attempts"] += 1
        if entry["attempts"] > MAX_ATTEMPTS:
            del _login_codes[pubkey]
            return False
        if hmac.compare_digest(entry["code"], code):
            del _login_codes[pubkey]
            return True
        return False


async def _send_to_relay(relay_url: str, event: dict, timeout: float = 5) -> bool:
    """Publish a single event to a relay. Returns True on success."""
    try:
        async with websockets.connect(relay_url, close_timeout=timeout, open_timeout=timeout) as ws:
            await ws.send(json.dumps(["EVENT", event]))
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                data = json.loads(msg)
                if data[0] == "OK" and data[2] is True:
                    return True
                logger.debug(f"Relay {relay_url} rejected event: {data}")
                return False
            except asyncio.TimeoutError:
                logger.debug(f"Relay {relay_url} no OK response within {timeout}s")
                return False
    except Exception as e:
        logger.debug(f"Failed to send to {relay_url}: {e}")
        return False


async def send_dm(pubkey_hex: str, message: str) -> int:
    """Send a DM via NIP-17 (kind 1059 gift wrap).
    Returns the number of relays that accepted the event."""
    if not RELAY_PRIVATE_KEY:
        logger.error("RELAY_PRIVATE_KEY not configured")
        return 0

    # Build NIP-17 event (kind 1059 gift wrap)
    nip17_event = nostr_crypto.make_nip17_dm(RELAY_PRIVATE_KEY, pubkey_hex, message)

    # Discover user's relays
    relays = await discovery.discover_relays(pubkey_hex)
    from relay_constants import PUBLIC_RELAYS
    extra = PUBLIC_RELAYS + [
        "wss://relay.ditto.pub",
        "wss://nostr.wine",
        "wss://auth.nostr1.com",
    ]
    all_relays = list(dict.fromkeys(relays + extra))

    tasks = []
    for relay in all_relays:
        tasks.append(_send_to_relay(relay, nip17_event))

    results = await asyncio.gather(*tasks)
    success_count = sum(1 for r in results if r)

    logger.info(f"DM sent to {pubkey_hex[:16]}... via {success_count}/{len(all_relays)} relays")
    return success_count


async def send_login_dm(pubkey_hex: str, code: str) -> int:
    """Send a login code via DM. Returns the number of relays that accepted."""
    message = f"Your {RELAY_DOMAIN} login code: {code}\n\nThis code expires in 5 minutes. Do not share it."
    return await send_dm(pubkey_hex, message)
