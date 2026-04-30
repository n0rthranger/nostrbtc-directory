"""FastAPI backend for the Nostr relay directory module."""

import asyncio
import base64
import hashlib
import hmac
import html as html_module
import json
import logging
import math
import os
import re
import secrets
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO

import websockets
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

import db
import directory_indexer
import discovery
import httpx
import nostr_auth
import nostr_crypto
import secrets_util
from nostr_crypto_shared import bolt11_to_sats as _bolt11_to_sats, is_valid_hex_pubkey as _is_valid_hex_pubkey

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("directory")

# Configurable domain (used in URLs, NIP-05 addresses, messages)
RELAY_DOMAIN = os.environ.get("RELAY_DOMAIN", "your-relay.example.com")

# Bitcoin block height cache
_block_cache = {"height": None, "ts": 0}

def _get_block_height():
    """Fetch current block height from mempool.space, cached 60s."""
    if time.time() - _block_cache["ts"] < 60 and _block_cache["height"]:
        return _block_cache["height"]
    try:
        r = httpx.get("https://mempool.space/api/blocks/tip/height", timeout=3)
        if r.status_code == 200:
            _block_cache["height"] = int(r.text.strip())
            _block_cache["ts"] = time.time()
            return _block_cache["height"]
    except Exception as e:
        logger.debug("Block height fetch failed: %s", e)
    return _block_cache["height"]  # return stale if available

# In-memory caches with bounded size (avoids hitting relays on every request)
CACHE_MAX_SIZE = 200  # max entries per cache


def _cache_get(cache, key, ttl):
    """Get from cache if not expired."""
    entry = cache.get(key)
    if entry and (time.time() - entry["ts"]) < ttl:
        return entry["data"]
    return None


_cache_lock = threading.Lock()


def _cache_set(cache, key, data):
    """Set cache entry, evicting oldest if full."""
    with _cache_lock:
        if len(cache) >= CACHE_MAX_SIZE:
            oldest_key = min(cache, key=lambda k: cache[k]["ts"])
            del cache[oldest_key]
        cache[key] = {"data": data, "ts": time.time()}


_card_cache = {}
CARD_CACHE_TTL = 300  # 5 minutes
_wrapped_cache = {}
WRAPPED_CACHE_TTL = 300  # 5 minutes

app = FastAPI(title=f"{RELAY_DOMAIN} Directory API", docs_url=None, redoc_url=None)


# Global request body size limit (64KB) to prevent memory exhaustion
MAX_BODY_SIZE = 65536

@app.middleware("http")
async def limit_body_size(request: Request, call_next):
    """Reject requests with bodies larger than MAX_BODY_SIZE."""
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_BODY_SIZE:
                return JSONResponse(status_code=413, content={"detail": "Payload too large"})
        except ValueError:
            return JSONResponse(status_code=400, content={"detail": "Invalid Content-Length"})
    if request.method in ("POST", "PUT", "PATCH"):
        body = await request.body()
        if len(body) > MAX_BODY_SIZE:
            return JSONResponse(status_code=413, content={"detail": "Payload too large"})
    return await call_next(request)


from url_safety import is_safe_url as _is_safe_url, resolve_safe_url as _resolve_safe_url, resolve_domain_to_safe_ip as _resolve_domain_to_safe_ip
from relay_constants import PUBLIC_RELAYS as _PUBLIC_RELAYS, INTERACTION_RELAYS as _INTERACTION_RELAYS


async def _safe_nip05_resolve(domain: str, user: str) -> str | None:
    """Resolve NIP-05 with DNS-pinning to prevent TOCTOU/rebinding SSRF.

    Resolves DNS once, validates all IPs are public, then connects httpx to the
    verified IP directly (using Host header + TLS server_hostname for SNI/cert).
    This eliminates the rebinding window entirely.
    """
    safe_ip = _resolve_domain_to_safe_ip(domain)
    if not safe_ip:
        return None
    try:
        is_ipv6 = ":" in safe_ip
        host_for_url = f"[{safe_ip}]" if is_ipv6 else safe_ip
        pinned_url = f"https://{host_for_url}/.well-known/nostr.json?name={user}"
        async with httpx.AsyncClient(
            timeout=5, headers={"Host": domain},
        ) as client:
            resp = await client.get(pinned_url, extensions={"sni_hostname": domain.encode()})
            if resp.status_code == 200:
                names = resp.json().get("names", {})
                pk = names.get(user) or names.get(user.lower())
                if pk and len(pk) == 64:
                    return pk
    except Exception:
        pass
    return None


# --- Rate Limiting (Redis-backed, survives restarts) ---

class RateLimiter:
    """Redis-backed rate limiter with in-memory fallback."""

    def __init__(self):
        self._fallback: dict[str, list[float]] = defaultdict(list)

    def _get_redis(self):
        try:
            return db._get_redis()
        except Exception:
            return None

    def check(self, key: str, max_requests: int, window_seconds: int):
        """Raise 429 if rate limit exceeded. Uses Redis INCR+EXPIRE."""
        r = self._get_redis()
        if r is not None:
            try:
                rkey = f"rl:{key}"
                count = r.incr(rkey)
                if count == 1:
                    r.expire(rkey, window_seconds)
                if count > max_requests:
                    raise HTTPException(status_code=429, detail="Too many requests. Try again later.")
                return
            except HTTPException:
                raise
            except Exception:
                pass  # fall through to in-memory
        # In-memory fallback if Redis unavailable
        now = time.time()
        cutoff = now - window_seconds
        self._fallback[key] = [t for t in self._fallback[key] if t > cutoff]
        if len(self._fallback[key]) >= max_requests:
            raise HTTPException(status_code=429, detail="Too many requests. Try again later.")
        self._fallback[key].append(now)
        if len(self._fallback) > 10000:
            stale = [k for k, v in self._fallback.items() if not v or v[-1] < cutoff]
            for k in stale:
                del self._fallback[k]


rate_limiter = RateLimiter()


_background_tasks: set[asyncio.Task] = set()

def _safe_task(coro, name: str = ""):
    """Create an asyncio task that logs exceptions instead of silently swallowing them."""
    async def _wrapper():
        try:
            await coro
        except Exception:
            logger.exception(f"Background task failed: {name}")
    task = asyncio.create_task(_wrapper(), name=name)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


# --- Auth Token ---

AUTH_SECRET = secrets_util.get_secret("AUTH_SECRET")
if not AUTH_SECRET:
    AUTH_SECRET = os.urandom(32).hex()
    logger.warning("AUTH_SECRET not configured — using random value (tokens will not survive restarts)")

OPERATOR_API_KEY = secrets_util.get_secret("OPERATOR_API_KEY") or os.environ.get("OPERATOR_API_KEY", "")
if not OPERATOR_API_KEY:
    OPERATOR_API_KEY = hashlib.sha256(f"op:{AUTH_SECRET}".encode()).hexdigest()
    logger.info("OPERATOR_API_KEY not configured — derived from AUTH_SECRET")


TOKEN_TTL = 30 * 86400  # 30 days
QUEUE_HMAC_SECRET = secrets_util.get_secret("GRAPERANK_QUEUE_SECRET") or AUTH_SECRET


def generate_token(npub: str) -> str:
    """Generate a time-limited HMAC token for an npub (30-day expiry)."""
    issued = int(time.time())
    payload = f"{npub}:{issued}"
    sig = hmac.new(AUTH_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{issued}:{sig}"


def _sign_queue_payload(payload: str) -> str:
    sig = hmac.new(QUEUE_HMAC_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return json.dumps({"payload": payload, "hmac": sig}, separators=(",", ":"))


def verify_token(npub: str, token: str) -> bool:
    """Verify an auth token for an npub, checking expiry."""
    if not token or ":" not in token:
        return False
    parts = token.split(":", 1)
    if len(parts) != 2:
        return False
    try:
        issued = int(parts[0])
    except ValueError:
        return False
    if time.time() - issued > TOKEN_TTL:
        return False
    payload = f"{npub}:{issued}"
    expected_sig = hmac.new(AUTH_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected_sig, parts[1])


SESSION_COOKIE = "directory_session"


def extract_token(request: Request) -> str:
    """Extract auth token from cookie or Authorization header."""
    # Prefer httpOnly cookie
    cookie_token = request.cookies.get(SESSION_COOKIE, "")
    if cookie_token:
        return cookie_token
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    return ""


def _tag_value(event: dict, name: str) -> str:
    for tag in event.get("tags", []):
        if isinstance(tag, list) and len(tag) >= 2 and tag[0] == name:
            return str(tag[1])
    return ""


FONT_DIR = os.environ.get("FONT_DIR", "/usr/share/fonts/truetype/dejavu/")


def _is_subscription_active(sub: dict) -> bool:
    """Check if subscription dict has non-expired expires_at."""
    ea = sub.get("expires_at")
    if not ea:
        return False
    if isinstance(ea, str):
        ea = datetime.fromisoformat(ea)
    if ea.tzinfo is None:
        ea = ea.replace(tzinfo=timezone.utc)
    return ea > datetime.now(timezone.utc)


def _extract_zap_sats(zap_event: dict) -> int:
    """Extract sats from a kind-9735 zap receipt event.

    Tries the 'amount' tag inside the embedded 'description' event first,
    then falls back to parsing the bolt11 invoice tag.
    """
    sats = 0
    desc_tag = next((t[1] for t in zap_event.get("tags", []) if len(t) >= 2 and t[0] == "description"), None)
    if desc_tag:
        try:
            desc_event = json.loads(desc_tag)
            amount_tag = next((t[1] for t in desc_event.get("tags", []) if len(t) >= 2 and t[0] == "amount"), None)
            if amount_tag:
                sats = int(amount_tag) // 1000
        except (json.JSONDecodeError, StopIteration, ValueError):
            pass
    if not sats:
        bolt11 = next((t[1] for t in zap_event.get("tags", []) if len(t) >= 2 and t[0] == "bolt11"), None)
        if bolt11:
            sats = _bolt11_to_sats(bolt11) or 0
    return sats


async def _resolve_identifier(value: str) -> str | None:
    """Resolve an npub, hex pubkey, or NIP-05 identifier to a hex pubkey.

    Returns the hex pubkey string or None if resolution fails.
    """
    value = value.strip()
    if value.startswith("npub1"):
        try:
            return npub_to_hex(value)
        except Exception:
            return None
    elif len(value) == 64 and _is_valid_hex_pubkey(value):
        return value
    elif "@" in value:
        name_part, _, domain_part = value.partition("@")
        if domain_part.lower() == RELAY_DOMAIN:
            pk = db.get_nip05(name_part)
            if pk:
                return pk
        try:
            user = name_part if name_part else "_"
            pk = await _safe_nip05_resolve(domain_part, user)
            if pk:
                return pk
        except Exception:
            pass
    return None


def _require_nip98_pubkey(request: Request, expected_pubkey: str) -> dict:
    """Validate NIP-98 HTTP auth and require it to be signed by expected_pubkey."""
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Nostr "):
        raise HTTPException(status_code=401, detail="NIP-98 authentication required")
    try:
        raw = base64.b64decode(auth_header[6:], validate=True)
        event = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid NIP-98 authorization")
    if event.get("kind") != 27235 or event.get("pubkey") != expected_pubkey:
        raise HTTPException(status_code=403, detail="NIP-98 pubkey mismatch")
    created_at = int(event.get("created_at", 0))
    if abs(time.time() - created_at) > 60:
        raise HTTPException(status_code=401, detail="Expired NIP-98 authorization")
    method = _tag_value(event, "method").upper()
    if method != request.method.upper():
        raise HTTPException(status_code=401, detail="NIP-98 method mismatch")
    url = _tag_value(event, "u")
    request_url = str(request.url)
    request_path = request.url.path
    if url not in (request_url, request_path):
        raise HTTPException(status_code=401, detail="NIP-98 URL mismatch")
    if not nostr_crypto.verify_event(event):
        raise HTTPException(status_code=401, detail="Invalid NIP-98 signature")
    return event

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        f"https://{RELAY_DOMAIN}",
        f"https://www.{RELAY_DOMAIN}",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Authorization"],
)


# --- Helpers ---

NPUB_REGEX = re.compile(r"^npub1[a-z0-9]{58}$")


def npub_to_hex(npub: str) -> str:
    """Convert bech32 npub to hex pubkey."""
    CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

    if not NPUB_REGEX.match(npub):
        raise ValueError("Invalid npub format")

    data_part = npub[5:]  # strip "npub1"
    values = [CHARSET.index(c) for c in data_part]

    # Verify bech32 checksum
    GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    hrp_expand = [ord(c) >> 5 for c in "npub"] + [0] + [ord(c) & 31 for c in "npub"]
    chk = 1
    for v in hrp_expand + values:
        b = chk >> 25
        chk = (chk & 0x1ffffff) << 5 ^ v
        for i in range(5):
            chk ^= GEN[i] if ((b >> i) & 1) else 0
    if chk != 1:
        raise ValueError("Invalid npub checksum")

    # Bech32 to bytes conversion (5-bit to 8-bit)
    acc = 0
    bits = 0
    result = []
    for v in values[:-6]:  # exclude 6-char checksum
        acc = (acc << 5) | v
        bits += 5
        while bits >= 8:
            bits -= 8
            result.append((acc >> bits) & 0xFF)

    return bytes(result).hex()


def hex_to_npub(hex_pubkey: str) -> str:
    """Convert hex pubkey to bech32 npub."""
    CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
    data = bytes.fromhex(hex_pubkey)

    # Convert 8-bit bytes to 5-bit values
    acc = 0
    bits = 0
    values = []
    for byte in data:
        acc = (acc << 8) | byte
        bits += 8
        while bits >= 5:
            bits -= 5
            values.append((acc >> bits) & 0x1F)
    if bits > 0:
        values.append((acc << (5 - bits)) & 0x1F)

    # Bech32 checksum (polymod)
    def bech32_polymod(vals):
        gen = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
        chk = 1
        for v in vals:
            b = chk >> 25
            chk = ((chk & 0x1FFFFFF) << 5) ^ v
            for i in range(5):
                chk ^= gen[i] if ((b >> i) & 1) else 0
        return chk

    hrp = "npub"
    hrp_expand = [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]
    polymod = bech32_polymod(hrp_expand + values + [0, 0, 0, 0, 0, 0]) ^ 1
    checksum = [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]

    return hrp + "1" + "".join(CHARSET[v] for v in values + checksum)


# --- Startup ---

@app.on_event("startup")
async def startup():
    # Initialize Postgres + Redis connections
    db.init_pg()
    _safe_task(directory_indexer.index_loop(), "directory-indexer")

    # Pre-warming: recompute scores for recently active observers every 4 hours
    async def _prewarm_loop():
        await asyncio.sleep(300)  # Wait for startup
        while True:
            try:
                observers = db.get_recently_active_observers(days=7)
                if observers:
                    logger.info(f"GrapeRank pre-warm: {len(observers)} active observers")
                    for obs in observers:
                        try:
                            await _ensure_scores(obs)
                        except Exception:
                            logger.debug(f"Pre-warm failed for {obs[:16]}")
                        await asyncio.sleep(2)  # Pace requests
            except Exception:
                logger.exception("Pre-warm cycle failed")
            await asyncio.sleep(14400)  # 4 hours

    _safe_task(_prewarm_loop(), "graperank-prewarm")


@app.on_event("shutdown")
async def shutdown():
    # Cancel all tracked background tasks
    for task in list(_background_tasks):
        task.cancel()
    if _background_tasks:
        await asyncio.gather(*_background_tasks, return_exceptions=True)
    logger.info("Shutdown complete")


# --- NIP-05 Endpoint ---

@app.get("/.well-known/nostr.json")
async def nostr_json(request: Request, name: str = ""):
    """NIP-05 verification endpoint."""
    client_ip = request.client.host if request.client else "unknown"
    rate_limiter.check(f"nip05:{client_ip}", max_requests=60, window_seconds=60)
    if not name:
        # NIP-05 spec does not require listing all names; return empty to prevent enumeration
        return {"names": {}}

    # System accounts (bots, services) — pubkeys from env vars
    SYSTEM_NIP05 = {}
    # Set these env vars to the hex pubkeys for your system bots
    _dobby_pk = os.environ.get("DOBBY_PUBKEY", "")
    _relay_pk = os.environ.get("RELAY_BOT_PUBKEY", "")
    _mail_pk = os.environ.get("BRIDGE_PUBKEY", "")
    if _dobby_pk:
        SYSTEM_NIP05["dobby"] = _dobby_pk
    if _relay_pk:
        SYSTEM_NIP05["relay"] = _relay_pk
    if _mail_pk:
        SYSTEM_NIP05["mail"] = _mail_pk
    lowered = name.lower()
    if lowered in SYSTEM_NIP05:
        return {"names": {lowered: SYSTEM_NIP05[lowered]}}

    pubkey = db.get_nip05(lowered)
    if not pubkey:
        raise HTTPException(status_code=404, detail="Name not found")

    response = {
        "names": {lowered: pubkey},
        "relays": {pubkey: [f"wss://{RELAY_DOMAIN}"]},
    }

    # Enhanced metadata for directory members
    dir_profile = db.get_directory_profile(pubkey)
    if dir_profile:
        response["_directory"] = {
            "badges": json.loads(dir_profile.get("badges", "[]")),
            "reputation_score": dir_profile.get("reputation_score", 0),
            "subscriber_since": dir_profile.get("subscription_created", ""),
            "last_active": dir_profile.get("last_active", 0),
            "directory_listed": True,
        }

    return response


# --- Profile Card Page ---

CARD_HTML_FALLBACK = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta http-equiv="refresh" content="0;url=/card.html"></head><body>Redirecting...</body></html>'


@app.get("/p/{identifier}")
async def card_page(identifier: str):
    """Serve the Nostr Card HTML page with server-side OG meta tags for social media previews."""
    rate_limiter.check(f"card-page:{identifier}", max_requests=30, window_seconds=600)

    # Resolve identifier to pubkey
    if identifier.startswith("npub1"):
        try:
            pubkey = npub_to_hex(identifier)
            npub = identifier
        except ValueError:
            npub = identifier
            pubkey = None
    else:
        pubkey = db.get_nip05(identifier)
        if pubkey:
            npub = hex_to_npub(pubkey)
        else:
            npub = identifier
            pubkey = None

    # Only active subscribers can have cards
    if pubkey:
        sub = db.get_subscription(pubkey)
        if not sub:
            raise HTTPException(status_code=404, detail="Profile not found")
        if not _is_subscription_active(sub):
            raise HTTPException(status_code=404, detail="Profile not found")
        # Redirect npub URLs to NIP-05 URL if available (canonical URL)
        if identifier.startswith("npub1"):
            nip05_redirect = db.get_nip05_name(pubkey)
            if nip05_redirect:
                return RedirectResponse(url=f"/p/{nip05_redirect}", status_code=301)
    else:
        raise HTTPException(status_code=404, detail="Profile not found")

    # OG tags — use Postgres directory_profiles (fast) instead of relay WebSocket calls
    og_title = "Nostr Card"
    og_description = "Decentralized social profile on Nostr"
    nip05_name = db.get_nip05_name(pubkey) if pubkey else None
    canonical_id = nip05_name if nip05_name else identifier
    og_url = f"https://{RELAY_DOMAIN}/p/{canonical_id}"
    og_image = f"https://{RELAY_DOMAIN}/img/logo-v5.png"

    dir_profile = db.get_directory_profile(pubkey) if pubkey else None
    if dir_profile:
        name = dir_profile.get("name", "")
        if name:
            og_title = f"{name} on Nostr"
        picture = dir_profile.get("picture", "")
        if picture and _is_safe_url(picture):
            og_image = picture
        bio = dir_profile.get("about", "")
        if bio:
            og_description = bio.replace('\n', ' ').replace('\r', ' ')[:200]
    json_ld_tag = ""
    if dir_profile:
        badges = json.loads(dir_profile.get("badges", "[]"))
        badge_text = ", ".join(b.replace("-", " ").title() for b in badges[:3])
        reputation = dir_profile.get("reputation_score", 0)
        if badge_text:
            og_description += f" | {badge_text}"
        if reputation > 0:
            og_description += f" | Reputation: {reputation}/100"
        og_description = og_description[:200]
        json_ld = {
            "@context": "https://schema.org",
            "@type": "Person",
            "name": dir_profile.get("name") or og_title,
            "url": og_url,
            "image": og_image,
            "description": og_description[:200],
            "memberOf": {
                "@type": "Organization",
                "name": RELAY_DOMAIN,
                "url": f"https://{RELAY_DOMAIN}"
            }
        }
        json_ld_str = json.dumps(json_ld, ensure_ascii=True).replace("</", "<\\/")
        json_ld_tag = f'\n    <script type="application/ld+json">{json_ld_str}</script>'

    og_title_esc = html_module.escape(og_title, quote=True)
    og_description_esc = html_module.escape(og_description, quote=True)
    og_image_esc = html_module.escape(og_image, quote=True)
    og_url_esc = html_module.escape(og_url, quote=True)

    # Read card template and inject OG tags
    try:
        with open("/srv/www/card.html", "r") as f:
            html = f.read()
    except FileNotFoundError:
        html = CARD_HTML_FALLBACK

    # Replace the placeholder OG tags
    og_tags = f'''<meta property="og:type" content="profile">
    <meta property="og:site_name" content="{RELAY_DOMAIN}">
    <meta property="og:title" content="{og_title_esc}">
    <meta property="og:description" content="{og_description_esc}">
    <meta property="og:image" content="{og_image_esc}">
    <meta property="og:url" content="{og_url_esc}">
    <meta name="twitter:card" content="summary">
    <meta name="twitter:title" content="{og_title_esc}">
    <meta name="twitter:description" content="{og_description_esc}">
    <meta name="twitter:image" content="{og_image_esc}">'''

    html = html.replace(
        '<!-- Dynamic OG tags set by JS -->\n    <meta property="og:type" content="profile">\n    <meta property="og:site_name" content="__RELAY_DOMAIN__">\n    <meta name="twitter:card" content="summary">',
        og_tags
    )
    # Also update the title
    html = html.replace('<title>Nostr Card</title>', f'<title>{og_title_esc} - {RELAY_DOMAIN}</title>')

    # Inject JSON-LD for directory members
    if json_ld_tag:
        html = html.replace('</head>', f'{json_ld_tag}\n</head>')

    return HTMLResponse(content=html)


# --- Wrapped Page ---

WRAPPED_HTML_FALLBACK = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta http-equiv="refresh" content="0;url=/wrapped.html"></head><body>Redirecting...</body></html>'


@app.get("/wrapped/{identifier}")
async def wrapped_page(identifier: str, request: Request):
    """Serve the Wrapped HTML page with server-side OG meta tags for social media previews."""
    rate_limiter.check(f"wrapped-page:{identifier}", max_requests=30, window_seconds=600)
    rate_limiter.check(f"wrapped-page-ip:{request.client.host}", max_requests=60, window_seconds=600)

    if identifier.startswith("npub1"):
        try:
            pubkey = npub_to_hex(identifier)
            npub = identifier
        except ValueError:
            npub = identifier
            pubkey = None
    else:
        pubkey = db.get_nip05(identifier)
        if pubkey:
            npub = hex_to_npub(pubkey)
        else:
            npub = identifier
            pubkey = None

    og_title = "Nostr Wrapped"
    og_description = f"Nostr activity summary on {RELAY_DOMAIN}"
    og_url = f"https://{RELAY_DOMAIN}/wrapped/{identifier}"
    og_image = f"https://{RELAY_DOMAIN}/img/logo-v5.png"

    if pubkey:
        sub = db.get_subscription(pubkey)
        if not sub:
            raise HTTPException(status_code=404, detail="Profile not found")
        if not _is_subscription_active(sub):
            raise HTTPException(status_code=404, detail="Profile not found")

        # Fetch wrapped data for OG tags
        try:
            wrapped_resp = await _generate_wrapped(npub, pubkey)
            wrapped_data = json.loads(wrapped_resp.body.decode())
            name = wrapped_data.get("user_name", "")
            if name:
                og_title = f"{name}'s Nostr Wrapped"
            picture = wrapped_data.get("user_picture", "")
            if picture and _is_safe_url(picture):
                og_image = picture
            # Build description from stats
            total = wrapped_data.get("total_events", 0)
            notes = wrapped_data.get("total_notes", 0)
            zaps = wrapped_data.get("total_zaps_received", 0)
            sats = wrapped_data.get("total_sats_received", 0)
            first_ts = wrapped_data.get("first_event_ts")
            tip_height = wrapped_data.get("block_height")
            tip_time = wrapped_data.get("block_time")
            followers = wrapped_data.get("followers_count", 0)
            parts = []
            if followers:
                parts.append(f"{followers:,} followers")
            if total:
                parts.append(f"{total:,} events")
            if notes:
                parts.append(f"{notes:,} notes")
            if zaps:
                parts.append(f"{zaps:,} zaps (earned {sats:,} sats)")
            if tip_height and first_ts and tip_time:
                created_block = tip_height - round((tip_time - first_ts) / 600)
                parts.append(f"since block #{created_block:,}")
            elif wrapped_data.get("account_age_days"):
                parts.append(f"{wrapped_data['account_age_days']:,} days on Nostr")
            if parts:
                og_description = " | ".join(parts)
        except Exception:
            pass
    else:
        raise HTTPException(status_code=404, detail="Profile not found")

    og_title_esc = html_module.escape(og_title, quote=True)
    og_description_esc = html_module.escape(og_description, quote=True)
    og_image_esc = html_module.escape(og_image, quote=True)
    og_url_esc = html_module.escape(og_url, quote=True)

    try:
        with open("/srv/www/wrapped.html", "r") as f:
            html_content = f.read()
    except FileNotFoundError:
        html_content = WRAPPED_HTML_FALLBACK

    og_tags = f'''<!-- OG_TAGS -->
    <meta property="og:type" content="website">
    <meta property="og:site_name" content="{RELAY_DOMAIN}">
    <meta property="og:title" content="{og_title_esc}">
    <meta property="og:description" content="{og_description_esc}">
    <meta property="og:image" content="{og_image_esc}">
    <meta property="og:url" content="{og_url_esc}">
    <meta name="twitter:card" content="summary">
    <meta name="twitter:title" content="{og_title_esc}">
    <meta name="twitter:description" content="{og_description_esc}">
    <meta name="twitter:image" content="{og_image_esc}">
    <!-- /OG_TAGS -->'''

    html_content = html_content.replace(
        '<!-- OG_TAGS -->\n    <meta property="og:type" content="website">\n    <meta property="og:site_name" content="__RELAY_DOMAIN__">\n    <meta name="twitter:card" content="summary">\n    <!-- /OG_TAGS -->',
        og_tags
    )
    html_content = html_content.replace(
        '<title>Nostr Wrapped</title>',
        f'<title>{og_title_esc} - {RELAY_DOMAIN}</title>'
    )

    return HTMLResponse(content=html_content)


# --- Wrapped Data Generation ---

async def _generate_wrapped(npub: str, pubkey: str):
    cached = _cache_get(_wrapped_cache, pubkey, WRAPPED_CACHE_TTL)
    if cached is not None:
        return JSONResponse(content=cached, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    my_events, tagged_events, archive_stats, primal_stats = await _wrapped_fetch_events(npub, pubkey)
    my_events = _wrapped_dedup_replaceable(my_events)

    stats = await _wrapped_compute_stats(pubkey, my_events, tagged_events, archive_stats, primal_stats)

    wrapped = _wrapped_build_result(npub, pubkey, stats)

    logger.info(f"Wrapped: generated for {npub[:20]}... ({stats['total_events']} events)")
    _cache_set(_wrapped_cache, pubkey, wrapped)
    return JSONResponse(content=wrapped, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})


async def _wrapped_fetch_events(npub, pubkey):
    """Fetch author events, tagged interactions, archive stats, and Primal stats in parallel."""
    interaction_relays = [discovery.STRFRY_URL] + _INTERACTION_RELAYS
    my_events, tagged_events, archive_stats, primal_stats = await asyncio.gather(
        discovery.fetch_author_events(pubkey, [discovery.STRFRY_URL], limit=50000),
        discovery.fetch_tagged_events(pubkey, interaction_relays, kinds=[1, 6, 7, 9735]),
        _fetch_nostrarchives_stats(pubkey),
        _fetch_primal_user_profile(pubkey),
        return_exceptions=True)
    if isinstance(my_events, Exception):
        my_events = []
    if isinstance(tagged_events, Exception):
        tagged_events = []
    if isinstance(archive_stats, Exception):
        archive_stats = None
    if isinstance(primal_stats, Exception):
        primal_stats = None
    if not my_events:
        logger.info(f"Wrapped: strfry empty for {npub[:20]}..., falling back to external relays")
        fallback_relays = await discovery.discover_relays(pubkey)
        my_events = await discovery.fetch_author_events(pubkey, fallback_relays, limit=50000)
    return my_events, tagged_events, archive_stats, primal_stats


def _wrapped_dedup_replaceable(events):
    """Deduplicate replaceable events (kind 0, 3, 10002) — keep newest per kind."""
    REPLACEABLE = {0, 3, 10002}
    best = {}
    non_rep = []
    for ev in events:
        k = ev.get("kind", 0)
        if k in REPLACEABLE:
            if k not in best or ev.get("created_at", 0) > best[k].get("created_at", 0):
                best[k] = ev
        else:
            non_rep.append(ev)
    return non_rep + list(best.values())


def _wrapped_classify_kind1(ev):
    """Return True if a kind 1 event is a root note (not a reply)."""
    tags = ev.get("tags", [])
    e_tags = [t for t in tags if len(t) >= 2 and t[0] == "e"]
    if not e_tags:
        return True
    markers = set()
    has_unmarked = False
    for t in e_tags:
        if len(t) >= 4:
            markers.add(t[3])
        else:
            has_unmarked = True
    return markers <= {"mention"} and not has_unmarked


def _wrapped_compute_fan_scores(pubkey, reactions, reposts_received, replies_received, zaps):
    """Compute weighted fan scores from all interaction types."""
    scores = {}
    for ev in reactions:
        fan = ev.get("pubkey", "")
        if fan:
            scores[fan] = scores.get(fan, 0) + 1
    for ev in reposts_received:
        fan = ev.get("pubkey", "")
        if fan:
            scores[fan] = scores.get(fan, 0) + 3
    for ev in replies_received:
        fan = ev.get("pubkey", "")
        if fan:
            scores[fan] = scores.get(fan, 0) + 5
    for z in zaps:
        fan = None
        desc_tag = next((t[1] for t in z.get("tags", []) if len(t) >= 2 and t[0] == "description"), None)
        if desc_tag:
            try:
                fan = json.loads(desc_tag).get("pubkey", "")
            except (json.JSONDecodeError, AttributeError):
                pass
        if not fan or fan == pubkey:
            continue
        zap_score = 10
        zap_sats = _extract_zap_sats(z)
        scores[fan] = scores.get(fan, 0) + zap_score + zap_sats // 1000
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]


def _wrapped_compute_zap_stats(zaps):
    """Compute total zap sats and most-zapped note."""
    total_sats = 0
    zapped_notes = {}
    for z in zaps:
        sats = _extract_zap_sats(z)
        total_sats += sats
        e_tag = next((t[1] for t in z.get("tags", []) if len(t) >= 2 and t[0] == "e"), None)
        if e_tag:
            zapped_notes[e_tag] = zapped_notes.get(e_tag, 0) + 1
    most_zapped = max(zapped_notes.items(), key=lambda x: x[1])[0] if zapped_notes else None
    most_zapped_count = max(zapped_notes.values()) if zapped_notes else 0
    return total_sats, most_zapped, most_zapped_count


async def _wrapped_compute_stats(pubkey, my_events, tagged_events, archive_stats, primal_stats=None):
    """Compute all wrapped statistics from events."""
    reactions = [ev for ev in tagged_events if ev.get("kind") == 7 and ev.get("pubkey") != pubkey]
    reposts_received = [ev for ev in tagged_events if ev.get("kind") == 6 and ev.get("pubkey") != pubkey]
    zaps = [ev for ev in tagged_events if ev.get("kind") == 9735]
    replies_received = [ev for ev in tagged_events if ev.get("kind") == 1 and ev.get("pubkey") != pubkey]

    total_events = len(my_events)
    kind_counts = {}
    hour_counts = [0] * 24
    for ev in my_events:
        k = ev.get("kind", 0)
        kind_counts[k] = kind_counts.get(k, 0) + 1
        hour_counts[datetime.fromtimestamp(ev.get("created_at", 0), tz=timezone.utc).hour] += 1

    total_notes = sum(1 for ev in my_events if ev.get("kind") == 1 and _wrapped_classify_kind1(ev))
    total_replies = kind_counts.get(1, 0) - total_notes

    top_reactors = _wrapped_compute_fan_scores(pubkey, reactions, reposts_received, replies_received, zaps)
    reactor_pubkeys = [pk for pk, _ in top_reactors]
    reactor_profiles = await discovery.fetch_profiles(list(set([pubkey] + reactor_pubkeys)))

    zap_sats, most_zapped_note, most_zapped_count = _wrapped_compute_zap_stats(zaps)

    timestamps = [ev.get("created_at", 0) for ev in my_events if ev.get("created_at")]
    first_event_id = None
    if timestamps:
        min_ts = min(timestamps)
        for ev in my_events:
            if ev.get("created_at") == min_ts and ev.get("id"):
                first_event_id = ev["id"]
                break

    return {
        "total_events": total_events, "kind_counts": kind_counts, "hour_counts": hour_counts,
        "total_notes": total_notes, "total_replies": total_replies,
        "reactions": reactions, "zaps": zaps, "zap_sats": zap_sats,
        "top_reactors": top_reactors, "reactor_profiles": reactor_profiles,
        "most_zapped_note": most_zapped_note, "most_zapped_count": most_zapped_count,
        "timestamps": timestamps, "first_event_id": first_event_id,
        "archive_stats": archive_stats, "primal_stats": primal_stats,
    }


def _wrapped_build_result(npub, pubkey, s):
    """Build the final wrapped response dict."""
    timestamps = s["timestamps"]
    kind_counts = s["kind_counts"]
    hour_counts = s["hour_counts"]
    profiles = s["reactor_profiles"]
    archive = s["archive_stats"]
    primal = s.get("primal_stats") or {}

    user_profile = profiles.get(pubkey, {})
    busiest_hour = hour_counts.index(max(hour_counts)) if s["total_events"] > 0 else 0

    followers_count = following_count = archive_zaps = archive_sats = zaps_sent_count = zaps_sent_sats = 0
    if archive and isinstance(archive, dict):
        followers_count = archive.get("followers_count", 0)
        following_count = archive.get("following_count", 0)
        archive_zaps = archive.get("zaps_received_count", 0)
        archive_sats = archive.get("zaps_received_sats", 0)
        zaps_sent_count = archive.get("zaps_sent_count", 0)
        zaps_sent_sats = archive.get("zaps_sent_sats", 0)

    # Primal provides uncapped counts — take max across all sources
    p_followers = primal.get("followers_count", 0)
    p_following = primal.get("follows_count", 0)
    p_notes = primal.get("note_count", 0)
    p_replies = primal.get("reply_count", 0)
    p_reposts = primal.get("repost_count", 0)
    p_zap_count = primal.get("total_zap_count", 0)
    p_zap_sats = primal.get("total_satszapped", 0)
    p_media = primal.get("media_count", 0)
    p_time_joined = primal.get("time_joined", 0)

    # Best first_event_ts: min of fetched events vs Primal time_joined
    first_ts_candidates = []
    if timestamps:
        first_ts_candidates.append(min(timestamps))
    if p_time_joined > 0:
        first_ts_candidates.append(p_time_joined)
    best_first_ts = min(first_ts_candidates) if first_ts_candidates else None

    return {
        "npub": npub,
        "user_name": user_profile.get("name", ""),
        "user_picture": user_profile.get("picture", ""),
        "user_nip05": user_profile.get("nip05", ""),
        "total_events": max(s["total_events"], p_notes + p_replies + p_reposts),
        "total_posts": max(kind_counts.get(1, 0), p_notes + p_replies),
        "total_notes": max(s["total_notes"], p_notes),
        "total_replies": max(s["total_replies"], p_replies),
        "total_reactions_sent": kind_counts.get(7, 0),
        "total_reposts": max(kind_counts.get(6, 0), p_reposts),
        "total_reactions_received": len(s["reactions"]),
        "total_zaps_received": max(len(s["zaps"]), archive_zaps, p_zap_count),
        "total_sats_received": max(s["zap_sats"], archive_sats, p_zap_sats),
        "zaps_sent_count": zaps_sent_count,
        "zaps_sent_sats": zaps_sent_sats,
        "followers_count": max(followers_count, p_followers),
        "following_count": max(following_count, p_following),
        "media_count": p_media,
        "most_zapped_note": s["most_zapped_note"],
        "most_zapped_count": s["most_zapped_count"],
        "top_reactors": [{
            "pubkey": p, "count": c,
            "name": profiles.get(p, {}).get("name", ""),
            "picture": profiles.get(p, {}).get("picture", ""),
            "nip05": profiles.get(p, {}).get("nip05", ""),
        } for p, c in s["top_reactors"]],
        "busiest_hour_utc": busiest_hour,
        "busiest_hour_events": max(hour_counts) if s["total_events"] > 0 else 0,
        "hour_distribution": hour_counts,
        "kind_breakdown": {str(k): v for k, v in sorted(kind_counts.items())},
        "first_event": datetime.fromtimestamp(best_first_ts, tz=timezone.utc).strftime("%Y-%m-%d") if best_first_ts else None,
        "first_event_ts": best_first_ts,
        "first_event_id": s["first_event_id"],
        "last_event": datetime.fromtimestamp(max(timestamps), tz=timezone.utc).strftime("%Y-%m-%d") if timestamps else None,
        "account_age_days": (int(time.time()) - best_first_ts) // 86400 if best_first_ts else 0,
        "active_days": len(set(datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d") for ts in timestamps)) if timestamps else 0,
        "block_height": _get_block_height(),
        "block_time": int(time.time()) if _get_block_height() else None,
    }


# --- External Data Fetchers ---

async def _fetch_latest_activity_external(hex_pubkey: str) -> int | None:
    """Fetch the most recent event timestamp from external relays."""
    best_ts = 0
    for relay_url in _PUBLIC_RELAYS:
        try:
            events = await discovery._fetch_from_relay(
                relay_url,
                {"authors": [hex_pubkey], "kinds": [1, 6, 7, 30023], "limit": 1},
                timeout=4,
            )
            for ev in events:
                ts = ev.get("created_at", 0)
                if ts > best_ts:
                    best_ts = ts
        except Exception:
            continue
    return best_ts if best_ts > 0 else None


async def _fetch_profile_from_relays(hex_pubkey: str) -> dict | None:
    """Fetch kind 0 (profile metadata) from relays for non-member lookup."""
    relays = ["ws://strfry:7777", "wss://purplepag.es"] + _PUBLIC_RELAYS[:2]

    async def _try_relay(url):
        try:
            events = await discovery._fetch_from_relay(
                url, {"kinds": [0], "authors": [hex_pubkey], "limit": 5}, timeout=4)
            if events:
                return max(events, key=lambda e: e.get("created_at", 0))
        except Exception:
            pass
        return None

    # Query all relays in parallel, take the newest result
    results = await asyncio.gather(*[_try_relay(r) for r in relays], return_exceptions=True)
    best = None
    for ev in results:
        if isinstance(ev, dict) and ev.get("created_at"):
            if not best or ev["created_at"] > best["created_at"]:
                best = ev
    if not best:
        return None
    try:
        content = json.loads(best.get("content", "{}"))
    except (json.JSONDecodeError, TypeError):
        content = {}
    try:
        npub = hex_to_npub(hex_pubkey)
    except Exception:
        npub = ""
    return {
        "pubkey": hex_pubkey,
        "npub": npub,
        "name": content.get("display_name") or content.get("name") or "",
        "picture": content.get("picture") or "",
        "nip05_display": content.get("nip05") or "",
        "about": content.get("about") or "",
        "lud16": (content.get("lud16") or "") if ("@" in (content.get("lud16") or "") and not (content.get("lud16") or "").endswith("@npub.cash")) else "",
    }


async def _fetch_nostrarchives_stats(hex_pubkey: str) -> dict | None:
    """Fetch profile stats from nostrarchives.com API (fast, pre-indexed)."""
    base = "https://api.nostrarchives.com"
    results = {}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            # Parallel fetch: social + zap-stats
            social_req = client.get(f"{base}/v1/social/{hex_pubkey}",
                                    params={"follows_limit": 0, "followers_limit": 0})
            zap_req = client.get(f"{base}/v1/profiles/{hex_pubkey}/zap-stats")

            social_resp, zap_resp = await asyncio.gather(
                social_req, zap_req, return_exceptions=True)

            if not isinstance(social_resp, Exception) and social_resp.status_code == 200:
                sd = social_resp.json()
                results["followers_count"] = sd.get("followers", {}).get("count", 0)
                results["following_count"] = sd.get("follows", {}).get("count", 0)

            if not isinstance(zap_resp, Exception) and zap_resp.status_code == 200:
                zd = zap_resp.json()
                recv = zd.get("received", {})
                sent = zd.get("sent", {})
                results["zaps_received_count"] = recv.get("zap_count", 0)
                results["zaps_received_sats"] = recv.get("total_sats", 0)
                results["zaps_sent_count"] = sent.get("zap_count", 0)
                results["zaps_sent_sats"] = sent.get("total_sats", 0)

        return results if results else None
    except Exception:
        return None


async def _fetch_primal_user_profile(hex_pubkey: str) -> dict | None:
    """Fetch uncapped user stats from Primal cache service (with retry)."""
    for attempt in range(3):
        try:
            sub_id = f"primal-{secrets.token_hex(4)}"
            async with websockets.connect("wss://cache.primal.net/v1", open_timeout=4, close_timeout=2) as ws:
                await ws.send(json.dumps(["REQ", sub_id, {"cache": ["user_profile", {"pubkey": hex_pubkey}]}]))
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        break
                    data = json.loads(msg)
                    if data[0] == "EOSE":
                        break
                    if data[0] == "EVENT" and len(data) >= 3:
                        ev = data[2]
                        if ev.get("kind") == 10000105:
                            try:
                                return json.loads(ev.get("content", "{}"))
                            except Exception:
                                pass
        except Exception:
            if attempt < 2:
                await asyncio.sleep(0.5)
    return None


async def _fetch_social_stats_nip45(hex_pubkey: str) -> dict | None:
    """Fetch social stats via NIP-45 COUNT + Primal cache + event lists from relays."""

    # NIP-45 COUNT relays — only for metrics Primal doesn't provide
    count_relays = ["wss://nos.lol", "wss://relay.ditto.pub"]
    # Event fetch relays — for muted_by/reported_by lists
    event_relays = ["wss://nos.lol", "wss://relay.ditto.pub", "wss://nostr.wine"]

    # Only query NIP-45 for what Primal doesn't give us
    queries = {
        "muted_by_count": {"kinds": [10000], "#p": [hex_pubkey]},
        "reported_by_count": {"kinds": [1984], "#p": [hex_pubkey]},
        "muting_count": {"kinds": [10000], "authors": [hex_pubkey]},
        "reporting_count": {"kinds": [1984], "authors": [hex_pubkey]},
    }

    results = {}

    # --- COUNT from multiple relays ---
    async def _count_from_relay(relay_url):
        relay_counts = {}
        try:
            async with websockets.connect(relay_url, open_timeout=2, close_timeout=1) as ws:
                sub_map = {}
                for key, filt in queries.items():
                    sub_id = f"cnt-{secrets.token_hex(3)}"
                    sub_map[sub_id] = key
                    await ws.send(json.dumps(["COUNT", sub_id, filt]))

                pending = set(sub_map.keys())
                while pending:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=3)
                    except asyncio.TimeoutError:
                        break
                    data = json.loads(msg)
                    if data[0] == "COUNT" and len(data) >= 3 and data[1] in sub_map:
                        key = sub_map[data[1]]
                        count_obj = data[2] if isinstance(data[2], dict) else {}
                        relay_counts[key] = count_obj.get("count", 0)
                        pending.discard(data[1])
        except Exception:
            pass
        return relay_counts

    async def _merge_counts():
        try:
            all_counts = await asyncio.gather(
                *[_count_from_relay(r) for r in count_relays],
                return_exceptions=True,
            )
            for rc in all_counts:
                if isinstance(rc, dict):
                    for key, val in rc.items():
                        results[key] = max(results.get(key, 0), val)
        except Exception:
            pass

    # --- Event fetch from multiple relays, deduplicate ---
    async def _fetch_multi(filters, timeout=3):
        async def _one(url):
            try:
                return await discovery._fetch_from_relay(url, filters, timeout=timeout)
            except Exception:
                return []
        batches = await asyncio.gather(*[_one(r) for r in event_relays], return_exceptions=True)
        seen = set()
        merged = []
        for batch in batches:
            if isinstance(batch, list):
                for ev in batch:
                    eid = ev.get("id", "")
                    if eid and eid not in seen:
                        seen.add(eid)
                        merged.append(ev)
        return merged

    async def _fetch_muted_by():
        try:
            events = await _fetch_multi({"kinds": [10000], "#p": [hex_pubkey], "limit": 200})
            results["muted_by_list"] = list(dict.fromkeys(e["pubkey"] for e in events if e.get("pubkey")))
        except Exception:
            results["muted_by_list"] = []

    async def _fetch_reported_by():
        try:
            events = await _fetch_multi({"kinds": [1984], "#p": [hex_pubkey], "limit": 200})
            results["reported_by_list"] = list(dict.fromkeys(e["pubkey"] for e in events if e.get("pubkey")))
        except Exception:
            results["reported_by_list"] = []

    async def _fix_muting():
        try:
            events = await _fetch_multi({"kinds": [10000], "authors": [hex_pubkey], "limit": 1})
            if events:
                best = max(events, key=lambda e: e.get("created_at", 0))
                pks = list(dict.fromkeys(t[1] for t in best.get("tags", []) if len(t) >= 2 and t[0] == "p"))
                results["muting_count"] = len(pks)
                results["muting_list"] = pks
            else:
                results["muting_list"] = []
        except Exception:
            results["muting_list"] = []

    async def _fetch_reporting():
        try:
            events = await _fetch_multi({"kinds": [1984], "authors": [hex_pubkey], "limit": 200})
            pks = list(dict.fromkeys(
                t[1] for e in events for t in e.get("tags", [])
                if len(t) >= 2 and t[0] == "p"
            ))
            results["reporting_list"] = pks
        except Exception:
            results["reporting_list"] = []

    async def _fetch_primal_stats():
        """Fetch uncapped stats from Primal cache service."""
        p = await _fetch_primal_user_profile(hex_pubkey)
        if not p:
            return
        mapping = {
            "notes_count": p.get("note_count", 0) + p.get("reply_count", 0),
            "followers_count": p.get("followers_count", 0),
            "following_count": p.get("follows_count", 0),
            "zaps_received_count": p.get("total_zap_count", 0),
        }
        for key, val in mapping.items():
            if val and val > results.get(key, 0):
                results[key] = val
        tj = p.get("time_joined", 0)
        if tj:
            results["_time_joined"] = tj
        mc = p.get("media_count", 0)
        if mc:
            results["media_count"] = mc

    # Run ALL phases in parallel
    await asyncio.gather(
        _fetch_primal_stats(),
        _merge_counts(),
        _fetch_muted_by(),
        _fetch_reported_by(),
        _fix_muting(),
        _fetch_reporting(),
    )
    return results if results else None


async def _resolve_pubkey_profiles(pubkeys: list[str], limit: int = 10,
                                   observer_pubkey: str | None = None) -> list[dict]:
    """Batch-resolve profiles for a list of pubkeys.
    Returns list of {pubkey, name, picture, npub, verified}.
    If observer_pubkey is provided, 'verified' is True when GrapeRank score >= 0.02.
    Results are sorted: verified first (by score desc), then unverified."""
    if not pubkeys:
        return []
    to_resolve = pubkeys[:limit]

    # Check local directory_profiles first
    profiles = {}
    try:
        pg = db._get_pg()
        with pg.cursor() as cur:
            ph = ",".join(["%s"] * len(to_resolve))
            cur.execute(
                f"SELECT hex_pubkey, name, picture FROM directory_profiles WHERE hex_pubkey IN ({ph})",
                to_resolve)
            for row in cur.fetchall():
                profiles[row["hex_pubkey"]] = {"name": row["name"] or "", "picture": row["picture"] or ""}
    except Exception:
        pass

    # Fetch missing from purplepag.es (fast for kind 0 batch lookups)
    missing = [pk for pk in to_resolve if pk not in profiles]
    if missing:
        try:
            events = await discovery._fetch_from_relay(
                "wss://purplepag.es",
                {"kinds": [0], "authors": missing, "limit": len(missing)},
                timeout=4,
            )
            for ev in events:
                pk = ev.get("pubkey", "")
                if pk and pk not in profiles:
                    try:
                        content = json.loads(ev.get("content", "{}"))
                        profiles[pk] = {
                            "name": content.get("display_name") or content.get("name") or "",
                            "picture": content.get("picture") or "",
                        }
                    except Exception:
                        pass
        except Exception:
            pass

    # Look up GrapeRank scores for verified flag
    scores = {}
    if observer_pubkey and to_resolve:
        try:
            pg = db._get_pg()
            with pg.cursor() as cur:
                ph = ",".join(["%s"] * len(to_resolve))
                cur.execute(
                    f"SELECT target_pubkey, score FROM personalized_scores "
                    f"WHERE observer_pubkey = %s AND target_pubkey IN ({ph})",
                    [observer_pubkey] + to_resolve)
                for row in cur.fetchall():
                    scores[row["target_pubkey"]] = row["score"] or 0
        except Exception:
            pass

    result = []
    for pk in to_resolve:
        p = profiles.get(pk, {})
        sc = scores.get(pk, 0)
        try:
            npub = hex_to_npub(pk)
        except Exception:
            npub = pk[:16] + "..."
        result.append({
            "pubkey": pk,
            "npub": npub,
            "name": p.get("name", ""),
            "picture": p.get("picture", ""),
            "verified": sc >= 0.02,
            "_score": sc,
        })

    # Sort: verified first (highest score), then unverified
    result.sort(key=lambda x: (-int(x["verified"]), -x["_score"]))
    for r in result:
        del r["_score"]

    return result


async def _fetch_user_follow_list(hex_pubkey: str) -> set[str] | None:
    """Fetch a user's kind 3 (follow list) — try strfry first, then external relays."""
    # Try local strfry first
    try:
        events = await discovery._fetch_from_relay(
            "ws://strfry:7777",
            {"kinds": [3], "authors": [hex_pubkey], "limit": 5},
            timeout=5,
        )
        if events:
            best = max(events, key=lambda e: e.get("created_at", 0))
            follows = set()
            for tag in best.get("tags", []):
                if len(tag) >= 2 and tag[0] == "p" and len(tag[1]) == 64:
                    follows.add(tag[1])
            if follows:
                return follows
    except Exception:
        pass

    # Fall back to external relays
    external_relays = _PUBLIC_RELAYS + ["wss://purplepag.es"]
    tasks = [
        discovery._fetch_from_relay(r, {"kinds": [3], "authors": [hex_pubkey], "limit": 5}, timeout=10)
        for r in external_relays
    ]
    results = await asyncio.gather(*tasks)
    all_events = []
    for relay_events in results:
        all_events.extend(relay_events)

    if not all_events:
        return None

    best = max(all_events, key=lambda e: e.get("created_at", 0))
    follows = set()
    for tag in best.get("tags", []):
        if len(tag) >= 2 and tag[0] == "p" and len(tag[1]) == 64:
            follows.add(tag[1])
    return follows if follows else None


# --- Subscriber Directory ---

_directory_cache = {}
_directory_stats_cache = {}
DIRECTORY_CACHE_TTL = 60  # 1 minute (data is pre-computed by indexer)


@app.get("/api/directory/stats")
async def directory_stats(request: Request):
    """Public endpoint: aggregate directory statistics."""
    rate_limiter.check(f"dir-stats:{request.client.host}", max_requests=60, window_seconds=600)
    cached = _cache_get(_directory_stats_cache, "stats", DIRECTORY_CACHE_TTL)
    if cached is not None:
        return cached
    result = db.get_directory_stats()
    _cache_set(_directory_stats_cache, "stats", result)
    return result


_directory_tags_cache = {}

@app.get("/api/directory/tags")
async def directory_all_tags(request: Request):
    """Public endpoint: all directory tags with member counts."""
    rate_limiter.check(f"dir-tags-all:{request.client.host}", max_requests=60, window_seconds=600)
    cached = _cache_get(_directory_tags_cache, "tags", DIRECTORY_CACHE_TTL)
    if cached is not None:
        return cached
    tags = db.get_all_directory_tags()
    result = [{"name": t[0], "count": t[1]} for t in tags]
    _cache_set(_directory_tags_cache, "tags", result)
    return result


@app.get("/api/directory/list-header")
async def directory_list_header(request: Request):
    """Public endpoint: return the decentralized list header event ID."""
    rate_limiter.check(f"dir-listhdr:{request.client.host}", max_requests=30, window_seconds=60)
    header_id = db.get_relay_state("list_header_event_id")
    if not header_id:
        raise HTTPException(status_code=404, detail="List header not published yet")
    return {"list_header_event_id": header_id}


@app.get("/api/directory/can-self-sign/{hex_pubkey}")
async def directory_can_self_sign(hex_pubkey: str, request: Request):
    """Public endpoint: check if a pubkey is eligible to self-sign."""
    rate_limiter.check(f"dir-selfsign:{request.client.host}", max_requests=10, window_seconds=60)
    if not _is_valid_hex_pubkey(hex_pubkey):
        return {"eligible": False}
    listed = db.get_directory_listed(hex_pubkey)
    return {"eligible": bool(listed)}


@app.get("/api/directory/status/{npub}")
async def directory_status(npub: str, request: Request, token: str = ""):
    """Check directory listing status and tags for a subscriber."""
    rate_limiter.check(f"dir-status:{npub}", max_requests=20, window_seconds=60)
    if not verify_token(npub, extract_token(request)):
        raise HTTPException(status_code=403, detail="Invalid auth token.")
    try:
        pubkey = npub_to_hex(npub)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid npub format")
    listed = db.get_directory_listed(pubkey)
    tags = db.get_directory_tags(pubkey)
    self_signed = db.is_self_signed(pubkey) if listed else False
    return {"listed": listed, "tags": tags, "self_signed": self_signed}


@app.post("/api/directory/toggle/{npub}")
async def directory_toggle(npub: str, request: Request, token: str = ""):
    """Toggle directory listing for a subscriber."""
    rate_limiter.check(f"dir-toggle:{npub}", max_requests=10, window_seconds=60)
    if not verify_token(npub, extract_token(request)):
        raise HTTPException(status_code=403, detail="Invalid auth token.")
    try:
        pubkey = npub_to_hex(npub)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid npub format")
    sub = db.get_subscription(pubkey)
    if not sub or not _is_subscription_active(sub):
        raise HTTPException(status_code=403, detail="Active subscription required")
    current = db.get_directory_listed(pubkey)
    new_state = not current
    db.set_directory_listed(pubkey, new_state)
    _directory_cache.clear()
    return {"listed": new_state}


@app.post("/api/directory/tags/{npub}")
async def directory_tags_update(npub: str, request: Request, token: str = ""):
    """Update directory tags for a subscriber."""
    rate_limiter.check(f"dir-tags:{npub}", max_requests=10, window_seconds=60)
    if not verify_token(npub, extract_token(request)):
        raise HTTPException(status_code=403, detail="Invalid auth token.")
    try:
        pubkey = npub_to_hex(npub)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid npub format")
    sub = db.get_subscription(pubkey)
    if not sub or not _is_subscription_active(sub):
        raise HTTPException(status_code=403, detail="Active subscription required")
    body = await request.json()
    tags = body.get("tags", [])
    if not isinstance(tags, list):
        raise HTTPException(status_code=400, detail="Tags must be a list")
    # Validate: max 10 tags, max 30 chars each, alphanumeric + hyphens
    cleaned = []
    for t in tags[:20]:
        # Split multi-word entries into individual tags
        words = re.split(r'[\s,]+', str(t).strip().lower())
        for w in words:
            w = w[:30]
            if w and re.match(r'^[a-z0-9\-]+$', w) and w not in cleaned:
                cleaned.append(w)
                if len(cleaned) >= 10:
                    break
        if len(cleaned) >= 10:
            break
    db.set_directory_tags(pubkey, cleaned)
    _directory_cache.clear()
    return {"tags": cleaned}


@app.post("/api/directory/reindex/{npub}")
async def directory_reindex(npub: str, request: Request):
    """Trigger quick-index for a manually added subscriber. Operator only."""
    op_key = request.headers.get("x-operator-key", "") or request.query_params.get("key", "")
    if not op_key or not hmac.compare_digest(op_key, OPERATOR_API_KEY):
        raise HTTPException(status_code=403, detail="Forbidden")
    rate_limiter.check(f"reindex:{npub}", max_requests=5, window_seconds=60)
    if not npub.startswith("npub1"):
        raise HTTPException(status_code=400, detail="Invalid npub")
    try:
        pubkey = npub_to_hex(npub)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid npub checksum")
    sub = db.get_subscription(pubkey)
    if not sub:
        raise HTTPException(status_code=404, detail="No subscription found")
    _safe_task(directory_indexer.index_single_pubkey(pubkey), f"manual-quick-index")
    _directory_cache.clear()
    return {"status": "indexing", "pubkey": pubkey}


# ---------------------------------------------------------------------------
# Discover Who to Follow — public recommendation engine
# ---------------------------------------------------------------------------

_recommendations_cache = {}
RECOMMENDATIONS_CACHE_TTL = 3600  # 1 hour


@app.get("/api/directory/recommendations/{npub}")
async def directory_recommendations(npub: str, request: Request):
    """Public endpoint: recommend directory members based on user's follow list."""
    rate_limiter.check(
        f"recommend:{request.client.host}",
        max_requests=30,
        window_seconds=600,
    )

    if not npub.startswith("npub1"):
        raise HTTPException(status_code=400, detail="Invalid npub format")
    try:
        hex_pubkey = npub_to_hex(npub)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid npub checksum")

    # Check Redis cache first
    cache_key = f"directory:recommendations:{hex_pubkey}"
    r = db._get_redis()
    if r:
        try:
            cached = r.get(cache_key)
            if cached:
                result = json.loads(cached)
                result["cached"] = True
                return result
        except Exception:
            pass

    # Determine if user is a member for score source selection
    is_member = db.is_subscribed(hex_pubkey)

    if is_member:
        # Tier 1: trigger on-demand GrapeRank for members
        has_scores, is_computing = await _ensure_scores(hex_pubkey)
    else:
        is_computing = False

    # Track visit for pre-warming
    _safe_task(asyncio.to_thread(db.update_directory_visit, hex_pubkey), "directory-visit")

    # Step 1: Fetch user's follow list
    user_follows = await _fetch_user_follow_list(hex_pubkey)
    if user_follows is None:
        raise HTTPException(
            status_code=404,
            detail="No follow list found. Follow some people using a Nostr client (Damus, Primal, Amethyst, etc.) first — recommendations are based on your network.",
        )

    # Step 2: Get all directory members
    members = db.get_all_directory_members()
    if not members:
        return {
            "npub": npub,
            "follows_count": len(user_follows),
            "directory_members_already_following": 0,
            "recommendations": [],
            "cached": False,
            "computed_at": int(time.time()),
        }

    member_by_pk = {m["pubkey"]: m for m in members}
    member_pks = set(member_by_pk.keys())

    already_following = member_pks & user_follows
    candidates = [m for m in members if m["pubkey"] not in user_follows and m["pubkey"] != hex_pubkey]

    if not candidates:
        result = {
            "npub": npub,
            "follows_count": len(user_follows),
            "directory_members_already_following": len(already_following),
            "recommendations": [],
            "cached": False,
            "computed_at": int(time.time()),
        }
        if r:
            try:
                r.setex(cache_key, RECOMMENDATIONS_CACHE_TTL, json.dumps(result))
            except Exception:
                pass
        return result

    # Step 3: Load trust edges into memory (one query) and build follower map
    edges = db.get_trust_edges()
    # followers_of[pk] = set of pubkeys that follow pk
    followers_of = defaultdict(set)
    for follower, followed in edges:
        followers_of[followed].add(follower)

    # Load personalized scores — Tier 1 for members, Tier 2 approximate for visitors
    if is_member:
        personalized_full = db.get_personalized_scores_with_hops(hex_pubkey)
        personalized = {pk: v[0] for pk, v in personalized_full.items()}
        personalized_hops = {pk: v[1] for pk, v in personalized_full.items()}
    else:
        # Tier 2: approximate scores from precomputed member data (1 batch query)
        precomputed = db.get_personalized_scores_batch(member_pks)
        global_scores = db.get_public_graperank_scores()
        personalized = db.approximate_visitor_scores(
            user_follows, dict(followers_of), precomputed, global_scores
        )
        personalized_hops = {}

    # Step 4: Score each candidate
    recommendations = []
    for candidate in candidates:
        cpk = candidate["pubkey"]

        # Mutual follows: how many of user's follows also follow this candidate?
        candidate_followers = followers_of.get(cpk, set())
        mutual_follows = user_follows & candidate_followers
        mutual_count = len(mutual_follows)

        # Average trust score of mutual follows (use personalized scores if available)
        avg_mutual_trust = 0
        if mutual_count > 0:
            trust_scores = [
                personalized.get(pk, 0.0)
                for pk in mutual_follows
                if pk in member_by_pk
            ]
            if trust_scores:
                avg_mutual_trust = min(1.0, sum(trust_scores) / len(trust_scores))

        # Candidate's trust: use personalized GrapeRank score, else 0
        candidate_trust = min(personalized.get(cpk, 0.0), 1.0)

        # Combined score
        trust_relevance = (
            0.5 * min(mutual_count / 20.0, 1.0) +
            0.3 * avg_mutual_trust +
            0.2 * candidate_trust
        )

        # Determine reason
        if mutual_count >= 3:
            reason = "followed_by_your_follows"
        elif candidate_trust > 0.7:
            reason = "highly_trusted"
        else:
            reason = "relevant_to_your_network"

        # Top 3 mutual follow names for display
        mutual_names = []
        if mutual_follows:
            named = []
            for pk in mutual_follows:
                if pk in member_by_pk:
                    named.append((member_by_pk[pk]["name"], personalized.get(pk, 0.0)))
            named.sort(key=lambda x: x[1], reverse=True)
            mutual_names = [n[0] for n in named[:3] if n[0]]

        recommendations.append({
            "hex_pubkey": cpk,
            "npub": candidate["npub"],
            "display_name": candidate["name"],
            "nip05": candidate["nip05_display"],
            "picture": candidate["picture"],
            "card_url": candidate["card_url"],
            "trust_score": round(personalized.get(cpk, 0.0), 4),
            "hops": personalized_hops.get(cpk),
            "reason": reason,
            "mutual_follow_count": mutual_count,
            "mutual_follow_names": mutual_names,
            "trust_relevance_score": round(trust_relevance, 3),
        })

    # Sort by relevance, return top 20
    recommendations.sort(key=lambda x: x["trust_relevance_score"], reverse=True)
    recommendations = recommendations[:20]

    result = {
        "npub": npub,
        "follows_count": len(user_follows),
        "directory_members_already_following": len(already_following),
        "recommendations": recommendations,
        "cached": False,
        "computed_at": int(time.time()),
    }
    if is_computing:
        result["trust_computing"] = True

    # Cache in Redis for 1 hour
    if r:
        try:
            r.setex(cache_key, RECOMMENDATIONS_CACHE_TTL, json.dumps(result))
        except Exception:
            pass

    return result


VALID_BADGES = {"nip05-live", "lightning-reachable", "relay-subscriber"}


@app.get("/api/directory")
async def directory_list(
    request: Request,
    page: int = 1,
    limit: int = 24,
    sort: str = "trust",
    badge: str = "",
    search: str = "",
    tag: str = "",
    observer: str = "",
    hops: int = 5,
    cluster: int = -1,
):
    """Directory listing. Personalized trust scores require NIP-98 auth by the observer."""
    rate_limiter.check(f"dir-list:{request.client.host}", max_requests=60, window_seconds=600)

    page = max(1, min(page, 1000))
    limit = max(1, min(limit, 100))
    valid_sorts = ("newest", "active", "name", "trust", "top")
    if sort not in valid_sorts:
        sort = "trust"
    badge_filter = badge if badge in VALID_BADGES else None
    search_term = search.strip()[:100] if search else None
    tag_filter = tag.strip().lower()[:30] if tag else None
    hops = max(1, min(hops, 8))
    cluster_filter = cluster if cluster >= 0 else None

    observer_pubkey = None
    _HEX64_RE = re.compile(r'^[0-9a-f]{64}$')
    if observer:
        try:
            if observer.startswith("npub1"):
                observer_pubkey = npub_to_hex(observer)
            elif _HEX64_RE.match(observer):
                observer_pubkey = observer
        except Exception:
            pass
    if observer and not observer_pubkey:
        raise HTTPException(status_code=400, detail="Invalid observer")
    if observer_pubkey:
        _require_nip98_pubkey(request, observer_pubkey)

    if observer_pubkey:
        # Determine tier: member/subscriber -> Tier 1, non-member -> Tier 2
        is_member = db.is_subscribed(observer_pubkey)

        if is_member:
            # --- Tier 1: precomputed personalized scores (members/subscribers) ---
            cache_key = f"p:{observer_pubkey}:{page}:{limit}:{sort}:{badge_filter}:{search_term}:{tag_filter}:{hops}:{cluster_filter}"
            cached = _cache_get(_directory_cache, cache_key, DIRECTORY_CACHE_TTL)
            if cached is not None:
                return cached

            # Ensure fresh scores — triggers on-demand GrapeRank if stale
            has_scores, is_computing = await _ensure_scores(observer_pubkey)

            # Track visit for pre-warming
            _safe_task(asyncio.to_thread(db.update_directory_visit, observer_pubkey), "directory-visit")

            members, total = db.get_directory_page_personalized(
                observer_pubkey, page, limit, sort, badge_filter, search_term, tag_filter, hops,
                cluster_filter=cluster_filter
            )

            for m in members:
                if "badges" in m:
                    m["badges"] = [b for b in m["badges"] if b in VALID_BADGES]

            result = {
                "members": members,
                "total": total,
                "page": page,
                "limit": limit,
                "pages": max(1, (total + limit - 1) // limit),
                "personalized": True,
                "tier": 1,
                "observer": observer_pubkey[:16] + "...",
            }
            if is_computing:
                result["trust_computing"] = True
            _cache_set(_directory_cache, cache_key, result)
            return result
        else:
            # --- Tier 2: approximate personalization for non-member visitors ---
            cache_key = f"t2:{observer_pubkey}:{page}:{limit}:{sort}:{badge_filter}:{search_term}:{tag_filter}:{hops}:{cluster_filter}"
            cached = _cache_get(_directory_cache, cache_key, DIRECTORY_CACHE_TTL)
            if cached is not None:
                return cached

            # Check Redis cache for pre-computed visitor scores
            visitor_scores = None
            r = db._get_redis()
            visitor_cache_key = f"directory:visitor_scores:{hashlib.sha256(observer_pubkey.encode()).hexdigest()}"
            if r:
                try:
                    cached_scores = r.get(visitor_cache_key)
                    if cached_scores:
                        visitor_scores = json.loads(cached_scores)
                except Exception:
                    pass

            if visitor_scores is None:
                if not r:
                    # Redis down — can't cache Tier 2 results, fall back to Tier 3
                    logger.warning("Tier 2 fallback to Tier 3: Redis unavailable for visitor score caching")
                    members, total = db.get_directory_page_global(
                        page, limit, sort, badge_filter, search_term, tag_filter,
                        cluster_filter=cluster_filter
                    )
                    for m in members:
                        if "badges" in m:
                            m["badges"] = [b for b in m["badges"] if b in VALID_BADGES]
                    result = {
                        "members": members, "total": total, "page": page, "limit": limit,
                        "pages": max(1, (total + limit - 1) // limit),
                        "tier": 3, "tier_fallback": True,
                    }
                    _cache_set(_directory_cache, cache_key, result)
                    return result

                # Compute approximate scores
                loop = asyncio.get_event_loop()
                user_follows = await _fetch_user_follow_list(observer_pubkey)
                if user_follows:
                    # Only load edges for the visitor's follows (not entire table)
                    edges = await loop.run_in_executor(None, db.get_trust_edges_for_sources, user_follows)
                    followers_of = {}
                    for follower, followed in edges:
                        if followed not in followers_of:
                            followers_of[followed] = set()
                        followers_of[followed].add(follower)

                    # Batch load all member precomputed scores (1 query instead of N)
                    members_list = await loop.run_in_executor(None, db.get_all_directory_members)
                    member_pks = {m["pubkey"] for m in members_list}
                    precomputed = await loop.run_in_executor(None, db.get_personalized_scores_batch, member_pks)

                    global_scores = await loop.run_in_executor(None, db.get_public_graperank_scores)

                    visitor_scores = db.approximate_visitor_scores(
                        user_follows, followers_of, precomputed, global_scores
                    )

                    # Cache in Redis with 6-hour TTL
                    if visitor_scores:
                        try:
                            r.setex(visitor_cache_key, 21600, json.dumps(
                                {pk: round(s, 6) for pk, s in visitor_scores.items()}
                            ))
                        except Exception:
                            pass
                else:
                    visitor_scores = {}

            # Track visit
            _safe_task(asyncio.to_thread(db.update_directory_visit, observer_pubkey), "directory-visit")

            members, total = db.get_directory_page_visitor(
                visitor_scores, page, limit, sort, badge_filter, search_term, tag_filter,
                cluster_filter=cluster_filter
            )

            for m in members:
                if "badges" in m:
                    m["badges"] = [b for b in m["badges"] if b in VALID_BADGES]

            result = {
                "members": members,
                "total": total,
                "page": page,
                "limit": limit,
                "pages": max(1, (total + limit - 1) // limit),
                "personalized": True,
                "tier": 2,
                "observer": observer_pubkey[:16] + "...",
            }
            _cache_set(_directory_cache, cache_key, result)
            return result
    else:
        # --- Tier 3: global consensus scores for anonymous visitors ---
        cache_key = f"g:{page}:{limit}:{sort}:{badge_filter}:{search_term}:{tag_filter}:{cluster_filter}"
        cached = _cache_get(_directory_cache, cache_key, DIRECTORY_CACHE_TTL)
        if cached is not None:
            return cached

        members, total = db.get_directory_page_global(page, limit, sort, badge_filter, search_term, tag_filter,
                                                       cluster_filter=cluster_filter)

        for m in members:
            if "badges" in m:
                m["badges"] = [b for b in m["badges"] if b in VALID_BADGES]

        result = {
            "members": members,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": max(1, (total + limit - 1) // limit),
            "tier": 3,
        }
        _cache_set(_directory_cache, cache_key, result)
        return result


# ---------------------------------------------------------------------------
# GrapeRank trust computation
# ---------------------------------------------------------------------------

_trust_lookup_cache = {}
_signal_list_cache = {}  # raw pubkey lists from NIP-45, keyed by target hex

# Track in-flight GrapeRank computations to avoid duplicates.
_graperank_computing: dict[str, asyncio.Event] = {}

SCORES_MAX_AGE_HOURS = 6


async def _ensure_scores(observer_pubkey: str) -> tuple[bool, bool]:
    """Ensure fresh personalized scores exist for an observer.

    Returns (has_scores, is_computing).
    - If fresh scores exist in Postgres, returns immediately.
    - If stale or missing, triggers on-demand GrapeRank with 10s timeout.
    - Uses Redis lock for dedup + asyncio.Event for in-process dedup.
    """
    # Check freshness
    computed_at = db.get_scores_freshness(observer_pubkey)
    if computed_at:
        if computed_at.tzinfo is None:
            computed_at = computed_at.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - computed_at
        if age < timedelta(hours=SCORES_MAX_AGE_HOURS):
            return True, False

    # In-process dedup: if already computing for this observer, wait on existing event
    if observer_pubkey in _graperank_computing:
        evt = _graperank_computing[observer_pubkey]
        try:
            await asyncio.wait_for(asyncio.shield(evt.wait()), timeout=10)
            return True, False
        except asyncio.TimeoutError:
            return computed_at is not None, True

    # Redis dedup: only one backend instance queues the job
    r = db._get_redis()
    lock_key = f"directory:graperank_computing:{observer_pubkey}"
    got_lock = False
    if r:
        try:
            got_lock = bool(r.set(lock_key, "1", nx=True, ex=150))
        except Exception as e:
            logger.warning("Redis lock failed for %s: %s", observer_pubkey[:16], e)

    if not got_lock and r:
        # Another process is computing — return immediately with "computing" status
        fresh = db.get_scores_freshness(observer_pubkey)
        if fresh:
            if fresh.tzinfo is None:
                fresh = fresh.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - fresh < timedelta(seconds=30):
                return True, False
        return computed_at is not None, True

    # We got the lock — trigger computation
    evt = asyncio.Event()
    _graperank_computing[observer_pubkey] = evt
    try:
        # Sync follow graph to Neo4j + Postgres trust_edges
        follows = await _fetch_user_follow_list(observer_pubkey)
        if follows:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, db.neo4j_sync_follows, observer_pubkey, follows)
            edges = [(observer_pubkey, f) for f in follows]
            await loop.run_in_executor(None, db.bulk_replace_trust_edges, edges)

        # Trigger GrapeRank with 10s timeout
        loop = asyncio.get_event_loop()
        try:
            count = await asyncio.wait_for(
                loop.run_in_executor(None, db.trigger_graperank_computation, observer_pubkey),
                timeout=10
            )
        except asyncio.TimeoutError:
            count = -1

        if count >= 0:
            # Clear caches for this observer
            stale_keys = [k for k in _trust_lookup_cache if k.startswith(f"tl:{observer_pubkey}:")]
            for k in stale_keys:
                _trust_lookup_cache.pop(k, None)
            stale_dir = [k for k in _directory_cache if k.startswith(f"p:{observer_pubkey}:")]
            for k in stale_dir:
                _directory_cache.pop(k, None)
            # Queue lazy NIP-85 publishing for this observer
            if r:
                try:
                    r.rpush("directory:nip85_publish_queue", _sign_queue_payload(observer_pubkey))
                except Exception as e:
                    logger.warning("NIP-85 publish queue push failed for %s: %s", observer_pubkey[:16], e)
            return True, False
        else:
            return computed_at is not None, True
    finally:
        evt.set()
        _graperank_computing.pop(observer_pubkey, None)
        if r:
            try:
                r.delete(lock_key)
            except Exception as e:
                logger.debug("Redis lock cleanup failed for %s: %s", lock_key, e)


@app.post("/api/directory/compute-trust")
async def compute_trust(request: Request):
    """Trigger on-demand GrapeRank computation for a user who has no scores yet.

    Expects JSON body: {"npub": "npub1..."}
    Returns immediately if scores already exist; otherwise triggers computation.
    """
    rate_limiter.check(f"compute-trust:{request.client.host}", max_requests=3, window_seconds=3600)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    npub = (body.get("npub") or "").strip()
    if not npub.startswith("npub1"):
        return JSONResponse({"error": "Invalid npub"}, status_code=400)

    try:
        hex_pubkey = npub_to_hex(npub)
    except (ValueError, Exception):
        return JSONResponse({"error": "Invalid npub"}, status_code=400)
    _require_nip98_pubkey(request, hex_pubkey)

    is_member = db.is_subscribed(hex_pubkey)

    if is_member:
        # Tier 1: full GrapeRank for members
        if db.has_personalized_scores(hex_pubkey):
            return JSONResponse({"status": "ready", "tier": 1, "message": "Scores already computed"})

        if hex_pubkey in _graperank_computing:
            evt = _graperank_computing[hex_pubkey]
            try:
                await asyncio.wait_for(asyncio.shield(evt.wait()), timeout=130)
            except asyncio.TimeoutError:
                return JSONResponse({"status": "timeout", "message": "Computation timed out"}, status_code=504)
            return JSONResponse({"status": "ready", "tier": 1, "message": "Scores computed"})

        follows = await _fetch_user_follow_list(hex_pubkey)
        if follows:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, db.neo4j_sync_follows, hex_pubkey, follows)

        evt = asyncio.Event()
        _graperank_computing[hex_pubkey] = evt
        try:
            loop = asyncio.get_event_loop()
            count = await asyncio.wait_for(
                loop.run_in_executor(None, db.trigger_graperank_computation, hex_pubkey),
                timeout=30)
            if count >= 0:
                stale_keys = [k for k in _trust_lookup_cache if k.startswith(f"tl:{hex_pubkey}:")]
                for k in stale_keys:
                    _trust_lookup_cache.pop(k, None)
                return JSONResponse({"status": "ready", "tier": 1, "message": f"Computed {count} scores"})
            else:
                return JSONResponse({"status": "error", "message": "Computation failed"}, status_code=502)
        except asyncio.TimeoutError:
            return JSONResponse({"status": "timeout", "message": "Computation timed out"}, status_code=504)
        finally:
            evt.set()
            _graperank_computing.pop(hex_pubkey, None)
    else:
        # Tier 2: approximate scores for non-members (no full GrapeRank)
        visitor_cache_key = f"directory:visitor_scores:{hashlib.sha256(hex_pubkey.encode()).hexdigest()}"
        r = db._get_redis()
        if r:
            try:
                cached = r.get(visitor_cache_key)
                if cached:
                    return JSONResponse({"status": "ready", "tier": 2, "message": "Approximate scores cached"})
            except Exception:
                pass

        follows = await _fetch_user_follow_list(hex_pubkey)
        if not follows:
            return JSONResponse({"status": "ready", "tier": 3, "message": "No follow list — using public GrapeRank"})

        loop = asyncio.get_event_loop()
        edges = await loop.run_in_executor(None, db.get_trust_edges_for_sources, follows)
        followers_of = {}
        for follower, followed in edges:
            if followed not in followers_of:
                followers_of[followed] = set()
            followers_of[followed].add(follower)

        members_list = await loop.run_in_executor(None, db.get_all_directory_members)
        member_pks = {m["pubkey"] for m in members_list}
        precomputed = await loop.run_in_executor(None, db.get_personalized_scores_batch, member_pks)

        global_scores = await loop.run_in_executor(None, db.get_public_graperank_scores)
        visitor_scores = db.approximate_visitor_scores(follows, followers_of, precomputed, global_scores)

        if r and visitor_scores:
            try:
                r.setex(visitor_cache_key, 21600, json.dumps(
                    {pk: round(s, 6) for pk, s in visitor_scores.items()}
                ))
            except Exception:
                pass

        return JSONResponse({"status": "ready", "tier": 2, "message": f"Approximate scores for {len(visitor_scores)} members"})


# ---------------------------------------------------------------------------
# Trust lookup
# ---------------------------------------------------------------------------

@app.get("/api/directory/trust-lookup")
async def trust_lookup(
    request: Request,
    target: str = "",
    observer: str = "",
):
    """Look up trust relationship between an observer and a target npub/hex."""
    rate_limiter.check(f"trust-lookup:{request.client.host}", max_requests=30, window_seconds=600)

    # Observer npub — optional; when absent, trust score is skipped
    observer_hex = None
    if observer:
        observer = observer.strip()
        try:
            if observer.startswith("npub1"):
                observer_hex = npub_to_hex(observer)
            elif len(observer) == 64:
                observer_hex = observer
            else:
                return JSONResponse({"error": "Invalid observer"}, status_code=400)
        except Exception:
            return JSONResponse({"error": "Invalid observer"}, status_code=400)
    if observer and not observer_hex:
        return JSONResponse({"error": "Invalid observer"}, status_code=400)
    if observer_hex:
        _require_nip98_pubkey(request, observer_hex)

    # Parse target (npub, hex, or NIP-05)
    if not target:
        return JSONResponse({"error": "Target required"}, status_code=400)

    target_hex = await _resolve_identifier(target)
    if not target_hex:
        return JSONResponse({"error": "Invalid target -- use npub, hex, or NIP-05"}, status_code=400)

    # Cache (5 min)
    cache_key = f"tl:{observer_hex}:{target_hex}"
    cached = _cache_get(_trust_lookup_cache, cache_key, 300)
    if cached is not None:
        return cached

    # Ensure fresh scores for observer
    trust_computing = False
    if observer_hex:
        has_scores, trust_computing = await _ensure_scores(observer_hex)
        _safe_task(asyncio.to_thread(db.update_directory_visit, observer_hex), "directory-visit")

    # Background: sync target's follow list to trust_edges for shared connections
    async def _sync_target_follows():
        try:
            follows = await _fetch_user_follow_list(target_hex)
            if follows:
                edges = [(target_hex, f) for f in follows]
                await asyncio.get_event_loop().run_in_executor(None, db.bulk_replace_trust_edges, edges)
        except Exception as e:
            logger.warning("Follow sync failed for %s: %s", target_hex[:16], e)
    if observer_hex and target_hex != observer_hex:
        _safe_task(_sync_target_follows(), f"sync-follows-{target_hex[:8]}")

    import time as _db_timer
    _db_t0 = _db_timer.time()
    result = db.get_trust_lookup(observer_hex, target_hex)
    logger.info(f"trust-lookup timing: db.get_trust_lookup = {_db_timer.time()-_db_t0:.2f}s")
    if result is None:
        return JSONResponse({"error": "Lookup failed"}, status_code=500)

    result["observer"] = (observer_hex[:16] + "...") if observer_hex else None
    result["target"] = target_hex

    # Run profile fetch, nostrarchives, NIP-45, trust path, and trust history ALL in parallel
    need_profile = not result.get("profile")

    async def _get_profile():
        if not need_profile:
            return None
        try:
            return await _fetch_profile_from_relays(target_hex)
        except Exception:
            return None

    async def _get_trust_path():
        if not observer_hex:
            return []
        try:
            loop = asyncio.get_event_loop()
            path_pks = await loop.run_in_executor(None, db.get_trust_path, observer_hex, target_hex)
            if path_pks and len(path_pks) > 1:
                return await loop.run_in_executor(None, db.get_trust_path_profiles, path_pks)
            return []
        except Exception:
            return []

    async def _get_trust_history():
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, db.get_trust_history, target_hex, 30)
        except Exception:
            return []

    async def _get_first_seen():
        """Fetch the earliest known event timestamp (DB cache, binary search in background)."""
        try:
            # 1. DB cache — instant
            cached = await asyncio.get_event_loop().run_in_executor(
                None, db.get_first_seen, target_hex)
            if cached and cached > 0:
                return cached

            # 2. Return 0 for now — fire binary search in background for next request
            async def _background_binary_search():
                try:
                    async def _binary_search_oldest(url):
                        try:
                            import time as _t
                            lo, hi = 1577836800, int(_t.time())
                            evs = await discovery._fetch_from_relay(
                                url, {"authors": [target_hex], "limit": 1}, timeout=4)
                            if not evs:
                                return 0
                            for _ in range(24):
                                if hi - lo < 86400 * 30:
                                    break
                                mid = (lo + hi) // 2
                                evs = await discovery._fetch_from_relay(
                                    url, {"authors": [target_hex], "until": mid, "limit": 1}, timeout=4)
                                if evs:
                                    hi = mid
                                else:
                                    lo = mid
                            evs = await discovery._fetch_from_relay(
                                url, {"authors": [target_hex], "since": lo, "until": hi, "limit": 500}, timeout=6)
                            if evs:
                                return min(e.get("created_at", 0) for e in evs if e.get("created_at"))
                        except Exception:
                            pass
                        return 0
                    results = await asyncio.gather(
                        _binary_search_oldest("wss://nos.lol"),
                        _binary_search_oldest("wss://relay.damus.io"),
                        _binary_search_oldest("wss://relay.primal.net"),
                        return_exceptions=True,
                    )
                    timestamps = [r for r in results if isinstance(r, int) and r > 0]
                    ts = min(timestamps) if timestamps else 0
                    if ts > 0:
                        db.set_first_seen(target_hex, ts)
                except Exception:
                    pass
            asyncio.ensure_future(_background_binary_search())
            return 0
        except Exception:
            return 0

    import time as _timer
    _t0 = _timer.time()

    async def _timed(name, coro):
        t = _timer.time()
        r = await coro
        logger.info(f"trust-lookup timing: {name} = {_timer.time()-t:.2f}s")
        return r

    profile_result, archive_stats, global_stats, trust_path_result, trust_history_result, first_seen_ts = await asyncio.gather(
        _timed("profile", _get_profile()),
        _timed("archives", _fetch_nostrarchives_stats(target_hex)),
        _timed("nip45+primal", _fetch_social_stats_nip45(target_hex)),
        _timed("trust_path", _get_trust_path()),
        _timed("trust_history", _get_trust_history()),
        _timed("first_seen", _get_first_seen()),
        return_exceptions=True,
    )
    logger.info(f"trust-lookup timing: TOTAL gather = {_timer.time()-_t0:.2f}s")

    if isinstance(profile_result, Exception):
        profile_result = None
    if need_profile and profile_result:
        result["profile"] = profile_result

    # First seen: combine binary search result with Primal's time_joined
    first_seen_candidates = []
    if isinstance(first_seen_ts, int) and first_seen_ts > 0:
        first_seen_candidates.append(first_seen_ts)
    if isinstance(global_stats, dict) and global_stats.get("_time_joined"):
        first_seen_candidates.append(global_stats["_time_joined"])
    if first_seen_candidates:
        best_first_seen = min(first_seen_candidates)
        result["first_seen"] = best_first_seen
        # Cache the best value
        _safe_task(asyncio.to_thread(db.set_first_seen, target_hex, best_first_seen), "set-first-seen")
    else:
        result["first_seen"] = 0
    if isinstance(archive_stats, Exception):
        archive_stats = None
    if isinstance(global_stats, Exception):
        global_stats = None
    result["trust_path"] = trust_path_result if not isinstance(trust_path_result, Exception) else []
    result["trust_history"] = trust_history_result if not isinstance(trust_history_result, Exception) else []

    # Merge: take the best data from each source
    if archive_stats:
        # Zap stats only come from nostrarchives
        for k in ["zaps_received_count", "zaps_received_sats",
                   "zaps_sent_count", "zaps_sent_sats"]:
            if k in archive_stats:
                result[k] = archive_stats[k]

    if global_stats:
        # NIP-45 + Primal provides mute/report counts + lists, and content counts
        for k in ["muted_by_count", "reported_by_count", "muting_count",
                   "reporting_count", "notes_count",
                   "media_count"]:
            if k in global_stats:
                result[k] = global_stats[k]

    # For follower/following/zaps_received: take the MAX from all sources
    for k in ["followers_count", "following_count", "zaps_received_count"]:
        vals = []
        if archive_stats and k in archive_stats:
            vals.append(archive_stats[k])
        if global_stats and k in global_stats:
            vals.append(global_stats[k])
        if k in result:
            vals.append(result[k])
        if vals:
            result[k] = max(vals)

    # Cache raw pubkey lists for the paginated signal-list endpoint
    list_keys = ["muted_by_list", "reported_by_list", "muting_list", "reporting_list"]
    raw_lists = {}
    if global_stats:
        for lk in list_keys:
            pks = global_stats.get(lk, [])
            if pks:
                raw_lists[lk] = pks
    if raw_lists:
        _cache_set(_signal_list_cache, target_hex, raw_lists)

    # Resolve profiles: single batch for all unique pubkeys, then split back
    if global_stats:
        try:
            all_pks_by_list = {}
            unique_pks = set()
            for lk in list_keys:
                pks = global_stats.get(lk, [])[:10]
                all_pks_by_list[lk] = pks
                unique_pks.update(pks)

            if unique_pks:
                _rp_t0 = _timer.time()
                all_resolved = await _resolve_pubkey_profiles(
                    list(unique_pks), limit=len(unique_pks), observer_pubkey=observer_hex)
                logger.info(f"trust-lookup timing: resolve_profiles ({len(unique_pks)} pks) = {_timer.time()-_rp_t0:.2f}s")
                resolved_map = {p["pubkey"]: p for p in all_resolved}

                for lk in list_keys:
                    pks = all_pks_by_list.get(lk, [])
                    if pks:
                        profiles = [resolved_map[pk] for pk in pks if pk in resolved_map]
                        # Sort: verified first
                        profiles.sort(key=lambda x: (-int(x.get("verified", False)),))
                        result[lk] = profiles
                        result[lk + "_total"] = len(global_stats.get(lk, []))
                        result[lk.replace("_list", "_verified")] = sum(
                            1 for p in profiles if p.get("verified"))
        except Exception:
            pass

    if trust_computing:
        result = dict(result)
        result["trust_computing"] = True

    _cache_set(_trust_lookup_cache, cache_key, result)

    return result


@app.get("/api/directory/trust-lookup/signals")
async def trust_lookup_signals(
    request: Request,
    target: str = "",
    observer: str = "",
    type: str = "",
    page: int = 1,
    limit: int = 20,
):
    """Paginated signal list — returns resolved profiles for muted_by, reported_by, etc."""
    rate_limiter.check(f"signals:{request.client.host}", max_requests=20, window_seconds=600)
    valid_types = ["muted_by_list", "reported_by_list", "muting_list", "reporting_list"]
    list_key = type if type in valid_types else type + "_list"
    if list_key not in valid_types:
        return JSONResponse({"error": "Invalid type"}, status_code=400)

    # Resolve target hex
    target_hex = await _resolve_identifier(target)
    if not target_hex:
        return JSONResponse({"error": "Invalid target"}, status_code=400)

    observer_hex = await _resolve_identifier(observer) or ""

    # Privacy: only subscribers can view their own full signal lists
    if not observer_hex or not db.is_subscribed(observer_hex):
        return JSONResponse({"error": "Subscriber access required"}, status_code=403)
    _require_nip98_pubkey(request, observer_hex)

    # Get raw pubkey list from cache
    cached_lists = _cache_get(_signal_list_cache, target_hex, 300)
    if not cached_lists or list_key not in cached_lists:
        return JSONResponse({
            "items": [], "total": 0, "page": page,
            "has_more": False, "error": "No cached data — run a lookup first"
        })

    all_pks = cached_lists[list_key]
    total = len(all_pks)

    # Paginate
    limit = min(limit, 50)
    offset = (page - 1) * limit
    page_pks = all_pks[offset:offset + limit]

    if not page_pks:
        return {"items": [], "total": total, "page": page, "has_more": False}

    # Resolve profiles with verified flags
    items = await _resolve_pubkey_profiles(
        page_pks, limit=len(page_pks),
        observer_pubkey=observer_hex or None)

    return {
        "items": items,
        "total": total,
        "page": page,
        "has_more": offset + limit < total,
    }


# ---------------------------------------------------------------------------
# Badge SVG endpoints
# ---------------------------------------------------------------------------

# Badge color mapping for SVG generation
_BADGE_COLORS = {
    "nip05-live": "#10b981",
    "lightning-reachable": "#eab308",
    "relay-subscriber": "#8b5cf6",
}


def _render_badge_svg(name, badges, score):
    """Generate an inline SVG verification badge."""
    import html as html_mod
    name_esc = html_mod.escape(name[:20])
    width = 380
    height = 56

    # Badge dots
    dots = ""
    x = 148
    for b in badges[:6]:
        color = _BADGE_COLORS.get(b, "#8b5cf6")
        dots += f'<circle cx="{x}" cy="38" r="4" fill="{color}"/>'
        x += 12

    # Score color
    if score >= 70:
        sc = "#22c55e"
    elif score >= 40:
        sc = "#eab308"
    else:
        sc = "#6a6a7a"

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect width="{width}" height="{height}" rx="8" fill="#12121c"/>
  <rect x="0.5" y="0.5" width="{width-1}" height="{height-1}" rx="7.5" fill="none" stroke="#8b5cf620" stroke-width="1"/>
  <text x="14" y="18" font-family="monospace" font-size="9" fill="#6a6a7a" letter-spacing="1">VERIFIED ON {RELAY_DOMAIN.upper()}</text>
  <text x="14" y="38" font-family="sans-serif" font-size="14" font-weight="bold" fill="#e0e0e8">{name_esc}</text>
  {dots}
  <circle cx="{width-28}" cy="28" r="16" fill="none" stroke="{sc}" stroke-width="2"/>
  <text x="{width-28}" y="33" font-family="monospace" font-size="12" font-weight="bold" fill="{sc}" text-anchor="middle">{score}</text>
</svg>'''
    return svg


@app.get("/api/badge/{identifier}.svg")
async def badge_svg(identifier: str, request: Request):
    """Dynamic SVG badge showing verification status."""
    rate_limiter.check(f"badge-svg:{request.client.host}", max_requests=60, window_seconds=60)

    # Resolve identifier
    if identifier.startswith("npub1"):
        try:
            pubkey = npub_to_hex(identifier)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid npub")
    else:
        pubkey = db.get_nip05(identifier)

    if not pubkey:
        raise HTTPException(status_code=404, detail="Not found")

    profile = db.get_directory_profile(pubkey)
    if not profile:
        raise HTTPException(status_code=404, detail="Not in directory")

    badges = json.loads(profile.get("badges", "[]"))
    name = profile.get("name") or (profile.get("npub", "")[:16] + "...")
    score = profile.get("reputation_score", 0)

    svg = _render_badge_svg(name, badges, score)

    return Response(
        content=svg,
        media_type="image/svg+xml",
        headers={
            "Cache-Control": "public, max-age=3600, s-maxage=3600",
        },
    )


# ---------------------------------------------------------------------------
# Activity heatmap
# ---------------------------------------------------------------------------

_heatmap_cache = {}

@app.get("/api/directory/activity/{identifier}")
async def activity_heatmap(identifier: str, request: Request):
    """Daily event counts for a directory member over the past year."""
    rate_limiter.check(f"heatmap:{request.client.host}", max_requests=30, window_seconds=600)

    # Resolve identifier
    if identifier.startswith("npub1"):
        try:
            pubkey = npub_to_hex(identifier)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid npub")
    else:
        pubkey = db.get_nip05(identifier)

    if not pubkey:
        raise HTTPException(status_code=404, detail="Not found")

    profile = db.get_directory_profile(pubkey)
    if not profile:
        raise HTTPException(status_code=404, detail="Not in directory")

    cache_key = pubkey
    cached = _cache_get(_heatmap_cache, cache_key, 900)
    if cached is not None:
        return cached

    days = db.get_activity_heatmap(pubkey)
    result = {"pubkey": pubkey, "days": days}
    _cache_set(_heatmap_cache, cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Trust graph (nodes + edges for visualization)
# ---------------------------------------------------------------------------

_trust_graph_cache = {}


@app.get("/api/directory/trust-graph")
async def trust_graph_endpoint(request: Request):
    """Public endpoint: full trust graph (nodes + edges) for visualization."""
    rate_limiter.check(f"trust-graph:{request.client.host}", max_requests=10, window_seconds=60)

    cached = _cache_get(_trust_graph_cache, "graph", 600)
    if cached is not None:
        return cached

    members = db.get_all_directory_members()
    edges = db.get_trust_edges()

    member_set = {m["pubkey"] for m in members}

    # Filter edges to only include edges between directory members
    filtered_edges = [
        {"source": s, "target": t}
        for s, t in edges
        if s in member_set and t in member_set
    ]

    cluster_meta = db.get_cluster_meta()
    cluster_assignments = db.get_cluster_assignments()

    result = {
        "nodes": members,
        "edges": filtered_edges,
        "clusters": cluster_meta,
        "assignments": cluster_assignments,
    }

    _cache_set(_trust_graph_cache, "graph", result)
    return result


# ---------------------------------------------------------------------------
# Trust Clusters
# ---------------------------------------------------------------------------

_clusters_cache = {}


@app.get("/api/directory/clusters")
async def clusters_endpoint(request: Request):
    """Public endpoint: trust cluster labels and assignments."""
    rate_limiter.check(f"clusters:{request.client.host}", max_requests=30, window_seconds=60)

    cached = _cache_get(_clusters_cache, "clusters", 600)
    if cached is not None:
        return cached

    meta = db.get_cluster_meta()
    assignments = db.get_cluster_assignments()

    result = {
        "clusters": [{"id": m["cluster_id"], "label": m["label"], "color": m["color"],
                       "member_count": m["member_count"]} for m in meta],
        "assignments": assignments,
    }

    _cache_set(_clusters_cache, "clusters", result)
    return result


# ---------------------------------------------------------------------------
# Trust Path — shortest follow path between two Nostr identities
# ---------------------------------------------------------------------------

_trust_path_cache = {}


@app.get("/api/directory/trust-path")
async def trust_path(
    request: Request,
    source: str = "",
    target: str = "",
):
    """Find shortest follow path between two Nostr identities (Six Degrees)."""
    rate_limiter.check(f"trust-path:{request.client.host}", max_requests=20, window_seconds=600)

    if not source or not target:
        return JSONResponse({"error": "Both source and target required"}, status_code=400)

    # Resolve source
    source_hex = await _resolve_identifier(source)
    if not source_hex:
        return JSONResponse({"error": "Could not resolve source"}, status_code=400)

    # Resolve target
    target_hex = await _resolve_identifier(target)
    if not target_hex:
        return JSONResponse({"error": "Could not resolve target"}, status_code=400)

    # Cache
    cache_key = f"tp:{source_hex}:{target_hex}"
    cached = _cache_get(_trust_path_cache, cache_key, 600)
    if cached is not None:
        return cached

    path_pks = db.get_trust_path(source_hex, target_hex)
    profiles = db.get_trust_path_profiles(path_pks) if path_pks else []

    result = {
        "source": source_hex,
        "target": target_hex,
        "path": profiles,
        "hops": len(path_pks) - 1 if path_pks else -1,
        "connected": len(path_pks) > 0,
    }

    _cache_set(_trust_path_cache, cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Community trust stats
# ---------------------------------------------------------------------------

_community_stats_cache = {}


@app.get("/api/directory/trust-stats")
async def community_trust_stats(request: Request):
    """Public endpoint: community-wide trust and reputation statistics."""
    rate_limiter.check(f"trust-stats:{request.client.host}", max_requests=30, window_seconds=600)

    cached = _cache_get(_community_stats_cache, "stats", 300)
    if cached is not None:
        return cached

    stats = db.get_community_trust_stats()
    zap_stats = db.get_zap_summary()
    stats["zaps"] = zap_stats

    _cache_set(_community_stats_cache, "stats", stats)
    return stats


# ---------------------------------------------------------------------------
# Zap flow visualization
# ---------------------------------------------------------------------------

_zap_flow_cache = {}


@app.get("/api/directory/zap-flow")
async def zap_flow(request: Request, limit: int = 50):
    """Public endpoint: top zap flows between directory members."""
    rate_limiter.check(f"zap-flow:{request.client.host}", max_requests=20, window_seconds=600)

    limit = min(limit, 100)
    cache_key = f"flow:{limit}"
    cached = _cache_get(_zap_flow_cache, cache_key, 300)
    if cached is not None:
        return cached

    flows = db.get_zap_flows(limit)
    summary = db.get_zap_summary()

    result = {"flows": flows, "summary": summary}
    _cache_set(_zap_flow_cache, cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Trust history (sparkline data)
# ---------------------------------------------------------------------------

_trust_history_cache = {}


@app.get("/api/directory/trust-history/{identifier}")
async def trust_history(identifier: str, request: Request, days: int = 90):
    """Daily trust/reputation snapshots for sparkline visualization."""
    rate_limiter.check(f"trust-hist:{request.client.host}", max_requests=30, window_seconds=600)

    if identifier.startswith("npub1"):
        try:
            pubkey = npub_to_hex(identifier)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid npub")
    else:
        pubkey = db.get_nip05(identifier)

    if not pubkey:
        raise HTTPException(status_code=404, detail="Not found")

    days = min(days, 365)
    cache_key = f"{pubkey}:{days}"
    cached = _cache_get(_trust_history_cache, cache_key, 900)
    if cached is not None:
        return cached

    history = db.get_trust_history(pubkey, days)
    result = {"pubkey": pubkey, "history": history}
    _cache_set(_trust_history_cache, cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Badge 2.0 — Enhanced embeddable badge with trust score
# ---------------------------------------------------------------------------

def _render_badge_v2_svg(name, badges, rep_score, trust_tier, trust_score_pct, npub):
    """Generate enhanced SVG badge with trust score, reputation, and embed-ready layout."""
    import html as html_mod
    name_esc = html_mod.escape(name[:24])
    npub_short = npub[:20] + "..." if npub else ""

    width = 440
    height = 120

    # Badge dots
    dots = ""
    x = 80
    for b in badges[:6]:
        color = _BADGE_COLORS.get(b, "#8b5cf6")
        dots += f'<circle cx="{x}" cy="96" r="4" fill="{color}"/>'
        x += 12

    # Trust tier colors
    tier_colors = {
        "highly_trusted": "#22c55e",
        "trusted": "#10b981",
        "neutral": "#eab308",
        "low_trust": "#f97316",
    }
    tc = tier_colors.get(trust_tier, "#6a6a7a")

    # Reputation color
    if rep_score >= 70:
        rc = "#22c55e"
    elif rep_score >= 40:
        rc = "#eab308"
    else:
        rc = "#6a6a7a"

    tier_labels = {
        "highly_trusted": "Highly Trusted",
        "trusted": "Trusted",
        "neutral": "Neutral",
        "low_trust": "Low Trust",
        "unverified": "Unverified",
        "unknown": "Unknown",
    }
    tier_label = tier_labels.get(trust_tier, "Unknown")

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#12121c"/>
      <stop offset="100%" stop-color="#1a1a2e"/>
    </linearGradient>
  </defs>
  <rect width="{width}" height="{height}" rx="12" fill="url(#bg)"/>
  <rect x="0.5" y="0.5" width="{width-1}" height="{height-1}" rx="11.5" fill="none" stroke="#8b5cf620" stroke-width="1"/>

  <!-- Header -->
  <text x="16" y="22" font-family="monospace" font-size="8" fill="#6a6a7a" letter-spacing="1.5">VERIFIED ON {RELAY_DOMAIN.upper()}</text>

  <!-- Name -->
  <text x="16" y="46" font-family="sans-serif" font-size="16" font-weight="bold" fill="#e0e0e8">{name_esc}</text>

  <!-- npub -->
  <text x="16" y="64" font-family="monospace" font-size="9" fill="#6a6a7a">{html_mod.escape(npub_short)}</text>

  <!-- Badges -->
  {dots}

  <!-- Trust score circle -->
  <circle cx="{width-80}" cy="42" r="24" fill="none" stroke="{tc}" stroke-width="2.5" opacity="0.8"/>
  <text x="{width-80}" y="39" font-family="monospace" font-size="18" font-weight="bold" fill="{tc}" text-anchor="middle">{trust_score_pct}</text>
  <text x="{width-80}" y="52" font-family="monospace" font-size="8" fill="{tc}" text-anchor="middle">TRUST</text>

  <!-- Reputation circle -->
  <circle cx="{width-28}" cy="42" r="18" fill="none" stroke="{rc}" stroke-width="2" opacity="0.6"/>
  <text x="{width-28}" y="39" font-family="monospace" font-size="13" font-weight="bold" fill="{rc}" text-anchor="middle">{rep_score}</text>
  <text x="{width-28}" y="51" font-family="monospace" font-size="7" fill="{rc}" text-anchor="middle">REP</text>

  <!-- Tier label -->
  <text x="{width-80}" y="84" font-family="sans-serif" font-size="10" fill="{tc}" text-anchor="middle">{tier_label}</text>

  <!-- Powered by -->
  <text x="{width-16}" y="108" font-family="monospace" font-size="7" fill="#4a4a5a" text-anchor="end">{RELAY_DOMAIN}</text>
</svg>'''
    return svg


@app.get("/api/badge/v2/{identifier}.svg")
async def badge_v2_svg(identifier: str, request: Request, observer: str = ""):
    """Enhanced SVG badge with trust score (personalized if observer provided)."""
    rate_limiter.check(f"badge-v2:{request.client.host}", max_requests=60, window_seconds=60)

    if identifier.startswith("npub1"):
        try:
            pubkey = npub_to_hex(identifier)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid npub")
    else:
        pubkey = db.get_nip05(identifier)

    if not pubkey:
        raise HTTPException(status_code=404, detail="Not found")

    profile = db.get_directory_profile(pubkey)
    if not profile:
        raise HTTPException(status_code=404, detail="Not in directory")

    badges = json.loads(profile.get("badges", "[]"))
    name = profile.get("name") or (profile.get("npub", "")[:16] + "...")
    rep_score = profile.get("reputation_score", 0)
    npub = profile.get("npub", "")

    # Get personalized trust score if observer provided
    trust_tier = "unknown"
    trust_score_pct = 0
    if observer:
        try:
            obs_hex = npub_to_hex(observer) if observer.startswith("npub1") else observer
            ps = db.get_personalized_scores(obs_hex)
            score = ps.get(pubkey, 0)
            trust_score_pct = min(100, round(score * 100))
            if score >= 0.50:
                trust_tier = "highly_trusted"
            elif score >= 0.20:
                trust_tier = "trusted"
            elif score >= 0.07:
                trust_tier = "neutral"
            elif score >= 0.02:
                trust_tier = "low_trust"
            else:
                trust_tier = "unverified"
        except Exception:
            pass

    svg = _render_badge_v2_svg(name, badges, rep_score, trust_tier, trust_score_pct, npub)

    return Response(
        content=svg,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=3600, s-maxage=3600"},
    )


# ---------------------------------------------------------------------------
# NIP-11 relay info
# ---------------------------------------------------------------------------

@app.get("/api/relay-info")
async def relay_info():
    """NIP-11 relay information document."""
    info = {
        "name": f"{RELAY_DOMAIN} Relay",
        "description": f"A Nostr relay with a member directory. Visit https://{RELAY_DOMAIN}",
        "pubkey": os.environ.get("RELAY_PUBKEY", ""),
        "contact": os.environ.get("RELAY_CONTACT", ""),
        "supported_nips": [1, 2, 4, 9, 11, 12, 15, 16, 17, 20, 22, 28, 33, 40, 42, 59, 77, 85],
        "software": "strfry",
        "icon": f"https://{RELAY_DOMAIN}/img/logo-v5.png",
        "limitation": {
            "max_message_length": 262144,
            "max_subscriptions": 200,
            "max_filters": 200,
            "max_event_tags": 2000,
            "max_content_length": 65536,
            "auth_required": True,
            "payment_required": True,
        },
        "fees": {},
    }
    return Response(
        content=json.dumps(info),
        media_type="application/nostr+json",
        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=3600"},
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {"status": "ok"}
