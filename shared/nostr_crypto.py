"""Nostr cryptographic primitives: event signing, verification, NIP-04, NIP-44, NIP-17.

Single canonical copy — imported by backend and indexer.
"""

import hashlib
import hmac as hmac_mod
import json
import os
import struct
import time
from base64 import b64decode, b64encode

import secp256k1

# cryptography is imported lazily inside NIP-04/NIP-44 functions so that
# services which only need verify_event / extract_p_tag_pubkeys (indexer)
# do not require the cryptography package.


# ---------------------------------------------------------------------------
# Schnorr / BIP-340 helpers
# ---------------------------------------------------------------------------

def privkey_to_pubkey(privkey_hex: str) -> str:
    """Derive the x-only public key (hex) from a 32-byte private key (hex)."""
    sk = secp256k1.PrivateKey(bytes.fromhex(privkey_hex))
    return sk.pubkey.serialize(compressed=True).hex()[2:]


def _serialize_event(event: dict) -> bytes:
    """Canonical JSON serialisation for event id computation (NIP-01)."""
    arr = [
        0,
        event["pubkey"],
        event["created_at"],
        event["kind"],
        event["tags"],
        event["content"],
    ]
    return json.dumps(arr, separators=(",", ":"), ensure_ascii=False).encode()


def compute_event_id(event: dict) -> str:
    return hashlib.sha256(_serialize_event(event)).hexdigest()


def verify_event(event: dict) -> bool:
    """Verify a Nostr event's id and Schnorr signature. Returns True if valid."""
    try:
        expected_id = compute_event_id(event)
        if event.get("id") != expected_id:
            return False
        pubkey_bytes = b"\x02" + bytes.fromhex(event["pubkey"])
        pk = secp256k1.PublicKey(pubkey_bytes, raw=True)
        return pk.schnorr_verify(
            bytes.fromhex(event["id"]),
            bytes.fromhex(event["sig"]),
            bip340tag=None,
            raw=True,
        )
    except Exception:
        return False


def sign_event(event: dict, privkey_hex: str) -> dict:
    """Fill in id + sig on an event dict and return it."""
    event["id"] = compute_event_id(event)
    sk = secp256k1.PrivateKey(bytes.fromhex(privkey_hex))
    sig = sk.schnorr_sign(bytes.fromhex(event["id"]), bip340tag=None, raw=True)
    event["sig"] = sig.hex()
    return event


def make_event(privkey_hex: str, kind: int, content: str, tags: list, created_at: int | None = None) -> dict:
    """Build and sign a complete Nostr event."""
    pubkey = privkey_to_pubkey(privkey_hex)
    event = {
        "pubkey": pubkey,
        "created_at": created_at if created_at is not None else int(time.time()),
        "kind": kind,
        "tags": tags,
        "content": content,
    }
    return sign_event(event, privkey_hex)


# ---------------------------------------------------------------------------
# Hex pubkey validation
# ---------------------------------------------------------------------------

_HEX_CHARS = frozenset("0123456789abcdef")


def is_valid_hex_pubkey(pubkey: str) -> bool:
    """Check if string is a valid 64-char lowercase hex pubkey."""
    return len(pubkey) == 64 and all(c in _HEX_CHARS for c in pubkey)


# ---------------------------------------------------------------------------
# P-tag extraction helper
# ---------------------------------------------------------------------------

def extract_p_tag_pubkeys(tags: list) -> list[str]:
    """Extract valid hex pubkeys from p-tags."""
    return [t[1] for t in tags if len(t) >= 2 and t[0] == "p" and is_valid_hex_pubkey(t[1])]


# ---------------------------------------------------------------------------
# ECDH shared secret (used by both NIP-04 and NIP-44)
# ---------------------------------------------------------------------------

def compute_shared_secret(privkey_hex: str, pubkey_hex: str) -> bytes:
    """ECDH shared secret (32 bytes) between a private key and a public key."""
    sk = secp256k1.PrivateKey(bytes.fromhex(privkey_hex))
    pk_bytes = b"\x02" + bytes.fromhex(pubkey_hex)
    pk = secp256k1.PublicKey(pk_bytes, raw=True)
    shared_point = pk.tweak_mul(bytes.fromhex(privkey_hex))
    compressed = shared_point.serialize(compressed=True)
    return compressed[1:]


# ---------------------------------------------------------------------------
# NIP-04: AES-256-CBC encryption (legacy DMs, kind 4)
# ---------------------------------------------------------------------------

def nip04_encrypt(privkey_hex: str, pubkey_hex: str, plaintext: str) -> str:
    """NIP-04 encrypt: AES-256-CBC with ECDH shared secret. Returns 'base64?iv=base64'."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives import padding as sym_padding
    shared = compute_shared_secret(privkey_hex, pubkey_hex)
    iv = os.urandom(16)
    padder = sym_padding.PKCS7(128).padder()
    padded = padder.update(plaintext.encode()) + padder.finalize()
    cipher = Cipher(algorithms.AES(shared), modes.CBC(iv))
    enc = cipher.encryptor()
    ct = enc.update(padded) + enc.finalize()
    return b64encode(ct).decode() + "?iv=" + b64encode(iv).decode()


def nip04_decrypt(privkey_hex: str, pubkey_hex: str, ciphertext: str) -> str:
    """NIP-04 decrypt: AES-256-CBC. Input format: 'base64?iv=base64'."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives import padding as sym_padding
    shared = compute_shared_secret(privkey_hex, pubkey_hex)
    parts = ciphertext.split("?iv=")
    if len(parts) != 2:
        raise ValueError("Invalid NIP-04 ciphertext format: missing ?iv= separator")
    ct = b64decode(parts[0])
    iv = b64decode(parts[1])
    if len(iv) != 16:
        raise ValueError(f"Invalid NIP-04 IV length: expected 16, got {len(iv)}")
    cipher = Cipher(algorithms.AES(shared), modes.CBC(iv))
    dec = cipher.decryptor()
    padded = dec.update(ct) + dec.finalize()
    unpadder = sym_padding.PKCS7(128).unpadder()
    return (unpadder.update(padded) + unpadder.finalize()).decode()


# ---------------------------------------------------------------------------
# NIP-44: XChaCha20 encryption (modern DMs)
# ---------------------------------------------------------------------------

def _nip44_conversation_key(privkey_hex: str, pubkey_hex: str) -> bytes:
    """Derive NIP-44 conversation key via ECDH + HKDF-Extract only."""
    shared = compute_shared_secret(privkey_hex, pubkey_hex)
    return hmac_mod.new(b"nip44-v2", shared, hashlib.sha256).digest()


def _nip44_message_keys(conv_key: bytes, nonce: bytes):
    """HKDF-Expand(PRK=conv_key, info=nonce, L=76) -> (chacha_key, chacha_nonce, hmac_key)."""
    from cryptography.hazmat.primitives.kdf.hkdf import HKDFExpand
    from cryptography.hazmat.primitives import hashes
    keys = HKDFExpand(algorithm=hashes.SHA256(), length=76, info=nonce).derive(conv_key)
    return keys[:32], keys[32:44], keys[44:76]


def _nip44_pad(plaintext: bytes) -> bytes:
    """NIP-44 padding: 2-byte big-endian length prefix + chunk-based padding per spec."""
    ulen = len(plaintext)
    if ulen < 1 or ulen > 65535:
        raise ValueError("Plaintext too long for NIP-44")
    if ulen <= 32:
        padded_len = 32
    else:
        next_power = 1 << ((ulen - 1).bit_length())
        chunk = 32 if next_power <= 256 else next_power // 8
        padded_len = chunk * (((ulen - 1) // chunk) + 1)
    return struct.pack(">H", ulen) + plaintext + b"\x00" * (padded_len - ulen)


def _nip44_unpad(padded: bytes) -> bytes:
    """NIP-44 unpadding: read 2-byte big-endian length prefix."""
    if len(padded) < 2:
        raise ValueError("NIP-44 padded data too short")
    ulen = struct.unpack(">H", padded[:2])[0]
    if ulen < 1 or 2 + ulen > len(padded):
        raise ValueError(f"Invalid NIP-44 padding length: {ulen}")
    return padded[2:2 + ulen]


def nip44_encrypt(privkey_hex: str, pubkey_hex: str, plaintext: str) -> str:
    """NIP-44v2 encrypt. Returns base64 payload with version byte prefix."""
    from cryptography.hazmat.primitives.ciphers import Cipher
    from cryptography.hazmat.primitives.ciphers.algorithms import ChaCha20
    conv_key = _nip44_conversation_key(privkey_hex, pubkey_hex)
    nonce = os.urandom(32)
    chacha_key, chacha_nonce, hmac_key = _nip44_message_keys(conv_key, nonce)
    padded = _nip44_pad(plaintext.encode())
    counter_nonce = b"\x00\x00\x00\x00" + chacha_nonce
    cipher = Cipher(ChaCha20(chacha_key, counter_nonce), mode=None)
    enc = cipher.encryptor()
    ciphertext = enc.update(padded) + enc.finalize()
    mac = hmac_mod.new(hmac_key, nonce + ciphertext, hashlib.sha256).digest()
    payload = b"\x02" + nonce + ciphertext + mac
    return b64encode(payload).decode()


def nip44_decrypt(privkey_hex: str, pubkey_hex: str, payload_b64: str) -> str:
    """NIP-44v2 decrypt. Input is base64 payload with version byte prefix."""
    from cryptography.hazmat.primitives.ciphers import Cipher
    from cryptography.hazmat.primitives.ciphers.algorithms import ChaCha20
    payload = b64decode(payload_b64)
    version = payload[0]
    if version != 2:
        raise ValueError(f"Unsupported NIP-44 version: {version}")
    nonce = payload[1:33]
    mac = payload[-32:]
    ciphertext = payload[33:-32]
    conv_key = _nip44_conversation_key(privkey_hex, pubkey_hex)
    chacha_key, chacha_nonce, hmac_key = _nip44_message_keys(conv_key, nonce)
    expected_mac = hmac_mod.new(hmac_key, nonce + ciphertext, hashlib.sha256).digest()
    if not hmac_mod.compare_digest(mac, expected_mac):
        raise ValueError("NIP-44 MAC verification failed")
    counter_nonce = b"\x00\x00\x00\x00" + chacha_nonce
    cipher = Cipher(ChaCha20(chacha_key, counter_nonce), mode=None)
    dec = cipher.decryptor()
    padded = dec.update(ciphertext) + dec.finalize()
    return _nip44_unpad(padded).decode()


# ---------------------------------------------------------------------------
# NIP-17: Gift-wrapped DMs (kind 14 → kind 13 seal → kind 1059 gift wrap)
# ---------------------------------------------------------------------------

def make_nip17_dm(sender_privkey: str, recipient_pubkey: str, plaintext: str) -> dict:
    """Create a NIP-17 gift-wrapped DM event (kind 1059) ready to publish."""
    import secrets
    sender_pubkey = privkey_to_pubkey(sender_privkey)
    now = int(time.time())
    seal_ts = now - secrets.randbelow(172800)
    wrap_ts = now - secrets.randbelow(172800)

    # Step 1: Kind 14 rumor (unsigned chat message)
    rumor = {
        "pubkey": sender_pubkey,
        "created_at": now,
        "kind": 14,
        "tags": [["p", recipient_pubkey]],
        "content": plaintext,
    }
    rumor["id"] = compute_event_id(rumor)

    # Step 2: Kind 13 seal — encrypt the rumor with NIP-44, sign with sender key
    rumor_json = json.dumps(rumor, separators=(",", ":"), ensure_ascii=False)
    sealed_content = nip44_encrypt(sender_privkey, recipient_pubkey, rumor_json)
    seal = {
        "pubkey": sender_pubkey,
        "created_at": seal_ts,
        "kind": 13,
        "tags": [],
        "content": sealed_content,
    }
    sign_event(seal, sender_privkey)

    # Step 3: Kind 1059 gift wrap — encrypt the seal with a random ephemeral key
    wrap_privkey = os.urandom(32).hex()
    wrap_pubkey = privkey_to_pubkey(wrap_privkey)
    seal_json = json.dumps(seal, separators=(",", ":"), ensure_ascii=False)
    wrapped_content = nip44_encrypt(wrap_privkey, recipient_pubkey, seal_json)
    gift_wrap = {
        "pubkey": wrap_pubkey,
        "created_at": wrap_ts,
        "kind": 1059,
        "tags": [["p", recipient_pubkey]],
        "content": wrapped_content,
    }
    sign_event(gift_wrap, wrap_privkey)
    return gift_wrap


# ---------------------------------------------------------------------------
# BOLT11 invoice parsing
# ---------------------------------------------------------------------------

import re

_BOLT11_RE = re.compile(r"^lnbc(\d+)([munp]?)")
_BOLT11_MULTIPLIERS = {"m": 100_000_000, "u": 100_000, "n": 100, "p": 0.1, "": 100_000_000_000}


def bolt11_to_msats(invoice: str) -> int | None:
    """Parse a BOLT11 invoice and return the amount in millisatoshis, or None."""
    m = _BOLT11_RE.match(invoice.lower())
    if not m:
        return None
    amount = int(m.group(1))
    multiplier = m.group(2)
    msats = int(amount * _BOLT11_MULTIPLIERS.get(multiplier, 0))
    return msats if msats > 0 else None


def bolt11_to_sats(invoice: str) -> int | None:
    """Parse a BOLT11 invoice and return the amount in satoshis, or None."""
    msats = bolt11_to_msats(invoice)
    return msats // 1000 if msats else None
