import base64
import dataclasses
import functools
import hashlib
import hmac
import json
import os

from .continuation import ContState


MAX_PAYLOAD_BYTES = 64 * 1024


class TokenError(Exception):
    pass


def _b64u_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _b64u_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _state_to_bytes(state: ContState) -> bytes:
    return json.dumps(dataclasses.asdict(state), separators=(",", ":")).encode()


def _sign(payload: bytes, key: bytes) -> bytes:
    return hmac.new(key, payload, hashlib.sha256).digest()


def encode(state: ContState, key: bytes) -> str:
    """
    Encode a ContState as an HMAC-signed token: b64u(payload).b64u(sig).

    Raises TokenError if the JSON-serialized payload exceeds MAX_PAYLOAD_BYTES.
    """
    payload = _state_to_bytes(state)
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise TokenError(
            f"continuation state too large ({len(payload)} > "
            f"{MAX_PAYLOAD_BYTES} bytes)"
        )
    sig = _sign(payload, key)
    return f"{_b64u_encode(payload)}.{_b64u_encode(sig)}"


def decode(token: str, key: bytes) -> ContState:
    """
    Verify the HMAC signature of `token` using `key`, then parse and return
    the embedded ContState. Raises TokenError on tampering, bad encoding,
    or bad payload.

    Security order: verify signature (constant-time) BEFORE parsing JSON.
    """
    # Reject oversize tokens before doing any decoding/HMAC work; otherwise an
    # attacker could force base64-decode + HMAC computation over an arbitrarily
    # large request body.
    if len(token) > MAX_PAYLOAD_BYTES * 2:  # generous; b64 + sig + separator
        raise TokenError("token too large")
    try:
        payload_b64, sig_b64 = token.rsplit(".", 1)
        payload = _b64u_decode(payload_b64)
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise TokenError("payload too large")
        sig = _b64u_decode(sig_b64)
    except (ValueError, base64.binascii.Error):
        raise TokenError("invalid token encoding")

    expected = _sign(payload, key)
    if not hmac.compare_digest(sig, expected):
        raise TokenError("invalid signature")

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        raise TokenError("invalid payload")

    try:
        state = ContState(**data)
    except TypeError:
        raise TokenError("invalid payload schema")

    return state


@functools.lru_cache(maxsize=1)
def signing_key() -> bytes:
    """
    Load the HMAC signing key from BIOINDEX_TOKEN_SIGNING_KEY (env var or
    Secrets Manager injection). Cached for the process lifetime.
    Accepts hex, base64url, or raw bytes (preferring hex).
    """
    raw = os.environ.get("BIOINDEX_TOKEN_SIGNING_KEY")
    if not raw:
        raise RuntimeError("BIOINDEX_TOKEN_SIGNING_KEY not set")
    key: bytes
    try:
        key = bytes.fromhex(raw)
    except ValueError:
        try:
            key = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        except Exception:
            key = raw.encode()
    if len(key) < 32:
        raise RuntimeError(
            f"BIOINDEX_TOKEN_SIGNING_KEY must decode to >=32 bytes (got {len(key)})"
        )
    return key
