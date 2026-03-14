"""Cryptographic and security utilities for the Expense Splitter API.

Provides token generation, API key hashing, and idempotency key
generation — all using standard library HMAC/SHA-256.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any


# Default secret used for development; override via environment in production.
_DEFAULT_SECRET = "expense-splitter-dev-secret-key-change-in-production"

# Token validity duration in seconds (default 24 hours).
DEFAULT_TOKEN_TTL = 86400

# API key prefix for identification.
API_KEY_PREFIX = "xps_"

# Length of generated random tokens (in bytes of entropy).
TOKEN_ENTROPY_BYTES = 32


def generate_secret_key(length: int = 64) -> str:
    """Generate a cryptographically secure random secret key.

    Returns:
        A URL-safe base64-encoded random string.
    """
    return secrets.token_urlsafe(length)


def generate_api_key() -> tuple[str, str]:
    """Generate a new API key and its hash.

    Returns:
        Tuple of (plaintext_key, key_hash). Store only the hash.
    """
    raw = secrets.token_urlsafe(TOKEN_ENTROPY_BYTES)
    plaintext = f"{API_KEY_PREFIX}{raw}"
    key_hash = hash_api_key(plaintext)
    return plaintext, key_hash


def hash_api_key(api_key: str) -> str:
    """Hash an API key for storage using SHA-256.

    The key is hashed with a salt derived from its prefix to prevent
    rainbow table attacks while remaining deterministic.
    """
    salt = api_key[:len(API_KEY_PREFIX)] if api_key.startswith(API_KEY_PREFIX) else "default"
    combined = f"{salt}:{api_key}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def verify_api_key(api_key: str, stored_hash: str) -> bool:
    """Compare an API key against its stored hash using constant-time comparison."""
    computed = hash_api_key(api_key)
    return hmac.compare_digest(computed, stored_hash)


def create_token(
    payload: dict[str, Any],
    *,
    secret: str = _DEFAULT_SECRET,
    ttl_seconds: int = DEFAULT_TOKEN_TTL,
) -> str:
    """Create an HMAC-signed token containing the given payload.

    The token format is: base64(payload_json).base64(signature)
    with an embedded expiration timestamp.

    Args:
        payload: Arbitrary JSON-serializable data to include.
        secret: HMAC secret key.
        ttl_seconds: Token validity duration in seconds.

    Returns:
        The signed token string.
    """
    enriched = {
        **payload,
        "_iat": int(time.time()),
        "_exp": int(time.time()) + ttl_seconds,
    }
    payload_bytes = json.dumps(enriched, sort_keys=True, separators=(",", ":")).encode("utf-8")
    payload_b64 = urlsafe_b64encode(payload_bytes).rstrip(b"=").decode("ascii")
    signature = _sign(payload_b64, secret)
    return f"{payload_b64}.{signature}"


def verify_token(
    token: str,
    *,
    secret: str = _DEFAULT_SECRET,
    check_expiry: bool = True,
) -> dict[str, Any] | None:
    """Verify and decode an HMAC-signed token.

    Args:
        token: The token string to verify.
        secret: HMAC secret key used during creation.
        check_expiry: Whether to enforce the expiration timestamp.

    Returns:
        The decoded payload dict if valid, or ``None`` if invalid/expired.
    """
    parts = token.split(".")
    if len(parts) != 2:
        return None

    payload_b64, provided_signature = parts
    expected_signature = _sign(payload_b64, secret)

    if not hmac.compare_digest(expected_signature, provided_signature):
        return None

    try:
        padded = payload_b64 + "=" * (4 - len(payload_b64) % 4)
        payload_bytes = urlsafe_b64decode(padded)
        payload = json.loads(payload_bytes)
    except (ValueError, json.JSONDecodeError):
        return None

    if check_expiry:
        exp = payload.get("_exp", 0)
        if time.time() > exp:
            return None

    # Strip internal fields from returned payload.
    return {k: v for k, v in payload.items() if not k.startswith("_")}


def generate_idempotency_key() -> str:
    """Generate a unique idempotency key for request deduplication.

    Format: ``idem_<timestamp_hex>_<random_hex>``
    """
    timestamp_part = format(int(time.time() * 1000), "x")
    random_part = secrets.token_hex(12)
    return f"idem_{timestamp_part}_{random_part}"


def compute_request_hash(data: dict[str, Any]) -> str:
    """Compute a deterministic hash of request data for idempotency checks.

    The data is JSON-serialized with sorted keys to ensure consistent
    hashing regardless of insertion order.
    """
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def generate_request_id() -> str:
    """Generate a unique request identifier for tracing.

    Format: ``req_<random_hex>``
    """
    return f"req_{secrets.token_hex(16)}"


def mask_sensitive_value(value: str, *, visible_chars: int = 4) -> str:
    """Mask a sensitive string, leaving only the last N characters visible.

    Example: ``mask_sensitive_value("sk_live_abc123def456", visible_chars=4)``
    returns ``"**************f456"``
    """
    if len(value) <= visible_chars:
        return "*" * len(value)
    masked_len = len(value) - visible_chars
    return "*" * masked_len + value[-visible_chars:]


def constant_time_compare(a: str, b: str) -> bool:
    """Compare two strings in constant time to prevent timing attacks."""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def _sign(data: str, secret: str) -> str:
    """Create an HMAC-SHA256 signature of the data."""
    sig = hmac.new(
        secret.encode("utf-8"),
        data.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")
