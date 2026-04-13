from __future__ import annotations

import hmac
import os
from functools import lru_cache

from fastapi import Header, HTTPException, Request, status

from internal.utils.crypto import verify_token as verify_signed_token


TOKEN_ENV_VAR = "SETTLEUP_API_TOKENS"
SECRET_ENV_VAR = "SETTLEUP_AUTH_SECRET"
ALLOW_DEV_TOKEN_ENV_VAR = "SETTLEUP_ALLOW_DEV_TOKEN"
DEV_DEFAULT_TOKEN = "dev-token-change-me"


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


@lru_cache(maxsize=1)
def _allowed_tokens() -> tuple[str, ...]:
    configured = os.getenv(TOKEN_ENV_VAR, "")
    return tuple(token.strip() for token in configured.split(",") if token.strip())


@lru_cache(maxsize=1)
def _auth_secret() -> str | None:
    secret = os.getenv(SECRET_ENV_VAR, "").strip()
    return secret or None


@lru_cache(maxsize=1)
def _allow_dev_token() -> bool:
    return os.getenv(ALLOW_DEV_TOKEN_ENV_VAR, "1").strip().lower() not in {"0", "false", "no"}


def _is_valid_static_token(token: str) -> bool:
    candidates = _allowed_tokens()
    if candidates:
        return any(hmac.compare_digest(token, candidate) for candidate in candidates)
    if _allow_dev_token():
        return hmac.compare_digest(token, DEV_DEFAULT_TOKEN)
    return False


def reset_auth_cache() -> None:
    """Clear auth token cache (primarily for tests)."""
    _allowed_tokens.cache_clear()
    _auth_secret.cache_clear()
    _allow_dev_token.cache_clear()


def verify_token(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    token = _extract_bearer_token(authorization)
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if _is_valid_static_token(token):
        request.state.auth_claims = {"sub": "service-token", "auth_type": "static"}
        return

    secret = _auth_secret()
    if secret:
        claims = verify_signed_token(token, secret=secret, check_expiry=True)
        if claims is not None:
            request.state.auth_claims = {**claims, "auth_type": "signed"}
            return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized",
        headers={"WWW-Authenticate": "Bearer"},
    )
