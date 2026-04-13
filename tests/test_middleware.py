from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from internal.middleware.auth import (
    ALLOW_DEV_TOKEN_ENV_VAR,
    SECRET_ENV_VAR,
    TOKEN_ENV_VAR,
    reset_auth_cache,
    verify_token,
)
from internal.middleware.rate_limiter import RateLimitConfig, RateLimiterMiddleware
from internal.utils.crypto import create_token


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        RateLimiterMiddleware,
        config=RateLimitConfig(
            requests_per_minute=2,
            burst_size=2,
            window_seconds=60,
            exclude_paths=set(),
        ),
    )

    @app.get("/secure", dependencies=[Depends(verify_token)])
    def secure() -> dict[str, bool]:
        return {"ok": True}

    return app


def test_auth_rejects_missing_or_invalid_token(monkeypatch):
    monkeypatch.setenv(TOKEN_ENV_VAR, "alpha-token")
    reset_auth_cache()
    client = TestClient(_build_app())

    missing = client.get("/secure")
    invalid = client.get("/secure", headers={"Authorization": "Bearer wrong-token"})
    valid = client.get("/secure", headers={"Authorization": "Bearer alpha-token"})

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert valid.status_code == 200


def test_rate_limiter_returns_429_after_limit(monkeypatch):
    monkeypatch.setenv(TOKEN_ENV_VAR, "rate-limit-token")
    reset_auth_cache()
    client = TestClient(_build_app())
    headers = {"Authorization": "Bearer rate-limit-token"}

    first = client.get("/secure", headers=headers)
    second = client.get("/secure", headers=headers)
    third = client.get("/secure", headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 429
    assert third.headers["X-RateLimit-Limit"] == "2"
    assert "Retry-After" in third.headers


def test_auth_accepts_signed_token(monkeypatch):
    monkeypatch.delenv(TOKEN_ENV_VAR, raising=False)
    monkeypatch.setenv(ALLOW_DEV_TOKEN_ENV_VAR, "0")
    monkeypatch.setenv(SECRET_ENV_VAR, "test-signing-secret")
    reset_auth_cache()
    client = TestClient(_build_app())

    signed = create_token({"sub": "alice", "role": "admin"}, secret="test-signing-secret", ttl_seconds=300)
    response = client.get("/secure", headers={"Authorization": f"Bearer {signed}"})
    assert response.status_code == 200


def test_auth_rejects_expired_signed_token(monkeypatch):
    monkeypatch.delenv(TOKEN_ENV_VAR, raising=False)
    monkeypatch.setenv(ALLOW_DEV_TOKEN_ENV_VAR, "0")
    monkeypatch.setenv(SECRET_ENV_VAR, "test-signing-secret")
    reset_auth_cache()
    client = TestClient(_build_app())

    expired = create_token({"sub": "alice"}, secret="test-signing-secret", ttl_seconds=-1)
    response = client.get("/secure", headers={"Authorization": f"Bearer {expired}"})
    assert response.status_code == 401
