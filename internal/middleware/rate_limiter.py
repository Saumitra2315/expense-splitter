"""Rate limiting middleware for the SettleUp API.

Implements a token-bucket rate limiter with per-IP and per-token
limits, sliding window tracking, and standard rate-limit headers.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


@dataclass
class RateLimitConfig:
    """Configuration for a rate limiter instance."""

    requests_per_minute: int = 60
    burst_size: int = 10
    window_seconds: int = 60
    by_token: bool = True
    by_ip: bool = True
    exclude_paths: set[str] = field(default_factory=lambda: {"/health", "/docs", "/openapi.json"})


@dataclass
class _TokenBucket:
    """Token bucket state for a single client."""

    tokens: float
    max_tokens: int
    refill_rate: float  # tokens per second
    last_refill: float

    @classmethod
    def create(cls, max_tokens: int, refill_rate: float) -> "_TokenBucket":
        return cls(
            tokens=float(max_tokens),
            max_tokens=max_tokens,
            refill_rate=refill_rate,
            last_refill=time.monotonic(),
        )

    def try_consume(self, count: int = 1) -> bool:
        """Attempt to consume tokens. Returns True if allowed."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

        if self.tokens >= count:
            self.tokens -= count
            return True
        return False

    @property
    def remaining(self) -> int:
        return max(0, int(self.tokens))

    @property
    def reset_seconds(self) -> int:
        """Seconds until the bucket is fully refilled."""
        deficit = self.max_tokens - self.tokens
        if deficit <= 0:
            return 0
        return int(deficit / self.refill_rate) + 1


@dataclass
class _SlidingWindowCounter:
    """Sliding window request counter for a single client."""

    window_seconds: int
    max_requests: int
    requests: list[float] = field(default_factory=list)

    def record_and_check(self) -> bool:
        """Record a request and check if within limits."""
        now = time.monotonic()
        cutoff = now - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]
        if len(self.requests) >= self.max_requests:
            return False
        self.requests.append(now)
        return True

    @property
    def remaining(self) -> int:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        active = sum(1 for t in self.requests if t > cutoff)
        return max(0, self.max_requests - active)

    @property
    def reset_seconds(self) -> int:
        if not self.requests:
            return 0
        oldest = min(self.requests)
        return max(0, int(self.window_seconds - (time.monotonic() - oldest)) + 1)


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware implementing rate limiting with token buckets and sliding windows."""

    def __init__(self, app, config: RateLimitConfig | None = None):
        super().__init__(app)
        self.config = config or RateLimitConfig()
        self._buckets: dict[str, _TokenBucket] = {}
        self._windows: dict[str, _SlidingWindowCounter] = {}
        self._refill_rate = self.config.requests_per_minute / 60.0

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in self.config.exclude_paths:
            return await call_next(request)

        client_key = self._get_client_key(request)
        bucket = self._get_or_create_bucket(client_key)
        window = self._get_or_create_window(client_key)

        bucket_ok = bucket.try_consume()
        window_ok = window.record_and_check()

        if not bucket_ok or not window_ok:
            remaining = min(bucket.remaining, window.remaining)
            reset = max(bucket.reset_seconds, window.reset_seconds)
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded",
                    "retry_after_seconds": reset,
                },
                headers={
                    "X-RateLimit-Limit": str(self.config.requests_per_minute),
                    "X-RateLimit-Remaining": str(remaining),
                    "X-RateLimit-Reset": str(reset),
                    "Retry-After": str(reset),
                },
            )

        response = await call_next(request)

        remaining = min(bucket.remaining, window.remaining)
        reset = max(bucket.reset_seconds, window.reset_seconds)
        response.headers["X-RateLimit-Limit"] = str(self.config.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset)

        return response

    def _get_client_key(self, request: Request) -> str:
        """Derive a rate-limiting key from the request."""
        parts: list[str] = []

        if self.config.by_ip:
            client_ip = request.client.host if request.client else "unknown"
            parts.append(f"ip:{client_ip}")

        if self.config.by_token:
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer "):
                token_hash = hash(auth) & 0xFFFFFFFF
                parts.append(f"tok:{token_hash}")

        return "|".join(parts) if parts else "global"

    def _get_or_create_bucket(self, key: str) -> _TokenBucket:
        if key not in self._buckets:
            self._buckets[key] = _TokenBucket.create(
                max_tokens=self.config.burst_size,
                refill_rate=self._refill_rate,
            )
        return self._buckets[key]

    def _get_or_create_window(self, key: str) -> _SlidingWindowCounter:
        if key not in self._windows:
            self._windows[key] = _SlidingWindowCounter(
                window_seconds=self.config.window_seconds,
                max_requests=self.config.requests_per_minute,
            )
        return self._windows[key]

    def reset(self) -> None:
        """Clear all rate limiting state (useful for testing)."""
        self._buckets.clear()
        self._windows.clear()
