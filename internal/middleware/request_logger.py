"""Structured request logging middleware for the Expense Splitter API.

Captures request/response metadata as structured JSON, generates
unique request IDs, measures timing, and redacts sensitive fields.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Callable, Any
from uuid import uuid4

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


logger = logging.getLogger("expense_splitter.requests")


# Headers whose values should be redacted in logs.
SENSITIVE_HEADERS: frozenset[str] = frozenset({
    "authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "x-auth-token",
    "proxy-authorization",
})

# Paths to exclude from logging (typically health checks).
EXCLUDE_PATHS: frozenset[str] = frozenset({
    "/health",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/favicon.ico",
})

# Maximum body size to capture in logs (bytes).
MAX_BODY_LOG_SIZE = 4096

# Request ID header name.
REQUEST_ID_HEADER = "X-Request-ID"


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """Middleware that logs structured request/response information."""

    def __init__(
        self,
        app,
        *,
        log_level: int = logging.INFO,
        log_request_body: bool = False,
        log_response_body: bool = False,
        exclude_paths: frozenset[str] | None = None,
    ):
        super().__init__(app)
        self.log_level = log_level
        self.log_request_body = log_request_body
        self.log_response_body = log_response_body
        self.exclude_paths = exclude_paths or EXCLUDE_PATHS

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in self.exclude_paths:
            return await call_next(request)

        request_id = request.headers.get(REQUEST_ID_HEADER, str(uuid4()))
        start_time = time.monotonic()

        log_entry: dict[str, Any] = {
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "client_ip": request.client.host if request.client else None,
            "headers": _redact_headers(dict(request.headers)),
        }

        if self.log_request_body and request.method in ("POST", "PUT", "PATCH"):
            try:
                body = await request.body()
                if len(body) <= MAX_BODY_LOG_SIZE:
                    log_entry["request_body"] = body.decode("utf-8", errors="replace")
                else:
                    log_entry["request_body"] = f"<truncated: {len(body)} bytes>"
            except Exception:
                log_entry["request_body"] = "<error reading body>"

        try:
            response = await call_next(request)
        except Exception as exc:
            duration = time.monotonic() - start_time
            log_entry.update({
                "status_code": 500,
                "duration_ms": round(duration * 1000, 2),
                "error": str(exc),
                "error_type": type(exc).__name__,
            })
            logger.log(logging.ERROR, json.dumps(log_entry, default=str))
            raise

        duration = time.monotonic() - start_time
        log_entry.update({
            "status_code": response.status_code,
            "duration_ms": round(duration * 1000, 2),
            "response_headers": _redact_headers(dict(response.headers)),
        })

        response.headers[REQUEST_ID_HEADER] = request_id
        response.headers["X-Response-Time"] = f"{round(duration * 1000, 2)}ms"

        level = self._determine_log_level(response.status_code)
        logger.log(level, json.dumps(log_entry, default=str))

        return response

    def _determine_log_level(self, status_code: int) -> int:
        """Choose log level based on response status code."""
        if status_code >= 500:
            return logging.ERROR
        if status_code >= 400:
            return logging.WARNING
        return self.log_level


def _redact_headers(headers: dict[str, str]) -> dict[str, str]:
    """Redact values of sensitive headers."""
    redacted = {}
    for key, value in headers.items():
        lower_key = key.lower()
        if lower_key in SENSITIVE_HEADERS:
            if len(value) > 8:
                redacted[key] = value[:4] + "****" + value[-4:]
            else:
                redacted[key] = "****"
        else:
            redacted[key] = value
    return redacted


def format_log_line(entry: dict[str, Any]) -> str:
    """Format a log entry as a single readable line for console output.

    Format: [request_id] METHOD /path -> status_code (duration_ms ms)
    """
    request_id = entry.get("request_id", "unknown")[:8]
    method = entry.get("method", "?")
    path = entry.get("path", "?")
    status = entry.get("status_code", "?")
    duration = entry.get("duration_ms", 0)
    return f"[{request_id}] {method} {path} -> {status} ({duration}ms)"
