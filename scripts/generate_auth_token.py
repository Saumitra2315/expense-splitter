#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime

from internal.utils.crypto import create_token


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a signed SettleUp bearer token.",
    )
    parser.add_argument("--sub", required=True, help="Subject (user/service id)")
    parser.add_argument("--role", default="user", help="Role claim")
    parser.add_argument(
        "--ttl-seconds",
        type=int,
        default=24 * 60 * 60,
        help="Token validity in seconds (default: 86400)",
    )
    parser.add_argument(
        "--secret",
        default=os.getenv("SETTLEUP_AUTH_SECRET", ""),
        help="Signing secret (defaults to SETTLEUP_AUTH_SECRET env var)",
    )
    parser.add_argument(
        "--claims-json",
        default="{}",
        help="Additional claims as JSON object",
    )
    args = parser.parse_args()

    if not args.secret:
        print("error: --secret is required when SETTLEUP_AUTH_SECRET is unset", file=sys.stderr)
        return 2

    try:
        extra_claims = json.loads(args.claims_json)
        if not isinstance(extra_claims, dict):
            raise ValueError("claims-json must be a JSON object")
    except Exception as exc:  # pragma: no cover
        print(f"error: invalid --claims-json: {exc}", file=sys.stderr)
        return 2

    payload = {
        "sub": args.sub,
        "role": args.role,
        **extra_claims,
    }
    token = create_token(payload, secret=args.secret, ttl_seconds=args.ttl_seconds)
    expires_at = datetime.now(UTC).timestamp() + args.ttl_seconds
    print(token)
    print(f"expires_at_unix={int(expires_at)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
