# SettleUp API

SettleUp is a **backend-first shared-expense ledger service** built with FastAPI and SQLite.

It is not positioned as a production deployment template yet. It is positioned as a **production-inspired backend project** with non-trivial financial logic and solid engineering scope.

## Why This Project Is Different

Compared with basic CRUD expense trackers, SettleUp includes:

- Multi-member group ledgers with versioned membership events
- Equal/fixed/percentage split modes with currency-aware rounding
- Multi-currency balances with time-aware FX conversion
- Recurring expense templates and materialization
- Debt settlement planning (optimal for small groups, greedy fallback for larger groups)
- Audit trail events and idempotent write handling
- Offline sync operation replay/conflict handling
- Budget tracking, notification preferences, and export flows
- Request logging and rate limiting middleware

## Tech Stack

- Python 3.12
- FastAPI
- SQLite (WAL mode, foreign keys enabled)
- Pydantic v2
- Pytest (test suite)
- GitHub Actions (CI)
- Docker

## Project Structure

- `internal/handler/`: HTTP routes
- `internal/service/`: business logic
- `internal/models/`: request/response schemas
- `internal/storage/`: DB schema and connection helpers
- `internal/middleware/`: auth, request logging, rate limiting
- `internal/utils/`: helper modules
- `tests/`: pytest suite
- `scripts/`: utility scripts (demo seed + token generation)

## Local Setup

```bash
make setup
source .venv/bin/activate
make run
```

Service starts on `http://localhost:8080`.

Health check:

```bash
curl http://localhost:8080/health
```

## Authentication

All protected endpoints require a bearer token.

You can use either static service tokens or signed auth tokens.

Static service tokens:

```bash
export SETTLEUP_API_TOKENS="local-dev-token,another-token"
```

Signed tokens (recommended for realistic backend behavior):

```bash
export SETTLEUP_AUTH_SECRET="replace-with-long-random-secret"
python scripts/generate_auth_token.py --sub alice --role admin
# or
make token
```

If no static token list and no signing secret are set, the development fallback token is:

```text
dev-token-change-me
```

Example request header:

```text
Authorization: Bearer local-dev-token
```

## Quick API Flow

```bash
# 1) Create group
curl -X POST http://localhost:8080/groups \
  -H "Authorization: Bearer local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Trip to Paris",
    "base_currency": "EUR",
    "members": [
      {"member_id": "alice", "display_name": "Alice"},
      {"member_id": "bob", "display_name": "Bob"},
      {"member_id": "cara", "display_name": "Cara"}
    ]
  }'

# 2) Add expense
curl -X POST http://localhost:8080/expenses \
  -H "Authorization: Bearer local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "<group-id>",
    "paid_by": "alice",
    "amount": "90",
    "currency_code": "EUR",
    "description": "Hotel",
    "split_mode": "equal",
    "participant_ids": ["alice", "bob", "cara"]
  }'

# 3) Get settlement plan
curl "http://localhost:8080/groups/<group-id>/settlement-plan" \
  -H "Authorization: Bearer local-dev-token"
```

## Testing

Run tests locally:

```bash
make test
```

The suite currently covers:

- Group creation
- Equal/fixed/percentage splits
- Rounding edge cases (zero and 3-decimal currencies)
- FX conversion in balance computation
- Settlement-plan correctness regression
- Recurring materialization
- Budget summary calculations
- Auth and rate-limit behavior

CI runs this suite on every push and pull request via GitHub Actions.

## Demo Seed Data

Generate a realistic demo group with expenses, FX rates, recurring entries, budgets, and a sample settlement:

```bash
make seed
```

Or pick a DB path:

```bash
python scripts/seed_demo.py --db-path ./data/demo.db
```

The seed script prints resulting balances and settlement transfers for quick verification.

## Docker

Build and run:

```bash
docker build -t settleup-api .
docker run --rm -p 8080:8080 \
  -e SETTLEUP_API_TOKENS=local-dev-token \
  -v $(pwd)/data:/data \
  settleup-api
```

The DB path in-container defaults to `/data/settleup.db`.

## Render Deployment

This repo includes `render.yaml` for one-click blueprint deployment.

### 1. Deploy

1. Push this repo to GitHub.
2. In Render: `New +` -> `Blueprint`.
3. Select this repository.
4. Render will detect `render.yaml` and provision the web service.

### 2. Verify env and storage

`render.yaml` already sets:

- `SETTLEUP_ALLOW_DEV_TOKEN=0` (disables fallback dev token)
- `SETTLEUP_AUTH_SECRET` (auto-generated)
- `SETTLEUP_DB_PATH=/var/data/settleup.db` with a persistent disk mount

### 3. Generate a token for demo calls

Locally generate a signed token with the same `SETTLEUP_AUTH_SECRET` value from Render:

```bash
python scripts/generate_auth_token.py --sub demo-user --role admin --secret "<RENDER_SECRET>"
```

### 4. Smoke test the live service

```bash
export API="https://<your-render-service>.onrender.com"
export TOKEN="<signed-token>"

curl "$API/health"

curl -X POST "$API/groups" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Render Demo Group",
    "base_currency": "USD",
    "members": [
      {"member_id": "alice", "display_name": "Alice"},
      {"member_id": "bob", "display_name": "Bob"},
      {"member_id": "cara", "display_name": "Cara"}
    ]
  }'
```

### Effective demo usage

- Use `/docs` as your live API demo UI.
- Keep one reusable signed admin token during interview/demo sessions.
- Start with the flow: create group -> add expense -> settlement plan.
- Use `/groups/{id}/audit` to show traceability and event history.

## Notes on Scope

- This repository is backend-only by design.
- If you need a product-facing demo, add a lightweight UI for group creation, expense entry, and settlement viewing.
- Current implementation is strongest as a backend/platform interview project.

## Deployment Status

- Local/Docker/CI flows are set up.
- Render blueprint deployment is configured in this repo.

## Resume-Friendly Description

"Built and tested a containerized FastAPI shared-expense ledger backend supporting multi-currency balance computation, recurring billing, audit logging, idempotent writes, budget tracking, and settlement planning."
