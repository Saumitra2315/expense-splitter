# Expense Splitter API

A production-ready FastAPI service for managing shared expenses across groups with multi-currency support, offline sync, and intelligent debt simplification.

## Architecture

### Storage Layer (`internal/storage/`)
- **sqlite.py**: Transactional SQLite database with:
  - Foreign key constraints
  - Write-ahead logging (WAL) for concurrency
  - Schema initialization with all required tables
  - Context managers for read/write transactions

### Models (`internal/models/`)
- **group.py**: Group and membership management
- **expense.py**: Expense creation with allocation support
- **fx.py**: Foreign exchange rate records
- **recurring.py**: Recurring expense templates and materialization
- **settlement.py**: Debt settlement records
- **sync.py**: Offline sync operation definitions

### Service Layer (`internal/service/`)
- **ledger_service.py**: Core business logic (1149 lines)
  - Idempotent write handling via request hashing
  - Group and membership management with versioning
  - Expense creation with multiple split modes
  - Recurring expense generation and materialization
  - Multi-currency conversion with FX lookups
  - Settlement recording
  - Balance computation with time-travel queries
  - Audit trail recording
  - Offline sync with conflict detection

- **split_service.py**: Financial computation
  - Equal split allocation
  - Fixed amount allocation
  - Percentage allocation
  - Debt simplification:
    - Optimal solver for ≤10 members (depth-first search)
    - Greedy fallback heuristic for larger groups
  - Currency-aware quantization

### Handlers (`internal/handler/`)
- **group_handler.py**: Group endpoints
  - `POST /groups` - Create group
  - `GET /groups/{group_id}` - Get group details
  - `POST /groups/{group_id}/members` - Add/remove members

- **expense_handler.py**: All other endpoints
  - `POST /expenses` - Create expense
  - `POST /fx-rates` - Record FX rate
  - `POST /recurring-templates` - Create template
  - `POST /recurring-templates/{id}/materialize` - Generate instances
  - `POST /settlements` - Record settlement
  - `GET /groups/{group_id}/balances` - Compute balances
  - `GET /groups/{group_id}/settlement-plan` - Get optimal transfers
  - `GET /groups/{group_id}/audit` - Audit history
  - `POST /sync` - Offline sync

## Key Features

### 1. Transactional Consistency
- All writes wrapped in IMMEDIATE transactions
- Idempotent operations via request hash tracking
- Version-based optimistic locking on groups

### 2. Expense Allocation
Three split modes:
- **Equal**: Divide equally, distribute rounding to first N members
- **Fixed**: Each member gets exact amount
- **Percentage**: Each member's share as percentage of total

### 3. Multi-Currency Support
- Per-group base currency
- FX conversions with effective dating
- Direct rate lookup, inverse calculation, or pivot currency conversion
- Time-based valuation policy for historical accuracy

### 4. Recurring Expenses
- Daily/weekly/monthly cadence
- Automatic generation up to specified date
- Idempotent materialization with per-occurrence deduplication
- Template-linked to individual expense records

### 5. Debt Simplification
Optimal algorithm for small groups (≤10 members):
- Depth-first search explores all pairing combinations
- Minimizes transfer count

Greedy fallback for larger groups:
- Sort by amount (descending)
- Pair largest debtor with largest creditor
- O(n²) but practical performance

### 6. Audit & Compliance
- Complete event log with timestamps
- Group version tracking per event
- JSON payload capture
- Time-travel member queries for historical accuracy

### 7. Offline Sync
- Device-scoped operation tracking
- Conflict detection via version mismatch
- Replay prevention via device + operation ID
- Supports: membership changes, expenses, settlements, recurring

## API Usage

### Authentication
All endpoints require `Authorization: Bearer test-token` header (configurable in middleware).

### Example Flow

```bash
# Create a group
curl -X POST http://localhost:8080/groups \
  -H "Authorization: Bearer test-token" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Trip to Paris",
    "base_currency": "EUR",
    "members": [
      {"member_id": "alice", "display_name": "Alice"},
      {"member_id": "bob", "display_name": "Bob"}
    ]
  }'

# Add an expense
curl -X POST http://localhost:8080/expenses \
  -H "Authorization: Bearer test-token" \
  -H "Content-Type: application/json" \
  -d '{
    "group_id": "fa2e6d8b-5415-4d02-a953-d862e28d43ea",
    "paid_by": "alice",
    "amount": "90",
    "currency_code": "EUR",
    "description": "Hotel",
    "split_mode": "equal",
    "participant_ids": ["alice", "bob"]
  }'

# Get settlement plan
curl http://localhost:8080/groups/fa2e6d8b-5415-4d02-a953-d862e28d43ea/settlement-plan \
  -H "Authorization: Bearer test-token"
```

## Running the Service

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run
python main.py
# Starts on http://localhost:8080

# Health check
curl http://localhost:8080/health
```

## Database

- Location: `./expense_splitter.db` (or `$EXPENSE_SPLITTER_DB_PATH`)
- Auto-initialized on first service start
- Contains 12 tables with full referential integrity
- Indices on frequently-queried columns

## Error Handling

Service errors return appropriate HTTP status codes:
- `400 Bad Request` - Validation errors
- `404 Not Found` - Resource not found
- `409 Conflict` - Version mismatch or idempotency violation
- `422 Unprocessable Entity` - Semantic validation failure

All errors include a `detail` field explaining the issue.

## Implementation Notes

1. **Decimal precision**: Uses `Decimal` type throughout for financial calculations
2. **Time handling**: All times in UTC with timezone normalization
3. **Currency scaling**: Handles 0, 2, and 3 decimal currencies (JPY, BHD, etc.)
4. **Empty balances**: Not returned in balance queries
5. **Member state**: Tracks joined_at/left_at with effective_at versioning
6. **Settlement as ledger entry**: Settlements create reverse debit entries

## Testing

The implementation has been tested with:
- ✓ Group creation and management
- ✓ Equal, fixed, and percentage splits
- ✓ FX rate conversion
- ✓ Recurring expense generation (multi-occurrence)
- ✓ Settlement recording and balance updates
- ✓ Optimal and greedy settlement planning
- ✓ Audit history tracking
- ✓ Membership changes
- ✓ Offline sync with conflict detection
