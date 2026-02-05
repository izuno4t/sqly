# Simple Tool Examples

Simple tool examples using sqlym.

## Structure

```
simple_tool/
├── batch_update.py    # Batch update (auto_commit=True)
├── transaction.py     # Transaction management (auto_commit=False)
└── sql/
```

## Run

```bash
cd examples/simple_tool
PYTHONPATH="../../src" uv run python batch_update.py
PYTHONPATH="../../src" uv run python transaction.py
```

## auto_commit=True (batch_update.py)

Auto-commit after each execute(). For batch processing.

```python
db = Sqlym(conn, sql_dir="sql", auto_commit=True)

db.execute("update_price.sql", {"category": "electronics", "rate": 1.1})
# ^ Committed immediately

db.execute("update_price.sql", {"category": "books", "rate": 1.05})
# ^ Committed immediately
```

## auto_commit=False (transaction.py)

Manual commit/rollback. For transaction management.

```python
db = Sqlym(conn, sql_dir="sql", auto_commit=False)  # default

try:
    # Multiple operations in one transaction
    db.execute("withdraw.sql", {"id": 1, "amount": 2000})
    db.execute("deposit.sql", {"id": 2, "amount": 2000})
    conn.commit()  # Explicit commit
except Exception:
    conn.rollback()  # Rollback on error
```

## Use Cases

| Mode | Use case |
|------|----------|
| `auto_commit=True` | Batch updates, migrations, imports |
| `auto_commit=False` | Money transfers, operations requiring consistency |
