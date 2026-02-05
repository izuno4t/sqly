#!/usr/bin/env python3
"""Transaction management example (auto_commit=False).

Demonstrates:
- Manual commit/rollback
- Transaction with multiple operations
- Error handling and rollback

Usage:
    cd examples/simple_tool
    PYTHONPATH="../../src" uv run python transaction.py
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from sqlym import Sqlym


@dataclass
class Account:
    id: int
    name: str
    balance: int


def setup_database() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            balance INTEGER NOT NULL
        )
    """)

    conn.executemany(
        "INSERT INTO accounts (name, balance) VALUES (?, ?)",
        [
            ("Alice", 10000),
            ("Bob", 5000),
            ("Charlie", 3000),
        ],
    )
    conn.commit()
    return conn


def show_balances(db: Sqlym) -> None:
    accounts = db.query(Account, "select_accounts.sql")
    for a in accounts:
        print(f"    {a.name}: {a.balance} yen")


def transfer(db: Sqlym, from_id: int, to_id: int, amount: int) -> None:
    """Transfer money between accounts."""
    # Check source balance
    source = db.query_one(Account, "select_account_by_id.sql", {"id": from_id})
    if source is None:
        raise ValueError(f"Account {from_id} not found")
    if source.balance < amount:
        raise ValueError(f"Insufficient balance: {source.balance} < {amount}")

    # Withdraw from source
    db.execute("update_balance.sql", {"id": from_id, "amount": -amount})

    # Deposit to destination
    db.execute("update_balance.sql", {"id": to_id, "amount": amount})


def main() -> None:
    print("=" * 50)
    print("Transaction Example (auto_commit=False)")
    print("=" * 50)

    conn = setup_database()
    sql_dir = Path(__file__).parent / "sql"

    # auto_commit=False (default): manual commit/rollback
    db = Sqlym(conn, sql_dir=sql_dir, auto_commit=False)

    print("\n[Initial balances]")
    show_balances(db)

    # Successful transaction
    print("\n[Transfer 2000 from Alice to Bob]")
    try:
        transfer(db, from_id=1, to_id=2, amount=2000)
        conn.commit()  # Explicit commit
        print("  Committed!")
    except ValueError as e:
        conn.rollback()
        print(f"  Rolled back: {e}")

    print("\n[After successful transfer]")
    show_balances(db)

    # Failed transaction (insufficient balance)
    print("\n[Transfer 50000 from Bob to Charlie (will fail)]")
    try:
        transfer(db, from_id=2, to_id=3, amount=50000)
        conn.commit()
        print("  Committed!")
    except ValueError as e:
        conn.rollback()  # Explicit rollback
        print(f"  Rolled back: {e}")

    print("\n[After failed transfer (no change)]")
    show_balances(db)

    # Multiple operations in one transaction
    print("\n[Multiple transfers in one transaction]")
    try:
        transfer(db, from_id=1, to_id=2, amount=1000)  # Alice -> Bob
        transfer(db, from_id=2, to_id=3, amount=1500)  # Bob -> Charlie
        conn.commit()
        print("  Both transfers committed!")
    except ValueError as e:
        conn.rollback()
        print(f"  Rolled back: {e}")

    print("\n[Final balances]")
    show_balances(db)

    print("\n" + "=" * 50)


if __name__ == "__main__":
    main()
