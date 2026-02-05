#!/usr/bin/env python3
"""Example: Get auto-generated ID after INSERT.

Demonstrates:
- Using db.insert() to get lastrowid
- Difference between execute() and insert()

Usage:
    cd examples/crud
    PYTHONPATH="../../src" uv run python insert_with_id.py
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from sqlym import Sqlym


@dataclass
class User:
    id: int | None = None
    name: str = ""
    email: str = ""


def setup_database() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE
        )
    """)
    return conn


def main() -> None:
    print("=" * 50)
    print("Insert with Auto-generated ID")
    print("=" * 50)

    conn = setup_database()
    sql_dir = Path(__file__).parent / "sql"
    db = Sqlym(conn, sql_dir=sql_dir)

    # execute() returns rowcount (affected rows)
    print("\n[Using execute() - returns rowcount]")
    rowcount = db.execute("insert.sql", {"name": "Alice", "email": "alice@example.com"})
    print(f"  rowcount: {rowcount}")

    # insert() returns lastrowid (auto-generated ID)
    print("\n[Using insert() - returns lastrowid]")
    user_id = db.insert("insert.sql", {"name": "Bob", "email": "bob@example.com"})
    print(f"  lastrowid: {user_id}")

    user_id = db.insert("insert.sql", {"name": "Charlie", "email": "charlie@example.com"})
    print(f"  lastrowid: {user_id}")

    conn.commit()

    # Verify
    print("\n[Verify]")
    users = db.query(User, "select_all.sql")
    for u in users:
        print(f"  {u.id}: {u.name} <{u.email}>")

    # Practical usage: Create and retrieve
    print("\n[Practical: Create user and get by ID]")
    new_id = db.insert("insert.sql", {"name": "David", "email": "david@example.com"})
    conn.commit()

    user = db.query_one(User, "select_by_id.sql", {"id": new_id})
    if user:
        print(f"  Created user: {user.id}: {user.name} <{user.email}>")

    print("\n" + "=" * 50)


if __name__ == "__main__":
    main()
