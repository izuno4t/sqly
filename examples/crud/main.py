#!/usr/bin/env python3
"""Basic CRUD example using sqlym.

Demonstrates:
- Create (INSERT)
- Read (SELECT)
- Update (UPDATE)
- Delete (DELETE)

Usage:
    cd examples/crud
    PYTHONPATH="../../src" uv run python main.py
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
    print("CRUD Example")
    print("=" * 50)

    conn = setup_database()
    sql_dir = Path(__file__).parent / "sql"
    db = Sqlym(conn, sql_dir=sql_dir)

    # CREATE
    print("\n[Create]")
    db.execute("insert.sql", {"name": "Alice", "email": "alice@example.com"})
    db.execute("insert.sql", {"name": "Bob", "email": "bob@example.com"})
    db.execute("insert.sql", {"name": "Charlie", "email": "charlie@example.com"})
    conn.commit()
    print("  Inserted 3 users")

    # READ (all)
    print("\n[Read all]")
    users = db.query(User, "select_all.sql")
    for u in users:
        print(f"  {u.id}: {u.name} <{u.email}>")

    # READ (by id)
    print("\n[Read by id]")
    user = db.query_one(User, "select_by_id.sql", {"id": 2})
    if user:
        print(f"  Found: {user.name} <{user.email}>")

    # READ (with condition)
    print("\n[Read with condition]")
    users = db.query(User, "select_by_name.sql", {"name": "Alice"})
    for u in users:
        print(f"  {u.id}: {u.name} <{u.email}>")

    # UPDATE
    print("\n[Update]")
    affected = db.execute(
        "update.sql", {"id": 2, "name": "Bob Updated", "email": "bob.new@example.com"}
    )
    conn.commit()
    print(f"  Updated {affected} row(s)")

    user = db.query_one(User, "select_by_id.sql", {"id": 2})
    if user:
        print(f"  After: {user.name} <{user.email}>")

    # DELETE
    print("\n[Delete]")
    affected = db.execute("delete.sql", {"id": 3})
    conn.commit()
    print(f"  Deleted {affected} row(s)")

    # Final state
    print("\n[Final state]")
    users = db.query(User, "select_all.sql")
    for u in users:
        print(f"  {u.id}: {u.name} <{u.email}>")

    print("\n" + "=" * 50)


if __name__ == "__main__":
    main()
