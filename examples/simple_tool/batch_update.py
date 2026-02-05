#!/usr/bin/env python3
"""Batch update tool using sqlym with auto_commit.

This example demonstrates:
- Using Sqlym with auto_commit=True for simple scripts
- Batch processing with 2-way SQL

Usage:
    cd examples/simple_tool
    PYTHONPATH="../../src:." uv run python batch_update.py
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from sqlym import Sqlym


@dataclass
class Product:
    id: int
    name: str
    price: int
    category: str


def setup_database() -> sqlite3.Connection:
    """Create sample database."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price INTEGER NOT NULL,
            category TEXT NOT NULL
        )
    """)

    conn.executemany(
        "INSERT INTO products (name, price, category) VALUES (?, ?, ?)",
        [
            ("Widget A", 1000, "electronics"),
            ("Widget B", 2000, "electronics"),
            ("Gadget X", 1500, "electronics"),
            ("Book 1", 500, "books"),
            ("Book 2", 800, "books"),
            ("Chair", 5000, "furniture"),
            ("Desk", 10000, "furniture"),
        ],
    )
    conn.commit()
    return conn


def main() -> None:
    print("=" * 50)
    print("Batch Update Tool (auto_commit example)")
    print("=" * 50)

    conn = setup_database()
    sql_dir = Path(__file__).parent / "sql"

    # auto_commit=True: each execute() commits immediately
    db = Sqlym(conn, sql_dir=sql_dir, auto_commit=True)

    # Show current state
    print("\n[Before update]")
    products = db.query(Product, "find_by_category.sql", {"category": None})
    for p in products:
        print(f"  {p.name}: {p.price} yen ({p.category})")

    # Batch update: 10% price increase for electronics
    print("\n[Updating electronics prices (+10%)]")
    affected = db.execute("update_price.sql", {"category": "electronics", "rate": 1.1})
    print(f"  Updated {affected} rows (auto-committed)")

    # Batch update: 5% price increase for books
    print("\n[Updating books prices (+5%)]")
    affected = db.execute("update_price.sql", {"category": "books", "rate": 1.05})
    print(f"  Updated {affected} rows (auto-committed)")

    # Show result
    print("\n[After update]")
    products = db.query(Product, "find_by_category.sql", {"category": None})
    for p in products:
        print(f"  {p.name}: {p.price} yen ({p.category})")

    # Conditional query using 2-way SQL
    print("\n[Query: electronics only]")
    products = db.query(Product, "find_by_category.sql", {"category": "electronics"})
    for p in products:
        print(f"  {p.name}: {p.price} yen")

    print("\n" + "=" * 50)


if __name__ == "__main__":
    main()
