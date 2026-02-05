# CRUD Example

Basic CRUD operations using sqlym.

## Structure

```
crud/
├── main.py              # Basic CRUD
├── insert_with_id.py    # Get auto-generated ID
└── sql/
    ├── insert.sql
    ├── select_all.sql
    ├── select_by_id.sql
    ├── select_by_name.sql
    ├── update.sql
    └── delete.sql
```

## Run

```bash
cd examples/crud
PYTHONPATH="../../src" uv run python main.py
PYTHONPATH="../../src" uv run python insert_with_id.py
```

## Operations

### Create

```python
# Get rowcount
rowcount = db.execute("insert.sql", {"name": "Alice", "email": "alice@example.com"})

# Get auto-generated ID
user_id = db.insert("insert.sql", {"name": "Bob", "email": "bob@example.com"})
conn.commit()
```

```sql
-- insert.sql
INSERT INTO users (name, email)
VALUES (/* name */'John', /* email */'john@example.com')
```

### Read

```python
# Get all
users = db.query(User, "select_all.sql")

# Get one
user = db.query_one(User, "select_by_id.sql", {"id": 2})

# Conditional ($name is None -> line removed)
users = db.query(User, "select_by_name.sql", {"name": "Alice"})
```

### Update

```python
affected = db.execute("update.sql", {"id": 2, "name": "New Name", "email": "new@example.com"})
conn.commit()
```

### Delete

```python
affected = db.execute("delete.sql", {"id": 3})
conn.commit()
```

## execute() vs insert()

| Method | Returns | Use case |
|--------|---------|----------|
| `execute()` | `rowcount` (affected rows) | UPDATE, DELETE |
| `insert()` | `lastrowid` (auto-generated ID) | INSERT |
