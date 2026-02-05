# Examples

Usage examples for sqlym.

## crud/

Basic CRUD operations.

```bash
cd examples/crud
PYTHONPATH="../../src" uv run python main.py
PYTHONPATH="../../src" uv run python insert_with_id.py
```

See [crud/README.md](crud/README.md) for details.

## simple_tool/

Simple tool examples.

| File | Description |
|------|-------------|
| batch_update.py | Batch update (`auto_commit=True`) |
| transaction.py | Transaction management (`auto_commit=False`) |

```bash
cd examples/simple_tool
PYTHONPATH="../../src" uv run python batch_update.py
PYTHONPATH="../../src" uv run python transaction.py
```

See [simple_tool/README.md](simple_tool/README.md) for details.

## clean_architecture/

Clean Architecture implementation example.

```
clean_architecture/
├── main.py                      # Composition Root
├── domain/
│   ├── models/                  # Domain models
│   └── repositories/            # Repository interfaces
├── application/
│   └── use_cases/               # Use cases
├── infrastructure/
│   ├── dao/
│   │   └── entities/            # Persistence entities
│   └── repositories/            # Repository (Entity→Model)
└── sql/
```

```bash
cd examples/clean_architecture
PYTHONPATH="../../src:." uv run python main.py
```

See [clean_architecture/README.md](clean_architecture/README.md) for details.
