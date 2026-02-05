"""SQLite 統合テスト: パース → DB実行 → マッピングの一連フロー検証."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import pytest

from sqlym import Column, Dialect, ParsedSQL, SqlLoader, create_mapper, escape_like, parse_sql
from sqlym.mapper.column import entity


@dataclass
class Employee:
    """テスト用エンティティ."""

    id: int
    name: str
    dept_id: int | None = None


@dataclass
class AnnotatedEmployee:
    """Annotated カラムマッピング付きエンティティ."""

    id: Annotated[int, Column("emp_id")]
    name: Annotated[str, Column("emp_name")]
    dept_id: Annotated[int | None, Column("department_id")] = None


@entity(naming="snake_to_camel")
@dataclass
class CamelEmployee:
    """CamelCase カラム名のエンティティ."""

    emp_id: int
    emp_name: str


@pytest.fixture
def db() -> sqlite3.Connection:
    """テスト用 SQLite インメモリ DB を作成し、テストデータを投入する."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            dept_id INTEGER
        )
    """)
    conn.executemany(
        "INSERT INTO employees (id, name, dept_id) VALUES (?, ?, ?)",
        [
            (1, "Alice", 10),
            (2, "Bob", 20),
            (3, "Charlie", 10),
            (4, "Diana", None),
        ],
    )
    conn.commit()
    return conn


@pytest.fixture
def db_aliased() -> sqlite3.Connection:
    """エイリアス付きカラム名の DB を作成する."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name TEXT NOT NULL,
            department_id INTEGER
        )
    """)
    conn.executemany(
        "INSERT INTO employees (emp_id, emp_name, department_id) VALUES (?, ?, ?)",
        [
            (1, "Alice", 10),
            (2, "Bob", 20),
        ],
    )
    conn.commit()
    return conn


def _fetch_all(conn: sqlite3.Connection, result: ParsedSQL) -> list[dict]:
    """ParsedSQL を実行し、行辞書のリストを返すヘルパー."""
    cursor = conn.execute(result.sql, result.params)
    return [dict(row) for row in cursor.fetchall()]


class TestBasicFlow:
    """基本フロー: パース → 実行 → マッピング."""

    def test_select_all(self, db: sqlite3.Connection) -> None:
        """全件取得."""
        sql = "SELECT * FROM employees"
        result = parse_sql(sql, {})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 4
        assert employees[0] == Employee(id=1, name="Alice", dept_id=10)

    def test_select_with_param(self, db: sqlite3.Connection) -> None:
        """パラメータ付き検索."""
        sql = "SELECT * FROM employees WHERE id = /* $id */999"
        result = parse_sql(sql, {"id": 2})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 1
        assert employees[0] == Employee(id=2, name="Bob", dept_id=20)


class TestLineRemoval:
    """行削除 → 実行 → マッピング."""

    def test_none_param_removes_condition(self, db: sqlite3.Connection) -> None:
        """None パラメータで条件行が削除され、全件返る."""
        sql = "SELECT * FROM employees\nWHERE\n    name = /* $name */'default'"
        result = parse_sql(sql, {"name": None})
        assert "WHERE" not in result.sql
        rows = _fetch_all(db, result)
        assert len(rows) == 4

    def test_partial_removal(self, db: sqlite3.Connection) -> None:
        """一部条件のみ削除、残った条件でフィルタリング."""
        sql = (
            "SELECT * FROM employees\n"
            "WHERE\n"
            "    dept_id = /* $dept_id */999\n"
            "    AND name = /* $name */'default'"
        )
        result = parse_sql(sql, {"dept_id": 10, "name": None})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 2
        assert all(e.dept_id == 10 for e in employees)

    def test_all_conditions_none(self, db: sqlite3.Connection) -> None:
        """全条件 None で WHERE ごと削除、全件返る."""
        sql = (
            "SELECT * FROM employees\n"
            "WHERE\n"
            "    dept_id = /* $dept_id */999\n"
            "    AND name = /* $name */'default'"
        )
        result = parse_sql(sql, {"dept_id": None, "name": None})
        assert "WHERE" not in result.sql
        rows = _fetch_all(db, result)
        assert len(rows) == 4


class TestInClause:
    """IN 句展開 → 実行 → マッピング."""

    def test_in_clause_list(self, db: sqlite3.Connection) -> None:
        """IN 句でリスト検索."""
        sql = "SELECT * FROM employees WHERE id IN /* $ids */(999)"
        result = parse_sql(sql, {"ids": [1, 3]})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 2
        assert {e.id for e in employees} == {1, 3}

    def test_in_clause_single(self, db: sqlite3.Connection) -> None:
        """IN 句で単一要素リスト."""
        sql = "SELECT * FROM employees WHERE id IN /* $ids */(999)"
        result = parse_sql(sql, {"ids": [2]})
        rows = _fetch_all(db, result)
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_in_clause_empty_list(self, db: sqlite3.Connection) -> None:
        """IN 句で空リスト → NULL → 0件."""
        sql = "SELECT * FROM employees WHERE id IN /* $ids */(999)"
        result = parse_sql(sql, {"ids": []})
        rows = _fetch_all(db, result)
        assert len(rows) == 0


class TestAnnotatedColumnMapping:
    """Annotated[T, Column('X')] によるカラムマッピング統合テスト."""

    def test_annotated_mapping(self, db_aliased: sqlite3.Connection) -> None:
        """DB カラム名→フィールド名のマッピングが動作する."""
        sql = "SELECT * FROM employees"
        result = parse_sql(sql, {})
        rows = _fetch_all(db_aliased, result)
        mapper = create_mapper(AnnotatedEmployee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 2
        assert employees[0] == AnnotatedEmployee(id=1, name="Alice", dept_id=10)


class TestEntityDecorator:
    """@entity デコレータ統合テスト."""

    def test_naming_convention_mapping(self, db: sqlite3.Connection) -> None:
        """Snake_to_camel naming でカラムマッピング.

        DB のカラム名がフィールド名と一致するケース
        (camelCase に変換されたカラム名が DB に存在しないのでフォールバック)。
        """
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE employees (
                empId INTEGER PRIMARY KEY,
                empName TEXT NOT NULL
            )
        """)
        conn.execute("INSERT INTO employees (empId, empName) VALUES (1, 'Alice')")
        conn.commit()

        sql = "SELECT * FROM employees"
        result = parse_sql(sql, {})
        rows = [dict(row) for row in conn.execute(result.sql, result.params).fetchall()]
        mapper = create_mapper(CamelEmployee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 1
        assert employees[0] == CamelEmployee(emp_id=1, emp_name="Alice")


class TestSqlLoaderIntegration:
    """SqlLoader → パース → 実行 → マッピングの一連フロー."""

    def test_load_and_execute(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """SQL ファイルを読み込んでパース・実行・マッピング."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "find_by_dept.sql").write_text(
            "SELECT * FROM employees\nWHERE\n    dept_id = /* $dept_id */999",
            encoding="utf-8",
        )

        loader = SqlLoader(sql_dir)
        sql_template = loader.load("find_by_dept.sql")
        result = parse_sql(sql_template, {"dept_id": 10})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 2
        assert all(e.dept_id == 10 for e in employees)

    def test_load_with_none_param(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """SQL ファイルを読み込み、None パラメータで条件削除."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "search.sql").write_text(
            "SELECT * FROM employees\n"
            "WHERE\n"
            "    dept_id = /* $dept_id */999\n"
            "    AND name = /* $name */'default'",
            encoding="utf-8",
        )

        loader = SqlLoader(sql_dir)
        sql_template = loader.load("search.sql")
        result = parse_sql(sql_template, {"dept_id": None, "name": "Alice"})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 1
        assert employees[0].name == "Alice"


class TestMultipleParams:
    """複数パラメータの組み合わせ."""

    def test_multiple_conditions(self, db: sqlite3.Connection) -> None:
        """複数条件でフィルタリング."""
        sql = (
            "SELECT * FROM employees\n"
            "WHERE\n"
            "    dept_id = /* $dept_id */999\n"
            "    AND name = /* $name */'default'"
        )
        result = parse_sql(sql, {"dept_id": 10, "name": "Alice"})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 1
        assert employees[0] == Employee(id=1, name="Alice", dept_id=10)

    def test_in_clause_with_regular_param(self, db: sqlite3.Connection) -> None:
        """IN 句と通常パラメータの混在."""
        sql = (
            "SELECT * FROM employees\n"
            "WHERE dept_id = /* $dept_id */999\n"
            "  AND id IN /* $ids */(999)"
        )
        result = parse_sql(sql, {"dept_id": 10, "ids": [1, 3]})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 2
        assert {e.name for e in employees} == {"Alice", "Charlie"}


class TestNullHandling:
    """NULL 値の扱い."""

    def test_non_removable_null_param(self, db: sqlite3.Connection) -> None:
        """非 removable パラメータの None は NULL バインド."""
        sql = "SELECT * FROM employees WHERE dept_id = /* dept_id */999"
        result = parse_sql(sql, {"dept_id": None})
        rows = _fetch_all(db, result)
        # SQLite の WHERE dept_id = NULL は0件（IS NULL が必要）
        assert len(rows) == 0

    def test_select_null_field(self, db: sqlite3.Connection) -> None:
        """NULL フィールドを持つレコードのマッピング."""
        sql = "SELECT * FROM employees WHERE id = /* $id */999"
        result = parse_sql(sql, {"id": 4})
        rows = _fetch_all(db, result)
        mapper = create_mapper(Employee)
        employees = mapper.map_rows(rows)
        assert len(employees) == 1
        assert employees[0] == Employee(id=4, name="Diana", dept_id=None)


class TestDialectLikeEscape:
    """LIKE エスケープ統合テスト (TASK-034)."""

    @pytest.fixture
    def db_with_special_names(self) -> sqlite3.Connection:
        """特殊文字を含む名前のテストデータ."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        conn.executemany(
            "INSERT INTO products (id, name) VALUES (?, ?)",
            [
                (1, "10% OFF Sale"),
                (2, "Product_A"),
                (3, "C# Programming"),
                (4, "Normal Product"),
                (5, "100% Pure"),
            ],
        )
        conn.commit()
        return conn

    def test_like_escape_percent(self, db_with_special_names: sqlite3.Connection) -> None:
        """% を含む値を LIKE 検索."""
        search = escape_like("10%", Dialect.SQLITE)
        sql = "SELECT * FROM products WHERE name LIKE /* $pattern */'%' ESCAPE '#'"
        result = parse_sql(sql, {"pattern": f"{search}%"}, dialect=Dialect.SQLITE)
        cursor = db_with_special_names.execute(result.sql, result.params)
        rows = [dict(row) for row in cursor.fetchall()]
        assert len(rows) == 1
        assert rows[0]["name"] == "10% OFF Sale"

    def test_like_escape_underscore(self, db_with_special_names: sqlite3.Connection) -> None:
        """_ を含む値を LIKE 検索."""
        search = escape_like("Product_", Dialect.SQLITE)
        sql = "SELECT * FROM products WHERE name LIKE /* $pattern */'%' ESCAPE '#'"
        result = parse_sql(sql, {"pattern": f"{search}%"}, dialect=Dialect.SQLITE)
        cursor = db_with_special_names.execute(result.sql, result.params)
        rows = [dict(row) for row in cursor.fetchall()]
        assert len(rows) == 1
        assert rows[0]["name"] == "Product_A"

    def test_like_escape_hash(self, db_with_special_names: sqlite3.Connection) -> None:
        """# を含む値を LIKE 検索."""
        search = escape_like("C#", Dialect.SQLITE)
        sql = "SELECT * FROM products WHERE name LIKE /* $pattern */'%' ESCAPE '#'"
        result = parse_sql(sql, {"pattern": f"{search}%"}, dialect=Dialect.SQLITE)
        cursor = db_with_special_names.execute(result.sql, result.params)
        rows = [dict(row) for row in cursor.fetchall()]
        assert len(rows) == 1
        assert rows[0]["name"] == "C# Programming"

    def test_like_without_escape(self, db_with_special_names: sqlite3.Connection) -> None:
        """エスケープなしの通常 LIKE 検索."""
        sql = "SELECT * FROM products WHERE name LIKE /* $pattern */'%' ESCAPE '#'"
        result = parse_sql(sql, {"pattern": "%Product%"}, dialect=Dialect.SQLITE)
        cursor = db_with_special_names.execute(result.sql, result.params)
        rows = [dict(row) for row in cursor.fetchall()]
        assert len(rows) == 2
        names = {r["name"] for r in rows}
        assert names == {"Product_A", "Normal Product"}


class TestDialectSqlLoaderIntegration:
    """Dialect 別 SQL ファイルロード統合テスト (TASK-035)."""

    def test_dialect_specific_file(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Dialect 固有ファイルが優先される."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        # 汎用ファイル
        (sql_dir / "count.sql").write_text(
            "SELECT COUNT(*) as cnt FROM employees",
            encoding="utf-8",
        )
        # SQLite 固有ファイル
        (sql_dir / "count.sqlite.sql").write_text(
            "SELECT COUNT(*) as cnt FROM employees WHERE dept_id IS NOT NULL",
            encoding="utf-8",
        )

        loader = SqlLoader(sql_dir)
        sql_template = loader.load("count.sql", dialect=Dialect.SQLITE)
        result = parse_sql(sql_template, {}, dialect=Dialect.SQLITE)
        rows = _fetch_all(db, result)
        # SQLite 固有ファイルが使われ、dept_id IS NOT NULL で3件
        assert rows[0]["cnt"] == 3

    def test_dialect_fallback_to_generic(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Dialect 固有ファイルがなければ汎用ファイルにフォールバック."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "count.sql").write_text(
            "SELECT COUNT(*) as cnt FROM employees",
            encoding="utf-8",
        )

        loader = SqlLoader(sql_dir)
        # MySQL固有ファイルは存在しないのでフォールバック
        sql_template = loader.load("count.sql", dialect=Dialect.MYSQL)
        result = parse_sql(sql_template, {}, dialect=Dialect.SQLITE)  # 実行はSQLite
        rows = _fetch_all(db, result)
        assert rows[0]["cnt"] == 4


# =============================================================================
# M2 Feature Integration Tests
# =============================================================================


class TestModifiersIntegration:
    """Modifier integration tests."""

    def test_bindless_modifier_keeps_line_without_binding(self, db: sqlite3.Connection) -> None:
        """& modifier keeps line without creating placeholder."""
        sql = "SELECT * FROM employees WHERE dept_id IS NOT NULL /* &has_dept */"
        result = parse_sql(sql, {"has_dept": True})
        rows = _fetch_all(db, result)
        assert len(rows) == 3  # 3 employees have dept_id

    def test_bindless_modifier_removes_line(self, db: sqlite3.Connection) -> None:
        """& modifier with None removes line."""
        sql = """\
SELECT * FROM employees
WHERE
    name = /* name */'default'
    AND dept_id IS NOT NULL /* &has_dept */"""
        result = parse_sql(sql, {"name": "Alice", "has_dept": None})
        assert "dept_id" not in result.sql
        rows = _fetch_all(db, result)
        assert len(rows) == 1

    def test_negation_modifier(self, db: sqlite3.Connection) -> None:
        """$! modifier inverts removal logic."""
        sql = """\
SELECT * FROM employees
WHERE
    name = /* name */'default'
    AND dept_id = /* $!skip_dept */10"""
        # skip_dept=10 (positive) -> line removed
        result = parse_sql(sql, {"name": "Alice", "skip_dept": 10})
        assert "dept_id" not in result.sql

    def test_fallback_modifier(self, db: sqlite3.Connection) -> None:
        """? modifier uses first positive value."""
        sql = "SELECT * FROM employees WHERE dept_id = /* ?primary ?secondary */0"
        result = parse_sql(sql, {"primary": None, "secondary": 10})
        rows = _fetch_all(db, result)
        assert len(rows) == 2
        assert all(r["dept_id"] == 10 for r in rows)

    def test_required_modifier_raises_on_none(self, db: sqlite3.Connection) -> None:
        """@ modifier raises error on None."""
        from sqlym.exceptions import SqlParseError

        sql = "SELECT * FROM employees WHERE id = /* @id */1"
        with pytest.raises(SqlParseError):
            parse_sql(sql, {"id": None})


class TestOperatorConversionIntegration:
    """Operator auto-conversion integration tests."""

    def test_equals_with_list_becomes_in(self, db: sqlite3.Connection) -> None:
        """= with list becomes IN."""
        sql = "SELECT * FROM employees WHERE id /* ids */= 1"
        result = parse_sql(sql, {"ids": [1, 2]})
        rows = _fetch_all(db, result)
        assert len(rows) == 2
        assert {r["id"] for r in rows} == {1, 2}

    def test_equals_with_none_becomes_is_null(self, db: sqlite3.Connection) -> None:
        """= with None becomes IS NULL."""
        sql = "SELECT * FROM employees WHERE dept_id /* dept */= 0"
        result = parse_sql(sql, {"dept": None})
        rows = _fetch_all(db, result)
        assert len(rows) == 1
        assert rows[0]["name"] == "Diana"

    def test_not_equals_with_none_becomes_is_not_null(self, db: sqlite3.Connection) -> None:
        """<> with None becomes IS NOT NULL."""
        sql = "SELECT * FROM employees WHERE dept_id /* dept */<> 0"
        result = parse_sql(sql, {"dept": None})
        rows = _fetch_all(db, result)
        assert len(rows) == 3


class TestLikeListExpansionIntegration:
    """LIKE list expansion integration tests."""

    def test_like_with_list_or_expansion(self, db: sqlite3.Connection) -> None:
        """LIKE with list expands to OR."""
        sql = "SELECT * FROM employees WHERE name /* patterns */LIKE 'pattern'"
        result = parse_sql(sql, {"patterns": ["A%", "B%"]})
        rows = _fetch_all(db, result)
        assert len(rows) == 2
        names = {r["name"] for r in rows}
        assert names == {"Alice", "Bob"}

    def test_like_with_empty_list(self, db: sqlite3.Connection) -> None:
        """LIKE with empty list returns no results."""
        sql = "SELECT * FROM employees WHERE name /* patterns */LIKE 'pattern'"
        result = parse_sql(sql, {"patterns": []})
        rows = _fetch_all(db, result)
        assert len(rows) == 0


class TestHelperFunctionsIntegration:
    """%concat, %L, %STR helper function integration tests."""

    def test_concat_helper(self, db: sqlite3.Connection) -> None:
        """%concat concatenates values."""
        sql = "SELECT * FROM employees WHERE name LIKE /* %concat('%', keyword, '%') */'%test%'"
        result = parse_sql(sql, {"keyword": "lic"})
        rows = _fetch_all(db, result)
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_like_escape_helper(self, db: sqlite3.Connection) -> None:
        """%L escapes LIKE special characters."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items (name) VALUES ('100% done')")
        conn.execute("INSERT INTO items (name) VALUES ('50 percent')")
        conn.commit()

        sql = "SELECT * FROM items WHERE name LIKE /*%L '%' keyword '%' */'%test%'"
        result = parse_sql(sql, {"keyword": "%"})
        cursor = conn.execute(result.sql, result.params)
        rows = [dict(row) for row in cursor.fetchall()]
        assert len(rows) == 1
        assert rows[0]["name"] == "100% done"

    def test_str_helper_embeds_directly(self, db: sqlite3.Connection) -> None:
        """%STR embeds value directly into SQL."""
        sql = "SELECT * FROM employees ORDER BY /* %STR(order_col) */id"
        result = parse_sql(sql, {"order_col": "name"})
        rows = _fetch_all(db, result)
        assert rows[0]["name"] == "Alice"  # Alphabetically first


class TestBlockDirectivesIntegration:
    """%IF/%ELSE block directive integration tests."""

    def test_if_block_true(self, db: sqlite3.Connection) -> None:
        """%IF with true condition includes block."""
        sql = """\
SELECT * FROM employees
WHERE 1=1
-- %IF filter_dept
    AND dept_id = /* dept_id */0
-- %END"""
        result = parse_sql(sql, {"filter_dept": True, "dept_id": 10})
        rows = _fetch_all(db, result)
        assert len(rows) == 2

    def test_if_else_block(self, db: sqlite3.Connection) -> None:
        """%IF/%ELSE selects appropriate block."""
        sql = """\
SELECT * FROM employees
WHERE 1=1
-- %IF use_name
    AND name = /* name */'default'
-- %ELSE
    AND dept_id IS NOT NULL
-- %END"""
        result = parse_sql(sql, {"use_name": False, "name": "Alice"})
        rows = _fetch_all(db, result)
        assert len(rows) == 3  # 3 employees have dept_id


class TestInlineConditionsIntegration:
    """Inline condition integration tests."""

    def test_inline_if_true(self, db: sqlite3.Connection) -> None:
        """Inline %if with true condition."""
        sql = (
            "SELECT * FROM employees "
            "WHERE dept_id = /*%if use_val */ /* val */0 /*%else */ 10 /*%end*/"
        )
        result = parse_sql(sql, {"use_val": True, "val": 20})
        rows = _fetch_all(db, result)
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_inline_if_false(self, db: sqlite3.Connection) -> None:
        """Inline %if with false condition uses else."""
        sql = (
            "SELECT * FROM employees "
            "WHERE dept_id = /*%if use_val */ /* val */0 /*%else */ 10 /*%end*/"
        )
        result = parse_sql(sql, {"use_val": False, "val": 20})
        rows = _fetch_all(db, result)
        assert len(rows) == 2


class TestIncludeDirectiveIntegration:
    """%include directive integration tests."""

    def test_include_expands_fragment(self, tmp_path: Path) -> None:
        """%include loads external SQL fragment."""
        from sqlym.parser.twoway import TwoWaySQLParser

        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "condition.sql").write_text("dept_id = /* dept_id */0")

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")
        conn.execute("INSERT INTO employees VALUES (1, 'Alice', 10)")
        conn.execute("INSERT INTO employees VALUES (2, 'Bob', 20)")
        conn.commit()

        sql = 'SELECT * FROM employees WHERE /* %include "condition.sql" */'
        parser = TwoWaySQLParser(sql, base_path=sql_dir)
        result = parser.parse({"dept_id": 10})

        cursor = conn.execute(result.sql, result.params)
        rows = [dict(row) for row in cursor.fetchall()]
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"


class TestUnionIntegration:
    """UNION operation integration tests."""

    def test_union_both_present(self, db: sqlite3.Connection) -> None:
        """UNION with both queries."""
        sql = """\
SELECT * FROM employees WHERE dept_id = /* $dept1 */0
UNION
SELECT * FROM employees WHERE dept_id = /* $dept2 */0"""
        result = parse_sql(sql, {"dept1": 10, "dept2": 20})
        rows = _fetch_all(db, result)
        assert len(rows) == 3

    def test_union_second_removed(self, db: sqlite3.Connection) -> None:
        """UNION with second query removed."""
        sql = """\
SELECT * FROM employees WHERE dept_id = /* $dept1 */0
UNION
SELECT * FROM employees WHERE dept_id = /* $dept2 */0"""
        result = parse_sql(sql, {"dept1": 10, "dept2": None})
        assert "UNION" not in result.sql
        rows = _fetch_all(db, result)
        assert len(rows) == 2


class TestTrailingDelimiterIntegration:
    """Trailing delimiter removal integration tests."""

    def test_trailing_and_removed(self, db: sqlite3.Connection) -> None:
        """Trailing AND is removed when next line is removed."""
        sql = """\
SELECT * FROM employees
WHERE
    dept_id = /* $dept_id */0 AND
    name = /* $name */'default'"""
        result = parse_sql(sql, {"dept_id": 10, "name": None})
        assert result.sql.strip().endswith("?")  # No trailing AND
        rows = _fetch_all(db, result)
        assert len(rows) == 2


class TestNegativePositiveExtendedIntegration:
    """Extended negative/positive evaluation integration tests."""

    def test_false_is_negative(self, db: sqlite3.Connection) -> None:
        """False is treated as negative for line removal."""
        sql = """\
SELECT * FROM employees
WHERE
    dept_id = /* $dept_id */0
    AND name = /* name */'default'"""
        result = parse_sql(sql, {"dept_id": False, "name": "Alice"})
        assert "dept_id" not in result.sql
        rows = _fetch_all(db, result)
        assert len(rows) == 1

    def test_empty_list_is_negative(self, db: sqlite3.Connection) -> None:
        """Empty list is treated as negative for line removal (with $ modifier)."""
        sql = """\
SELECT * FROM employees
WHERE
    dept_id = /* $dept_id */0
    AND name = /* name */'default'"""
        result = parse_sql(sql, {"dept_id": [], "name": "Alice"})
        assert "dept_id" not in result.sql
        rows = _fetch_all(db, result)
        assert len(rows) == 1
