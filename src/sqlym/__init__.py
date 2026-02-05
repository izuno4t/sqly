"""sqly: SQL-first database access library for Python."""

from sqlym._parse import parse_sql
from sqlym.dialect import Dialect
from sqlym.escape_utils import escape_like
from sqlym.exceptions import MappingError, SqlFileNotFoundError, SqlParseError, SqlyError
from sqlym.loader import SqlLoader
from sqlym.mapper import ManualMapper, RowMapper, create_mapper
from sqlym.mapper.column import Column, entity
from sqlym.parser.twoway import ParsedSQL, TwoWaySQLParser

__all__ = [
    "Column",
    "Dialect",
    "ManualMapper",
    "MappingError",
    "ParsedSQL",
    "RowMapper",
    "SqlFileNotFoundError",
    "SqlLoader",
    "SqlParseError",
    "SqlyError",
    "TwoWaySQLParser",
    "create_mapper",
    "entity",
    "escape_like",
    "parse_sql",
]
