import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils import db as db_module


TABLE_ORDER = [
    "projects",
    "query_master",
    "submission",
    "presence_records",
    "source_records",
    "entity_mapping",
    "source_mapping",
]

PRIMARY_KEY_COLUMNS: Dict[str, Sequence[str]] = {
    "projects": ("project_id",),
    "query_master": ("id",),
    "submission": ("submission_id",),
    "presence_records": ("id",),
    "source_records": ("id",),
    "entity_mapping": ("id",),
    "source_mapping": ("id",),
}

NATURAL_KEY_COLUMNS: Dict[str, Optional[Sequence[str]]] = {
    "projects": ("project_name",),
    "query_master": ("project_id", "query_number"),
    "submission": None,
    "presence_records": None,
    "source_records": None,
    "entity_mapping": ("project_id", "entity_name_cn"),
    "source_mapping": ("project_id", "source_name"),
}


@dataclass
class VerificationResult:
    sqlite_count: int
    postgres_count: int
    matched_count: int
    missing_count: int
    conflicting_count: int
    missing_examples: List[Tuple]
    conflicting_examples: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify row counts and migrated keys between local SQLite and PostgreSQL."
    )
    parser.add_argument(
        "--sqlite-path",
        default=str(ROOT_DIR / "geo_data_v2.db"),
        help="Path to the source SQLite database. Defaults to ./geo_data_v2.db",
    )
    return parser.parse_args()


def mask_database_url(database_url: str) -> str:
    if "://" not in database_url or "@" not in database_url:
        return database_url

    scheme, remainder = database_url.split("://", 1)
    credentials, suffix = remainder.split("@", 1)
    if ":" not in credentials:
        return f"{scheme}://{credentials}@{suffix}"

    username, _password = credentials.split(":", 1)
    return f"{scheme}://{username}:***@{suffix}"


def require_postgres_database_url() -> str:
    database_url = db_module.get_database_url()
    if db_module.get_db_backend() != "postgres":
        raise RuntimeError(
            "DATABASE_URL must be set to a PostgreSQL connection string before running verification."
        )
    return db_module._normalize_postgres_dsn(database_url)


def get_row_count_sqlite(conn: sqlite3.Connection, table_name: str) -> int:
    cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cursor.fetchone()[0])


def get_row_count_postgres(cursor, table_name: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cursor.fetchone()[0])


def fetch_key_map_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    key_columns: Sequence[str],
) -> Dict[Tuple, Tuple]:
    column_sql = ", ".join(key_columns)
    cursor = conn.execute(f"SELECT {column_sql} FROM {table_name}")
    return {tuple(row): tuple(row) for row in cursor.fetchall()}


def fetch_key_map_postgres(cursor, table_name: str, key_columns: Sequence[str]) -> Dict[Tuple, Tuple]:
    column_sql = ", ".join(key_columns)
    cursor.execute(f"SELECT {column_sql} FROM {table_name}")
    rows = cursor.fetchall()
    return {tuple(row): tuple(row) for row in rows}


def fetch_dual_key_map_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    primary_key_columns: Sequence[str],
    natural_key_columns: Sequence[str],
) -> Dict[Tuple, Tuple]:
    column_sql = ", ".join(list(primary_key_columns) + list(natural_key_columns))
    cursor = conn.execute(f"SELECT {column_sql} FROM {table_name}")
    rows = cursor.fetchall()
    result: Dict[Tuple, Tuple] = {}
    pk_len = len(primary_key_columns)
    for row in rows:
        pk = tuple(row[:pk_len])
        natural = tuple(row[pk_len:])
        result[pk] = natural
    return result


def fetch_dual_key_map_postgres(cursor, table_name: str, primary_key_columns: Sequence[str], natural_key_columns: Sequence[str]) -> Dict[Tuple, Tuple]:
    column_sql = ", ".join(list(primary_key_columns) + list(natural_key_columns))
    cursor.execute(f"SELECT {column_sql} FROM {table_name}")
    rows = cursor.fetchall()
    result: Dict[Tuple, Tuple] = {}
    pk_len = len(primary_key_columns)
    for row in rows:
        pk = tuple(row[:pk_len])
        natural = tuple(row[pk_len:])
        result[pk] = natural
    return result


def verify_table(sqlite_conn: sqlite3.Connection, pg_cursor, table_name: str) -> VerificationResult:
    sqlite_count = get_row_count_sqlite(sqlite_conn, table_name)
    postgres_count = get_row_count_postgres(pg_cursor, table_name)
    pk_columns = PRIMARY_KEY_COLUMNS[table_name]
    natural_columns = NATURAL_KEY_COLUMNS[table_name]

    sqlite_pk_map = fetch_key_map_sqlite(sqlite_conn, table_name, pk_columns)
    postgres_pk_map = fetch_key_map_postgres(pg_cursor, table_name, pk_columns)

    sqlite_keys = set(sqlite_pk_map.keys())
    postgres_keys = set(postgres_pk_map.keys())
    matched_keys = sqlite_keys & postgres_keys
    missing_pk_keys = sorted(sqlite_keys - postgres_keys)

    conflicting_examples: List[str] = []
    missing_examples: List[Tuple] = []

    if natural_columns:
        sqlite_dual = fetch_dual_key_map_sqlite(sqlite_conn, table_name, pk_columns, natural_columns)
        postgres_dual = fetch_dual_key_map_postgres(pg_cursor, table_name, pk_columns, natural_columns)
        postgres_natural_to_pk = {natural: pk for pk, natural in postgres_dual.items()}

        for pk in missing_pk_keys:
            natural = sqlite_dual[pk]
            postgres_pk = postgres_natural_to_pk.get(natural)
            if postgres_pk is not None:
                conflicting_examples.append(
                    f"source pk={pk} has natural key={natural}, but PostgreSQL already has pk={postgres_pk}"
                )
            else:
                missing_examples.append(pk)
    else:
        missing_examples = list(missing_pk_keys)

    return VerificationResult(
        sqlite_count=sqlite_count,
        postgres_count=postgres_count,
        matched_count=len(matched_keys),
        missing_count=len(missing_examples),
        conflicting_count=len(conflicting_examples),
        missing_examples=missing_examples[:10],
        conflicting_examples=conflicting_examples[:10],
    )


def print_summary(results: Dict[str, VerificationResult]):
    print("\nVerification summary")
    print("====================")
    for table_name in TABLE_ORDER:
        result = results[table_name]
        print(
            f"{table_name}: sqlite={result.sqlite_count} postgres={result.postgres_count} "
            f"matched={result.matched_count} missing={result.missing_count} conflicts={result.conflicting_count}"
        )
        if result.missing_examples:
            print(f"  missing examples: {result.missing_examples}")
        if result.conflicting_examples:
            print(f"  conflict examples: {result.conflicting_examples}")


def main():
    args = parse_args()
    sqlite_path = Path(args.sqlite_path).resolve()
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")

    postgres_dsn = require_postgres_database_url()

    print("Source SQLite database:", sqlite_path)
    print("Target backend: postgres")
    print("Target database URL:", mask_database_url(db_module.get_database_url()))

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    try:
        try:
            import psycopg
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "psycopg is required to run this verification. Install dependencies with `pip install -r requirements.txt`."
            ) from exc

        pg_conn = psycopg.connect(postgres_dsn)
        try:
            results: Dict[str, VerificationResult] = {}
            with pg_conn.cursor() as pg_cursor:
                for table_name in TABLE_ORDER:
                    results[table_name] = verify_table(sqlite_conn, pg_cursor, table_name)
        finally:
            pg_conn.close()
    finally:
        sqlite_conn.close()

    print_summary(results)


if __name__ == "__main__":
    main()
