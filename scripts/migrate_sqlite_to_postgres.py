import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils import db as db_module


TABLE_COLUMNS: Dict[str, Sequence[str]] = {
    "projects": ("project_id", "project_name", "status", "created_at", "updated_at"),
    "query_master": (
        "id",
        "project_id",
        "query_number",
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
        "publish_month_default",
        "active",
        "updated_at",
    ),
    "entity_mapping": ("id", "project_id", "entity_name_cn", "entity_name_en", "updated_at"),
    "source_mapping": ("id", "project_id", "source_name", "source_url", "updated_at"),
    "submission": (
        "submission_id",
        "project_id",
        "query_number",
        "record_month",
        "ai_platform",
        "check_date",
        "created_by",
        "created_at",
        "notes",
    ),
    "presence_records": (
        "id",
        "submission_id",
        "query_number",
        "check_date",
        "entity_name_cn",
        "entity_name_en",
        "position",
    ),
    "source_records": (
        "id",
        "submission_id",
        "query_number",
        "check_date",
        "source_name",
        "source_url",
        "occurrence_number",
        "quoted_or_not",
        "quoted_url",
    ),
}

SERIAL_COLUMNS: Dict[str, str] = {
    "projects": "project_id",
    "query_master": "id",
    "entity_mapping": "id",
    "source_mapping": "id",
    "presence_records": "id",
    "source_records": "id",
}

TABLE_ORDER = [
    "projects",
    "query_master",
    "entity_mapping",
    "source_mapping",
    "submission",
    "presence_records",
    "source_records",
]


@dataclass
class TableStats:
    copied: int = 0
    skipped: int = 0
    conflicts: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time migration from local SQLite to PostgreSQL using DATABASE_URL."
    )
    parser.add_argument(
        "--sqlite-path",
        default=str(ROOT_DIR / "geo_data_v2.db"),
        help="Path to the source SQLite database. Defaults to ./geo_data_v2.db",
    )
    return parser.parse_args()


def require_postgres_database_url() -> str:
    database_url = db_module.get_database_url()
    backend = db_module.get_db_backend()
    if backend != "postgres":
        raise RuntimeError(
            "DATABASE_URL must be set to a PostgreSQL connection string before running this migration."
        )
    return db_module._normalize_postgres_dsn(database_url)


def mask_database_url(database_url: str) -> str:
    if "://" not in database_url or "@" not in database_url:
        return database_url

    scheme, remainder = database_url.split("://", 1)
    credentials, suffix = remainder.split("@", 1)
    if ":" not in credentials:
        return f"{scheme}://{credentials}@{suffix}"

    username, _password = credentials.split(":", 1)
    return f"{scheme}://{username}:***@{suffix}"


def fetch_sqlite_rows(sqlite_conn: sqlite3.Connection, table_name: str) -> List[Tuple[Any, ...]]:
    columns = ", ".join(TABLE_COLUMNS[table_name])
    cursor = sqlite_conn.execute(f"SELECT {columns} FROM {table_name}")
    return cursor.fetchall()


def load_existing_projects(pg_cursor) -> Tuple[set, Dict[str, int]]:
    pg_cursor.execute("SELECT project_id, project_name FROM projects")
    rows = pg_cursor.fetchall()
    return {int(row[0]) for row in rows}, {str(row[1]): int(row[0]) for row in rows}


def load_existing_keys(pg_cursor, table_name: str, key_sql: str) -> set:
    pg_cursor.execute(key_sql)
    return {tuple(row) if isinstance(row, (list, tuple)) else (row,) for row in pg_cursor.fetchall()}


def insert_row(pg_cursor, table_name: str, row: Sequence[Any]):
    columns = TABLE_COLUMNS[table_name]
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    pg_cursor.execute(
        f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})",
        tuple(row),
    )


def migrate_projects(sqlite_conn, pg_cursor, stats: Dict[str, TableStats], conflicts: Dict[str, List[str]]):
    existing_ids, existing_names = load_existing_projects(pg_cursor)
    for row in fetch_sqlite_rows(sqlite_conn, "projects"):
        project_id, project_name, status, created_at, updated_at = row
        if int(project_id) in existing_ids:
            stats["projects"].skipped += 1
            continue
        existing_id_for_name = existing_names.get(str(project_name))
        if existing_id_for_name is not None and existing_id_for_name != int(project_id):
            stats["projects"].conflicts += 1
            conflicts["projects"].append(
                f"project_name={project_name!r} already exists in PostgreSQL with project_id={existing_id_for_name}, "
                f"source project_id={project_id} skipped"
            )
            continue

        insert_row(pg_cursor, "projects", row)
        existing_ids.add(int(project_id))
        existing_names[str(project_name)] = int(project_id)
        stats["projects"].copied += 1


def migrate_query_master(sqlite_conn, pg_cursor, stats: Dict[str, TableStats], conflicts: Dict[str, List[str]]):
    existing_ids = load_existing_keys(pg_cursor, "query_master", "SELECT id FROM query_master")
    existing_natural = load_existing_keys(
        pg_cursor,
        "query_master",
        "SELECT project_id, query_number FROM query_master",
    )
    existing_ids = {int(row[0]) for row in existing_ids}

    for row in fetch_sqlite_rows(sqlite_conn, "query_master"):
        row_id = int(row[0])
        natural_key = (int(row[1]), str(row[2]))
        if row_id in existing_ids:
            stats["query_master"].skipped += 1
            continue
        if natural_key in existing_natural:
            stats["query_master"].conflicts += 1
            conflicts["query_master"].append(
                f"project_id={natural_key[0]}, query_number={natural_key[1]!r} already exists with a different id; source id={row_id} skipped"
            )
            continue

        insert_row(pg_cursor, "query_master", row)
        existing_ids.add(row_id)
        existing_natural.add(natural_key)
        stats["query_master"].copied += 1


def migrate_mapping_table(
    sqlite_conn,
    pg_cursor,
    table_name: str,
    natural_key_fields: Tuple[int, int],
    stats: Dict[str, TableStats],
    conflicts: Dict[str, List[str]],
):
    existing_ids = {int(row[0]) for row in load_existing_keys(pg_cursor, table_name, f"SELECT id FROM {table_name}")}
    key_sql = f"SELECT project_id, {TABLE_COLUMNS[table_name][2]} FROM {table_name}"
    existing_natural = load_existing_keys(pg_cursor, table_name, key_sql)

    for row in fetch_sqlite_rows(sqlite_conn, table_name):
        row_id = int(row[0])
        natural_key = (int(row[natural_key_fields[0]]), str(row[natural_key_fields[1]]))
        if row_id in existing_ids:
            stats[table_name].skipped += 1
            continue
        if natural_key in existing_natural:
            stats[table_name].conflicts += 1
            conflicts[table_name].append(
                f"project_id={natural_key[0]}, key={natural_key[1]!r} already exists with a different id; source id={row_id} skipped"
            )
            continue

        insert_row(pg_cursor, table_name, row)
        existing_ids.add(row_id)
        existing_natural.add(natural_key)
        stats[table_name].copied += 1


def migrate_submission(sqlite_conn, pg_cursor, stats: Dict[str, TableStats]):
    existing_ids = {
        str(row[0]) for row in load_existing_keys(pg_cursor, "submission", "SELECT submission_id FROM submission")
    }
    for row in fetch_sqlite_rows(sqlite_conn, "submission"):
        submission_id = str(row[0])
        if submission_id in existing_ids:
            stats["submission"].skipped += 1
            continue

        insert_row(pg_cursor, "submission", row)
        existing_ids.add(submission_id)
        stats["submission"].copied += 1


def migrate_id_only_table(sqlite_conn, pg_cursor, table_name: str, stats: Dict[str, TableStats]):
    existing_ids = {int(row[0]) for row in load_existing_keys(pg_cursor, table_name, f"SELECT id FROM {table_name}")}
    for row in fetch_sqlite_rows(sqlite_conn, table_name):
        row_id = int(row[0])
        if row_id in existing_ids:
            stats[table_name].skipped += 1
            continue

        insert_row(pg_cursor, table_name, row)
        existing_ids.add(row_id)
        stats[table_name].copied += 1


def reset_postgres_sequences(pg_cursor):
    for table_name, column_name in SERIAL_COLUMNS.items():
        pg_cursor.execute(
            """
            SELECT pg_get_serial_sequence(%s, %s)
            """,
            (table_name, column_name),
        )
        row = pg_cursor.fetchone()
        sequence_name = row[0] if row else None
        if not sequence_name:
            continue

        pg_cursor.execute(
            f"""
            SELECT setval(
                %s,
                COALESCE((SELECT MAX({column_name}) FROM {table_name}), 1),
                (SELECT COUNT(*) > 0 FROM {table_name})
            )
            """,
            (sequence_name,),
        )


def print_summary(stats: Dict[str, TableStats], conflicts: Dict[str, List[str]]):
    print("\nMigration summary")
    print("=================")
    for table_name in TABLE_ORDER:
        table_stats = stats[table_name]
        print(
            f"{table_name}: copied={table_stats.copied} skipped={table_stats.skipped} conflicts={table_stats.conflicts}"
        )

    conflict_total = sum(len(items) for items in conflicts.values())
    print(f"\nTotal conflicts: {conflict_total}")
    if conflict_total:
        print("Conflict details:")
        for table_name in TABLE_ORDER:
            for message in conflicts[table_name]:
                print(f"- {table_name}: {message}")


def main():
    args = parse_args()
    sqlite_path = Path(args.sqlite_path).resolve()
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")

    postgres_dsn = require_postgres_database_url()

    print("Source SQLite database:", sqlite_path)
    print("Target backend: postgres")
    print("Target database URL:", mask_database_url(db_module.get_database_url()))
    print("\nRecommendation: back up the SQLite file before running this migration.")
    print("This script only reads SQLite, but a backup is still the safest option.\n")

    db_module.create_tables()

    stats = {table_name: TableStats() for table_name in TABLE_ORDER}
    conflicts = {table_name: [] for table_name in TABLE_ORDER}

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = None

    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required to run this migration. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    pg_conn = psycopg.connect(postgres_dsn)
    try:
        with pg_conn.transaction():
            with pg_conn.cursor() as pg_cursor:
                migrate_projects(sqlite_conn, pg_cursor, stats, conflicts)
                migrate_query_master(sqlite_conn, pg_cursor, stats, conflicts)
                migrate_mapping_table(
                    sqlite_conn,
                    pg_cursor,
                    "entity_mapping",
                    natural_key_fields=(1, 2),
                    stats=stats,
                    conflicts=conflicts,
                )
                migrate_mapping_table(
                    sqlite_conn,
                    pg_cursor,
                    "source_mapping",
                    natural_key_fields=(1, 2),
                    stats=stats,
                    conflicts=conflicts,
                )
                migrate_submission(sqlite_conn, pg_cursor, stats)
                migrate_id_only_table(sqlite_conn, pg_cursor, "presence_records", stats)
                migrate_id_only_table(sqlite_conn, pg_cursor, "source_records", stats)
                reset_postgres_sequences(pg_cursor)
    finally:
        sqlite_conn.close()
        pg_conn.close()

    print_summary(stats, conflicts)


if __name__ == "__main__":
    main()
