import os
import sqlite3
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd


# =========================================================
# Paths
# =========================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_NAME = os.path.join(BASE_DIR, "geo_data_v2.db")
ENTITY_MAPPING_FILE = os.path.join(BASE_DIR, "entity_mapping.xlsx")
SOURCE_MAPPING_FILE = os.path.join(BASE_DIR, "source_mapping.xlsx")


# =========================================================
# Constants
# =========================================================
MONTHS = [
    "2026-02", "2026-03", "2026-04", "2026-05",
    "2026-06", "2026-07", "2026-08", "2026-09",
    "2026-10", "2026-11", "2026-12",
]

QUERY_MASTER_COLUMNS = [
    "query_number",
    "query_type",
    "query_name_cn",
    "query_name_en",
    "product_category",
    "publish_month_default",
    "active",
]

PRESENCE_IMPORT_COLUMNS = [
    "query_number",
    "record_month",
    "ai_platform",
    "check_date",
    "entity_name_cn",
    "entity_name_en",
    "position",
    "created_by",
]

SOURCE_IMPORT_COLUMNS = [
    "query_number",
    "record_month",
    "ai_platform",
    "check_date",
    "source_name",
    "source_url",
    "occurrence_number",
    "quoted_or_not",
    "quoted_url",
    "created_by",
]


# =========================================================
# Helpers
# =========================================================
def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan"}:
        return ""
    return text


def normalize_yes_no(value: Any) -> str:
    v = normalize_text(value).upper()
    if v in {"Y", "YES", "1", "TRUE"}:
        return "Y"
    if v in {"N", "NO", "0", "FALSE"}:
        return "N"
    return v


def normalize_publish_months(value: Any) -> str:
    """
    Store publish months as comma-separated string, e.g.:
    2026-02,2026-03
    """
    if value is None:
        return ""

    # list / tuple / set
    if isinstance(value, (list, tuple, set)):
        raw_items = [normalize_text(v) for v in value]
    else:
        text = normalize_text(value)
        if not text:
            return ""
        text = text.replace("，", ",").replace(";", ",").replace("|", ",")
        raw_items = [normalize_text(x) for x in text.split(",")]

    items = []
    seen = set()
    for item in raw_items:
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            items.append(item)

    # known months first, unknown months after
    ordered = [m for m in MONTHS if m in items]
    extras = [x for x in items if x not in MONTHS]
    final_items = ordered + extras

    return ",".join(final_items)


def split_publish_months(value: Any) -> List[str]:
    text = normalize_publish_months(value)
    if not text:
        return []
    return [x for x in text.split(",") if normalize_text(x)]


def expand_query_publish_months(queries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Helper for dashboard later:
    explode publish_month_default into one row per month
    """
    if queries_df is None or queries_df.empty:
        return pd.DataFrame(columns=list(queries_df.columns) + ["publish_month_single"] if queries_df is not None else [])

    df = queries_df.copy()
    df["publish_month_list"] = df["publish_month_default"].apply(split_publish_months)
    df["publish_month_single"] = df["publish_month_list"]
    df = df.explode("publish_month_single")
    df["publish_month_single"] = df["publish_month_single"].fillna("")
    return df.drop(columns=["publish_month_list"])


def generate_submission_id() -> str:
    return f"SUB_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6].upper()}"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# =========================================================
# Project Helpers
# =========================================================
def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    )
    return cursor.fetchone() is not None


def _column_exists(cursor: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def ensure_default_project() -> int:
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO projects (project_name, status, created_at, updated_at)
        VALUES (?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(project_name) DO UPDATE SET
            status = 'active',
            updated_at = CURRENT_TIMESTAMP
        """,
        ("Default Project",),
    )

    cursor.execute(
        """
        SELECT project_id
        FROM projects
        WHERE project_name = ?
        LIMIT 1
        """,
        ("Default Project",),
    )
    row = cursor.fetchone()
    conn.commit()
    conn.close()
    return int(row[0])


def create_project(project_name: str) -> int:
    name = normalize_text(project_name)
    if not name:
        raise ValueError("Project name is required.")

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO projects (project_name, status, created_at, updated_at)
        VALUES (?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (name,),
    )
    project_id = int(cursor.lastrowid)
    conn.commit()
    conn.close()
    return project_id


def rename_project(project_id: int, new_name: str):
    name = normalize_text(new_name)
    if not name:
        raise ValueError("Project name cannot be empty.")

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE projects
        SET project_name = ?, updated_at = CURRENT_TIMESTAMP
        WHERE project_id = ?
        """,
        (name, int(project_id)),
    )

    if cursor.rowcount == 0:
        conn.close()
        raise ValueError("Project not found.")

    conn.commit()
    conn.close()


def delete_project_cascade(project_id: int):
    project_id = int(project_id)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT 1
        FROM projects
        WHERE project_id = ?
        LIMIT 1
        """,
        (project_id,),
    )
    if cursor.fetchone() is None:
        conn.close()
        raise ValueError("Project not found.")

    cursor.execute("DELETE FROM presence_records WHERE submission_id IN (SELECT submission_id FROM submission WHERE project_id = ?)", (project_id,))
    cursor.execute("DELETE FROM source_records WHERE submission_id IN (SELECT submission_id FROM submission WHERE project_id = ?)", (project_id,))
    cursor.execute("DELETE FROM submission WHERE project_id = ?", (project_id,))
    cursor.execute("DELETE FROM query_master WHERE project_id = ?", (project_id,))
    cursor.execute("DELETE FROM entity_mapping WHERE project_id = ?", (project_id,))
    cursor.execute("DELETE FROM source_mapping WHERE project_id = ?", (project_id,))
    cursor.execute("DELETE FROM projects WHERE project_id = ?", (project_id,))

    conn.commit()
    conn.close()


def touch_project(project_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE projects
        SET updated_at = CURRENT_TIMESTAMP
        WHERE project_id = ?
        """,
        (int(project_id),),
    )
    conn.commit()
    conn.close()


def list_projects() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            project_id,
            project_name,
            status,
            created_at,
            updated_at
        FROM projects
        ORDER BY
            CASE WHEN project_name = 'Default Project' THEN 0 ELSE 1 END,
            updated_at DESC,
            project_name ASC
        """,
        conn,
    )
    conn.close()
    return df


def get_project(project_id: int) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT project_id, project_name, status, created_at, updated_at
        FROM projects
        WHERE project_id = ?
        LIMIT 1
        """,
        (int(project_id),),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    cols = ["project_id", "project_name", "status", "created_at", "updated_at"]
    return dict(zip(cols, row))


def get_project_by_name(project_name: str) -> Optional[Dict[str, Any]]:
    name = normalize_text(project_name)
    if not name:
        return None

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT project_id, project_name, status, created_at, updated_at
        FROM projects
        WHERE project_name = ?
        LIMIT 1
        """,
        (name,),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    cols = ["project_id", "project_name", "status", "created_at", "updated_at"]
    return dict(zip(cols, row))


# =========================================================
# Table Creation
# =========================================================
def _create_entity_mapping_table(cursor: sqlite3.Cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS entity_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        entity_name_cn TEXT NOT NULL,
        entity_name_en TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(project_id, entity_name_cn),
        FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_entity_mapping_project
    ON entity_mapping(project_id)
    """)


def _create_source_mapping_table(cursor: sqlite3.Cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        source_name TEXT NOT NULL,
        source_url TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(project_id, source_name),
        FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_source_mapping_project
    ON source_mapping(project_id)
    """)


def _migrate_mapping_table(
    cursor: sqlite3.Cursor,
    table_name: str,
    key_column: str,
    value_column: str,
    default_project_id: int,
):
    if not _table_exists(cursor, table_name):
        if table_name == "entity_mapping":
            _create_entity_mapping_table(cursor)
        else:
            _create_source_mapping_table(cursor)
        return

    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    required_columns = {"id", "project_id", key_column, value_column, "updated_at"}
    if required_columns.issubset(set(columns)):
        if table_name == "entity_mapping":
            _create_entity_mapping_table(cursor)
        else:
            _create_source_mapping_table(cursor)
        return

    legacy_table_name = f"{table_name}_legacy"
    if _table_exists(cursor, legacy_table_name):
        cursor.execute(f"DROP TABLE {legacy_table_name}")

    cursor.execute(f"ALTER TABLE {table_name} RENAME TO {legacy_table_name}")

    if table_name == "entity_mapping":
        _create_entity_mapping_table(cursor)
    else:
        _create_source_mapping_table(cursor)

    cursor.execute(f"PRAGMA table_info({legacy_table_name})")
    legacy_columns = [row[1] for row in cursor.fetchall()]
    if key_column in legacy_columns and value_column in legacy_columns:
        project_expr = "COALESCE(project_id, ?)" if "project_id" in legacy_columns else "?"
        updated_expr = "COALESCE(NULLIF(updated_at, ''), ?)" if "updated_at" in legacy_columns else "?"
        params: list[Any] = [int(default_project_id), now_ts()]
        cursor.execute(
            f"""
            INSERT INTO {table_name} (project_id, {key_column}, {value_column}, updated_at)
            SELECT
                {project_expr},
                TRIM(COALESCE({key_column}, '')),
                TRIM(COALESCE({value_column}, '')),
                {updated_expr}
            FROM {legacy_table_name}
            WHERE TRIM(COALESCE({key_column}, '')) <> ''
              AND TRIM(COALESCE({value_column}, '')) <> ''
            ON CONFLICT(project_id, {key_column}) DO UPDATE SET
                {value_column} = excluded.{value_column},
                updated_at = excluded.updated_at
            """,
            params,
        )

    cursor.execute(f"DROP TABLE {legacy_table_name}")


def _create_query_master_table(cursor: sqlite3.Cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS query_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        query_number TEXT NOT NULL,
        query_type TEXT NOT NULL,
        query_name_cn TEXT NOT NULL,
        query_name_en TEXT,
        product_category TEXT NOT NULL,
        publish_month_default TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT NOT NULL,
        UNIQUE(project_id, query_number),
        FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_query_master_project
    ON query_master(project_id)
    """)


def _create_submission_table(cursor: sqlite3.Cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS submission (
        submission_id TEXT PRIMARY KEY,
        project_id INTEGER NOT NULL,
        query_number TEXT NOT NULL,
        record_month TEXT NOT NULL,
        ai_platform TEXT NOT NULL,
        check_date TEXT NOT NULL,
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        notes TEXT,
        FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
        FOREIGN KEY (project_id, query_number) REFERENCES query_master(project_id, query_number)
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_submission_project
    ON submission(project_id)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_submission_main
    ON submission(project_id, query_number, record_month, ai_platform, created_by, check_date)
    """)


def _create_presence_records_table(cursor: sqlite3.Cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS presence_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        submission_id TEXT NOT NULL,
        query_number TEXT NOT NULL,
        check_date TEXT NOT NULL,
        entity_name_cn TEXT NOT NULL,
        entity_name_en TEXT NOT NULL,
        position INTEGER NOT NULL,
        FOREIGN KEY (submission_id) REFERENCES submission(submission_id) ON DELETE CASCADE
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_presence_submission
    ON presence_records(submission_id)
    """)


def _create_source_records_table(cursor: sqlite3.Cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        submission_id TEXT NOT NULL,
        query_number TEXT NOT NULL,
        check_date TEXT NOT NULL,
        source_name TEXT NOT NULL,
        source_url TEXT,
        occurrence_number INTEGER NOT NULL,
        quoted_or_not TEXT NOT NULL,
        quoted_url TEXT,
        FOREIGN KEY (submission_id) REFERENCES submission(submission_id) ON DELETE CASCADE
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_source_submission
    ON source_records(submission_id)
    """)


def _migrate_query_related_tables(cursor: sqlite3.Cursor, default_project_id: int):
    cursor.execute("PRAGMA foreign_keys = OFF")

    query_master_needs_migration = True
    if _table_exists(cursor, "query_master"):
        cursor.execute("PRAGMA table_info(query_master)")
        qm_columns = [row[1] for row in cursor.fetchall()]
        query_master_needs_migration = not {"id", "project_id", "query_number"}.issubset(set(qm_columns))

    submission_needs_migration = True
    if _table_exists(cursor, "submission"):
        cursor.execute("PRAGMA table_info(submission)")
        submission_columns = [row[1] for row in cursor.fetchall()]
        submission_needs_migration = (
            "project_id" not in submission_columns
            or "status" in submission_columns
            or "is_official" in submission_columns
        )

    presence_needs_migration = False
    source_needs_migration = False
    if _table_exists(cursor, "presence_records"):
        cursor.execute("PRAGMA foreign_key_list(presence_records)")
        presence_needs_migration = any(row[3] == "query_number" for row in cursor.fetchall())
    if _table_exists(cursor, "source_records"):
        cursor.execute("PRAGMA foreign_key_list(source_records)")
        source_needs_migration = any(row[3] == "query_number" for row in cursor.fetchall())

    if not any([query_master_needs_migration, submission_needs_migration, presence_needs_migration, source_needs_migration]):
        _create_query_master_table(cursor)
        _create_submission_table(cursor)
        _create_presence_records_table(cursor)
        _create_source_records_table(cursor)
        cursor.execute("PRAGMA foreign_keys = ON")
        return

    if _table_exists(cursor, "query_master"):
        cursor.execute("ALTER TABLE query_master RENAME TO query_master_legacy")
    if _table_exists(cursor, "submission"):
        cursor.execute("ALTER TABLE submission RENAME TO submission_legacy")
    if _table_exists(cursor, "presence_records"):
        cursor.execute("ALTER TABLE presence_records RENAME TO presence_records_legacy")
    if _table_exists(cursor, "source_records"):
        cursor.execute("ALTER TABLE source_records RENAME TO source_records_legacy")

    _create_query_master_table(cursor)
    _create_submission_table(cursor)
    _create_presence_records_table(cursor)
    _create_source_records_table(cursor)

    if _table_exists(cursor, "query_master_legacy"):
        cursor.execute("PRAGMA table_info(query_master_legacy)")
        legacy_columns = [row[1] for row in cursor.fetchall()]
        project_expr = "COALESCE(project_id, ?)" if "project_id" in legacy_columns else "?"
        cursor.execute(
            f"""
            INSERT INTO query_master (
                project_id,
                query_number,
                query_type,
                query_name_cn,
                query_name_en,
                product_category,
                publish_month_default,
                active,
                updated_at
            )
            SELECT
                {project_expr},
                TRIM(COALESCE(query_number, '')),
                TRIM(COALESCE(query_type, '')),
                TRIM(COALESCE(query_name_cn, '')),
                TRIM(COALESCE(query_name_en, '')),
                TRIM(COALESCE(product_category, '')),
                COALESCE(publish_month_default, ''),
                COALESCE(active, 1),
                COALESCE(NULLIF(updated_at, ''), ?)
            FROM query_master_legacy
            WHERE TRIM(COALESCE(query_number, '')) <> ''
              AND TRIM(COALESCE(query_name_cn, '')) <> ''
            ON CONFLICT(project_id, query_number) DO UPDATE SET
                query_type = excluded.query_type,
                query_name_cn = excluded.query_name_cn,
                query_name_en = excluded.query_name_en,
                product_category = excluded.product_category,
                publish_month_default = excluded.publish_month_default,
                active = excluded.active,
                updated_at = excluded.updated_at
            """,
            [int(default_project_id), now_ts()],
        )

    if _table_exists(cursor, "submission_legacy"):
        cursor.execute("PRAGMA table_info(submission_legacy)")
        legacy_columns = [row[1] for row in cursor.fetchall()]
        project_expr = "COALESCE(project_id, ?)" if "project_id" in legacy_columns else "?"
        cursor.execute(
            f"""
            INSERT INTO submission (
                submission_id,
                project_id,
                query_number,
                record_month,
                ai_platform,
                check_date,
                created_by,
                created_at,
                notes
            )
            SELECT
                submission_id,
                {project_expr},
                TRIM(COALESCE(query_number, '')),
                TRIM(COALESCE(record_month, '')),
                TRIM(COALESCE(ai_platform, '')),
                TRIM(COALESCE(check_date, '')),
                TRIM(COALESCE(created_by, '')),
                COALESCE(NULLIF(created_at, ''), ?),
                COALESCE(notes, '')
            FROM submission_legacy
            WHERE TRIM(COALESCE(submission_id, '')) <> ''
              AND TRIM(COALESCE(query_number, '')) <> ''
            """,
            [int(default_project_id), now_ts()],
        )

    if _table_exists(cursor, "presence_records_legacy"):
        cursor.execute("""
        INSERT INTO presence_records (
            id,
            submission_id,
            query_number,
            check_date,
            entity_name_cn,
            entity_name_en,
            position
        )
        SELECT
            id,
            submission_id,
            TRIM(COALESCE(query_number, '')),
            TRIM(COALESCE(check_date, '')),
            TRIM(COALESCE(entity_name_cn, '')),
            TRIM(COALESCE(entity_name_en, '')),
            COALESCE(position, 0)
        FROM presence_records_legacy
        WHERE TRIM(COALESCE(submission_id, '')) <> ''
        """)

    if _table_exists(cursor, "source_records_legacy"):
        cursor.execute("""
        INSERT INTO source_records (
            id,
            submission_id,
            query_number,
            check_date,
            source_name,
            source_url,
            occurrence_number,
            quoted_or_not,
            quoted_url
        )
        SELECT
            id,
            submission_id,
            TRIM(COALESCE(query_number, '')),
            TRIM(COALESCE(check_date, '')),
            TRIM(COALESCE(source_name, '')),
            TRIM(COALESCE(source_url, '')),
            COALESCE(occurrence_number, 0),
            COALESCE(NULLIF(quoted_or_not, ''), 'N'),
            COALESCE(quoted_url, '')
        FROM source_records_legacy
        WHERE TRIM(COALESCE(submission_id, '')) <> ''
        """)

    for legacy_table in ["source_records_legacy", "presence_records_legacy", "submission_legacy", "query_master_legacy"]:
        if _table_exists(cursor, legacy_table):
            cursor.execute(f"DROP TABLE {legacy_table}")

    cursor.execute("PRAGMA foreign_keys = ON")


def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        project_id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute(
        """
        INSERT INTO projects (project_name, status, created_at, updated_at)
        VALUES (?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(project_name) DO UPDATE SET
            status = 'active',
            updated_at = CURRENT_TIMESTAMP
        """,
        ("Default Project",),
    )
    cursor.execute(
        """
        SELECT project_id
        FROM projects
        WHERE project_name = ?
        LIMIT 1
        """,
        ("Default Project",),
    )
    default_project_id = int(cursor.fetchone()[0])

    _migrate_query_related_tables(cursor, default_project_id)

    _migrate_mapping_table(
        cursor=cursor,
        table_name="entity_mapping",
        key_column="entity_name_cn",
        value_column="entity_name_en",
        default_project_id=default_project_id,
    )
    _migrate_mapping_table(
        cursor=cursor,
        table_name="source_mapping",
        key_column="source_name",
        value_column="source_url",
        default_project_id=default_project_id,
    )

    conn.commit()
    conn.close()


# =========================================================
# Query Master
# =========================================================
def upsert_query_master(
    project_id: int,
    query_number: str,
    query_type: str,
    query_name_cn: str,
    query_name_en: str,
    product_category: str,
    publish_month_default: Any = "",
    active: int = 1,
):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO query_master (
        project_id,
        query_number,
        query_type,
        query_name_cn,
        query_name_en,
        product_category,
        publish_month_default,
        active,
        updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(project_id, query_number) DO UPDATE SET
        query_type = excluded.query_type,
        query_name_cn = excluded.query_name_cn,
        query_name_en = excluded.query_name_en,
        product_category = excluded.product_category,
        publish_month_default = excluded.publish_month_default,
        active = excluded.active,
        updated_at = excluded.updated_at
    """, (
        int(project_id),
        normalize_text(query_number),
        normalize_text(query_type),
        normalize_text(query_name_cn),
        normalize_text(query_name_en),
        normalize_text(product_category),
        normalize_publish_months(publish_month_default),
        int(active),
        now_ts(),
    ))

    conn.commit()
    conn.close()
    touch_project(project_id)


def bulk_upsert_query_master(rows: List[Dict[str, Any]]):
    if not rows:
        return

    for row in rows:
        upsert_query_master(
            project_id=int(row.get("project_id", 0)),
            query_number=row.get("query_number", ""),
            query_type=row.get("query_type", ""),
            query_name_cn=row.get("query_name_cn", ""),
            query_name_en=row.get("query_name_en", ""),
            product_category=row.get("product_category", ""),
            publish_month_default=row.get("publish_month_default", ""),
            active=int(row.get("active", 1)),
        )


def bulk_update_query_master(query_numbers: List[str], update_fields: Dict[str, Any], project_id: int):
    query_numbers = [normalize_text(x) for x in query_numbers if normalize_text(x)]
    if not query_numbers:
        return

    allowed_fields = [
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
        "publish_month_default",
        "active",
    ]
    payload = {k: v for k, v in (update_fields or {}).items() if k in allowed_fields}
    if not payload:
        return

    set_clauses = []
    params = []

    if "query_type" in payload:
        set_clauses.append("query_type = ?")
        params.append(normalize_text(payload["query_type"]))

    if "query_name_cn" in payload:
        set_clauses.append("query_name_cn = ?")
        params.append(normalize_text(payload["query_name_cn"]))

    if "query_name_en" in payload:
        set_clauses.append("query_name_en = ?")
        params.append(normalize_text(payload["query_name_en"]))

    if "product_category" in payload:
        set_clauses.append("product_category = ?")
        params.append(normalize_text(payload["product_category"]))

    if "publish_month_default" in payload:
        set_clauses.append("publish_month_default = ?")
        params.append(normalize_publish_months(payload["publish_month_default"]))

    if "active" in payload:
        set_clauses.append("active = ?")
        params.append(int(payload["active"]))

    set_clauses.append("updated_at = ?")
    params.append(now_ts())

    conn = get_connection()
    cursor = conn.cursor()

    sql = f"""
    UPDATE query_master
    SET {", ".join(set_clauses)}
    WHERE project_id = ?
      AND query_number IN ({",".join(["?"] * len(query_numbers))})
    """
    cursor.execute(sql, params + [int(project_id)] + query_numbers)

    conn.commit()
    conn.close()
    touch_project(project_id)


def get_all_queries(project_id: int, active_only: bool = False) -> pd.DataFrame:
    conn = get_connection()
    sql = """
    SELECT
        query_number,
        query_type,
        query_name_cn,
        query_name_en,
        product_category,
        publish_month_default,
        active,
        updated_at
    FROM query_master
    WHERE project_id = ?
    """
    params = [int(project_id)]
    if active_only:
        sql += " AND active = 1"
    sql += " ORDER BY query_number"

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def get_query_by_number(query_number: str, project_id: int) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        query_number,
        query_type,
        query_name_cn,
        query_name_en,
        product_category,
        publish_month_default,
        active,
        updated_at
    FROM query_master
    WHERE query_number = ?
      AND project_id = ?
    """, (normalize_text(query_number), int(project_id)))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    cols = [
        "query_number",
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
        "publish_month_default",
        "active",
        "updated_at",
    ]
    return dict(zip(cols, row))


def get_query_master(project_id: int, query_number: str) -> Optional[Dict[str, Any]]:
    return get_query_by_number(query_number=query_number, project_id=project_id)


def delete_query_master(project_id: int, query_number: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM query_master
        WHERE project_id = ?
          AND query_number = ?
        """,
        (int(project_id), normalize_text(query_number)),
    )
    conn.commit()
    conn.close()
    touch_project(project_id)


def delete_query_master_batch(project_id: int, query_numbers: List[str]) -> int:
    cleaned_query_numbers = [normalize_text(x) for x in query_numbers if normalize_text(x)]
    if not cleaned_query_numbers:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        DELETE FROM query_master
        WHERE project_id = ?
          AND query_number = ?
        """,
        [(int(project_id), query_number) for query_number in cleaned_query_numbers],
    )
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    touch_project(project_id)
    return deleted_count


def set_query_active(query_number: str, active: int, project_id: int):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE query_master
    SET active = ?, updated_at = ?
    WHERE query_number = ?
      AND project_id = ?
    """, (int(active), now_ts(), normalize_text(query_number), int(project_id)))

    conn.commit()
    conn.close()
    if project_id is not None:
        touch_project(project_id)


# =========================================================
# Entity Mapping
# =========================================================
def upsert_entity_mapping(project_id: int, entity_name_cn: str, entity_name_en: str) -> str:
    project_id = int(project_id)
    entity_name_cn = normalize_text(entity_name_cn)
    entity_name_en = normalize_text(entity_name_en)
    if not entity_name_cn or not entity_name_en:
        return "skipped"

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT entity_name_en
        FROM entity_mapping
        WHERE project_id = ?
          AND entity_name_cn = ?
        LIMIT 1
        """,
        (project_id, entity_name_cn),
    )
    existing_row = cursor.fetchone()

    cursor.execute("""
    INSERT INTO entity_mapping (project_id, entity_name_cn, entity_name_en, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(project_id, entity_name_cn) DO UPDATE SET
        entity_name_en = excluded.entity_name_en,
        updated_at = excluded.updated_at
    """, (
        project_id,
        entity_name_cn,
        entity_name_en,
        now_ts(),
    ))

    conn.commit()
    conn.close()
    touch_project(project_id)
    return "updated" if existing_row else "inserted"


def get_entity_name_en(project_id: int, entity_name_cn: str) -> str:
    entity_name_cn = normalize_text(entity_name_cn)
    if not entity_name_cn:
        return ""

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT entity_name_en
    FROM entity_mapping
    WHERE project_id = ?
      AND entity_name_cn = ?
    """, (int(project_id), entity_name_cn))

    row = cursor.fetchone()
    conn.close()
    return row[0] if row else ""


def get_all_entity_mappings(project_id: int) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
    SELECT entity_name_cn, entity_name_en, updated_at
    FROM entity_mapping
    WHERE project_id = ?
    ORDER BY entity_name_cn
    """, conn, params=[int(project_id)])
    conn.close()
    return df


def delete_entity_mapping(project_id: int, entity_name_cn: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM entity_mapping
        WHERE project_id = ?
          AND entity_name_cn = ?
        """,
        (int(project_id), normalize_text(entity_name_cn)),
    )
    conn.commit()
    conn.close()
    touch_project(project_id)


def delete_entity_mapping_batch(project_id: int, entity_name_cns: List[str]) -> int:
    cleaned_names = [normalize_text(x) for x in entity_name_cns if normalize_text(x)]
    if not cleaned_names:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        DELETE FROM entity_mapping
        WHERE project_id = ?
          AND entity_name_cn = ?
        """,
        [(int(project_id), name) for name in cleaned_names],
    )
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    touch_project(project_id)
    return deleted_count


def load_entity_mapping_from_excel(project_id: int, uploaded_file=None):
    project_id = int(project_id)
    source = uploaded_file if uploaded_file is not None else ENTITY_MAPPING_FILE
    if uploaded_file is None and not os.path.exists(ENTITY_MAPPING_FILE):
        return False, "entity_mapping.xlsx not found"

    try:
        df = pd.read_excel(source)
        df.columns = [str(c).strip() for c in df.columns]

        required_cols = ["entity_name_cn", "entity_name_en"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return False, f"Missing columns: {', '.join(missing)}"

        df = df.dropna(subset=required_cols).copy()
        df["entity_name_cn"] = df["entity_name_cn"].astype(str).str.strip()
        df["entity_name_en"] = df["entity_name_en"].astype(str).str.strip()
        df = df[(df["entity_name_cn"] != "") & (df["entity_name_en"] != "")]
        df = df.drop_duplicates(subset=["entity_name_cn"], keep="last")

        inserted_count = 0
        updated_count = 0

        for _, row in df.iterrows():
            action = upsert_entity_mapping(project_id, row["entity_name_cn"], row["entity_name_en"])
            if action == "inserted":
                inserted_count += 1
            elif action == "updated":
                updated_count += 1

        return True, f"Successfully uploaded {len(df)} rows. Inserted {inserted_count}, updated {updated_count}."

    except Exception as e:
        return False, str(e)


# =========================================================
# Source Mapping
# =========================================================
def upsert_source_mapping(project_id: int, source_name: str, source_url: str) -> str:
    project_id = int(project_id)
    source_name = normalize_text(source_name)
    source_url = normalize_text(source_url)
    if not source_name or not source_url:
        return "skipped"

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT source_url
        FROM source_mapping
        WHERE project_id = ?
          AND source_name = ?
        LIMIT 1
        """,
        (project_id, source_name),
    )
    existing_row = cursor.fetchone()

    cursor.execute("""
    INSERT INTO source_mapping (project_id, source_name, source_url, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(project_id, source_name) DO UPDATE SET
        source_url = excluded.source_url,
        updated_at = excluded.updated_at
    """, (
        project_id,
        source_name,
        source_url,
        now_ts(),
    ))

    conn.commit()
    conn.close()
    touch_project(project_id)
    return "updated" if existing_row else "inserted"


def get_source_url(project_id: int, source_name: str) -> str:
    source_name = normalize_text(source_name)
    if not source_name:
        return ""

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT source_url
    FROM source_mapping
    WHERE project_id = ?
      AND source_name = ?
    """, (int(project_id), source_name))

    row = cursor.fetchone()
    conn.close()
    return row[0] if row else ""


def get_all_source_mappings(project_id: int) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
    SELECT source_name, source_url, updated_at
    FROM source_mapping
    WHERE project_id = ?
    ORDER BY source_name
    """, conn, params=[int(project_id)])
    conn.close()
    return df


def delete_source_mapping(project_id: int, source_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM source_mapping
        WHERE project_id = ?
          AND source_name = ?
        """,
        (int(project_id), normalize_text(source_name)),
    )
    conn.commit()
    conn.close()
    touch_project(project_id)


def delete_source_mapping_batch(project_id: int, source_names: List[str]) -> int:
    cleaned_names = [normalize_text(x) for x in source_names if normalize_text(x)]
    if not cleaned_names:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        DELETE FROM source_mapping
        WHERE project_id = ?
          AND source_name = ?
        """,
        [(int(project_id), name) for name in cleaned_names],
    )
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    touch_project(project_id)
    return deleted_count


def load_source_mapping_from_excel(project_id: int, uploaded_file=None):
    project_id = int(project_id)
    source = uploaded_file if uploaded_file is not None else SOURCE_MAPPING_FILE
    if uploaded_file is None and not os.path.exists(SOURCE_MAPPING_FILE):
        return False, "source_mapping.xlsx not found"

    try:
        df = pd.read_excel(source)
        df.columns = [str(c).strip() for c in df.columns]

        required_cols = ["source_name", "source_url"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return False, f"Missing columns: {', '.join(missing)}"

        df = df.dropna(subset=["source_name", "source_url"]).copy()
        df["source_name"] = df["source_name"].astype(str).str.strip()
        df["source_url"] = df["source_url"].astype(str).str.strip()
        df = df[(df["source_name"] != "") & (df["source_url"] != "")]
        df = df.drop_duplicates(subset=["source_name"], keep="last")

        inserted_count = 0
        updated_count = 0

        for _, row in df.iterrows():
            action = upsert_source_mapping(project_id, row["source_name"], row["source_url"])
            if action == "inserted":
                inserted_count += 1
            elif action == "updated":
                updated_count += 1

        return True, f"Successfully uploaded {len(df)} rows. Inserted {inserted_count}, updated {updated_count}."

    except Exception as e:
        return False, str(e)


# =========================================================
# Submission
# =========================================================
def submission_exists(
    project_id: int,
    query_number: str,
    record_month: str,
    ai_platform: str,
    check_date: str,
    created_by: str,
) -> bool:
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT 1
    FROM submission
    WHERE project_id = ?
      AND query_number = ?
      AND record_month = ?
      AND ai_platform = ?
      AND check_date = ?
      AND created_by = ?
    LIMIT 1
    """, (
        int(project_id),
        normalize_text(query_number),
        normalize_text(record_month),
        normalize_text(ai_platform),
        normalize_text(check_date),
        normalize_text(created_by),
    ))

    row = cursor.fetchone()
    conn.close()
    return row is not None


def create_submission(
    project_id: int,
    query_number: str,
    record_month: str,
    ai_platform: str,
    check_date: str,
    created_by: str,
    notes: str = "",
) -> str:
    if submission_exists(project_id, query_number, record_month, ai_platform, check_date, created_by):
        raise ValueError(
            "A submission already exists for the same creator under the same "
            "query / month / platform / check date."
        )

    submission_id = generate_submission_id()

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO submission (
        submission_id,
        project_id,
        query_number,
        record_month,
        ai_platform,
        check_date,
        created_by,
        created_at,
        notes
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        submission_id,
        int(project_id),
        normalize_text(query_number),
        normalize_text(record_month),
        normalize_text(ai_platform),
        normalize_text(check_date),
        normalize_text(created_by),
        now_ts(),
        normalize_text(notes),
    ))

    conn.commit()
    conn.close()
    touch_project(project_id)
    return submission_id


def get_all_submissions(project_id: int) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
    SELECT
        submission_id,
        query_number,
        record_month,
        ai_platform,
        check_date,
        created_by,
        created_at,
        notes
    FROM submission
    WHERE project_id = ?
    ORDER BY created_at DESC
    """, conn, params=[int(project_id)])
    conn.close()
    return df


def delete_submission(submission_id: str, project_id: Optional[int] = None):
    conn = get_connection()
    cursor = conn.cursor()

    if project_id is None:
        cursor.execute("DELETE FROM submission WHERE submission_id = ?", (normalize_text(submission_id),))
    else:
        cursor.execute(
            "DELETE FROM submission WHERE submission_id = ? AND project_id = ?",
            (normalize_text(submission_id), int(project_id)),
        )

    conn.commit()
    conn.close()
    if project_id is not None:
        touch_project(project_id)


def bulk_delete_submissions(submission_ids: List[str], project_id: int):
    submission_ids = [normalize_text(x) for x in submission_ids if normalize_text(x)]
    if not submission_ids:
        return

    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "DELETE FROM submission WHERE submission_id = ? AND project_id = ?",
        [(sid, int(project_id)) for sid in submission_ids]
    )
    conn.commit()
    conn.close()
    touch_project(project_id)


# =========================================================
# Presence / Source Record Inserts
# =========================================================
def insert_presence_record(
    project_id: int,
    submission_id: str,
    query_number: str,
    check_date: str,
    entity_name_cn: str,
    entity_name_en: str,
    position: int,
):
    entity_name_cn = normalize_text(entity_name_cn)
    entity_name_en = normalize_text(entity_name_en)

    if not entity_name_en and entity_name_cn:
        entity_name_en = get_entity_name_en(project_id, entity_name_cn)

    if not entity_name_en:
        raise ValueError(f"Missing English mapping for entity_name_cn: {entity_name_cn}")

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO presence_records (
        submission_id,
        query_number,
        check_date,
        entity_name_cn,
        entity_name_en,
        position
    )
    VALUES (?, ?, ?, ?, ?, ?)
    """, (
        normalize_text(submission_id),
        normalize_text(query_number),
        normalize_text(check_date),
        entity_name_cn,
        entity_name_en,
        int(position),
    ))

    conn.commit()
    conn.close()


def insert_source_record(
    project_id: int,
    submission_id: str,
    query_number: str,
    check_date: str,
    source_name: str,
    source_url: str,
    occurrence_number: int,
    quoted_or_not: str,
    quoted_url: str = "",
):
    source_name = normalize_text(source_name)
    source_url = normalize_text(source_url) or get_source_url(project_id, source_name)

    if not source_url:
        raise ValueError(f"Missing source_url for source_name: {source_name}")

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO source_records (
        submission_id,
        query_number,
        check_date,
        source_name,
        source_url,
        occurrence_number,
        quoted_or_not,
        quoted_url
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        normalize_text(submission_id),
        normalize_text(query_number),
        normalize_text(check_date),
        source_name,
        source_url,
        int(occurrence_number),
        normalize_yes_no(quoted_or_not),
        normalize_text(quoted_url),
    ))

    conn.commit()
    conn.close()


# =========================================================
# Save One Manual Submission
# =========================================================
def save_manual_submission(
    project_id: int,
    query_number: str,
    record_month: str,
    ai_platform: str,
    check_date: str,
    created_by: str,
    presence_rows: List[Dict[str, Any]],
    source_rows: List[Dict[str, Any]],
    notes: str = "",
) -> str:
    query_number = normalize_text(query_number)

    query_info = get_query_by_number(query_number, project_id=project_id)
    if not query_info:
        raise ValueError(f"Unknown query_number: {query_number}")

    if not query_info["active"]:
        raise ValueError(f"Query is archived and cannot be used: {query_number}")

    if not presence_rows and not source_rows:
        raise ValueError("Please provide at least one presence row or one source row")

    submission_id = create_submission(
        project_id=project_id,
        query_number=query_number,
        record_month=record_month,
        ai_platform=ai_platform,
        check_date=check_date,
        created_by=created_by,
        notes=notes,
    )

    try:
        for row in presence_rows:
            insert_presence_record(
                project_id=project_id,
                submission_id=submission_id,
                query_number=query_number,
                check_date=check_date,
                entity_name_cn=row.get("entity_name_cn", ""),
                entity_name_en=row.get("entity_name_en", ""),
                position=row.get("position", 0),
            )

        for row in source_rows:
            insert_source_record(
                project_id=project_id,
                submission_id=submission_id,
                query_number=query_number,
                check_date=check_date,
                source_name=row.get("source_name", ""),
                source_url=row.get("source_url", ""),
                occurrence_number=row.get("occurrence_number", 0),
                quoted_or_not=row.get("quoted_or_not", "N"),
                quoted_url=row.get("quoted_url", ""),
            )

    except Exception:
        delete_submission(submission_id, project_id=project_id)
        raise

    return submission_id


# =========================================================
# Reads - Master Tables
# =========================================================
def get_all_presence_records(project_id: int) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
    SELECT
        p.id,
        p.submission_id,
        s.query_number,
        q.query_type,
        q.query_name_cn,
        q.query_name_en,
        q.product_category,
        q.publish_month_default,
        s.record_month,
        s.ai_platform,
        s.check_date AS submission_check_date,
        s.created_by,
        s.created_at,
        p.check_date,
        p.entity_name_cn,
        p.entity_name_en,
        p.position
    FROM presence_records p
    JOIN submission s
      ON p.submission_id = s.submission_id
    JOIN query_master q
      ON s.query_number = q.query_number
     AND s.project_id = q.project_id
    WHERE s.project_id = ?
    ORDER BY p.id DESC
    """, conn, params=[int(project_id)])
    conn.close()
    return df


def get_all_source_records(project_id: int) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
    SELECT
        r.id,
        r.submission_id,
        s.query_number,
        q.query_type,
        q.query_name_cn,
        q.query_name_en,
        q.product_category,
        q.publish_month_default,
        s.record_month,
        s.ai_platform,
        s.check_date AS submission_check_date,
        s.created_by,
        s.created_at,
        r.check_date,
        r.source_name,
        r.source_url,
        r.occurrence_number,
        r.quoted_or_not,
        r.quoted_url
    FROM source_records r
    JOIN submission s
      ON r.submission_id = s.submission_id
    JOIN query_master q
      ON s.query_number = q.query_number
     AND s.project_id = q.project_id
    WHERE s.project_id = ?
    ORDER BY r.id DESC
    """, conn, params=[int(project_id)])
    conn.close()
    return df


def get_dashboard_tables(
    project_id: int,
    query_status_filter: str = "active_only",
) -> Dict[str, pd.DataFrame]:
    conn = get_connection()

    query_sql = """
    SELECT
        query_number,
        query_type,
        query_name_cn,
        query_name_en,
        product_category,
        publish_month_default,
        active,
        updated_at
    FROM query_master
    WHERE project_id = ?
    """
    query_params = [int(project_id)]
    if query_status_filter == "active_only":
        query_sql += " AND active = 1"
    elif query_status_filter == "archived_only":
        query_sql += " AND active = 0"
    query_sql += " ORDER BY query_number"

    params = [int(project_id)]

    queries_df = pd.read_sql_query(query_sql, conn, params=query_params)

    presence_sql = f"""
    SELECT
        p.id,
        p.submission_id,
        s.query_number,
        q.query_type,
        q.query_name_cn,
        q.query_name_en,
        q.product_category,
        q.publish_month_default,
        s.record_month,
        s.ai_platform,
        s.created_by,
        s.created_at,
        p.check_date,
        p.entity_name_cn,
        p.entity_name_en,
        p.position
    FROM presence_records p
    JOIN submission s
      ON p.submission_id = s.submission_id
    JOIN query_master q
      ON s.query_number = q.query_number
     AND s.project_id = q.project_id
    WHERE s.project_id = ?
    """
    if query_status_filter == "active_only":
        presence_sql += " AND q.active = 1"
    elif query_status_filter == "archived_only":
        presence_sql += " AND q.active = 0"

    source_sql = f"""
    SELECT
        r.id,
        r.submission_id,
        s.query_number,
        q.query_type,
        q.query_name_cn,
        q.query_name_en,
        q.product_category,
        q.publish_month_default,
        s.record_month,
        s.ai_platform,
        s.created_by,
        s.created_at,
        r.check_date,
        r.source_name,
        r.source_url,
        r.occurrence_number,
        r.quoted_or_not,
        r.quoted_url
    FROM source_records r
    JOIN submission s
      ON r.submission_id = s.submission_id
    JOIN query_master q
      ON s.query_number = q.query_number
     AND s.project_id = q.project_id
    WHERE s.project_id = ?
    """
    if query_status_filter == "active_only":
        source_sql += " AND q.active = 1"
    elif query_status_filter == "archived_only":
        source_sql += " AND q.active = 0"

    presence_df = pd.read_sql_query(presence_sql, conn, params=params)
    source_df = pd.read_sql_query(source_sql, conn, params=params)

    conn.close()

    return {
        "queries": queries_df,
        "presence_records": presence_df,
        "source_records": source_df,
    }


# =========================================================
# Deletes - Raw Master Tables
# =========================================================
def delete_presence_record(record_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM presence_records WHERE id = ?", (int(record_id),))
    conn.commit()
    conn.close()


def bulk_delete_presence_records(record_ids: List[int]):
    record_ids = [int(x) for x in record_ids]
    if not record_ids:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "DELETE FROM presence_records WHERE id = ?",
        [(rid,) for rid in record_ids]
    )
    conn.commit()
    conn.close()


def delete_source_record(record_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM source_records WHERE id = ?", (int(record_id),))
    conn.commit()
    conn.close()


def bulk_delete_source_records(record_ids: List[int]):
    record_ids = [int(x) for x in record_ids]
    if not record_ids:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "DELETE FROM source_records WHERE id = ?",
        [(rid,) for rid in record_ids]
    )
    conn.commit()
    conn.close()


# =========================================================
# Excel Templates / Upload Helpers
# =========================================================
def _dataframe_to_excel_bytes(sheet_map: Dict[str, pd.DataFrame]) -> bytes:
    from io import BytesIO

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheet_map.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)
    return output.getvalue()


def build_query_master_template_bytes() -> bytes:
    template_df = pd.DataFrame([
        {
            "query_number": "Q001",
            "query_type": "Generic Query",
            "query_name_cn": "示例中文Query",
            "query_name_en": "Example English Query",
            "product_category": "Cream",
            "publish_month_default": "2026-02,2026-03",
            "active": 1,
        }
    ])
    return _dataframe_to_excel_bytes({"query_master": template_df})


def build_entity_mapping_template_bytes() -> bytes:
    template_df = pd.DataFrame([
        {"entity_name_cn": "理肤泉", "entity_name_en": "La Roche-Posay"}
    ])
    return _dataframe_to_excel_bytes({"entity_mapping": template_df})


def build_source_mapping_template_bytes() -> bytes:
    template_df = pd.DataFrame([
        {"source_name": "知乎", "source_url": "https://www.zhihu.com"}
    ])
    return _dataframe_to_excel_bytes({"source_mapping": template_df})


def build_monthly_results_template_bytes() -> bytes:
    presence_df = pd.DataFrame([
        {
            "query_number": "Q001",
            "record_month": "2026-02",
            "ai_platform": "Doubao",
            "check_date": "2026-02-15",
            "entity_name_cn": "理肤泉",
            "entity_name_en": "La Roche-Posay",
            "position": 1,
            "created_by": "Alice",
        }
    ])
    source_df = pd.DataFrame([
        {
            "query_number": "Q001",
            "record_month": "2026-02",
            "ai_platform": "Doubao",
            "check_date": "2026-02-15",
            "source_name": "知乎",
            "source_url": "https://www.zhihu.com",
            "occurrence_number": 1,
            "quoted_or_not": "Y",
            "quoted_url": "https://www.zhihu.com/question/example",
            "created_by": "Alice",
        }
    ])
    return _dataframe_to_excel_bytes({
        "presence_records": presence_df,
        "source_records": source_df,
    })


# =========================================================
# Bulk Import Helpers
# =========================================================
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _validate_required_columns(df: pd.DataFrame, required_cols: list, sheet_name: str):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet_name}' missing columns: {', '.join(missing)}")


def import_query_master_excel(uploaded_file, project_id: int) -> Dict[str, int]:
    df = _normalize_columns(pd.read_excel(uploaded_file, sheet_name=0))
    _validate_required_columns(df, QUERY_MASTER_COLUMNS, "query_master")

    df = df[QUERY_MASTER_COLUMNS].copy()

    for col in [
        "query_number",
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
    ]:
        df[col] = df[col].apply(normalize_text)

    df["publish_month_default"] = df["publish_month_default"].apply(normalize_publish_months)
    df["active"] = pd.to_numeric(df["active"], errors="coerce").fillna(1).astype(int)
    df = df[(df["query_number"] != "") & (df["query_name_cn"] != "")].copy()

    if df.empty:
        raise ValueError("Query Master template does not contain any valid rows.")

    inserted_count = 0
    updated_count = 0

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT query_number
        FROM query_master
        WHERE project_id = ?
        """,
        (int(project_id),),
    )
    existing_query_numbers = {row[0] for row in cursor.fetchall()}

    for _, row in df.iterrows():
        query_number = row["query_number"]
        if query_number in existing_query_numbers:
            updated_count += 1
        else:
            inserted_count += 1

        cursor.execute("""
        INSERT INTO query_master (
            project_id,
            query_number,
            query_type,
            query_name_cn,
            query_name_en,
            product_category,
            publish_month_default,
            active,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(project_id, query_number) DO UPDATE SET
            query_type = excluded.query_type,
            query_name_cn = excluded.query_name_cn,
            query_name_en = excluded.query_name_en,
            product_category = excluded.product_category,
            publish_month_default = excluded.publish_month_default,
            active = excluded.active,
            updated_at = excluded.updated_at
        """, (
            int(project_id),
            row["query_number"],
            row["query_type"],
            row["query_name_cn"],
            row["query_name_en"],
            row["product_category"],
            row["publish_month_default"],
            int(row["active"]),
            now_ts(),
        ))

    conn.commit()
    conn.close()
    touch_project(project_id)

    return {
        "query_master": inserted_count,
        "updated_existing": updated_count,
    }


# =========================================================
# Bulk Import - Monthly Result Template
# =========================================================
def import_monthly_results_excel(uploaded_file, project_id: int):
    xls = pd.ExcelFile(uploaded_file)
    required_sheets = ["presence_records", "source_records"]
    missing_sheets = [s for s in required_sheets if s not in xls.sheet_names]
    if missing_sheets:
        raise ValueError(f"Missing sheet(s): {', '.join(missing_sheets)}")

    presence_df = _normalize_columns(pd.read_excel(uploaded_file, sheet_name="presence_records"))
    source_df = _normalize_columns(pd.read_excel(uploaded_file, sheet_name="source_records"))

    _validate_required_columns(presence_df, PRESENCE_IMPORT_COLUMNS, "presence_records")
    _validate_required_columns(source_df, SOURCE_IMPORT_COLUMNS, "source_records")

    presence_df = presence_df[PRESENCE_IMPORT_COLUMNS].copy()
    source_df = source_df[SOURCE_IMPORT_COLUMNS].copy()

    presence_df = presence_df.dropna(subset=["query_number", "record_month", "ai_platform", "check_date", "entity_name_cn", "position", "created_by"])
    for col in ["query_number", "record_month", "ai_platform", "check_date", "entity_name_cn", "entity_name_en", "created_by"]:
        presence_df[col] = presence_df[col].apply(normalize_text)
    presence_df["position"] = pd.to_numeric(presence_df["position"], errors="coerce")
    presence_df = presence_df.dropna(subset=["position"])
    presence_df["position"] = presence_df["position"].astype(int)

    source_df = source_df.dropna(subset=["query_number", "record_month", "ai_platform", "check_date", "source_name", "occurrence_number", "quoted_or_not", "created_by"])
    for col in ["query_number", "record_month", "ai_platform", "check_date", "source_name", "source_url", "quoted_or_not", "quoted_url", "created_by"]:
        source_df[col] = source_df[col].apply(normalize_text)
    source_df["occurrence_number"] = pd.to_numeric(source_df["occurrence_number"], errors="coerce")
    source_df = source_df.dropna(subset=["occurrence_number"])
    source_df["occurrence_number"] = source_df["occurrence_number"].astype(int)
    source_df["quoted_or_not"] = source_df["quoted_or_not"].apply(normalize_yes_no)

    valid_queries = set(get_all_queries(project_id=project_id, active_only=False)["query_number"].tolist())

    invalid_presence = presence_df[~presence_df["query_number"].isin(valid_queries)]
    invalid_source = source_df[~source_df["query_number"].isin(valid_queries)]

    if not invalid_presence.empty:
        raise ValueError(
            f"Presence sheet contains unknown query_number: {invalid_presence.iloc[0]['query_number']}"
        )

    if not invalid_source.empty:
        raise ValueError(
            f"Source sheet contains unknown query_number: {invalid_source.iloc[0]['query_number']}"
        )

    for idx, row in presence_df.iterrows():
        if not normalize_text(row["entity_name_en"]):
            mapped_en = get_entity_name_en(project_id, row["entity_name_cn"])
            if mapped_en:
                presence_df.at[idx, "entity_name_en"] = mapped_en

    for idx, row in source_df.iterrows():
        if not normalize_text(row["source_url"]):
            mapped_url = get_source_url(project_id, row["source_name"])
            if mapped_url:
                source_df.at[idx, "source_url"] = mapped_url

    missing_en = presence_df[presence_df["entity_name_en"].apply(normalize_text) == ""]
    if not missing_en.empty:
        sample_cn = missing_en.iloc[0]["entity_name_cn"]
        raise ValueError(
            f"Presence sheet has unmapped entity_name_cn: {sample_cn}. "
            f"Please provide entity_name_en."
        )

    missing_source_url = source_df[source_df["source_url"].apply(normalize_text) == ""]
    if not missing_source_url.empty:
        sample_source = missing_source_url.iloc[0]["source_name"]
        raise ValueError(
            f"Source sheet has unmapped source_name: {sample_source}. "
            f"Please provide source_url."
        )

    group_cols = ["query_number", "record_month", "ai_platform", "check_date", "created_by"]

    presence_groups = set(tuple(x) for x in presence_df[group_cols].drop_duplicates().values.tolist())
    source_groups = set(tuple(x) for x in source_df[group_cols].drop_duplicates().values.tolist())
    all_groups = sorted(presence_groups.union(source_groups))

    created_submissions = 0
    inserted_presence = 0
    inserted_source = 0
    updated_submissions = 0
    updated_presence = 0
    updated_source = 0
    skipped_duplicate_submissions = 0
    skipped_duplicate_presence = 0
    skipped_duplicate_source = 0

    for query_number, record_month, ai_platform, check_date, created_by in all_groups:
        p_sub = presence_df[
            (presence_df["query_number"] == query_number) &
            (presence_df["record_month"] == record_month) &
            (presence_df["ai_platform"] == ai_platform) &
            (presence_df["check_date"] == check_date) &
            (presence_df["created_by"] == created_by)
        ]
        s_sub = source_df[
            (source_df["query_number"] == query_number) &
            (source_df["record_month"] == record_month) &
            (source_df["ai_platform"] == ai_platform) &
            (source_df["check_date"] == check_date) &
            (source_df["created_by"] == created_by)
        ]

        if submission_exists(
            project_id=project_id,
            query_number=query_number,
            record_month=record_month,
            ai_platform=ai_platform,
            check_date=check_date,
            created_by=created_by,
        ):
            skipped_duplicate_submissions += 1
            skipped_duplicate_presence += len(p_sub)
            skipped_duplicate_source += len(s_sub)
            continue

        submission_id = create_submission(
            project_id=project_id,
            query_number=query_number,
            record_month=record_month,
            ai_platform=ai_platform,
            check_date=check_date,
            created_by=created_by,
            notes="Imported from Excel",
        )
        created_submissions += 1

        try:
            for _, row in p_sub.iterrows():
                insert_presence_record(
                    project_id=project_id,
                    submission_id=submission_id,
                    query_number=query_number,
                    check_date=row["check_date"],
                    entity_name_cn=row["entity_name_cn"],
                    entity_name_en=row["entity_name_en"],
                    position=int(row["position"]),
                )
                inserted_presence += 1

            for _, row in s_sub.iterrows():
                insert_source_record(
                    project_id=project_id,
                    submission_id=submission_id,
                    query_number=query_number,
                    check_date=row["check_date"],
                    source_name=row["source_name"],
                    source_url=row["source_url"],
                    occurrence_number=int(row["occurrence_number"]),
                    quoted_or_not=row["quoted_or_not"],
                    quoted_url=row["quoted_url"],
                )
                inserted_source += 1

        except Exception:
            delete_submission(submission_id, project_id=project_id)
            raise

    return {
        "success": True,
        "submissions": created_submissions,
        "presence_records": inserted_presence,
        "source_records": inserted_source,
        "updated_submissions": updated_submissions,
        "updated_presence_records": updated_presence,
        "updated_source_records": updated_source,
        "skipped_duplicate_submissions": skipped_duplicate_submissions,
        "skipped_duplicate_presence_records": skipped_duplicate_presence,
        "skipped_duplicate_source_records": skipped_duplicate_source,
    }
