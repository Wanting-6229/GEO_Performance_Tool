import pandas as pd
from typing import Optional, Dict, Any, List

from utils.db import get_dashboard_tables


# =========================================================
# Basic Helpers
# =========================================================
def _safe_copy_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    return df.copy()


def _ensure_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Make sure dataframe contains all required columns.
    Missing columns will be created with empty values.
    """
    df = _safe_copy_df(df)
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df


def _normalize_text_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = _safe_copy_df(df)
    for col in cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
    return df


def _normalize_int_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = _safe_copy_df(df)
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _normalize_date_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = _safe_copy_df(df)
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _standardize_yes_no(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df = _safe_copy_df(df)
    if col in df.columns:
        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({
                "YES": "Y",
                "NO": "N",
                "TRUE": "Y",
                "FALSE": "N",
                "1": "Y",
                "0": "N"
            })
        )
    return df


# =========================================================
# Query Master
# =========================================================
def prepare_queries_df(queries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize query_master dataframe.
    """
    queries_df = _ensure_columns(
        queries_df,
        [
            "query_number",
            "query_type",
            "query_name_cn",
            "query_name_en",
            "product_category",
            "publish_month_default",
            "active",
            "updated_at",
        ],
    )

    queries_df = _normalize_text_columns(
        queries_df,
        [
            "query_number",
            "query_type",
            "query_name_cn",
            "query_name_en",
            "product_category",
            "publish_month_default",
            "updated_at",
        ],
    )

    if "active" in queries_df.columns:
        queries_df["active"] = pd.to_numeric(queries_df["active"], errors="coerce").fillna(1).astype(int)

    queries_df = queries_df.sort_values(by=["query_number"], ascending=True).reset_index(drop=True)
    return queries_df


# =========================================================
# Presence
# =========================================================
def prepare_presence_df(presence_df: pd.DataFrame, queries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich presence records with query master fields.
    """
    presence_df = _ensure_columns(
        presence_df,
        [
            "id",
            "submission_id",
            "query_number",
            "record_month",
            "ai_platform",
            "created_by",
            "created_at",
            "check_date",
            "entity_name_cn",
            "entity_name_en",
            "position",
        ],
    )

    presence_df = _normalize_text_columns(
        presence_df,
        [
            "submission_id",
            "query_number",
            "record_month",
            "ai_platform",
            "created_by",
            "created_at",
            "check_date",
            "entity_name_cn",
            "entity_name_en",
        ],
    )

    presence_df = _normalize_int_columns(presence_df, ["id", "position"])
    presence_df = _normalize_date_columns(presence_df, ["check_date", "created_at"])

    # Merge query info
    queries_merge_cols = [
        "query_number",
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
        "publish_month_default",
        "active",
    ]
    queries_df = _ensure_columns(queries_df, queries_merge_cols)

    presence_df = presence_df.merge(
        queries_df[queries_merge_cols],
        on="query_number",
        how="left",
        suffixes=("", "_query")
    )

    # Convenience fields
    if "position" in presence_df.columns:
        presence_df["position"] = pd.to_numeric(presence_df["position"], errors="coerce")

    if "record_month" in presence_df.columns:
        presence_df["record_month"] = presence_df["record_month"].fillna("").astype(str).str.strip()

    presence_df["entity_name_display"] = presence_df["entity_name_en"].where(
        presence_df["entity_name_en"].astype(str).str.strip() != "",
        presence_df["entity_name_cn"]
    )

    return presence_df


# =========================================================
# Source
# =========================================================
def prepare_source_df(source_df: pd.DataFrame, queries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich source records with query master fields.
    """
    source_df = _ensure_columns(
        source_df,
        [
            "id",
            "submission_id",
            "query_number",
            "record_month",
            "ai_platform",
            "created_by",
            "created_at",
            "check_date",
            "source_name",
            "source_url",
            "occurrence_number",
            "quoted_or_not",
            "quoted_url",
        ],
    )

    source_df = _normalize_text_columns(
        source_df,
        [
            "submission_id",
            "query_number",
            "record_month",
            "ai_platform",
            "created_by",
            "created_at",
            "check_date",
            "source_name",
            "source_url",
            "quoted_or_not",
            "quoted_url",
        ],
    )

    source_df = _normalize_int_columns(source_df, ["id", "occurrence_number"])
    source_df = _normalize_date_columns(source_df, ["check_date", "created_at"])
    source_df = _standardize_yes_no(source_df, "quoted_or_not")

    queries_merge_cols = [
        "query_number",
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
        "publish_month_default",
        "active",
    ]
    queries_df = _ensure_columns(queries_df, queries_merge_cols)

    source_df = source_df.merge(
        queries_df[queries_merge_cols],
        on="query_number",
        how="left",
        suffixes=("", "_query")
    )

    if "record_month" in source_df.columns:
        source_df["record_month"] = source_df["record_month"].fillna("").astype(str).str.strip()

    return source_df


# =========================================================
# Main Loader
# =========================================================
def load_data(
    project_id: int,
    query_status_filter: str = "active_only",
) -> Dict[str, pd.DataFrame]:
    """
    Main loader for V2 dashboard / app usage.

    Returns:
        {
            "queries": query_master_df,
            "presence_records": enriched_presence_df,
            "source_records": enriched_source_df
        }
    """
    raw_tables = get_dashboard_tables(
        project_id=project_id,
        query_status_filter=query_status_filter,
    )

    queries_df = prepare_queries_df(raw_tables.get("queries", pd.DataFrame()))
    presence_df = prepare_presence_df(raw_tables.get("presence_records", pd.DataFrame()), queries_df)
    source_df = prepare_source_df(raw_tables.get("source_records", pd.DataFrame()), queries_df)

    return {
        "queries": queries_df,
        "presence_records": presence_df,
        "source_records": source_df,
    }


# =========================================================
# Optional Convenience Wrappers
# =========================================================
def load_official_data(project_id: int, query_status_filter: str = "active_only") -> Dict[str, pd.DataFrame]:
    return load_data(
        project_id=project_id,
        query_status_filter=query_status_filter,
    )


def load_all_submissions_data(project_id: int, query_status_filter: str = "active_only") -> Dict[str, pd.DataFrame]:
    return load_data(
        project_id=project_id,
        query_status_filter=query_status_filter,
    )


def load_creator_data(project_id: int, created_by: str, query_status_filter: str = "active_only") -> Dict[str, pd.DataFrame]:
    return load_data(
        project_id=project_id,
        query_status_filter=query_status_filter,
    )


# =========================================================
# Filter Helpers
# =========================================================
def apply_common_filters(
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame,
    query_type: Optional[List[str]] = None,
    product_category: Optional[List[str]] = None,
    ai_platform: Optional[List[str]] = None,
    query_number: Optional[List[str]] = None,
    record_month: Optional[List[str]] = None,
    created_by: Optional[List[str]] = None,
    check_date_range: Optional[List[pd.Timestamp]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Apply common filters to both presence and source dataframes.
    """
    p = _safe_copy_df(presence_df)
    s = _safe_copy_df(source_df)

    if query_type:
        if "query_type" in p.columns:
            p = p[p["query_type"].isin(query_type)]
        if "query_type" in s.columns:
            s = s[s["query_type"].isin(query_type)]

    if product_category:
        if "product_category" in p.columns:
            p = p[p["product_category"].isin(product_category)]
        if "product_category" in s.columns:
            s = s[s["product_category"].isin(product_category)]

    if ai_platform:
        if "ai_platform" in p.columns:
            p = p[p["ai_platform"].isin(ai_platform)]
        if "ai_platform" in s.columns:
            s = s[s["ai_platform"].isin(ai_platform)]

    if query_number:
        if "query_number" in p.columns:
            p = p[p["query_number"].isin(query_number)]
        if "query_number" in s.columns:
            s = s[s["query_number"].isin(query_number)]

    if record_month:
        if "record_month" in p.columns:
            p = p[p["record_month"].isin(record_month)]
        if "record_month" in s.columns:
            s = s[s["record_month"].isin(record_month)]

    if created_by:
        if "created_by" in p.columns:
            p = p[p["created_by"].isin(created_by)]
        if "created_by" in s.columns:
            s = s[s["created_by"].isin(created_by)]

    if check_date_range and len(check_date_range) == 2:
        start_date, end_date = check_date_range

        if "check_date" in p.columns:
            p = p[
                (pd.to_datetime(p["check_date"], errors="coerce") >= pd.to_datetime(start_date)) &
                (pd.to_datetime(p["check_date"], errors="coerce") <= pd.to_datetime(end_date))
            ]

        if "check_date" in s.columns:
            s = s[
                (pd.to_datetime(s["check_date"], errors="coerce") >= pd.to_datetime(start_date)) &
                (pd.to_datetime(s["check_date"], errors="coerce") <= pd.to_datetime(end_date))
            ]

    return {
        "presence_records": p.reset_index(drop=True),
        "source_records": s.reset_index(drop=True),
    }


# =========================================================
# Selector Options
# =========================================================
def get_filter_options(
    queries_df: pd.DataFrame,
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame
) -> Dict[str, List[Any]]:
    """
    Build filter option lists for app.py.
    """
    options = {}

    def _sorted_unique(series: pd.Series):
        vals = [x for x in series.dropna().astype(str).str.strip().unique().tolist() if x != ""]
        return sorted(vals)

    if not queries_df.empty:
        if "query_type" in queries_df.columns:
            options["query_type"] = _sorted_unique(queries_df["query_type"])
        if "product_category" in queries_df.columns:
            options["product_category"] = _sorted_unique(queries_df["product_category"])
        if "query_number" in queries_df.columns:
            options["query_number"] = _sorted_unique(queries_df["query_number"])
    else:
        options["query_type"] = []
        options["product_category"] = []
        options["query_number"] = []

    merged_platform_source = pd.concat(
        [
            presence_df["ai_platform"] if "ai_platform" in presence_df.columns else pd.Series(dtype=str),
            source_df["ai_platform"] if "ai_platform" in source_df.columns else pd.Series(dtype=str),
        ],
        ignore_index=True
    )
    options["ai_platform"] = _sorted_unique(merged_platform_source)

    merged_month_source = pd.concat(
        [
            presence_df["record_month"] if "record_month" in presence_df.columns else pd.Series(dtype=str),
            source_df["record_month"] if "record_month" in source_df.columns else pd.Series(dtype=str),
        ],
        ignore_index=True
    )
    options["record_month"] = _sorted_unique(merged_month_source)

    merged_creator_source = pd.concat(
        [
            presence_df["created_by"] if "created_by" in presence_df.columns else pd.Series(dtype=str),
            source_df["created_by"] if "created_by" in source_df.columns else pd.Series(dtype=str),
        ],
        ignore_index=True
    )
    options["created_by"] = _sorted_unique(merged_creator_source)

    return options


# =========================================================
# One-step dashboard-ready loader
# =========================================================
def load_dashboard_ready_data(
    project_id: int,
    query_status_filter: str = "active_only",
) -> Dict[str, Any]:
    """
    Returns:
        {
            "queries": ...,
            "presence_records": ...,
            "source_records": ...,
            "filter_options": ...
        }
    """
    data = load_data(
        project_id=project_id,
        query_status_filter=query_status_filter,
    )

    filter_options = get_filter_options(
        queries_df=data["queries"],
        presence_df=data["presence_records"],
        source_df=data["source_records"],
    )

    data["filter_options"] = filter_options
    return data
