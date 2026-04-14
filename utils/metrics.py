import pandas as pd
import numpy as np


# =========================================================
# Constants
# =========================================================
MONTHS = [
    "2026-02", "2026-03", "2026-04", "2026-05",
    "2026-06", "2026-07", "2026-08", "2026-09",
    "2026-10", "2026-11", "2026-12",
]


# =========================================================
# Helpers
# =========================================================
def _safe_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return df.copy()


def _normalize_text(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.lower() in {"nan", "none"}:
        return ""
    return s


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _normalize_yes_no(value) -> str:
    v = _normalize_text(value).upper()
    if v in {"Y", "YES", "TRUE", "1"}:
        return "Y"
    if v in {"N", "NO", "FALSE", "0"}:
        return "N"
    return v


def _normalize_publish_months(value) -> str:
    if value is None:
        return ""

    if isinstance(value, (list, tuple, set)):
        raw_items = [_normalize_text(v) for v in value]
    else:
        text = _normalize_text(value)
        if not text:
            return ""
        text = text.replace("，", ",").replace(";", ",").replace("|", ",")
        raw_items = [_normalize_text(x) for x in text.split(",")]

    items = []
    seen = set()
    for item in raw_items:
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            items.append(item)

    ordered = [m for m in MONTHS if m in items]
    extras = [x for x in items if x not in MONTHS]
    return ",".join(ordered + extras)


def split_publish_months(value) -> list[str]:
    text = _normalize_publish_months(value)
    if not text:
        return []
    return [x for x in text.split(",") if _normalize_text(x)]


def explode_publish_months(df: pd.DataFrame, source_col: str = "publish_month_default") -> pd.DataFrame:
    """
    Expand publish_month_default into one row per publish month.
    Adds column: publish_month_single
    """
    df = _safe_df(df)
    if df.empty:
        return df

    if source_col not in df.columns:
        df["publish_month_single"] = ""
        return df

    out = df.copy()
    out["publish_month_list"] = out[source_col].apply(split_publish_months)
    out["publish_month_single"] = out["publish_month_list"]
    out = out.explode("publish_month_single")
    out["publish_month_single"] = out["publish_month_single"].fillna("")
    out = out.drop(columns=["publish_month_list"])
    return out


def filter_by_publish_month(df: pd.DataFrame, publish_month: str) -> pd.DataFrame:
    """
    Filter rows where publish_month_default contains a target month.
    """
    df = _safe_df(df)
    if df.empty:
        return df
    if not publish_month or "publish_month_default" not in df.columns:
        return df

    return df[
        df["publish_month_default"].apply(lambda x: publish_month in split_publish_months(x))
    ].copy()


def _get_brand_col(df: pd.DataFrame) -> str:
    """
    Priority:
    1. entity_name
    2. entity_name_en
    3. entity_name_cn
    """
    if "entity_name" in df.columns:
        return "entity_name"
    if "entity_name_en" in df.columns:
        return "entity_name_en"
    if "entity_name_cn" in df.columns:
        return "entity_name_cn"
    return ""


def _ensure_brand_display(df: pd.DataFrame) -> pd.DataFrame:
    df = _safe_df(df)
    if df.empty:
        return df

    if "brand_display" in df.columns:
        return df

    if "entity_name" in df.columns:
        df["brand_display"] = _normalize_text_series(df["entity_name"])
        return df

    if "entity_name_en" in df.columns and "entity_name_cn" in df.columns:
        en = _normalize_text_series(df["entity_name_en"])
        cn = _normalize_text_series(df["entity_name_cn"])
        df["brand_display"] = np.where(en != "", en, cn)
        return df

    if "entity_name_en" in df.columns:
        df["brand_display"] = _normalize_text_series(df["entity_name_en"])
        return df

    if "entity_name_cn" in df.columns:
        df["brand_display"] = _normalize_text_series(df["entity_name_cn"])
        return df

    df["brand_display"] = ""
    return df


def _normalize_presence_df(presence_df: pd.DataFrame) -> pd.DataFrame:
    presence_df = _ensure_brand_display(presence_df)
    if presence_df.empty:
        return presence_df

    if "position" in presence_df.columns:
        presence_df["position"] = pd.to_numeric(presence_df["position"], errors="coerce")

    for col in [
        "query_number",
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
        "record_month",
        "ai_platform",
        "created_by",
        "brand_display",
        "publish_month_default",
    ]:
        if col in presence_df.columns:
            presence_df[col] = _normalize_text_series(presence_df[col])

    return presence_df


def _normalize_source_df(source_df: pd.DataFrame) -> pd.DataFrame:
    source_df = _safe_df(source_df)
    if source_df.empty:
        return source_df

    for col in [
        "query_number",
        "query_type",
        "query_name_cn",
        "query_name_en",
        "product_category",
        "record_month",
        "ai_platform",
        "created_by",
        "source_name",
        "source_url",
        "quoted_url",
        "publish_month_default",
    ]:
        if col in source_df.columns:
            source_df[col] = _normalize_text_series(source_df[col])

    if "quoted_or_not" in source_df.columns:
        source_df["quoted_or_not"] = source_df["quoted_or_not"].apply(_normalize_yes_no)

    if "occurrence_number" in source_df.columns:
        source_df["occurrence_number"] = pd.to_numeric(
            source_df["occurrence_number"], errors="coerce"
        ).fillna(0)

    return source_df


def _normalize_content_publish_df(content_publish_df: pd.DataFrame) -> pd.DataFrame:
    content_publish_df = _safe_df(content_publish_df)
    if content_publish_df.empty:
        return content_publish_df

    for col in [
        "query_id",
        "publish_platform",
        "publish_url",
    ]:
        if col in content_publish_df.columns:
            content_publish_df[col] = _normalize_text_series(content_publish_df[col])

    if "quoted_or_not" in content_publish_df.columns:
        content_publish_df["quoted_or_not"] = content_publish_df["quoted_or_not"].apply(_normalize_yes_no)

    return content_publish_df


def _position_to_score(pos) -> int:
    try:
        pos = int(pos)
    except Exception:
        return 0

    if pos <= 1:
        return 10
    if pos == 2:
        return 8
    if pos == 3:
        return 6
    if pos == 4:
        return 5
    if pos == 5:
        return 4
    if pos == 6:
        return 3
    if pos == 7:
        return 2
    return 1


# =========================================================
# KPI Metrics
# =========================================================
def compute_kpis(
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame,
    content_publish_df: pd.DataFrame | None = None,
) -> dict:
    """
    KPI logic:
    - total_queries: distinct query_number in filtered pool
    - source_occurance: sum of occurrence_number
    - quote_rate: content publish quoted URLs / total published URLs
    """
    presence_df = _normalize_presence_df(presence_df)
    source_df = _normalize_source_df(source_df)
    content_publish_df = _normalize_content_publish_df(content_publish_df)

    query_pool = set()

    if not presence_df.empty and "query_number" in presence_df.columns:
        query_pool.update(
            _normalize_text_series(presence_df["query_number"]).replace("", np.nan).dropna().tolist()
        )

    if not source_df.empty and "query_number" in source_df.columns:
        query_pool.update(
            _normalize_text_series(source_df["query_number"]).replace("", np.nan).dropna().tolist()
        )

    total_queries = len(query_pool)

    if not source_df.empty and "occurrence_number" in source_df.columns:
        source_occurance = int(source_df["occurrence_number"].fillna(0).sum())
    else:
        source_occurance = 0

    if not content_publish_df.empty:
        total_publish_urls = len(content_publish_df)
        quoted_records = (
            len(content_publish_df[content_publish_df["quoted_or_not"] == "Y"])
            if "quoted_or_not" in content_publish_df.columns
            else 0
        )
        quote_rate = (quoted_records / total_publish_urls) if total_publish_urls > 0 else 0.0
    else:
        quote_rate = 0.0

    return {
        "total_queries": total_queries,
        "source_occurance": source_occurance,
        "quote_rate": quote_rate,
    }


# =========================================================
# Presence Metrics
# =========================================================
def get_brand_ranking(presence_df: pd.DataFrame) -> pd.DataFrame:
    """
    Output columns:
    - Brand
    - Brand Mention
    - Avg Position
    - Best Position
    """
    presence_df = _normalize_presence_df(presence_df)
    if presence_df.empty or "brand_display" not in presence_df.columns:
        return pd.DataFrame(columns=["Brand", "Brand Mention", "Avg Position", "Best Position"])

    presence_df = presence_df[presence_df["brand_display"] != ""]
    if presence_df.empty:
        return pd.DataFrame(columns=["Brand", "Brand Mention", "Avg Position", "Best Position"])

    ranking = (
        presence_df.groupby("brand_display", dropna=False)
        .agg(
            **{
                "Brand Mention": ("brand_display", "count"),
                "Avg Position": ("position", "mean"),
                "Best Position": ("position", "min"),
            }
        )
        .reset_index()
        .rename(columns={"brand_display": "Brand"})
    )

    ranking["Avg Position"] = ranking["Avg Position"].round(2)
    ranking = ranking.sort_values(
        by=["Brand Mention", "Avg Position", "Best Position"],
        ascending=[False, True, True]
    ).reset_index(drop=True)

    return ranking


def get_channel_ranking(presence_df: pd.DataFrame) -> pd.DataFrame:
    """
    If query_type contains 'Channel', aggregate displayed brands.
    Output columns:
    - Channel
    - Channel Mention
    - Avg Position
    """
    presence_df = _normalize_presence_df(presence_df)
    if presence_df.empty:
        return pd.DataFrame(columns=["Channel", "Channel Mention", "Avg Position"])

    if "query_type" in presence_df.columns:
        presence_df = presence_df[
            presence_df["query_type"].fillna("").astype(str).str.lower().str.contains("channel")
        ]

    if presence_df.empty:
        return pd.DataFrame(columns=["Channel", "Channel Mention", "Avg Position"])

    ranking = (
        presence_df.groupby("brand_display", dropna=False)
        .agg(
            **{
                "Channel Mention": ("brand_display", "count"),
                "Avg Position": ("position", "mean"),
            }
        )
        .reset_index()
        .rename(columns={"brand_display": "Channel"})
    )

    ranking["Avg Position"] = ranking["Avg Position"].round(2)
    ranking = ranking.sort_values(
        by=["Channel Mention", "Avg Position"],
        ascending=[False, True]
    ).reset_index(drop=True)

    return ranking


def get_brand_visibility_by_category(presence_df: pd.DataFrame, category: str) -> pd.DataFrame:
    """
    Core visibility logic:
    1. Filter one product category
    2. For each query_number + brand, get best position
    3. Convert best position into reciprocal rank (1 / best_position)
    4. Visibility Score = (sum(reciprocal rank) / query pool) * 5
    """
    presence_df = _normalize_presence_df(presence_df)
    if presence_df.empty:
        return pd.DataFrame(columns=[
            "Brand", "Covered Queries", "Query Pool", "Coverage Rate",
            "Avg Best Position", "Visibility Score"
        ])

    if "product_category" not in presence_df.columns:
        return pd.DataFrame(columns=[
            "Brand", "Covered Queries", "Query Pool", "Coverage Rate",
            "Avg Best Position", "Visibility Score"
        ])

    filtered = presence_df[
        presence_df["product_category"].fillna("").astype(str).str.strip() == str(category).strip()
    ].copy()

    if filtered.empty:
        return pd.DataFrame(columns=[
            "Brand", "Covered Queries", "Query Pool", "Coverage Rate",
            "Avg Best Position", "Visibility Score"
        ])

    filtered = filtered.dropna(subset=["query_number", "brand_display", "position"])
    filtered = filtered[(filtered["query_number"] != "") & (filtered["brand_display"] != "")]
    if filtered.empty:
        return pd.DataFrame(columns=[
            "Brand", "Covered Queries", "Query Pool", "Coverage Rate",
            "Avg Best Position", "Visibility Score"
        ])

    query_pool = filtered["query_number"].nunique()
    if query_pool == 0:
        return pd.DataFrame(columns=[
            "Brand", "Covered Queries", "Query Pool", "Coverage Rate",
            "Avg Best Position", "Visibility Score"
        ])

    per_query_best = (
        filtered.groupby(["query_number", "brand_display"], as_index=False)
        .agg(best_position=("position", "min"))
    )

    per_query_best["reciprocal_rank"] = per_query_best["best_position"].apply(
        lambda x: (1 / x) if pd.notna(x) and float(x) > 0 else 0
    )

    brand_visibility = (
        per_query_best.groupby("brand_display", as_index=False)
        .agg(
            **{
                "Covered Queries": ("query_number", "nunique"),
                "Avg Best Position": ("best_position", "mean"),
                "Visibility Score Raw": ("reciprocal_rank", "sum"),
            }
        )
        .rename(columns={"brand_display": "Brand"})
    )

    brand_visibility["Query Pool"] = query_pool
    brand_visibility["Coverage Rate"] = (
        brand_visibility["Covered Queries"] / query_pool
    ).round(4)

    brand_visibility["Visibility Score"] = (
        (brand_visibility["Visibility Score Raw"] / query_pool) * 5
    ).round(2)

    brand_visibility["Avg Best Position"] = brand_visibility["Avg Best Position"].round(2)

    brand_visibility = brand_visibility[
        ["Brand", "Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"]
    ].sort_values(
        by=["Visibility Score", "Coverage Rate", "Avg Best Position"],
        ascending=[False, False, True]
    ).reset_index(drop=True)

    return brand_visibility


def get_brand_visibility_by_category_and_publish_month(
    presence_df: pd.DataFrame,
    category: str,
    publish_month: str,
) -> pd.DataFrame:
    """
    Same visibility logic, but filter by category + publish month.
    publish_month comes from query_master.publish_month_default exploded logic.
    """
    presence_df = _normalize_presence_df(presence_df)
    if presence_df.empty:
        return pd.DataFrame(columns=[
            "Brand", "Covered Queries", "Query Pool", "Coverage Rate",
            "Avg Best Position", "Visibility Score"
        ])

    if "publish_month_default" in presence_df.columns and publish_month:
        presence_df = filter_by_publish_month(presence_df, publish_month)

    return get_brand_visibility_by_category(presence_df, category)


# =========================================================
# Query / Publish Month Metrics
# =========================================================
def get_query_master_publish_month_table(queries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Exploded query master by publish month.
    Output columns:
    - query_number
    - query_type
    - query_name_cn
    - query_name_en
    - product_category
    - publish_month_single
    - active
    """
    queries_df = _safe_df(queries_df)
    if queries_df.empty:
        return pd.DataFrame(columns=[
            "query_number", "query_type", "query_name_cn", "query_name_en",
            "product_category", "publish_month_single", "active"
        ])

    if "publish_month_default" not in queries_df.columns:
        out = queries_df.copy()
        out["publish_month_single"] = ""
        return out

    out = explode_publish_months(queries_df, "publish_month_default")
    cols = [
        c for c in [
            "query_number", "query_type", "query_name_cn", "query_name_en",
            "product_category", "publish_month_single", "active"
        ]
        if c in out.columns
    ]
    return out[cols].copy()


def get_query_count_by_publish_month(queries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Output:
    - Publish Month
    - Query Count
    """
    queries_df = get_query_master_publish_month_table(queries_df)
    if queries_df.empty or "publish_month_single" not in queries_df.columns:
        return pd.DataFrame(columns=["Publish Month", "Query Count"])

    df = queries_df.copy()
    df["publish_month_single"] = _normalize_text_series(df["publish_month_single"])
    df = df[df["publish_month_single"] != ""]
    if df.empty:
        return pd.DataFrame(columns=["Publish Month", "Query Count"])

    result = (
        df.groupby("publish_month_single", as_index=False)
        .agg(**{"Query Count": ("query_number", "nunique")})
        .rename(columns={"publish_month_single": "Publish Month"})
    )

    month_order = {m: i for i, m in enumerate(MONTHS)}
    result["_sort"] = result["Publish Month"].map(month_order).fillna(9999)
    result = result.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)
    return result


# =========================================================
# Source Metrics
# =========================================================
def get_source_occurrence_ranking(source_df: pd.DataFrame) -> pd.DataFrame:
    """
    Output columns:
    - Source
    - source occurance
    """
    source_df = _normalize_source_df(source_df)
    if source_df.empty or "source_name" not in source_df.columns:
        return pd.DataFrame(columns=["Source", "source occurance"])

    source_df = source_df[source_df["source_name"] != ""]
    if source_df.empty:
        return pd.DataFrame(columns=["Source", "source occurance"])

    ranking = (
        source_df.groupby("source_name", as_index=False)
        .agg(**{"source occurance": ("occurrence_number", "sum")})
        .rename(columns={"source_name": "Source"})
        .sort_values(by="source occurance", ascending=False)
        .reset_index(drop=True)
    )

    ranking["source occurance"] = ranking["source occurance"].astype(int)
    return ranking


def get_source_distribution_by_platform(source_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Output columns:
    - Source
    - AI Platform
    - source occurance
    """
    source_df = _normalize_source_df(source_df)
    if source_df.empty:
        return pd.DataFrame(columns=["Source", "AI Platform", "source occurance"])

    needed_cols = {"source_name", "ai_platform", "occurrence_number"}
    if not needed_cols.issubset(source_df.columns):
        return pd.DataFrame(columns=["Source", "AI Platform", "source occurance"])

    source_df = source_df[
        (source_df["source_name"] != "") &
        (source_df["ai_platform"] != "")
    ]
    if source_df.empty:
        return pd.DataFrame(columns=["Source", "AI Platform", "source occurance"])

    total_by_source = (
        source_df.groupby("source_name", as_index=False)
        .agg(total_occurrence=("occurrence_number", "sum"))
        .sort_values(by="total_occurrence", ascending=False)
    )

    top_sources = total_by_source.head(top_n)["source_name"].tolist()

    distribution = (
        source_df[source_df["source_name"].isin(top_sources)]
        .groupby(["source_name", "ai_platform"], as_index=False)
        .agg(**{"source occurance": ("occurrence_number", "sum")})
        .rename(columns={
            "source_name": "Source",
            "ai_platform": "AI Platform"
        })
        .sort_values(by=["Source", "AI Platform"])
        .reset_index(drop=True)
    )

    distribution["source occurance"] = distribution["source occurance"].astype(int)
    return distribution


def get_source_platform_comparison(
    source_df: pd.DataFrame,
    primary_platform: str = "Doubao",
    secondary_platform: str = "Deepseek",
    top_n: int = 20,
) -> dict[str, pd.DataFrame]:
    source_df = _normalize_source_df(source_df)
    empty_common = pd.DataFrame(
        columns=["Source", "Total Occurrence", f"{primary_platform} Occurrence", f"{secondary_platform} Occurrence"]
    )
    empty_primary = pd.DataFrame(columns=["Source", f"{primary_platform} Occurrence"])
    empty_secondary = pd.DataFrame(columns=["Source", f"{secondary_platform} Occurrence"])

    if source_df.empty:
        return {"common": empty_common, "primary_only": empty_primary, "secondary_only": empty_secondary}

    needed_cols = {"source_name", "ai_platform", "occurrence_number"}
    if not needed_cols.issubset(source_df.columns):
        return {"common": empty_common, "primary_only": empty_primary, "secondary_only": empty_secondary}

    working = source_df[
        (source_df["source_name"] != "") &
        (source_df["ai_platform"] != "")
    ].copy()
    if working.empty:
        return {"common": empty_common, "primary_only": empty_primary, "secondary_only": empty_secondary}

    platform_norm = working["ai_platform"].astype(str).str.strip().str.lower()
    primary_df = working[platform_norm == primary_platform.strip().lower()].copy()
    secondary_df = working[platform_norm == secondary_platform.strip().lower()].copy()

    if primary_df.empty and secondary_df.empty:
        return {"common": empty_common, "primary_only": empty_primary, "secondary_only": empty_secondary}

    def _top_platform_sources(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["Source", f"{label} Occurrence"])
        out = (
            df.groupby("source_name", as_index=False)
            .agg(**{f"{label} Occurrence": ("occurrence_number", "sum")})
            .rename(columns={"source_name": "Source"})
            .sort_values(by=f"{label} Occurrence", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        out[f"{label} Occurrence"] = out[f"{label} Occurrence"].astype(int)
        return out

    primary_top = _top_platform_sources(primary_df, primary_platform)
    secondary_top = _top_platform_sources(secondary_df, secondary_platform)

    primary_sources = set(primary_top["Source"].tolist())
    secondary_sources = set(secondary_top["Source"].tolist())
    common_sources = primary_sources & secondary_sources
    primary_only_sources = primary_sources - secondary_sources
    secondary_only_sources = secondary_sources - primary_sources

    primary_lookup = dict(zip(primary_top["Source"], primary_top[f"{primary_platform} Occurrence"]))
    secondary_lookup = dict(zip(secondary_top["Source"], secondary_top[f"{secondary_platform} Occurrence"]))

    common_rows = [
        {
            "Source": source,
            f"{primary_platform} Occurrence": primary_lookup.get(source, 0),
            f"{secondary_platform} Occurrence": secondary_lookup.get(source, 0),
            "Total Occurrence": primary_lookup.get(source, 0) + secondary_lookup.get(source, 0),
        }
        for source in common_sources
    ]
    common_df = pd.DataFrame(common_rows)
    if not common_df.empty:
        common_df = common_df.sort_values(
            by=["Total Occurrence", f"{primary_platform} Occurrence", f"{secondary_platform} Occurrence", "Source"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)

    primary_only_df = primary_top[primary_top["Source"].isin(primary_only_sources)].reset_index(drop=True)
    secondary_only_df = secondary_top[secondary_top["Source"].isin(secondary_only_sources)].reset_index(drop=True)

    return {
        "common": common_df if not common_df.empty else empty_common,
        "primary_only": primary_only_df if not primary_only_df.empty else empty_primary,
        "secondary_only": secondary_only_df if not secondary_only_df.empty else empty_secondary,
    }


def get_quoted_source_ranking(source_df: pd.DataFrame) -> pd.DataFrame:
    """
    Output columns:
    - Source
    - Quoted Source number
    """
    source_df = _normalize_source_df(source_df)
    if source_df.empty:
        return pd.DataFrame(columns=["Source", "Quoted Source number"])

    needed_cols = {"source_name", "quoted_or_not"}
    if not needed_cols.issubset(source_df.columns):
        return pd.DataFrame(columns=["Source", "Quoted Source number"])

    quoted_df = source_df[source_df["quoted_or_not"] == "Y"].copy()
    if quoted_df.empty:
        return pd.DataFrame(columns=["Source", "Quoted Source number"])

    quoted_df = quoted_df[quoted_df["source_name"] != ""]
    if quoted_df.empty:
        return pd.DataFrame(columns=["Source", "Quoted Source number"])

    ranking = (
        quoted_df.groupby("source_name", as_index=False)
        .agg(**{"Quoted Source number": ("source_name", "count")})
        .rename(columns={"source_name": "Source"})
        .sort_values(by="Quoted Source number", ascending=False)
        .reset_index(drop=True)
    )

    return ranking


def get_quote_rate(content_publish_df: pd.DataFrame) -> dict:
    """
    Quote rate is calculated by record count:
    quoted_or_not == Y records / total publish_url records
    """
    content_publish_df = _normalize_content_publish_df(content_publish_df)

    if content_publish_df.empty:
        return {
            "quoted_records": 0,
            "total_records": 0,
            "quote_rate": 0.0,
        }

    total_records = len(content_publish_df)
    quoted_records = (
        len(content_publish_df[content_publish_df["quoted_or_not"] == "Y"])
        if "quoted_or_not" in content_publish_df.columns
        else 0
    )
    quote_rate = quoted_records / total_records if total_records > 0 else 0.0

    return {
        "quoted_records": int(quoted_records),
        "total_records": int(total_records),
        "quote_rate": float(quote_rate),
    }


# =========================================================
# Optional Trend Helpers
# =========================================================
def get_brand_ranking_by_month(presence_df: pd.DataFrame) -> pd.DataFrame:
    """
    Output:
    - record_month
    - Brand
    - Brand Mention
    - Avg Position
    """
    presence_df = _normalize_presence_df(presence_df)
    if presence_df.empty:
        return pd.DataFrame(columns=["record_month", "Brand", "Brand Mention", "Avg Position"])

    needed_cols = {"record_month", "brand_display", "position"}
    if not needed_cols.issubset(presence_df.columns):
        return pd.DataFrame(columns=["record_month", "Brand", "Brand Mention", "Avg Position"])

    result = (
        presence_df.groupby(["record_month", "brand_display"], as_index=False)
        .agg(
            **{
                "Brand Mention": ("brand_display", "count"),
                "Avg Position": ("position", "mean"),
            }
        )
        .rename(columns={"brand_display": "Brand"})
        .sort_values(by=["record_month", "Brand Mention", "Avg Position"], ascending=[True, False, True])
        .reset_index(drop=True)
    )

    result["Avg Position"] = result["Avg Position"].round(2)
    return result


def get_brand_visibility_by_publish_month(presence_df: pd.DataFrame) -> pd.DataFrame:
    """
    Explode publish_month_default and calculate brand visibility by publish month.
    Output:
    - Publish Month
    - Brand
    - Covered Queries
    - Query Pool
    - Coverage Rate
    - Avg Best Position
    - Visibility Score
    """
    presence_df = _normalize_presence_df(presence_df)
    if presence_df.empty or "publish_month_default" not in presence_df.columns:
        return pd.DataFrame(columns=[
            "Publish Month", "Brand", "Covered Queries", "Query Pool",
            "Coverage Rate", "Avg Best Position", "Visibility Score"
        ])

    exploded = explode_publish_months(presence_df, "publish_month_default")
    exploded = exploded[exploded["publish_month_single"] != ""].copy()
    if exploded.empty:
        return pd.DataFrame(columns=[
            "Publish Month", "Brand", "Covered Queries", "Query Pool",
            "Coverage Rate", "Avg Best Position", "Visibility Score"
        ])

    results = []

    for month in exploded["publish_month_single"].dropna().unique().tolist():
        month_df = exploded[exploded["publish_month_single"] == month].copy()
        vis_df = get_brand_ranking(month_df)

        if vis_df.empty:
            continue

        # build a visibility version per month using best position per query
        temp = month_df.dropna(subset=["query_number", "brand_display", "position"]).copy()
        temp = temp[(temp["query_number"] != "") & (temp["brand_display"] != "")]
        if temp.empty:
            continue

        query_pool = temp["query_number"].nunique()
        per_query_best = (
            temp.groupby(["query_number", "brand_display"], as_index=False)
            .agg(best_position=("position", "min"))
        )
        per_query_best["reciprocal_rank"] = per_query_best["best_position"].apply(
            lambda x: (1 / x) if pd.notna(x) and float(x) > 0 else 0
        )

        month_vis = (
            per_query_best.groupby("brand_display", as_index=False)
            .agg(
                **{
                    "Covered Queries": ("query_number", "nunique"),
                    "Avg Best Position": ("best_position", "mean"),
                    "Visibility Score Raw": ("reciprocal_rank", "sum"),
                }
            )
            .rename(columns={"brand_display": "Brand"})
        )

        month_vis["Query Pool"] = query_pool
        month_vis["Coverage Rate"] = (month_vis["Covered Queries"] / query_pool).round(4)
        month_vis["Visibility Score"] = (
            (month_vis["Visibility Score Raw"] / query_pool) * 5
        ).round(2)
        month_vis["Avg Best Position"] = month_vis["Avg Best Position"].round(2)
        month_vis["Publish Month"] = month

        month_vis = month_vis[
            ["Publish Month", "Brand", "Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"]
        ]

        results.append(month_vis)

    if not results:
        return pd.DataFrame(columns=[
            "Publish Month", "Brand", "Covered Queries", "Query Pool",
            "Coverage Rate", "Avg Best Position", "Visibility Score"
        ])

    out = pd.concat(results, ignore_index=True)

    month_order = {m: i for i, m in enumerate(MONTHS)}
    out["_sort"] = out["Publish Month"].map(month_order).fillna(9999)
    out = out.sort_values(
        by=["_sort", "Visibility Score", "Coverage Rate", "Avg Best Position"],
        ascending=[True, False, False, True]
    ).drop(columns="_sort").reset_index(drop=True)

    return out


def get_brand_visibility_by_record_month(presence_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate brand visibility by record month.
    Output:
    - Record Month
    - Brand
    - Covered Queries
    - Query Pool
    - Coverage Rate
    - Avg Best Position
    - Visibility Score
    """
    presence_df = _normalize_presence_df(presence_df)
    if presence_df.empty or "record_month" not in presence_df.columns:
        return pd.DataFrame(columns=[
            "Record Month", "Brand", "Covered Queries", "Query Pool",
            "Coverage Rate", "Avg Best Position", "Visibility Score"
        ])

    working_df = presence_df[presence_df["record_month"] != ""].copy()
    if working_df.empty:
        return pd.DataFrame(columns=[
            "Record Month", "Brand", "Covered Queries", "Query Pool",
            "Coverage Rate", "Avg Best Position", "Visibility Score"
        ])

    results = []

    for month in working_df["record_month"].dropna().unique().tolist():
        month_df = working_df[working_df["record_month"] == month].copy()
        temp = month_df.dropna(subset=["query_number", "brand_display", "position"]).copy()
        temp = temp[(temp["query_number"] != "") & (temp["brand_display"] != "")]
        if temp.empty:
            continue

        query_pool = temp["query_number"].nunique()
        per_query_best = (
            temp.groupby(["query_number", "brand_display"], as_index=False)
            .agg(best_position=("position", "min"))
        )
        per_query_best["reciprocal_rank"] = per_query_best["best_position"].apply(
            lambda x: (1 / x) if pd.notna(x) and float(x) > 0 else 0
        )

        month_vis = (
            per_query_best.groupby("brand_display", as_index=False)
            .agg(
                **{
                    "Covered Queries": ("query_number", "nunique"),
                    "Avg Best Position": ("best_position", "mean"),
                    "Visibility Score Raw": ("reciprocal_rank", "sum"),
                }
            )
            .rename(columns={"brand_display": "Brand"})
        )

        month_vis["Query Pool"] = query_pool
        month_vis["Coverage Rate"] = (month_vis["Covered Queries"] / query_pool).round(4)
        month_vis["Visibility Score"] = (
            (month_vis["Visibility Score Raw"] / query_pool) * 5
        ).round(2)
        month_vis["Avg Best Position"] = month_vis["Avg Best Position"].round(2)
        month_vis["Record Month"] = month
        month_vis = month_vis[
            ["Record Month", "Brand", "Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"]
        ]
        results.append(month_vis)

    if not results:
        return pd.DataFrame(columns=[
            "Record Month", "Brand", "Covered Queries", "Query Pool",
            "Coverage Rate", "Avg Best Position", "Visibility Score"
        ])

    out = pd.concat(results, ignore_index=True)
    month_order = {m: i for i, m in enumerate(MONTHS)}
    out["_sort"] = out["Record Month"].map(month_order).fillna(9999)
    out = out.sort_values(
        by=["_sort", "Visibility Score", "Coverage Rate", "Avg Best Position"],
        ascending=[True, False, False, True]
    ).drop(columns="_sort").reset_index(drop=True)
    return out
