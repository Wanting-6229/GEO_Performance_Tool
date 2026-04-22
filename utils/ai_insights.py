import json
import os
from typing import Any, Dict, List
from urllib import error, request

import pandas as pd


DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _compact_records(df: pd.DataFrame, columns: List[str], limit: int = 8) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    available_columns = [col for col in columns if col in df.columns]
    if not available_columns:
        return []

    records: List[Dict[str, Any]] = []
    for row in df[available_columns].head(limit).to_dict(orient="records"):
        cleaned_row: Dict[str, Any] = {}
        for key, value in row.items():
            if pd.isna(value):
                continue
            if isinstance(value, float):
                cleaned_row[key] = round(value, 4)
            else:
                cleaned_row[key] = value
        if cleaned_row:
            records.append(cleaned_row)
    return records


def _build_brand_side_summary(presence_df: pd.DataFrame) -> Dict[str, Any]:
    empty_summary = {
        "sinodis": {
            "brands": 0,
            "mentions": 0,
            "avg_position": 0.0,
            "top_brands": [],
        },
        "competitor": {
            "brands": 0,
            "mentions": 0,
            "avg_position": 0.0,
            "top_brands": [],
        },
    }

    if presence_df is None or presence_df.empty or "sinodis_flag" not in presence_df.columns:
        return empty_summary

    df = presence_df.copy()
    if "brand_display" not in df.columns:
        if "entity_name_display" in df.columns:
            df["brand_display"] = df["entity_name_display"]
        elif "entity_name_en" in df.columns:
            df["brand_display"] = df["entity_name_en"]
        elif "entity_name_cn" in df.columns:
            df["brand_display"] = df["entity_name_cn"]
        else:
            df["brand_display"] = ""

    df["sinodis_flag"] = df["sinodis_flag"].fillna("N").astype(str).str.strip().str.upper()
    df["brand_display"] = df["brand_display"].fillna("").astype(str).str.strip()

    result = empty_summary.copy()
    mapping = {"Y": "sinodis", "N": "competitor"}

    for flag, side_name in mapping.items():
        side_df = df[df["sinodis_flag"] == flag].copy()
        if side_df.empty:
            continue

        avg_position = 0.0
        if "position" in side_df.columns:
            avg_position = round(pd.to_numeric(side_df["position"], errors="coerce").dropna().mean() or 0.0, 2)

        top_brands_df = (
            side_df[side_df["brand_display"] != ""]
            .groupby("brand_display", as_index=False)
            .agg(
                **{
                    "mentions": ("brand_display", "count"),
                    "avg_position": ("position", "mean"),
                }
            )
            .sort_values(by=["mentions", "avg_position"], ascending=[False, True])
            .head(5)
            .rename(columns={"brand_display": "brand"})
        )

        if not top_brands_df.empty and "avg_position" in top_brands_df.columns:
            top_brands_df["avg_position"] = top_brands_df["avg_position"].round(2)

        result[side_name] = {
            "brands": int(side_df.loc[side_df["brand_display"] != "", "brand_display"].nunique()),
            "mentions": int(len(side_df)),
            "avg_position": avg_position,
            "top_brands": top_brands_df.to_dict(orient="records") if not top_brands_df.empty else [],
        }

    return result


def build_ai_insight_prompt(
    filters: Dict[str, Any],
    payload: Dict[str, Any],
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame,
    queries_df: pd.DataFrame,
    content_publish_df: pd.DataFrame,
) -> str:
    kpis = payload.get("kpis", {}) if isinstance(payload, dict) else {}
    brand_ranking_table = payload.get("brand_ranking_table", pd.DataFrame())
    channel_ranking_table = payload.get("channel_ranking_table", pd.DataFrame())
    source_occurrence_table = payload.get("source_occurrence_table", pd.DataFrame())
    quoted_source_table = payload.get("quoted_source_table", pd.DataFrame())
    category_visibility_table = payload.get("brand_visibility_by_category_table", pd.DataFrame())
    record_month_visibility_table = payload.get("brand_visibility_by_record_month_table", pd.DataFrame())
    brand_side_summary = _build_brand_side_summary(presence_df)

    context = {
        "filters": filters,
        "dataset_size": {
            "queries": len(queries_df) if queries_df is not None else 0,
            "presence_records": len(presence_df) if presence_df is not None else 0,
            "source_records": len(source_df) if source_df is not None else 0,
            "content_publish_records": len(content_publish_df) if content_publish_df is not None else 0,
        },
        "kpis": {
            "total_query": _safe_int(kpis.get("Total Query")),
            "source_occurrence": _safe_int(kpis.get("source occurance")),
            "quote_rate": round(_safe_float(kpis.get("Quote Rate")), 4),
        },
        "brand_side_summary": brand_side_summary,
        "top_brand_ranking": _compact_records(
            brand_ranking_table,
            ["Brand", "Brand Mention", "Avg Position", "Visibility Score"],
            limit=8,
        ),
        "top_channel_ranking": _compact_records(
            channel_ranking_table,
            ["Channel", "Brand Mention", "Avg Position"],
            limit=8,
        ),
        "top_sources": _compact_records(
            source_occurrence_table,
            ["Source", "source occurance"],
            limit=8,
        ),
        "top_quoted_sources": _compact_records(
            quoted_source_table,
            ["Source", "Quoted Source number"],
            limit=8,
        ),
        "category_visibility": _compact_records(
            category_visibility_table,
            ["Brand", "Covered Queries", "Coverage Rate", "Avg Best Position", "Visibility Score"],
            limit=8,
        ),
        "record_month_visibility": _compact_records(
            record_month_visibility_table,
            ["Record Month", "Brand", "Visibility Score"],
            limit=12,
        ),
    }

    return (
        "You are a sharp GEO dashboard analyst. Based on the filtered dashboard snapshot below, "
        "write a concise business insight summary in English.\n\n"
        "Requirements:\n"
        "1. Output markdown only.\n"
        "2. Start with a short overview paragraph.\n"
        "3. Then provide exactly 3 bullet points: Sinodis vs competitor performance, source pattern, and recommended action.\n"
        "4. Every point must be grounded in the provided data only.\n"
        "5. If filtered data is too limited, say so explicitly and avoid over-claiming.\n"
        "6. Explicitly compare Sinodis-owned brands versus competitor brands whenever brand_side_summary is available.\n"
        "7. If the current filters already isolate one side only, mention that the comparison is filtered and therefore partial.\n"
        "8. Keep the total response under 180 words.\n\n"
        f"Dashboard snapshot:\n{json.dumps(context, ensure_ascii=False, indent=2)}"
    )


def request_deepseek_insight(
    prompt: str,
    api_key: str = "",
    base_url: str = "",
    model: str = "",
) -> str:
    api_key = _clean_text(api_key) or _clean_text(os.getenv("DEEPSEEK_API_KEY"))
    if not api_key:
        raise RuntimeError("Missing DEEPSEEK_API_KEY")

    base_url = _clean_text(base_url) or _clean_text(os.getenv("DEEPSEEK_BASE_URL")) or DEFAULT_DEEPSEEK_BASE_URL
    model = _clean_text(model) or _clean_text(os.getenv("DEEPSEEK_MODEL")) or DEFAULT_DEEPSEEK_MODEL
    endpoint = f"{base_url.rstrip('/')}/chat/completions"

    payload = {
        "model": model,
        "temperature": 0.3,
        "stream": False,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an analytics copilot for a GEO performance dashboard. "
                    "Be precise, concise, and data-grounded."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }

    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=60) as response:
            body = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"DeepSeek API error: {exc.code} {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"DeepSeek network error: {exc.reason}") from exc

    choices = body.get("choices") or []
    if not choices:
        raise RuntimeError("DeepSeek returned no choices")

    message = choices[0].get("message") or {}
    content = _clean_text(message.get("content"))
    if not content:
        raise RuntimeError("DeepSeek returned empty content")

    return content
