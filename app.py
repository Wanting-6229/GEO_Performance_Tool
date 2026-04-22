import os
import html
import base64
import time
import hashlib
import pandas as pd
import streamlit as st
import utils.db as db_module

from utils.db import (
    create_tables,
    create_project,
    rename_project,
    delete_project_cascade,
    list_projects,
    get_project,
    get_project_by_name,
    get_all_content_publish,
    split_publish_months,
    get_app_setting,
    set_app_setting,
    delete_app_setting,
)
from utils.loader import (
    load_dashboard_ready_data,
    apply_common_filters,
)
from utils.forms import (
    render_data_entry_page,
    render_data_record_page,
)
from utils.ai_insights import (
    build_ai_insight_prompt,
    request_deepseek_insight,
)
from utils.charts import (
    build_dashboard_payload,
    build_brand_ranking_table,
    build_brand_visibility_by_category_table,
    build_brand_visibility_by_category_and_publish_month_table,
    build_brand_visibility_by_publish_month_table,
    build_brand_visibility_by_record_month_table,
    build_brand_ranking_chart_from_table,
    build_channel_ranking_chart_from_table,
    build_source_occurrence_chart_from_table,
    build_quoted_source_chart_from_table,
    build_source_distribution_by_platform_chart_from_table,
    build_brand_visibility_by_category_chart_from_table,
    build_brand_visibility_by_publish_month_chart_from_table,
    build_brand_visibility_by_record_month_chart_from_table,
)


# =========================================================
# Page Config
# =========================================================
st.set_page_config(
    page_title="GEO Performance Analysis",
    layout="wide"
)


# =========================================================
# Initial Setup
# =========================================================
def _log_perf(label: str, start_time: float, **metrics):
    elapsed = time.perf_counter() - start_time
    suffix = " ".join(f"{key}={value}" for key, value in metrics.items() if value is not None)
    message = f"[perf] {label} elapsed={elapsed:.3f}s"
    if suffix:
        message = f"{message} {suffix}"
    print(message, flush=True)


@st.cache_resource(show_spinner=False)
def initialize_database_once():
    start = time.perf_counter()
    create_tables()
    _log_perf("database initialization", start, backend=db_module.get_db_backend())
    getattr(db_module, "log_final_backend_selection", lambda: None)()
    return True


initialize_database_once()

if "page" not in st.session_state:
    st.session_state.page = "Projects"

if "current_project_id" not in st.session_state:
    st.session_state.current_project_id = None

if "current_project_name" not in st.session_state:
    st.session_state.current_project_name = ""

if "show_create_project_panel" not in st.session_state:
    st.session_state.show_create_project_panel = False

if "show_rename_project_panel" not in st.session_state:
    st.session_state.show_rename_project_panel = False

if "show_delete_project_panel" not in st.session_state:
    st.session_state.show_delete_project_panel = False

if "project_dialog_target" not in st.session_state:
    st.session_state.project_dialog_target = None

if "data_record_section" not in st.session_state:
    st.session_state.data_record_section = "Query Master"

if "raw_data_section" not in st.session_state:
    st.session_state.raw_data_section = "Presence Master Table"

if "entry_section" not in st.session_state:
    st.session_state.entry_section = "Manual Entry"


# =========================================================
# Constants
# =========================================================
WORKSPACE_NAV_OPTIONS = ["Dashboard", "Data Entry", "Data Record"]

MONTHS = [
    "2026-02", "2026-03", "2026-04", "2026-05",
    "2026-06", "2026-07", "2026-08", "2026-09",
    "2026-10", "2026-11", "2026-12",
]


# =========================================================
# Cached Loaders
# =========================================================
@st.cache_data(show_spinner=False)
def cached_dashboard_data(
    project_id: int,
    query_status_filter: str = "active_only",
    project_updated_at: str = "",
):
    return load_dashboard_ready_data(
        project_id=project_id,
        query_status_filter=query_status_filter,
    )


def clear_all_caches():
    cached_dashboard_data.clear()


def render_api_key_settings_panel():
    stored_api_key = get_app_setting("deepseek_api_key", "")
    input_key = "settings_deepseek_api_key"

    if input_key not in st.session_state:
        st.session_state[input_key] = stored_api_key

    chart_card_start("API Key Settings")
    st.caption("Configure your DeepSeek API key once. It will be saved locally and reused for future AI Insight requests.")

    api_col, action_col, clear_col = st.columns([5.2, 1.2, 1.2], vertical_alignment="bottom")

    with api_col:
        st.text_input(
            "DeepSeek API Key",
            key=input_key,
            type="password",
            placeholder="Paste your DeepSeek API key here",
        )

    with action_col:
        if st.button("Save Key", use_container_width=True, key="save_deepseek_api_key"):
            api_key_value = st.session_state.get(input_key, "").strip()
            if not api_key_value:
                st.error("Please enter a DeepSeek API key before saving.")
            else:
                set_app_setting("deepseek_api_key", api_key_value)
                st.success("DeepSeek API key saved.")

    with clear_col:
        if st.button("Clear Key", use_container_width=True, key="clear_deepseek_api_key"):
            delete_app_setting("deepseek_api_key")
            st.session_state[input_key] = ""
            st.success("Saved DeepSeek API key cleared.")
            st.rerun()

    if stored_api_key:
        masked_key = f"{stored_api_key[:6]}...{stored_api_key[-4:]}" if len(stored_api_key) > 10 else "Saved"
        st.caption(f"Saved key detected: {masked_key}")
    else:
        st.caption("No DeepSeek API key saved yet.")

    chart_card_end()


def _build_dashboard_filter_snapshot(
    query_status_filter: str,
    selected_query_type: str,
    selected_record_month: str,
    selected_category: str,
    selected_platform: str,
    selected_publish_month: str,
    selected_query_numbers,
    selected_creators,
    selected_sinodis_brand: str,
    selected_check_date_range,
):
    date_range = []
    if selected_check_date_range and len(selected_check_date_range) == 2:
        date_range = [str(selected_check_date_range[0]), str(selected_check_date_range[1])]

    return {
        "query_status": query_status_filter,
        "query_type": selected_query_type,
        "record_month": selected_record_month,
        "product_category": selected_category,
        "ai_platform": selected_platform,
        "publish_month": selected_publish_month,
        "query_numbers": list(selected_query_numbers or []),
        "created_by": list(selected_creators or []),
        "sinodis_brand": selected_sinodis_brand,
        "check_date_range": date_range,
    }


def render_ai_insight_panel(
    project_id: int,
    filters: dict,
    payload: dict,
    queries_df: pd.DataFrame,
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame,
    content_publish_df: pd.DataFrame,
):
    chart_card_start("AI Insight")

    saved_api_key = get_app_setting("deepseek_api_key", "")
    if not saved_api_key and not os.getenv("DEEPSEEK_API_KEY", "").strip():
        st.info(
            "Please save your DeepSeek API key on the home page first, then come back to generate AI insights."
        )
        chart_card_end()
        return

    filter_signature = hashlib.md5(
        (
            f"{project_id}|"
            f"{filters}|"
            f"{len(queries_df)}|{len(presence_df)}|{len(source_df)}|{len(content_publish_df)}"
        ).encode("utf-8")
    ).hexdigest()
    result_key = f"ai_insight_result_{project_id}"
    signature_key = f"ai_insight_signature_{project_id}"
    error_key = f"ai_insight_error_{project_id}"

    action_col, meta_col = st.columns([1.2, 3.8])
    with action_col:
        generate_clicked = st.button(
            "Generate Insight",
            use_container_width=True,
            key=f"generate_ai_insight_{project_id}",
        )
    with meta_col:
        st.caption(
            "Uses the currently selected filters and visible dashboard aggregates to produce a short summary."
        )

    if generate_clicked:
        try:
            prompt = build_ai_insight_prompt(
                filters=filters,
                payload=payload,
                presence_df=presence_df,
                source_df=source_df,
                queries_df=queries_df,
                content_publish_df=content_publish_df,
            )
            with st.spinner("Generating AI insight..."):
                insight = request_deepseek_insight(prompt, api_key=saved_api_key)
            st.session_state[result_key] = insight
            st.session_state[signature_key] = filter_signature
            st.session_state.pop(error_key, None)
        except Exception as exc:
            st.session_state[error_key] = str(exc)
            st.session_state.pop(result_key, None)
            st.session_state.pop(signature_key, None)

    if st.session_state.get(error_key):
        st.error(st.session_state[error_key])

    if st.session_state.get(signature_key) == filter_signature and st.session_state.get(result_key):
        st.markdown(st.session_state[result_key])
    else:
        st.caption("No AI summary generated for the current filter set yet.")

    chart_card_end()


# =========================================================
# UI / CSS Helpers
# =========================================================
def inject_dashboard_css():
    st.markdown(
        """
        <style>
        :root {
            --bg: #f8f9fc;
            --panel: #ffffff;
            --border: #eef2f6;
            --text: #2d2d2d;
            --muted: #6c7a89;
            --accent: #2e1452;
            --accent-hover: #1f0e38;
            --secondary: #2bd6d9;
            --secondary-hover: #23b8bb;
            --error-bg: #fef0ed;
            --error-text: #e74c3c;
        }

        .stApp {
            background: linear-gradient(180deg, #f8f9fc 0%, #f0f2f5 100%);
        }

        .main .block-container {
            padding-top: 1.45rem;
            padding-bottom: 2.2rem;
            max-width: 1400px;
        }

        .nav-shell {
            margin-bottom: 1rem;
        }

        .hero-wrap {
            position: relative;
            padding: 1.2rem 1.25rem 1.15rem 1.25rem;
            border-radius: 16px;
            background:
                radial-gradient(circle at top right, rgba(125, 211, 252, 0.20), transparent 28%),
                linear-gradient(135deg, #fffef8 0%, #f8fbff 48%, #f5fffb 100%);
            border: 1px solid rgba(203, 213, 225, 0.85);
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.06);
            margin-bottom: 1rem;
        }

        .hero-title {
            font-size: 1.78rem;
            font-weight: 800;
            color: var(--text);
            margin: 0;
            line-height: 1.15;
        }

        .hero-subtitle {
            margin-top: 0.4rem;
            font-size: 0.92rem;
            color: var(--muted);
        }

        .section-title {
            font-size: 1.08rem;
            font-weight: 800;
            margin: 0;
            color: var(--text);
        }

        .section-desc {
            margin-top: 0.24rem;
            color: var(--muted);
            font-size: 0.86rem;
        }

        .section-wrap {
            margin-top: 0.55rem;
            margin-bottom: 0.8rem;
            padding: 0.2rem 0 0.25rem 0;
        }

        .dashboard-section {
            padding: 0;
            margin: 0 0 1.15rem 0;
        }

        .dashboard-section + .dashboard-section {
            border-top: 1px solid var(--border);
            padding-top: 1rem;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) {
            background: #ffffff;
            border: 1px solid #e6ebf2;
            border-radius: 18px;
            padding: 1.05rem 1.05rem 0.8rem 1.05rem;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
            margin-bottom: 1rem;
        }

        .dashboard-filters-marker {
            height: 0;
            overflow: hidden;
        }

        .dashboard-filters-title {
            margin: 0 0 0.95rem 0;
            font-size: 0.96rem;
            font-weight: 800;
            color: var(--accent);
            padding-bottom: 0.7rem;
            border-bottom: 1px solid #edf1f6;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) label,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) .stMarkdown,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-testid="stWidgetLabel"] p {
            color: var(--accent) !important;
            font-weight: 800 !important;
            font-size: 0.8rem !important;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) [data-testid="column"] {
            background: #ffffff;
            border: 1px solid #edf1f6;
            border-radius: 14px;
            padding: 0.8rem 0.85rem 0.35rem 0.85rem;
            box-shadow: 0 2px 8px rgba(15, 23, 42, 0.03);
            min-height: 104px;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="select"] > div,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="input"] > div,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="popover"] > div,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-testid="stDateInput"] [data-baseweb="input"] {
            background: #ffffff !important;
            border-color: #dfe6ef !important;
            border-radius: 12px !important;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04);
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="select"] > div:hover,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="input"] > div:hover,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-testid="stDateInput"] [data-baseweb="input"]:hover {
            border-color: var(--accent) !important;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="select"] > div:focus-within,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="input"] > div:focus-within,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-testid="stDateInput"] [data-baseweb="input"]:focus-within {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 3px rgba(43, 214, 217, 0.14) !important;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) input,
        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) textarea {
            color: var(--text) !important;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="tag"] {
            background: var(--secondary) !important;
            border: 1px solid var(--border) !important;
            color: var(--text) !important;
        }

        div[data-testid="stVerticalBlock"]:has(.dashboard-filters-marker) div[data-baseweb="tag"] span {
            color: var(--text) !important;
            font-weight: 700 !important;
        }

        .kpi-card {
            position: relative;
            overflow: hidden;
            background: linear-gradient(180deg, var(--panel) 0%, #f8f6fc 100%);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.15rem 1.1rem 1.05rem 1.1rem;
            box-shadow: 0 8px 24px rgba(46, 20, 82, 0.06);
            min-height: 122px;
        }

        .kpi-label {
            color: var(--muted);
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.58rem;
        }

        .kpi-value {
            color: var(--text);
            font-size: 2.05rem;
            font-weight: 800;
            line-height: 1.02;
            letter-spacing: -0.02em;
        }

        .chart-card {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.08rem 1.15rem 0.95rem 1.15rem;
            box-shadow: 0 8px 24px rgba(46, 20, 82, 0.06);
            margin-bottom: 1.05rem;
        }

        .chart-card-title {
            font-size: 0.94rem;
            font-weight: 800;
            color: var(--text);
            margin-bottom: 0.8rem;
            padding-bottom: 0.68rem;
            border-bottom: 1px solid var(--border);
        }

        .subsection-kicker {
            margin: 0.15rem 0 0.7rem 0.1rem;
            color: #5d6f85;
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }

        .metric-table-shell {
            border: 1px solid rgba(46, 20, 82, 0.10);
            border-radius: 14px;
            overflow: hidden;
            background: linear-gradient(180deg, #ffffff 0%, #fcfbff 100%);
        }

        .metric-table-header {
            display: grid;
            grid-template-columns: minmax(0, 1.2fr) 180px;
            gap: 0;
            background: linear-gradient(180deg, #fffdfa 0%, #fff 100%);
            border-bottom: 1px solid rgba(46, 20, 82, 0.08);
        }

        .metric-table-head-cell {
            padding: 0.9rem 1rem 0.82rem 1rem;
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            color: #68798f;
            position: relative;
        }

        .metric-table-head-cell::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #b9d8ff 0%, #63b3ff 100%);
        }

        .metric-table-head-cell.metric-table-head-value::before {
            background: linear-gradient(90deg, #ffe8b5 0%, #ffb44c 100%);
        }

        .metric-table-body {
            overflow-y: auto;
        }

        .metric-table-row {
            display: grid;
            grid-template-columns: minmax(0, 1.2fr) 180px;
            gap: 0;
            align-items: center;
            background: rgba(255, 255, 255, 0.96);
            border-bottom: 1px dashed rgba(46, 20, 82, 0.10);
        }

        .metric-table-row:last-child {
            border-bottom: none;
        }

        .metric-table-cell {
            padding: 0.95rem 1rem;
            color: #2d2d2d;
            font-size: 0.9rem;
            line-height: 1.35;
        }

        .metric-table-value {
            text-align: right;
            font-weight: 800;
            color: #d66a00;
        }

        .ranking-card {
            background: linear-gradient(180deg, #ffffff 0%, #faf7ff 100%);
            border: 1px solid rgba(46, 20, 82, 0.14);
            border-radius: 16px;
            padding: 1.08rem 1.15rem 1rem 1.15rem;
            box-shadow: 0 10px 28px rgba(46, 20, 82, 0.08);
            margin-bottom: 1.05rem;
        }

        .ranking-card .chart-card-title {
            color: #2e1452;
            border-bottom-color: rgba(46, 20, 82, 0.12);
        }

        .ranking-card-brand .chart-card-title {
            color: #1f5874;
        }

        .ranking-card-brand .ranking-row.top3 {
            background: linear-gradient(135deg, rgba(178, 233, 238, 0.58), rgba(236, 252, 251, 0.96));
            border-color: rgba(57, 197, 211, 0.58);
        }

        .ranking-card-brand .ranking-score {
            background: rgba(116, 225, 232, 0.18);
            color: #167e8f;
        }

        .ranking-card-channel .chart-card-title {
            color: #31559b;
        }

        .ranking-card-channel .ranking-row.top3 {
            background: linear-gradient(135deg, rgba(191, 221, 255, 0.58), rgba(255, 247, 204, 0.92));
            border-color: rgba(118, 171, 255, 0.58);
        }

        .ranking-card-channel .ranking-score {
            background: rgba(255, 212, 92, 0.20);
            color: #9f6a00;
        }

        .ranking-card-source-common {
            min-height: 55.4rem;
        }

        .ranking-card-source-common .chart-card-title {
            color: #244a73;
        }

        .ranking-card-source-common .ranking-row,
        .ranking-card-source-doubao .ranking-row,
        .ranking-card-source-deepseek .ranking-row {
            padding: 0.6rem 0.72rem;
            border-radius: 10px;
        }

        .ranking-card-source-common .ranking-left,
        .ranking-card-source-doubao .ranking-left,
        .ranking-card-source-deepseek .ranking-left {
            gap: 0.58rem;
        }

        .ranking-card-source-common .ranking-rank,
        .ranking-card-source-doubao .ranking-rank,
        .ranking-card-source-deepseek .ranking-rank {
            min-width: 1.75rem;
            font-size: 0.86rem;
        }

        .ranking-card-source-common .ranking-brand,
        .ranking-card-source-doubao .ranking-brand,
        .ranking-card-source-deepseek .ranking-brand {
            font-size: 0.8rem;
            line-height: 1.25;
        }

        .ranking-card-source-doubao .ranking-score,
        .ranking-card-source-deepseek .ranking-score {
            padding: 0.28rem 0.62rem;
            font-size: 0.74rem;
        }

        .ranking-card-source-doubao {
            min-height: 26.6rem;
            background: linear-gradient(180deg, #fff4ea 0%, #fffaf6 100%);
            border-color: rgba(222, 112, 28, 0.24);
            box-shadow: 0 12px 28px rgba(222, 112, 28, 0.10);
        }

        .ranking-card-source-doubao .chart-card-title {
            color: #d66700;
            border-bottom-color: rgba(222, 112, 28, 0.16);
        }

        .ranking-card-source-doubao .ranking-header {
            border-bottom-color: rgba(222, 112, 28, 0.12);
        }

        .ranking-card-source-doubao .ranking-row.top3 {
            background: linear-gradient(135deg, rgba(255, 208, 173, 0.65), rgba(255, 244, 234, 0.98));
            border-color: rgba(222, 112, 28, 0.42);
        }

        .ranking-card-source-doubao .ranking-score {
            background: rgba(222, 112, 28, 0.12);
            color: #c85a00;
        }

        .ranking-card-source-deepseek {
            min-height: 26.6rem;
            background: linear-gradient(180deg, #eef6ff 0%, #f8fbff 100%);
            border-color: rgba(53, 116, 204, 0.24);
            box-shadow: 0 12px 28px rgba(53, 116, 204, 0.10);
        }

        .ranking-card-source-deepseek .chart-card-title {
            color: #2f66d2;
            border-bottom-color: rgba(53, 116, 204, 0.16);
        }

        .ranking-card-source-deepseek .ranking-header {
            border-bottom-color: rgba(53, 116, 204, 0.12);
        }

        .ranking-card-source-deepseek .ranking-row.top3 {
            background: linear-gradient(135deg, rgba(187, 220, 255, 0.72), rgba(248, 251, 255, 0.98));
            border-color: rgba(53, 116, 204, 0.40);
        }

        .ranking-card-source-deepseek .ranking-score {
            background: rgba(53, 116, 204, 0.12);
            color: #2f66d2;
        }

        .ranking-list {
            display: flex;
            flex-direction: column;
            gap: 0.7rem;
        }

        .ranking-grid {
            display: grid;
            grid-template-columns: repeat(var(--ranking-grid-columns, 2), minmax(0, 1fr));
            column-gap: 0.85rem;
            row-gap: 0.7rem;
        }

        .ranking-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.9rem;
            padding: 0 0.9rem 0.7rem 0.9rem;
            margin-bottom: 0.2rem;
            border-bottom: 1px solid rgba(46, 20, 82, 0.12);
        }

        .ranking-header-left {
            padding-left: 3rem;
            color: var(--muted);
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }

        .ranking-header-right {
            color: var(--muted);
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            text-align: right;
            white-space: nowrap;
        }

        .ranking-list-scroll {
            max-height: 25.5rem;
            overflow-y: auto;
            padding-right: 0.25rem;
            scroll-snap-type: y proximity;
        }

        .ranking-list-scroll::-webkit-scrollbar {
            width: 8px;
        }

        .ranking-list-scroll::-webkit-scrollbar-track {
            background: rgba(46, 20, 82, 0.06);
            border-radius: 999px;
        }

        .ranking-list-scroll::-webkit-scrollbar-thumb {
            background: rgba(43, 214, 217, 0.65);
            border-radius: 999px;
        }

        .ranking-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.9rem;
            padding: 0.82rem 0.9rem;
            border-radius: 12px;
            background: linear-gradient(135deg, rgba(248, 246, 252, 0.98), rgba(255, 255, 255, 0.98));
            border: 1px solid rgba(46, 20, 82, 0.10);
            scroll-snap-align: start;
        }

        .ranking-row.top3 {
            background: linear-gradient(135deg, rgba(46, 20, 82, 0.10), rgba(43, 214, 217, 0.12));
            border-color: rgba(43, 214, 217, 0.55);
        }

        .ranking-left {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            min-width: 0;
        }

        .ranking-rank {
            min-width: 2rem;
            color: #2e1452;
            font-size: 0.94rem;
            font-weight: 800;
            line-height: 1;
        }

        .ranking-brand {
            color: #2d2d2d;
            font-size: 0.9rem;
            font-weight: 700;
            line-height: 1.35;
            word-break: break-word;
        }

        .ranking-score {
            color: #23b8bb;
            font-size: 0.95rem;
            font-weight: 800;
            white-space: nowrap;
        }

        .ranking-score {
            flex-shrink: 0;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.34rem 0.72rem;
            border-radius: 999px;
            background: rgba(43, 214, 217, 0.14);
            color: var(--accent);
            font-size: 0.8rem;
            font-weight: 800;
            line-height: 1;
        }

        .ranking-row-empty {
            visibility: hidden;
            pointer-events: none;
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
            background: var(--panel);
        }

        div[data-testid="stPlotlyChart"] {
            border-radius: 12px;
            overflow: hidden;
        }

        div[data-testid="stExpander"] {
            border: 1px solid var(--border);
            border-radius: 14px;
            background: var(--panel);
            box-shadow: 0 6px 18px rgba(46, 20, 82, 0.04);
        }

        div[data-testid="stExpander"] summary {
            font-weight: 700;
            color: var(--text);
        }

        .project-toolbar-shell {
            margin: 0 0 0.85rem 0;
            padding: 0.1rem 0 0.2rem 0;
        }

        .project-card {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1rem 1rem 1rem 1rem;
            box-shadow: 0 8px 24px rgba(46, 20, 82, 0.04);
            min-height: 176px;
        }

        .project-card-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.75rem;
            margin-bottom: 0.8rem;
        }

        .project-card-title {
            color: var(--text);
            font-size: 1.08rem;
            font-weight: 800;
            line-height: 1.2;
            margin-bottom: 0;
            display: flex;
            align-items: center;
            min-height: 2.6rem;
        }

        .project-card-actions-label {
            min-height: 2.6rem;
            display: flex;
            align-items: center;
        }

        .project-card-meta {
            color: var(--muted);
            font-size: 0.92rem;
            margin-bottom: 0.3rem;
            line-height: 1.4;
        }

        .project-card-open {
            margin-top: 0.45rem;
        }

        div[data-testid="stTextInput"] > div[data-baseweb="input"] {
            min-height: 42px;
        }

        div[data-testid="stTextInput"] input {
            height: 42px;
        }

        div[data-testid="stButton"] {
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        div[data-testid="stButton"] > button {
            min-height: 46px;
            min-width: 46px;
            padding: 0.55rem 0.95rem;
            display: flex;
            align-items: center;
            justify-content: center;
            line-height: 1;
            border-radius: 999px;
            background: var(--accent) !important;
            color: #fff !important;
            border: none !important;
        }

        div[data-testid="stButton"] > button:hover {
            background: var(--accent-hover) !important;
        }

        div[data-testid="stButton"] > button[kind="secondary"] {
            background: var(--secondary) !important;
            color: var(--text) !important;
        }

        div[data-testid="stButton"] > button[kind="secondary"]:hover {
            background: var(--secondary-hover) !important;
        }

        div[data-testid="stButton"] > button > div {
            width: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        div[data-testid="stButton"] > button p {
            margin: 0;
            line-height: 1;
        }

        .project-grid-spacer {
            height: 0.55rem;
        }

        .avatar-container {
            position: fixed;
            top: 78px;
            left: 22px;
            z-index: 9999;
        }

        .avatar-badge {
            position: relative;
            display: inline-block;
        }

        .avatar-container img {
            width: 74px;
            height: 74px;
            object-fit: cover;
            object-position: center;
            border-radius: 50%;
            border: 3px solid var(--panel);
            box-shadow: 0 10px 28px rgba(46, 20, 82, 0.12);
            background: var(--panel);
            transition: transform 0.18s ease;
            display: block;
        }

        .avatar-container img:hover {
            transform: scale(1.06);
        }

        .avatar-tooltip {
            position: absolute;
            left: 86px;
            top: 50%;
            transform: translateY(-50%);
            background: var(--text);
            color: var(--panel);
            font-size: 14px;
            font-weight: 700;
            padding: 8px 12px;
            border-radius: 10px;
            white-space: nowrap;
            box-shadow: 0 8px 20px rgba(46, 20, 82, 0.18);
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.18s ease;
        }

        .avatar-badge:hover .avatar-tooltip {
            opacity: 1;
        }

        div[data-testid="stSegmentedControl"] {
            background: transparent !important;
        }

        div[data-testid="stSegmentedControl"] button {
            font-weight: 700 !important;
            background: var(--secondary) !important;
            color: var(--text) !important;
        }

        div[data-testid="stSegmentedControl"] button[aria-selected="true"] {
            background: var(--accent) !important;
            color: #fff !important;
        }

        .stAlert {
            background: var(--error-bg) !important;
            color: var(--error-text) !important;
            border: 1px solid var(--border) !important;
        }

        hr {
            border-color: var(--border) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero(title: str, subtitle: str):
    st.markdown(
        f"""
        <div class="hero-wrap">
            <div class="hero-title">{title}</div>
            <div class="hero-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_card(label: str, value: str):
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_header(title: str, desc: str = ""):
    st.markdown(
        f"""
        <div class="section-wrap">
            <div class="section-title">{title}</div>
            <div class="section-desc">{desc}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def chart_card_start(title: str):
    st.markdown(
        f"""
        <div class="chart-card">
            <div class="chart-card-title">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def chart_card_end():
    st.markdown("</div>", unsafe_allow_html=True)


def ranking_card_start(title: str):
    st.markdown(
        f"""
        <div class="ranking-card">
            <div class="chart-card-title">{html.escape(title)}</div>
        """,
        unsafe_allow_html=True,
    )


def ranking_card_end():
    st.markdown("</div>", unsafe_allow_html=True)


def section_card_start():
    st.markdown('<div class="dashboard-section">', unsafe_allow_html=True)


def section_card_end():
    st.markdown("</div>", unsafe_allow_html=True)


def render_ranking_list(
    table_df: pd.DataFrame,
    title: str,
    name_column: str,
    name_label: str,
    value_column: str,
    value_label: str,
    scrollable: bool = False,
    scroll_height_px: int | None = None,
    two_column_top20: bool = False,
    grid_columns: int | None = None,
    items_per_column: int | None = None,
    card_class: str = "",
):
    if table_df.empty:
        st.info("No data available")
        return

    if name_column not in table_df.columns or value_column not in table_df.columns:
        st.info("No data available")
        return

    top_rows = table_df.head(20).copy() if two_column_top20 else table_df.copy()
    medal_map = {1: "🥇", 2: "🥈", 3: "🥉"}
    row_html = []

    for rank, (_, row) in enumerate(top_rows.iterrows(), start=1):
        brand = html.escape(str(row.get(name_column, "")))
        score_value = row.get(value_column)
        if pd.isna(score_value):
            score_text = "-"
        else:
            score_text = html.escape(str(score_value).strip())

        show_score = value_label.strip() != "" and score_text not in {"", "-"}

        rank_label = medal_map.get(rank, str(rank))
        row_class = "ranking-row top3" if rank <= 3 else "ranking-row"
        score_html = f'<div class="ranking-score">{score_text}</div>' if show_score else ""
        row_html.append(
            (
                f'<div class="{row_class}">'
                f'<div class="ranking-left">'
                f'<div class="ranking-rank">{rank_label}</div>'
                f'<div class="ranking-brand">{brand}</div>'
                f'</div>'
                f'{score_html}'
                f'</div>'
            )
        )

    if grid_columns and items_per_column:
        total_slots = grid_columns * items_per_column
        row_html = row_html[:total_slots]
        while len(row_html) < total_slots:
            row_html.append(
                (
                    '<div class="ranking-row ranking-row-empty">'
                    '<div class="ranking-left">'
                    '<div class="ranking-rank">0</div>'
                    f'<div class="ranking-brand">{html.escape(name_label)}</div>'
                    '</div>'
                    '<div class="ranking-score">0</div>'
                    '</div>'
                )
            )
        column_html = []
        for column_index in range(grid_columns):
            start = column_index * items_per_column
            end = start + items_per_column
            column_html.append(f'<div class="ranking-list">{"".join(row_html[start:end])}</div>')
        list_html = (
            f'<div class="ranking-grid" style="--ranking-grid-columns: {grid_columns};">'
            f'{"".join(column_html)}'
            '</div>'
        )
    elif two_column_top20:
        while len(row_html) < 20:
            row_html.append(
                (
                    '<div class="ranking-row ranking-row-empty">'
                    '<div class="ranking-left">'
                    '<div class="ranking-rank">0</div>'
                    f'<div class="ranking-brand">{html.escape(name_label)}</div>'
                    '</div>'
                    '<div class="ranking-score">0</div>'
                    '</div>'
                )
            )
        list_html = (
            '<div class="ranking-grid" style="--ranking-grid-columns: 2;">'
            f'<div class="ranking-list">{"".join(row_html[:10])}</div>'
            f'<div class="ranking-list">{"".join(row_html[10:20])}</div>'
            '</div>'
        )
    else:
        list_class = "ranking-list ranking-list-scroll" if scrollable else "ranking-list"
        list_style = f' style="max-height: {scroll_height_px}px;"' if scrollable and scroll_height_px else ""
        list_html = f'<div class="{list_class}"{list_style}>{"".join(row_html)}</div>'

    header_html = (
        '<div class="ranking-header">'
        f'<div class="ranking-header-left">{html.escape(name_label)}</div>'
        f'<div class="ranking-header-right">{html.escape(value_label)}</div>'
        '</div>'
    )

    st.markdown(
        (
            f'<div class="ranking-card {html.escape(card_class).strip()}">'
            f'<div class="chart-card-title">{html.escape(title)}</div>'
            f'{header_html}'
            f'{list_html}'
            f'</div>'
        ),
        unsafe_allow_html=True,
    )


def render_visibility_ranking_list(table_df: pd.DataFrame, title: str):
    render_ranking_list(
        table_df,
        title,
        "Brand",
        "Brand Name",
        "Visibility Score",
        "Visibility Score",
        grid_columns=3,
        items_per_column=5,
    )


def render_presence_ranking_list(table_df: pd.DataFrame, title: str):
    render_ranking_list(
        table_df,
        title,
        "Brand",
        "Brand Name",
        "Brand Mention",
        "Presence Volume",
        grid_columns=3,
        items_per_column=5,
        card_class="ranking-card-brand",
    )


def render_channel_ranking_list(table_df: pd.DataFrame, title: str):
    render_ranking_list(
        table_df,
        title,
        "Channel",
        "Channel Name",
        "Channel Mention",
        "Channel Mention",
        grid_columns=3,
        items_per_column=5,
        card_class="ranking-card-channel",
    )


def render_source_ranking_list(
    table_df: pd.DataFrame,
    title: str,
    value_column: str,
    value_label: str,
    grid_columns: int | None = None,
    items_per_column: int | None = None,
    scrollable: bool = False,
    scroll_height_px: int | None = None,
    card_class: str = "",
):
    render_ranking_list(
        table_df,
        title,
        "Source",
        "Source Name",
        value_column,
        value_label,
        scrollable=scrollable,
        scroll_height_px=scroll_height_px,
        grid_columns=grid_columns,
        items_per_column=items_per_column,
        card_class=card_class,
    )


def render_source_name_list(
    table_df: pd.DataFrame,
    title: str,
    grid_columns: int = 2,
    items_per_column: int = 10,
    card_class: str = "",
):
    if table_df is None or table_df.empty or "Source" not in table_df.columns:
        st.info("No data available")
        return

    source_df = table_df[["Source"]].copy().head(grid_columns * items_per_column)
    source_df["Label"] = ""
    render_ranking_list(
        source_df,
        title,
        "Source",
        "Source Name",
        "Label",
        "",
        grid_columns=grid_columns,
        items_per_column=items_per_column,
        card_class=card_class,
    )


def render_metric_table_card(
    table_df: pd.DataFrame,
    title: str,
    name_column: str,
    name_label: str,
    value_column: str,
    value_label: str,
    height: int = 360,
):
    ranking_card_start(title)
    if table_df is None or table_df.empty or name_column not in table_df.columns or value_column not in table_df.columns:
        st.info("No data available")
        ranking_card_end()
        return

    display_df = table_df[[name_column, value_column]].copy()
    display_df.columns = [name_label, value_label]
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=height,
    )
    ranking_card_end()


def render_designed_metric_table_card(
    table_df: pd.DataFrame,
    title: str,
    name_column: str,
    name_label: str,
    value_column: str,
    value_label: str,
    height: int = 360,
):
    ranking_card_start(title)
    if table_df is None or table_df.empty or name_column not in table_df.columns or value_column not in table_df.columns:
        st.info("No data available")
        ranking_card_end()
        return

    body_rows = []
    for _, row in table_df.iterrows():
        name_text = html.escape(str(row.get(name_column, "")).strip())
        raw_value = row.get(value_column, "")
        if pd.isna(raw_value):
            value_text = "-"
        else:
            value_text = html.escape(str(raw_value).strip())

        body_rows.append(
            '<div class="metric-table-row">'
            f'<div class="metric-table-cell">{name_text}</div>'
            f'<div class="metric-table-cell metric-table-value">{value_text}</div>'
            '</div>'
        )

    st.markdown(
        (
            '<div class="metric-table-shell">'
            '<div class="metric-table-header">'
            f'<div class="metric-table-head-cell">{html.escape(name_label)}</div>'
            f'<div class="metric-table-head-cell metric-table-head-value">{html.escape(value_label)}</div>'
            '</div>'
            f'<div class="metric-table-body" style="max-height: {int(height)}px;">'
            f'{"".join(body_rows)}'
            '</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )
    ranking_card_end()


def _sync_workspace_page_from_nav():
    selected_page = st.session_state.get("workspace_page_nav")
    if selected_page in WORKSPACE_NAV_OPTIONS and selected_page != st.session_state.get("page"):
        st.session_state.page = selected_page


def render_top_nav():
    if not get_current_project():
        return

    st.markdown('<div class="nav-shell">', unsafe_allow_html=True)

    if st.session_state.page not in WORKSPACE_NAV_OPTIONS:
        st.session_state.page = "Dashboard"

    if st.session_state.get("workspace_page_nav") != st.session_state.page:
        st.session_state.workspace_page_nav = st.session_state.page

    st.segmented_control(
        "Navigation",
        options=WORKSPACE_NAV_OPTIONS,
        key="workspace_page_nav",
        on_change=_sync_workspace_page_from_nav,
        label_visibility="collapsed",
        selection_mode="single",
    )

    st.markdown("</div>", unsafe_allow_html=True)


def get_current_project():
    project_id = st.session_state.get("current_project_id")
    if project_id in (None, ""):
        return None

    project = get_project(int(project_id))
    if not project:
        clear_current_project()
        return None

    st.session_state.current_project_name = project["project_name"]
    return project


def set_current_project(project_id: int, project_name: str):
    st.session_state.current_project_id = int(project_id)
    st.session_state.current_project_name = project_name
    st.session_state.page = "Dashboard"
    st.session_state.project_enter_started_at = time.perf_counter()


def clear_current_project():
    st.session_state.current_project_id = None
    st.session_state.current_project_name = ""
    st.session_state.project_dialog_target = None
    st.session_state.show_rename_project_panel = False
    st.session_state.show_delete_project_panel = False
    st.session_state.page = "Projects"


def _set_project_dialog_target(project_row: dict | None):
    st.session_state.project_dialog_target = project_row


def _get_project_dialog_target():
    target = st.session_state.get("project_dialog_target")
    if not target:
        return None
    return target


def render_project_context_bar():
    project = get_current_project()
    if not project:
        return

    st.caption(f"Current Project: {project['project_name']}")


def render_workspace_back_nav():
    if not get_current_project():
        return

    if st.button("Back to Projects", key=f"back_to_projects_{st.session_state.page}"):
        clear_current_project()
        st.rerun()


def require_project_selection() -> bool:
    if get_current_project():
        return True

    st.info("Please select a project first from the Projects page.")
    if st.button("Go to Projects", key=f"go_projects_{st.session_state.page}"):
        clear_current_project()
        st.rerun()
    return False


def _validate_project_name_input(project_name: str, exclude_project_id: int | None = None) -> str:
    name = project_name.strip()
    if not name:
        raise ValueError("Project name cannot be empty.")

    existing = get_project_by_name(name)
    if existing and int(existing["project_id"]) != int(exclude_project_id or 0):
        raise ValueError("Project name must be unique.")

    return name


def _render_create_project_form():
    project_name = st.text_input("Project Name", placeholder="Enter project name").strip()
    c1, c2 = st.columns(2)

    with c1:
        if st.button("Create", type="primary", use_container_width=True, key="create_project_confirm"):
            try:
                validated_name = _validate_project_name_input(project_name)
                project_id = create_project(validated_name)
                project = get_project(project_id)
                if project:
                    st.session_state.show_create_project_panel = False
                    set_current_project(project["project_id"], project["project_name"])
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create project: {e}")

    with c2:
        if st.button("Cancel", use_container_width=True, key="create_project_cancel"):
            st.session_state.show_create_project_panel = False
            st.rerun()


def _render_rename_project_form(project_row: dict):
    project_name = st.text_input(
        "Project Name",
        value=str(project_row.get("project_name", "")),
        key=f"rename_project_name_{int(project_row['project_id'])}",
    ).strip()
    c1, c2 = st.columns(2)

    with c1:
        if st.button("Save", type="primary", use_container_width=True, key=f"rename_project_save_{int(project_row['project_id'])}"):
            try:
                validated_name = _validate_project_name_input(project_name, exclude_project_id=int(project_row["project_id"]))
                rename_project(int(project_row["project_id"]), validated_name)
                if st.session_state.get("current_project_id") == int(project_row["project_id"]):
                    st.session_state.current_project_name = validated_name
                st.session_state.show_rename_project_panel = False
                _set_project_dialog_target(None)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to rename project: {e}")

    with c2:
        if st.button("Cancel", use_container_width=True, key=f"rename_project_cancel_{int(project_row['project_id'])}"):
            st.session_state.show_rename_project_panel = False
            _set_project_dialog_target(None)
            st.rerun()


def _render_delete_project_form(project_row: dict):
    st.write("Are you sure you want to permanently delete this project? This action cannot be undone.")
    c1, c2 = st.columns(2)

    with c1:
        if st.button("Cancel", use_container_width=True, key=f"delete_project_cancel_{int(project_row['project_id'])}"):
            st.session_state.show_delete_project_panel = False
            _set_project_dialog_target(None)
            st.rerun()

    with c2:
        if st.button("Delete", type="primary", use_container_width=True, key=f"delete_project_confirm_{int(project_row['project_id'])}"):
            try:
                target_project_id = int(project_row["project_id"])
                delete_project_cascade(target_project_id)
                if st.session_state.get("current_project_id") == target_project_id:
                    clear_current_project()
                else:
                    st.session_state.project_dialog_target = None
                    st.session_state.show_delete_project_panel = False
                clear_all_caches()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete project: {e}")


if hasattr(st, "dialog"):
    @st.dialog("Create New Project")
    def render_create_project_dialog():
        _render_create_project_form()

    @st.dialog("Rename Project")
    def render_rename_project_dialog(project_row: dict):
        _render_rename_project_form(project_row)

    @st.dialog("Delete Project")
    def render_delete_project_dialog(project_row: dict):
        _render_delete_project_form(project_row)
else:
    def render_create_project_dialog():
        st.session_state.show_create_project_panel = True

    def render_rename_project_dialog(project_row: dict):
        _set_project_dialog_target(project_row)
        st.session_state.show_rename_project_panel = True

    def render_delete_project_dialog(project_row: dict):
        _set_project_dialog_target(project_row)
        st.session_state.show_delete_project_panel = True


def _encode_image_base64(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def render_avatar_badge():
    app_dir = os.path.dirname(os.path.abspath(__file__))
    avatar_path = os.path.join(app_dir, "avatar.png")
    b64 = _encode_image_base64(avatar_path)

    if not b64:
        return

    st.markdown(
        f"""
        <div class="avatar-container">
            <div class="avatar-badge">
                <img src="data:image/png;base64,{b64}" alt="avatar" />
                <div class="avatar-tooltip">汪宝宝是俺拉</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _prepare_metrics_compatibility(
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    p = presence_df.copy()
    s = source_df.copy()

    if "entity_name" not in p.columns:
        if "entity_name_en" in p.columns and "entity_name_cn" in p.columns:
            p["entity_name"] = p["entity_name_en"].where(
                p["entity_name_en"].astype(str).str.strip() != "",
                p["entity_name_cn"]
            )
        elif "entity_name_en" in p.columns:
            p["entity_name"] = p["entity_name_en"]
        elif "entity_name_cn" in p.columns:
            p["entity_name"] = p["entity_name_cn"]
        else:
            p["entity_name"] = ""

    return p, s


def _attach_entity_mapping_flags(project_id: int, presence_df: pd.DataFrame) -> pd.DataFrame:
    p = presence_df.copy()
    if p.empty or "entity_name_cn" not in p.columns:
        return p

    if "sinodis_flag" in p.columns:
        p = p.drop(columns=["sinodis_flag"])

    mapping_df = getattr(db_module, "get_all_entity_mappings", lambda _project_id: pd.DataFrame())(project_id)
    if mapping_df is None or mapping_df.empty or "entity_name_cn" not in mapping_df.columns:
        p["sinodis_flag"] = "N"
        return p

    mapping = mapping_df.copy()
    mapping["entity_name_cn"] = mapping["entity_name_cn"].fillna("").astype(str).str.strip()
    mapping["sinodis_flag"] = (
        mapping.get("sinodis_flag", "N")
        .fillna("N")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    mapping = mapping[mapping["entity_name_cn"] != ""]
    mapping = mapping.drop_duplicates(subset=["entity_name_cn"], keep="last")

    p["entity_name_cn"] = p["entity_name_cn"].fillna("").astype(str).str.strip()
    p = p.merge(
        mapping[["entity_name_cn", "sinodis_flag"]],
        on="entity_name_cn",
        how="left",
    )
    p["sinodis_flag"] = p["sinodis_flag"].fillna("N").astype(str).str.strip().str.upper()
    return p


def _filter_presence_by_query_kind(presence_df: pd.DataFrame, kind: str) -> pd.DataFrame:
    p = presence_df.copy()
    if p.empty or "query_type" not in p.columns:
        return p

    kind_text = str(kind).strip().lower()
    if kind_text == "":
        return p

    query_type_series = p["query_type"].fillna("").astype(str).str.strip().str.lower()
    return p[query_type_series.str.contains(kind_text, na=False)].reset_index(drop=True)


def _build_brand_only_payload(
    presence_df: pd.DataFrame,
    visibility_category: str,
    selected_publish_month_value: str,
) -> dict:
    payload = {}

    payload["brand_ranking_table"] = build_brand_ranking_table(presence_df)
    payload["brand_ranking_chart"] = build_brand_ranking_chart_from_table(payload["brand_ranking_table"])

    payload["brand_visibility_by_publish_month_table"] = build_brand_visibility_by_publish_month_table(presence_df)
    payload["brand_visibility_by_publish_month_chart"] = build_brand_visibility_by_publish_month_chart_from_table(
        payload["brand_visibility_by_publish_month_table"]
    )

    payload["brand_visibility_by_record_month_table"] = build_brand_visibility_by_record_month_table(presence_df)
    payload["brand_visibility_by_record_month_chart"] = build_brand_visibility_by_record_month_chart_from_table(
        payload["brand_visibility_by_record_month_table"]
    )

    if visibility_category:
        title = "Brand Visibility Ranking" if not visibility_category else f"Brand Visibility Ranking - {visibility_category}"
        payload["brand_visibility_by_category_table"] = build_brand_visibility_by_category_table(
            presence_df,
            visibility_category,
        )
        payload["brand_visibility_by_category_chart"] = build_brand_visibility_by_category_chart_from_table(
            payload["brand_visibility_by_category_table"],
            title=title,
        )

    if visibility_category and selected_publish_month_value:
        title = f"Brand Visibility - {visibility_category} / {selected_publish_month_value}"
        payload["brand_visibility_by_category_and_publish_month_table"] = (
            build_brand_visibility_by_category_and_publish_month_table(
                presence_df,
                visibility_category,
                selected_publish_month_value,
            )
        )
        payload["brand_visibility_by_category_and_publish_month_chart"] = (
            build_brand_visibility_by_category_chart_from_table(
                payload["brand_visibility_by_category_and_publish_month_table"],
                title=title,
            )
        )

    return payload


def _filter_queries_for_dashboard(
    queries_df: pd.DataFrame,
    selected_query_type: str,
    selected_category: str,
    selected_query_numbers: list[str],
    selected_publish_month: str,
):
    q = queries_df.copy()

    if not q.empty and selected_query_type != "All" and "query_type" in q.columns:
        q = q[q["query_type"].astype(str).str.strip() == selected_query_type]

    if not q.empty and selected_category != "All" and "product_category" in q.columns:
        q = q[q["product_category"].astype(str).str.strip() == selected_category]

    if not q.empty and selected_query_numbers and "query_number" in q.columns:
        q = q[q["query_number"].isin(selected_query_numbers)]

    if (
        not q.empty
        and selected_publish_month != "All"
        and "publish_month_default" in q.columns
    ):
        publish_month_series = q["publish_month_default"].fillna("").astype(str)
        q = q[
            publish_month_series.apply(
                lambda value: selected_publish_month in split_publish_months(value)
            )
        ]

    return q


def _safe_numeric_from_display(value) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if text == "":
        return 0.0
    if text.endswith("%"):
        try:
            return float(text[:-1]) / 100.0
        except Exception:
            return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def _format_average_number(value: float) -> str:
    if pd.isna(value):
        return "0.0"
    return f"{value:.1f}"


def _average_kpis(payloads: list[dict]) -> dict:
    count = max(len(payloads), 1)
    total_query = sum(_safe_numeric_from_display(p["kpis"].get("Total Query", 0)) for p in payloads) / count
    source_occ = sum(_safe_numeric_from_display(p["kpis"].get("source occurance", 0)) for p in payloads) / count
    quote_rate = sum(_safe_numeric_from_display(p["kpis"].get("Quote Rate", 0)) for p in payloads) / count
    return {
        "Total Query": _format_average_number(total_query),
        "source occurance": _format_average_number(source_occ),
        "Quote Rate": f"{quote_rate:.1%}",
    }


def _average_table_from_payloads(
    payloads: list[dict],
    table_key: str,
    key_cols: list[str],
    numeric_cols: list[str],
    sort_by: list[str],
    ascending: list[bool],
    format_coverage_rate: bool = False,
    nested_table_key: str | None = None,
) -> pd.DataFrame:
    if not payloads:
        return pd.DataFrame(columns=key_cols + numeric_cols)

    key_order = []
    key_seen = set()
    for payload in payloads:
        table = payload.get(table_key, pd.DataFrame())
        if nested_table_key:
            table = table.get(nested_table_key, pd.DataFrame()) if isinstance(table, dict) else pd.DataFrame()
        if table is None or table.empty:
            continue
        for _, row in table.iterrows():
            key = tuple(str(row[col]) for col in key_cols)
            if key not in key_seen:
                key_seen.add(key)
                key_order.append(key)

    if not key_order:
        return pd.DataFrame(columns=key_cols + numeric_cols)

    averaged_rows = []
    creator_count = len(payloads)

    for key in key_order:
        numeric_totals = {col: 0.0 for col in numeric_cols}
        for payload in payloads:
            table = payload.get(table_key, pd.DataFrame())
            if nested_table_key:
                table = table.get(nested_table_key, pd.DataFrame()) if isinstance(table, dict) else pd.DataFrame()
            if table is None or table.empty:
                row = None
            else:
                current = table.copy()
                for idx, col in enumerate(key_cols):
                    current = current[current[col].astype(str) == key[idx]]
                row = current.iloc[0] if not current.empty else None

            for col in numeric_cols:
                numeric_totals[col] += _safe_numeric_from_display(row[col]) if row is not None and col in row.index else 0.0

        out_row = {col: key[idx] for idx, col in enumerate(key_cols)}
        for col in numeric_cols:
            out_row[col] = numeric_totals[col] / creator_count
        averaged_rows.append(out_row)

    out = pd.DataFrame(averaged_rows)
    if out.empty:
        return out

    for col in numeric_cols:
        if col in out.columns:
            out[col] = out[col].round(1)

    out = out.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)

    if format_coverage_rate and "Coverage Rate" in out.columns:
        out["Coverage Rate"] = out["Coverage Rate"].apply(
            lambda v: f"{v * 100:.1f}%".replace(".0%", "%")
        )

    return out


def _build_average_dashboard_payload(
    creator_payloads: list[dict],
    visibility_category: str,
    selected_publish_month: str,
) -> dict:
    payload = {
        "kpis": _average_kpis(creator_payloads),
        "brand_ranking_table": _average_table_from_payloads(
            creator_payloads,
            table_key="brand_ranking_table",
            key_cols=["Brand"],
            numeric_cols=["Brand Mention", "Avg Position", "Best Position"],
            sort_by=["Brand Mention", "Avg Position", "Best Position"],
            ascending=[False, True, True],
        ),
        "channel_ranking_table": _average_table_from_payloads(
            creator_payloads,
            table_key="channel_ranking_table",
            key_cols=["Channel"],
            numeric_cols=["Channel Mention", "Avg Position"],
            sort_by=["Channel Mention", "Avg Position"],
            ascending=[False, True],
        ),
        "source_occurrence_table": _average_table_from_payloads(
            creator_payloads,
            table_key="source_occurrence_table",
            key_cols=["Source"],
            numeric_cols=["source occurance"],
            sort_by=["source occurance"],
            ascending=[False],
        ),
        "quoted_source_table": _average_table_from_payloads(
            creator_payloads,
            table_key="quoted_source_table",
            key_cols=["Source"],
            numeric_cols=["Quoted Source number"],
            sort_by=["Quoted Source number"],
            ascending=[False],
        ),
        "source_distribution_by_platform_table": _average_table_from_payloads(
            creator_payloads,
            table_key="source_distribution_by_platform_table",
            key_cols=["Source", "AI Platform"],
            numeric_cols=["source occurance"],
            sort_by=["Source", "AI Platform"],
            ascending=[True, True],
        ),
        "source_platform_common_table": _average_table_from_payloads(
            creator_payloads,
            table_key="source_platform_comparison_tables",
            nested_table_key="common",
            key_cols=["Source"],
            numeric_cols=["Total Occurrence", "Doubao Occurrence", "Deepseek Occurrence"],
            sort_by=["Total Occurrence", "Doubao Occurrence", "Deepseek Occurrence"],
            ascending=[False, False, False],
        ),
        "source_platform_doubao_only_table": _average_table_from_payloads(
            creator_payloads,
            table_key="source_platform_comparison_tables",
            nested_table_key="primary_only",
            key_cols=["Source"],
            numeric_cols=["Doubao Occurrence"],
            sort_by=["Doubao Occurrence"],
            ascending=[False],
        ),
        "source_platform_deepseek_only_table": _average_table_from_payloads(
            creator_payloads,
            table_key="source_platform_comparison_tables",
            nested_table_key="secondary_only",
            key_cols=["Source"],
            numeric_cols=["Deepseek Occurrence"],
            sort_by=["Deepseek Occurrence"],
            ascending=[False],
        ),
        "brand_visibility_by_publish_month_table": _average_table_from_payloads(
            creator_payloads,
            table_key="brand_visibility_by_publish_month_table",
            key_cols=["Publish Month", "Brand"],
            numeric_cols=["Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"],
            sort_by=["Publish Month", "Visibility Score", "Coverage Rate", "Avg Best Position"],
            ascending=[True, False, False, True],
            format_coverage_rate=True,
        ),
        "brand_visibility_by_record_month_table": _average_table_from_payloads(
            creator_payloads,
            table_key="brand_visibility_by_record_month_table",
            key_cols=["Record Month", "Brand"],
            numeric_cols=["Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"],
            sort_by=["Record Month", "Visibility Score", "Coverage Rate", "Avg Best Position"],
            ascending=[True, False, False, True],
            format_coverage_rate=True,
        ),
    }

    payload["brand_ranking_chart"] = build_brand_ranking_chart_from_table(payload["brand_ranking_table"])
    payload["channel_ranking_chart"] = build_channel_ranking_chart_from_table(payload["channel_ranking_table"])
    payload["source_occurrence_chart"] = build_source_occurrence_chart_from_table(payload["source_occurrence_table"])
    payload["quoted_source_chart"] = build_quoted_source_chart_from_table(payload["quoted_source_table"])
    payload["source_distribution_by_platform_chart"] = build_source_distribution_by_platform_chart_from_table(
        payload["source_distribution_by_platform_table"]
    )
    payload["brand_visibility_by_publish_month_chart"] = build_brand_visibility_by_publish_month_chart_from_table(
        payload["brand_visibility_by_publish_month_table"]
    )
    payload["brand_visibility_by_record_month_chart"] = build_brand_visibility_by_record_month_chart_from_table(
        payload["brand_visibility_by_record_month_table"]
    )

    if visibility_category:
        title = "Brand Visibility Ranking" if not visibility_category else f"Brand Visibility Ranking - {visibility_category}"
        payload["brand_visibility_by_category_table"] = _average_table_from_payloads(
            creator_payloads,
            table_key="brand_visibility_by_category_table",
            key_cols=["Brand"],
            numeric_cols=["Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"],
            sort_by=["Visibility Score", "Coverage Rate", "Avg Best Position"],
            ascending=[False, False, True],
            format_coverage_rate=True,
        )
        payload["brand_visibility_by_category_chart"] = build_brand_visibility_by_category_chart_from_table(
            payload["brand_visibility_by_category_table"],
            title=title,
        )

    if visibility_category and selected_publish_month != "All":
        title = f"Brand Visibility - {visibility_category} / {selected_publish_month}"
        payload["brand_visibility_by_category_and_publish_month_table"] = _average_table_from_payloads(
            creator_payloads,
            table_key="brand_visibility_by_category_and_publish_month_table",
            key_cols=["Brand"],
            numeric_cols=["Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"],
            sort_by=["Visibility Score", "Coverage Rate", "Avg Best Position"],
            ascending=[False, False, True],
            format_coverage_rate=True,
        )
        payload["brand_visibility_by_category_and_publish_month_chart"] = build_brand_visibility_by_category_chart_from_table(
            payload["brand_visibility_by_category_and_publish_month_table"],
            title=title,
        )

    return payload


# =========================================================
# Projects
# =========================================================
def render_projects_page():
    render_hero(
        "GNEO-D",
        ""
    )
    render_api_key_settings_panel()
    clear_all_caches()

    projects_df = list_projects()

    st.markdown('<div class="project-toolbar-shell">', unsafe_allow_html=True)
    c1, c2 = st.columns([5.2, 1.6], vertical_alignment="bottom")
    with c1:
        search_text = st.text_input(
            "Search Projects",
            placeholder="Search by project name",
            label_visibility="collapsed",
            key="projects_search_input",
        ).strip().lower()
    with c2:
        if st.button("Create New Project", type="primary", use_container_width=True):
            render_create_project_dialog()
    st.markdown('</div>', unsafe_allow_html=True)

    if st.session_state.show_create_project_panel and not hasattr(st, "dialog"):
        with st.container(border=True):
            st.markdown("### Create New Project")
            _render_create_project_form()
        st.markdown('<div style="height: 0.8rem;"></div>', unsafe_allow_html=True)

    fallback_target = _get_project_dialog_target()
    if fallback_target and not get_project(int(fallback_target["project_id"])):
        st.session_state.project_dialog_target = None
        st.session_state.show_rename_project_panel = False
        st.session_state.show_delete_project_panel = False
        fallback_target = None
    if st.session_state.show_rename_project_panel and fallback_target and not hasattr(st, "dialog"):
        with st.container(border=True):
            st.markdown("### Rename Project")
            _render_rename_project_form(fallback_target)
        st.markdown('<div style="height: 0.8rem;"></div>', unsafe_allow_html=True)

    if st.session_state.show_delete_project_panel and fallback_target and not hasattr(st, "dialog"):
        with st.container(border=True):
            st.markdown("### Delete Project")
            _render_delete_project_form(fallback_target)
        st.markdown('<div style="height: 0.8rem;"></div>', unsafe_allow_html=True)

    if search_text:
        projects_df = projects_df[
            projects_df["project_name"].astype(str).str.lower().str.contains(search_text, na=False)
        ].reset_index(drop=True)

    if projects_df.empty:
        st.info("No projects found.")
        return

    cols = st.columns(3)
    for idx, row in projects_df.iterrows():
        with cols[idx % 3]:
            project_row = {
                "project_id": int(row["project_id"]),
                "project_name": str(row["project_name"]),
                "status": str(row["status"]),
                "updated_at": str(row["updated_at"]),
            }

            with st.container(border=True):
                head1, head2 = st.columns([6.5, 2.2], vertical_alignment="center")
                with head1:
                    st.markdown(
                        f"""<div class="project-card-title">{project_row['project_name']}</div>""",
                        unsafe_allow_html=True,
                    )
                with head2:
                    st.markdown('<div class="project-card-actions-label"></div>', unsafe_allow_html=True)
                    a1, a2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                    with a1:
                        if st.button("✎", key=f"rename_project_{project_row['project_id']}_{idx}", use_container_width=True):
                            render_rename_project_dialog(project_row)
                    with a2:
                        if st.button("🗑", key=f"delete_project_{project_row['project_id']}_{idx}", use_container_width=True):
                            render_delete_project_dialog(project_row)

                open_c1, open_c2 = st.columns([2.6, 1.1])
                with open_c2:
                    if st.button(
                        "Enter",
                        key=f"open_project_body_{project_row['project_id']}_{idx}",
                        use_container_width=True,
                        type="secondary",
                    ):
                        set_current_project(project_row["project_id"], project_row["project_name"])
                        st.rerun()

                st.markdown(
                    f"""
                    <div class="project-card-open">
                        <div class="project-card-meta"><strong>Status:</strong> {project_row['status']}</div>
                        <div class="project-card-meta"><strong>Updated At:</strong> {project_row['updated_at']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            st.markdown('<div class="project-grid-spacer"></div>', unsafe_allow_html=True)


# =========================================================
# Dashboard
# =========================================================
def render_dashboard():
    dashboard_render_start = time.perf_counter()
    if not require_project_selection():
        return

    project = get_current_project()
    render_hero(
        "GEO Performance Analysis",
        "Analyze presence, sources, and quoted results with one unified dashboard."
    )
    render_workspace_back_nav()
    render_project_context_bar()
    render_top_nav()

    with st.container():
        st.markdown('<div class="dashboard-filters-marker"></div>', unsafe_allow_html=True)
        st.markdown('<div class="dashboard-filters-title">Dashboard Filters</div>', unsafe_allow_html=True)
        f1, f2, f3, f4, f5 = st.columns(5)

        with f1:
            query_status_filter = st.selectbox(
                "Query Status",
                options=["active_only", "archived_only", "all_statuses"],
                format_func=lambda x: {
                    "active_only": "Active only",
                    "archived_only": "Archived only",
                    "all_statuses": "Active + Archived",
                }[x],
            )

        dashboard_data_start = time.perf_counter()
        dashboard_data = cached_dashboard_data(
            project_id=int(project["project_id"]),
            query_status_filter=query_status_filter,
            project_updated_at=str(project.get("updated_at", "")),
        )
        _log_perf(
            "dashboard data load",
            dashboard_data_start,
            project_id=int(project["project_id"]),
            queries=len(dashboard_data.get("queries", [])),
            presence=len(dashboard_data.get("presence_records", [])),
            source=len(dashboard_data.get("source_records", [])),
        )

        queries_df = dashboard_data["queries"]
        presence_enriched = dashboard_data["presence_records"]
        source_enriched = dashboard_data["source_records"]
        filter_options = dashboard_data["filter_options"]

        presence_enriched, source_enriched = _prepare_metrics_compatibility(
            presence_enriched,
            source_enriched
        )
        presence_enriched = _attach_entity_mapping_flags(int(project["project_id"]), presence_enriched)

        query_type_options = ["All"] + filter_options.get("query_type", [])
        record_month_options = ["All"] + filter_options.get("record_month", [])
        product_category_options = ["All"] + filter_options.get("product_category", [])
        ai_platform_options = ["All"] + filter_options.get("ai_platform", [])
        query_number_options = filter_options.get("query_number", [])
        creator_options = ["All"] + filter_options.get("created_by", [])

        with f2:
            selected_query_type = st.selectbox("Query Type", query_type_options)

        with f3:
            selected_record_month = st.selectbox("Record Month", record_month_options)

        with f4:
            selected_category = st.selectbox("Product Category", product_category_options)

        with f5:
            selected_sinodis_brand = st.selectbox(
                "Sinodis Brand",
                ["All", "Sinodis", "Non-Sinodis"],
            )

        f6, f7, f8, f9 = st.columns(4)

        with f6:
            selected_platform = st.selectbox("AI Platform", ai_platform_options)

        with f7:
            selected_query_numbers = st.multiselect("Query Number", query_number_options)

        with f8:
            selected_creators = st.multiselect("Created By", creator_options, default=["All"])

        with f9:
            selected_publish_month = st.selectbox(
                "Publish Month",
                ["All"] + MONTHS
            )

        date_candidates = []
        if not presence_enriched.empty and "check_date" in presence_enriched.columns:
            date_candidates.extend(
                pd.to_datetime(presence_enriched["check_date"], errors="coerce").dropna().tolist()
            )
        if not source_enriched.empty and "check_date" in source_enriched.columns:
            date_candidates.extend(
                pd.to_datetime(source_enriched["check_date"], errors="coerce").dropna().tolist()
            )

        if len(date_candidates) > 0:
            min_date = min(date_candidates).date()
            max_date = max(date_candidates).date()
            selected_check_date_range = st.date_input(
                "Check Date Range",
                value=(min_date, max_date),
            )
        else:
            selected_check_date_range = None
            st.caption("No check_date available yet.")

    all_creators_mode = (not selected_creators) or ("All" in selected_creators)
    creator_filter_list = None if all_creators_mode else selected_creators
    record_month_filter_list = None if selected_record_month == "All" else [selected_record_month]
    query_type_filter_list = None if selected_query_type == "All" else [selected_query_type]
    category_filter_list = None if selected_category == "All" else [selected_category]
    platform_filter_list = None if selected_platform == "All" else [selected_platform]
    query_number_filter_list = selected_query_numbers if selected_query_numbers else None

    filtered = apply_common_filters(
        presence_df=presence_enriched,
        source_df=source_enriched,
        query_type=query_type_filter_list,
        product_category=category_filter_list,
        ai_platform=platform_filter_list,
        query_number=query_number_filter_list,
        record_month=record_month_filter_list,
        created_by=creator_filter_list,
        check_date_range=list(selected_check_date_range) if selected_check_date_range and len(selected_check_date_range) == 2 else None,
    )

    presence_filtered = filtered["presence_records"]
    source_filtered = filtered["source_records"]

    presence_filtered, source_filtered = _prepare_metrics_compatibility(
        presence_filtered,
        source_filtered
    )
    presence_filtered = _attach_entity_mapping_flags(int(project["project_id"]), presence_filtered)
    content_publish_filtered = get_all_content_publish(int(project["project_id"]))

    queries_filtered = _filter_queries_for_dashboard(
        queries_df=queries_df,
        selected_query_type=selected_query_type,
        selected_category=selected_category,
        selected_query_numbers=selected_query_numbers,
        selected_publish_month=selected_publish_month,
    )

    filtered_query_pool = set(
        queries_filtered["query_number"].dropna().astype(str).str.strip().tolist()
    ) if not queries_filtered.empty and "query_number" in queries_filtered.columns else set()

    if filtered_query_pool:
        if not presence_filtered.empty and "query_number" in presence_filtered.columns:
            presence_filtered = presence_filtered[
                presence_filtered["query_number"].astype(str).isin(filtered_query_pool)
            ].reset_index(drop=True)
        if not source_filtered.empty and "query_number" in source_filtered.columns:
            source_filtered = source_filtered[
                source_filtered["query_number"].astype(str).isin(filtered_query_pool)
            ].reset_index(drop=True)
        if not content_publish_filtered.empty and "query_id" in content_publish_filtered.columns:
            content_publish_filtered = content_publish_filtered[
                content_publish_filtered["query_id"].astype(str).isin(filtered_query_pool)
            ].reset_index(drop=True)
    else:
        presence_filtered = presence_filtered.iloc[0:0].copy()
        source_filtered = source_filtered.iloc[0:0].copy()
        content_publish_filtered = content_publish_filtered.iloc[0:0].copy()

    if (
        selected_platform != "All"
        and not content_publish_filtered.empty
        and "publish_platform" in content_publish_filtered.columns
    ):
        content_publish_filtered = content_publish_filtered[
            content_publish_filtered["publish_platform"].astype(str).str.strip() == str(selected_platform).strip()
        ].reset_index(drop=True)

    visibility_category = "" if selected_category == "All" else selected_category

    selected_publish_month_value = "" if selected_publish_month == "All" else selected_publish_month

    payload = build_dashboard_payload(
        queries_df=queries_filtered,
        presence_df=presence_filtered,
        source_df=source_filtered,
        content_publish_df=content_publish_filtered,
        selected_category=visibility_category,
        selected_publish_month=selected_publish_month_value,
    )

    brand_presence_filtered = _filter_presence_by_query_kind(presence_filtered, "generic")

    if selected_sinodis_brand != "All" and not brand_presence_filtered.empty and "sinodis_flag" in brand_presence_filtered.columns:
        target_flag = "Y" if selected_sinodis_brand == "Sinodis" else "N"
        brand_presence_filtered = brand_presence_filtered[
            brand_presence_filtered["sinodis_flag"].astype(str).str.strip().str.upper() == target_flag
        ].reset_index(drop=True)

    brand_payload = _build_brand_only_payload(
        presence_df=brand_presence_filtered,
        visibility_category=visibility_category,
        selected_publish_month_value=selected_publish_month_value,
    )

    for key in [
        "brand_ranking_table",
        "brand_ranking_chart",
        "brand_visibility_by_publish_month_table",
        "brand_visibility_by_publish_month_chart",
        "brand_visibility_by_record_month_table",
        "brand_visibility_by_record_month_chart",
    ]:
        if key in brand_payload:
            payload[key] = brand_payload[key]

    for optional_key in [
        "brand_visibility_by_category_table",
        "brand_visibility_by_category_chart",
        "brand_visibility_by_category_and_publish_month_table",
        "brand_visibility_by_category_and_publish_month_chart",
    ]:
        if optional_key in brand_payload:
            payload[optional_key] = brand_payload[optional_key]
        elif optional_key in payload:
            del payload[optional_key]

    kpis = payload["kpis"]

    k1, k2, k3 = st.columns(3)
    with k1:
        render_kpi_card("Total Query", f"{kpis['Total Query']}")
    with k2:
        render_kpi_card("Source Occurance", f"{kpis['source occurance']}")
    with k3:
        render_kpi_card("Quote Rate", f"{kpis['Quote Rate']}")

    current_filter_snapshot = _build_dashboard_filter_snapshot(
        query_status_filter=query_status_filter,
        selected_query_type=selected_query_type,
        selected_record_month=selected_record_month,
        selected_category=selected_category,
        selected_platform=selected_platform,
        selected_publish_month=selected_publish_month,
        selected_query_numbers=selected_query_numbers,
        selected_creators=selected_creators,
        selected_sinodis_brand=selected_sinodis_brand,
        selected_check_date_range=selected_check_date_range,
    )

    render_ai_insight_panel(
        project_id=int(project["project_id"]),
        filters=current_filter_snapshot,
        payload=payload,
        queries_df=queries_filtered,
        presence_df=presence_filtered,
        source_df=source_filtered,
        content_publish_df=content_publish_filtered,
    )

    visibility_ranking_title = (
        "Brand Visibility Ranking"
        if not visibility_category
        else f"Brand Visibility Ranking - {visibility_category}"
    )
    visibility_ranking_table = payload.get("brand_visibility_by_category_table", pd.DataFrame())

    section_card_start()
    render_section_header(
        "Presence Analysis",
        "Compare brand performance, channel mentions, and category visibility under the current filters."
    )
    render_presence_ranking_list(payload["brand_ranking_table"], "Brand Presence Ranking")
    render_visibility_ranking_list(visibility_ranking_table, visibility_ranking_title)

    chart_card_start(f"{visibility_ranking_title} Table")
    if not visibility_ranking_table.empty:
        table_height = min(680, 42 + (len(visibility_ranking_table) + 1) * 35)
        st.dataframe(
            visibility_ranking_table,
            use_container_width=True,
            hide_index=True,
            height=table_height,
        )
    else:
        st.info("No data available")
    chart_card_end()

    chart_card_start("Brand Visibility Score by Record Month")
    record_month_visibility_table = payload.get("brand_visibility_by_record_month_table", pd.DataFrame())
    record_month_compare_key = f"brand_visibility_record_month_compare_{int(project['project_id'])}"
    available_record_month_brands = []
    if not record_month_visibility_table.empty and "Brand" in record_month_visibility_table.columns:
        available_record_month_brands = sorted(
            {
                str(value).strip()
                for value in record_month_visibility_table["Brand"].dropna().tolist()
                if str(value).strip()
            }
        )

    current_record_month_selection = st.session_state.get(record_month_compare_key, [])
    if not isinstance(current_record_month_selection, list):
        current_record_month_selection = []
    current_record_month_selection = [
        brand for brand in current_record_month_selection if brand in available_record_month_brands
    ]
    st.session_state[record_month_compare_key] = current_record_month_selection

    compare_col, clear_col = st.columns([6, 1])
    with compare_col:
        selected_record_month_brands = st.multiselect(
            "Compare brands",
            options=available_record_month_brands,
            default=current_record_month_selection,
            key=record_month_compare_key,
        )
    with clear_col:
        st.markdown("<div style='height: 1.9rem;'></div>", unsafe_allow_html=True)
        if st.button("Clear", key=f"{record_month_compare_key}_clear", use_container_width=True):
            st.session_state[record_month_compare_key] = []
            st.rerun()

    filtered_record_month_chart = payload["brand_visibility_by_record_month_chart"]
    if selected_record_month_brands and not record_month_visibility_table.empty and "Brand" in record_month_visibility_table.columns:
        filtered_record_month_table = record_month_visibility_table[
            record_month_visibility_table["Brand"].astype(str).isin(selected_record_month_brands)
        ].reset_index(drop=True)
        filtered_record_month_chart = build_brand_visibility_by_record_month_chart_from_table(
            filtered_record_month_table
        )

    st.plotly_chart(filtered_record_month_chart, use_container_width=True)
    if not record_month_visibility_table.empty:
        display_columns = [
            col for col in ["Record Month", "Brand", "Visibility Score"]
            if col in record_month_visibility_table.columns
        ]
        st.dataframe(
            record_month_visibility_table[display_columns],
            use_container_width=True,
            hide_index=True,
            height=248,
        )
    else:
        st.info("No data available")
    chart_card_end()

    render_channel_ranking_list(payload["channel_ranking_table"], "Channel Ranking")
    ranking_card_start("Channel Ranking Table")
    if not payload["channel_ranking_table"].empty:
        st.dataframe(
            payload["channel_ranking_table"].head(15),
            use_container_width=True,
            hide_index=True,
            height=248,
        )
    else:
        st.info("No data available")
    ranking_card_end()
    section_card_end()

    if (
        visibility_category
        and selected_publish_month != "All"
        and "brand_visibility_by_category_and_publish_month_chart" in payload
    ):
        chart_card_start(f"Brand Visibility - {visibility_category} / {selected_publish_month}")
        st.plotly_chart(
            payload["brand_visibility_by_category_and_publish_month_chart"],
            use_container_width=True
        )
        if not payload["brand_visibility_by_category_and_publish_month_table"].empty:
            st.dataframe(
                payload["brand_visibility_by_category_and_publish_month_table"].head(20),
                use_container_width=True,
                hide_index=True,
            )
        chart_card_end()

    section_card_start()
    render_section_header(
        "Source Analysis",
        "Understand which sources appear most often and how source usage differs across AI platforms."
    )

    s1, s2 = st.columns(2)

    with s1:
        chart_card_start("Source Frequency Ranking")
        st.plotly_chart(payload["source_occurrence_chart"], use_container_width=True)
        chart_card_end()

    with s2:
        render_designed_metric_table_card(
            payload["quoted_source_table"],
            "Quoted Source Ranking",
            "Source",
            "Source Name",
            "Quoted Source number",
            "Quoted Source Number",
            height=356,
        )

    source_platform_comparison = payload.get("source_platform_comparison_tables", {})
    source_common_table = payload.get(
        "source_platform_common_table",
        source_platform_comparison.get("common", pd.DataFrame()) if isinstance(source_platform_comparison, dict) else pd.DataFrame(),
    )
    source_doubao_only_table = payload.get(
        "source_platform_doubao_only_table",
        source_platform_comparison.get("primary_only", pd.DataFrame()) if isinstance(source_platform_comparison, dict) else pd.DataFrame(),
    )
    source_deepseek_only_table = payload.get(
        "source_platform_deepseek_only_table",
        source_platform_comparison.get("secondary_only", pd.DataFrame()) if isinstance(source_platform_comparison, dict) else pd.DataFrame(),
    )

    comparison_left, comparison_right = st.columns([0.72, 1.28])

    st.markdown(
        '<div class="subsection-kicker">Source Preference Summary</div>',
        unsafe_allow_html=True,
    )

    with comparison_left:
        render_source_name_list(
            source_common_table,
            "Common Preferred Sources Across Platform",
            grid_columns=1,
            items_per_column=20,
            card_class="ranking-card-source-common",
        )

    with comparison_right:
        render_source_ranking_list(
            source_doubao_only_table,
            "Doubao Only Sources",
            "Doubao Occurrence",
            "Doubao Occurrence",
            grid_columns=2,
            items_per_column=10,
            card_class="ranking-card-source-doubao",
        )
        render_source_ranking_list(
            source_deepseek_only_table,
            "Deepseek Only Sources",
            "Deepseek Occurrence",
            "Deepseek Occurrence",
            grid_columns=2,
            items_per_column=10,
            card_class="ranking-card-source-deepseek",
        )
    section_card_end()

    show_filtered_preview = st.toggle("Show Filtered Data Preview", value=False, key="dashboard_filtered_preview_toggle")
    if show_filtered_preview:
        st.markdown("#### Presence Records")
        st.dataframe(presence_filtered, use_container_width=True, hide_index=True)

        st.markdown("#### Source Records")
        st.dataframe(source_filtered, use_container_width=True, hide_index=True)

    _log_perf(
        "dashboard render",
        dashboard_render_start,
        project_id=int(project["project_id"]),
        filtered_presence=len(presence_filtered),
        filtered_source=len(source_filtered),
    )

    enter_started_at = st.session_state.pop("project_enter_started_at", None)
    if enter_started_at is not None:
        _log_perf(
            "project enter",
            enter_started_at,
            project_id=int(project["project_id"]),
            page="Dashboard",
        )


# =========================================================
# Data Entry
# =========================================================
def render_data_entry():
    data_entry_start = time.perf_counter()
    if not require_project_selection():
        return

    render_hero(
        "GEO Performance Analysis",
        "Manually add new monthly results or upload Excel files for batch entry."
    )
    render_workspace_back_nav()
    render_project_context_bar()
    render_top_nav()
    render_data_entry_page()
    _log_perf("data entry render", data_entry_start, project_id=int(st.session_state.get("current_project_id") or 0))


# =========================================================
# Data Record
# =========================================================
def render_data_record():
    data_record_start = time.perf_counter()
    if not require_project_selection():
        return

    render_hero(
        "GEO Performance Analysis",
        "Manage Query Master, mappings, submissions, and raw data master tables."
    )
    render_workspace_back_nav()
    render_project_context_bar()
    render_top_nav()
    render_data_record_page()
    _log_perf("data record render", data_record_start, project_id=int(st.session_state.get("current_project_id") or 0))


# =========================================================
# Main
# =========================================================
inject_dashboard_css()
render_avatar_badge()

if st.session_state.page == "Projects":
    render_projects_page()
elif st.session_state.page == "Dashboard":
    render_dashboard()
elif st.session_state.page == "Data Entry":
    render_data_entry()
else:
    render_data_record()
