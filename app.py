import os
import base64
import pandas as pd
import streamlit as st

from utils.db import (
    create_tables,
    log_backend_selection_once,
    log_schema_initialization_complete,
    create_project,
    rename_project,
    delete_project_cascade,
    list_projects,
    get_project,
    get_project_by_name,
    split_publish_months,
)
from utils.loader import (
    load_dashboard_ready_data,
    apply_common_filters,
)
from utils.forms import (
    render_data_entry_page,
    render_data_record_page,
)
from utils.charts import (
    build_dashboard_payload,
    build_brand_ranking_chart_from_table,
    build_channel_ranking_chart_from_table,
    build_source_occurrence_chart_from_table,
    build_quoted_source_chart_from_table,
    build_source_distribution_by_platform_chart_from_table,
    build_brand_visibility_by_category_chart_from_table,
    build_brand_visibility_by_publish_month_chart_from_table,
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
log_backend_selection_once()
create_tables()
log_schema_initialization_complete()

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
):
    return load_dashboard_ready_data(
        project_id=project_id,
        query_status_filter=query_status_filter,
    )


def clear_all_caches():
    cached_dashboard_data.clear()


# =========================================================
# UI / CSS Helpers
# =========================================================
def inject_dashboard_css():
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #f8fafc 0%, #f3f7fb 100%);
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
            border-radius: 20px;
            background: linear-gradient(135deg, #f8fbff 0%, #f4f9ff 46%, #ffffff 100%);
            border: 1px solid rgba(191, 219, 254, 0.55);
            box-shadow: 0 8px 24px rgba(148, 163, 184, 0.10);
            margin-bottom: 1rem;
        }

        .hero-title {
            font-size: 1.95rem;
            font-weight: 800;
            color: #1f2d3d;
            margin: 0;
            line-height: 1.15;
        }

        .hero-subtitle {
            margin-top: 0.4rem;
            font-size: 0.98rem;
            color: #5e6b7a;
        }

        .section-title {
            font-size: 1.18rem;
            font-weight: 800;
            margin: 0;
            color: #334155;
        }

        .section-desc {
            margin-top: 0.24rem;
            color: #64748b;
            font-size: 0.92rem;
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
            border-top: 1px solid rgba(255, 255, 255, 0.92);
            box-shadow: 0 -1px 0 rgba(226, 232, 240, 0.9);
            padding-top: 1rem;
        }

        .dashboard-filters-shell {
            background: rgba(255, 255, 255, 0.96);
            border: 1px solid rgba(203, 213, 225, 0.95);
            border-radius: 20px;
            padding: 1rem 1rem 0.7rem 1rem;
            box-shadow: 0 8px 20px rgba(148, 163, 184, 0.10);
            margin-bottom: 1rem;
        }

        .dashboard-filters-title {
            margin: 0 0 0.8rem 0;
            font-size: 1.02rem;
            font-weight: 800;
            color: #334155;
        }

        .dashboard-filters-shell label,
        .dashboard-filters-shell .stMarkdown,
        .dashboard-filters-shell div[data-testid="stWidgetLabel"] p {
            color: #475569 !important;
            font-weight: 700 !important;
        }

        .dashboard-filters-shell div[data-baseweb="select"] > div,
        .dashboard-filters-shell div[data-baseweb="input"] > div,
        .dashboard-filters-shell div[data-baseweb="popover"] > div,
        .dashboard-filters-shell div[data-testid="stDateInput"] [data-baseweb="input"] {
            background: #ffffff !important;
            border-color: rgba(148, 163, 184, 0.70) !important;
            box-shadow: 0 1px 2px rgba(148, 163, 184, 0.08);
        }

        .dashboard-filters-shell div[data-baseweb="select"] > div:hover,
        .dashboard-filters-shell div[data-baseweb="input"] > div:hover,
        .dashboard-filters-shell div[data-testid="stDateInput"] [data-baseweb="input"]:hover {
            border-color: rgba(100, 116, 139, 0.88) !important;
        }

        .dashboard-filters-shell div[data-baseweb="select"] > div:focus-within,
        .dashboard-filters-shell div[data-baseweb="input"] > div:focus-within,
        .dashboard-filters-shell div[data-testid="stDateInput"] [data-baseweb="input"]:focus-within {
            border-color: #7aa8f8 !important;
            box-shadow: 0 0 0 3px rgba(110, 168, 254, 0.18) !important;
        }

        .dashboard-filters-shell input,
        .dashboard-filters-shell textarea {
            color: #334155 !important;
        }

        .dashboard-filters-shell div[data-baseweb="tag"] {
            background: #edf4ff !important;
            border: 1px solid rgba(147, 197, 253, 0.85) !important;
            color: #31558f !important;
        }

        .dashboard-filters-shell div[data-baseweb="tag"] span {
            color: #31558f !important;
            font-weight: 700 !important;
        }

        .kpi-card {
            position: relative;
            overflow: hidden;
            background:
                radial-gradient(circle at top right, rgba(110, 168, 254, 0.14), transparent 34%),
                linear-gradient(180deg, #ffffff 0%, #fbfdff 100%);
            border: 1px solid rgba(203, 213, 225, 0.70);
            border-radius: 20px;
            padding: 1.15rem 1.1rem 1.05rem 1.1rem;
            box-shadow: 0 8px 22px rgba(148, 163, 184, 0.10);
            min-height: 122px;
        }

        .kpi-label {
            color: #5b6b84;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.58rem;
        }

        .kpi-value {
            color: #1e293b;
            font-size: 2.28rem;
            font-weight: 800;
            line-height: 1.02;
            letter-spacing: -0.02em;
        }

        .chart-card {
            background: #ffffff;
            border: 1px solid rgba(203, 213, 225, 0.78);
            border-radius: 20px;
            padding: 1.08rem 1.15rem 0.95rem 1.15rem;
            box-shadow: 0 8px 24px rgba(148, 163, 184, 0.09);
            margin-bottom: 1.05rem;
        }

        .chart-card-title {
            font-size: 1rem;
            font-weight: 800;
            color: #334155;
            margin-bottom: 0.8rem;
            padding-bottom: 0.68rem;
            border-bottom: 1px solid rgba(226, 232, 240, 0.95);
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(226, 232, 240, 1);
            border-radius: 16px;
            overflow: hidden;
            background: #fcfdff;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.85);
        }

        div[data-testid="stPlotlyChart"] {
            border-radius: 16px;
            overflow: hidden;
        }

        div[data-testid="stExpander"] {
            border: 1px solid rgba(226, 232, 240, 0.95);
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.98);
            box-shadow: 0 6px 18px rgba(148, 163, 184, 0.08);
        }

        div[data-testid="stExpander"] summary {
            font-weight: 700;
            color: #475569;
        }

        .project-toolbar-shell {
            margin: 0 0 0.85rem 0;
            padding: 0.1rem 0 0.2rem 0;
        }

        .project-card {
            background: #ffffff;
            border: 1px solid rgba(16, 24, 40, 0.06);
            border-radius: 18px;
            padding: 1rem 1rem 1rem 1rem;
            box-shadow: 0 8px 24px rgba(16, 24, 40, 0.05);
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
            color: #111827;
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
            color: #667085;
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
            border-radius: 12px;
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
            border: 3px solid rgba(255,255,255,0.98);
            box-shadow: 0 10px 28px rgba(0,0,0,0.16);
            background: #fff;
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
            background: rgba(31, 45, 61, 0.95);
            color: #fff;
            font-size: 14px;
            font-weight: 700;
            padding: 8px 12px;
            border-radius: 10px;
            white-space: nowrap;
            box-shadow: 0 8px 20px rgba(0,0,0,0.18);
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


def section_card_start():
    st.markdown('<div class="dashboard-section">', unsafe_allow_html=True)


def section_card_end():
    st.markdown("</div>", unsafe_allow_html=True)


def render_top_nav():
    if not get_current_project():
        return

    st.markdown('<div class="nav-shell">', unsafe_allow_html=True)

    current_page = st.session_state.page if st.session_state.page in WORKSPACE_NAV_OPTIONS else "Dashboard"

    selected_page = st.segmented_control(
        "Navigation",
        options=WORKSPACE_NAV_OPTIONS,
        default=current_page,
        label_visibility="collapsed",
        selection_mode="single",
    )

    if selected_page and selected_page != st.session_state.page:
        st.session_state.page = selected_page

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
    """
    Keep compatibility with old/new metrics expectations.
    """
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
) -> pd.DataFrame:
    if not payloads:
        return pd.DataFrame(columns=key_cols + numeric_cols)

    key_order = []
    key_seen = set()
    for payload in payloads:
        table = payload.get(table_key, pd.DataFrame())
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
        "brand_visibility_by_publish_month_table": _average_table_from_payloads(
            creator_payloads,
            table_key="brand_visibility_by_publish_month_table",
            key_cols=["Publish Month", "Brand"],
            numeric_cols=["Covered Queries", "Query Pool", "Coverage Rate", "Avg Best Position", "Visibility Score"],
            sort_by=["Publish Month", "Visibility Score", "Coverage Rate", "Avg Best Position"],
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
        "Project List",
        "Create a project or enter an existing workspace to view its Dashboard, Data Entry, and Data Record."
    )
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
        st.markdown('<div class="dashboard-filters-shell">', unsafe_allow_html=True)
        st.markdown('<div class="dashboard-filters-title">Dashboard Filters</div>', unsafe_allow_html=True)
        f1, f2, f3, f4 = st.columns(4)

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

        dashboard_data = cached_dashboard_data(
            project_id=int(project["project_id"]),
            query_status_filter=query_status_filter,
        )

        queries_df = dashboard_data["queries"]
        presence_enriched = dashboard_data["presence_records"]
        source_enriched = dashboard_data["source_records"]
        filter_options = dashboard_data["filter_options"]

        presence_enriched, source_enriched = _prepare_metrics_compatibility(
            presence_enriched,
            source_enriched
        )

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

        f5, f6, f7, f8 = st.columns(4)

        with f5:
            selected_platform = st.selectbox("AI Platform", ai_platform_options)

        with f6:
            selected_query_numbers = st.multiselect("Query Number", query_number_options)

        with f7:
            selected_creators = st.multiselect("Created By", creator_options, default=["All"])

        with f8:
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

        st.markdown("</div>", unsafe_allow_html=True)

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
    else:
        presence_filtered = presence_filtered.iloc[0:0].copy()
        source_filtered = source_filtered.iloc[0:0].copy()

    visibility_category = "" if selected_category == "All" else selected_category

    selected_publish_month_value = "" if selected_publish_month == "All" else selected_publish_month

    payload = build_dashboard_payload(
        queries_df=queries_filtered,
        presence_df=presence_filtered,
        source_df=source_filtered,
        selected_category=visibility_category,
        selected_publish_month=selected_publish_month_value,
    )

    kpis = payload["kpis"]

    k1, k2, k3 = st.columns(3)
    with k1:
        render_kpi_card("Total Query", f"{kpis['Total Query']}")
    with k2:
        render_kpi_card("Source Occurance", f"{kpis['source occurance']}")
    with k3:
        render_kpi_card("Quote Rate", f"{kpis['Quote Rate']}")

    section_card_start()
    render_section_header(
        "Presence Analysis",
        "Compare brand performance, channel mentions, and category visibility under the current filters."
    )

    c1, c2 = st.columns(2)

    with c1:
        chart_card_start("Brand Ranking")
        st.plotly_chart(payload["brand_ranking_chart"], use_container_width=True)
        if not payload["brand_ranking_table"].empty:
            st.dataframe(payload["brand_ranking_table"].head(15), use_container_width=True, hide_index=True)
        chart_card_end()

    with c2:
        chart_card_start("Channel Ranking")
        st.plotly_chart(payload["channel_ranking_chart"], use_container_width=True)
        if not payload["channel_ranking_table"].empty:
            st.dataframe(payload["channel_ranking_table"].head(15), use_container_width=True, hide_index=True)
        chart_card_end()
    section_card_end()

    section_card_start()
    render_section_header(
        "Brand Visibility",
        "Review category visibility and how brand visibility changes across publish months."
    )

    chart_card_start(
        "Brand Visibility Ranking" if not visibility_category
        else f"Brand Visibility Ranking - {visibility_category}"
    )

    if "brand_visibility_by_category_chart" in payload:
        st.plotly_chart(
            payload["brand_visibility_by_category_chart"],
            use_container_width=True
        )
    else:
        st.info("No data available")

    if "brand_visibility_by_category_table" in payload and not payload["brand_visibility_by_category_table"].empty:
        st.dataframe(
            payload["brand_visibility_by_category_table"].head(20),
            use_container_width=True,
            hide_index=True
        )

    chart_card_end()

    chart_card_start("Brand Visibility by Publish Month")
    st.plotly_chart(payload["brand_visibility_by_publish_month_chart"], use_container_width=True)
    brand_month_table = payload.get("brand_visibility_by_publish_month_table", pd.DataFrame())
    if not brand_month_table.empty:
        st.dataframe(brand_month_table.head(30), use_container_width=True, hide_index=True)
    chart_card_end()

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
    section_card_end()

    section_card_start()
    render_section_header(
        "Source Analysis",
        "Understand which sources appear most often and how source usage differs across AI platforms."
    )

    s1, s2 = st.columns(2)

    with s1:
        chart_card_start("Source Frequency Ranking")
        st.plotly_chart(payload["source_occurrence_chart"], use_container_width=True)
        if not payload["source_occurrence_table"].empty:
            st.dataframe(payload["source_occurrence_table"].head(15), use_container_width=True, hide_index=True)
        chart_card_end()

    with s2:
        chart_card_start("Quoted Source Ranking")
        st.plotly_chart(payload["quoted_source_chart"], use_container_width=True)
        if not payload["quoted_source_table"].empty:
            st.dataframe(payload["quoted_source_table"].head(15), use_container_width=True, hide_index=True)
        chart_card_end()

    chart_card_start("Source Distribution by AI Platform")
    st.plotly_chart(payload["source_distribution_by_platform_chart"], use_container_width=True)
    source_distribution_table = payload.get("source_distribution_by_platform_table", pd.DataFrame())
    if not source_distribution_table.empty:
        st.dataframe(source_distribution_table, use_container_width=True, hide_index=True)
    chart_card_end()
    section_card_end()

    with st.expander("Filtered Data Preview", expanded=False):
        st.markdown("#### Presence Records")
        st.dataframe(presence_filtered, use_container_width=True, hide_index=True)

        st.markdown("#### Source Records")
        st.dataframe(source_filtered, use_container_width=True, hide_index=True)


# =========================================================
# Data Entry
# =========================================================
def render_data_entry():
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
    clear_all_caches()


# =========================================================
# Data Record
# =========================================================
def render_data_record():
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
    clear_all_caches()


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
