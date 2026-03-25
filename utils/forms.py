import pandas as pd
import streamlit as st

from utils.db import (
    # query master
    upsert_query_master,
    get_all_queries,
    get_query_by_number,
    delete_query_master_batch,
    set_query_active,
    bulk_upsert_query_master,
    bulk_update_query_master,
    split_publish_months,

    # entity mapping
    upsert_entity_mapping,
    get_entity_name_en,
    get_all_entity_mappings,
    delete_entity_mapping_batch,
    load_entity_mapping_from_excel,
    build_entity_mapping_template_bytes,

    # source mapping
    upsert_source_mapping,
    get_source_url,
    get_all_source_mappings,
    delete_source_mapping_batch,
    load_source_mapping_from_excel,
    build_source_mapping_template_bytes,

    # manual / upload
    save_manual_submission,
    import_monthly_results_excel,
    import_query_master_excel,
    build_query_master_template_bytes,
    build_monthly_results_template_bytes,

    # submission
    get_all_submissions,
    bulk_delete_submissions,

    # raw master data
    get_all_presence_records,
    get_all_source_records,
    bulk_delete_presence_records,
    bulk_delete_source_records,
)


QUERY_TYPES = [
    "Generic Query",
    "Channel Query",
    "Reputation Query",
]

PLATFORMS = [
    "Doubao",
    "DeepSeek",
    "Kimi",
    "Qianwen",
    "Yuanbao",
    "Wenxinyiyan",
]

CATEGORIES = [
    "Cream",
    "Butter",
    "Chocolate",
    "Fruit Puree",
    "Cheese",
    "Finished",
    "Semi Finished Product",
    "MySINODIS",
]

MONTHS = [
    "2026-02", "2026-03", "2026-04", "2026-05",
    "2026-06", "2026-07", "2026-08", "2026-09",
    "2026-10", "2026-11", "2026-12",
]

SUBMISSION_STATUS = [
    "draft",
    "archived",
]

PRESENCE_EDITOR_COLUMNS = ["entity_name_cn", "entity_name_en", "position"]
PRESENCE_EDITOR_DEFAULTS = {"entity_name_cn": "", "entity_name_en": "", "position": ""}

SOURCE_EDITOR_COLUMNS = ["source_name", "source_url", "occurrence_number", "quoted_or_not", "quoted_url"]
SOURCE_EDITOR_DEFAULTS = {"source_name": "", "source_url": "", "occurrence_number": "", "quoted_or_not": "", "quoted_url": ""}


# =========================================================
# Helpers
# =========================================================
def _safe_df(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    if df is None or df.empty:
        if columns:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame()
    return df.copy()


def _norm(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"none", "nan"}:
        return ""
    return s


def _download_template_button(label: str, data: bytes, file_name: str, key: str):
    st.download_button(
        label=label,
        data=data,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=key,
    )


def _current_project_id() -> int | None:
    project_id = st.session_state.get("current_project_id")
    if project_id in (None, ""):
        return None
    return int(project_id)


def _set_monthly_import_feedback(message_type: str, message: str):
    st.session_state.monthly_import_result_type = message_type
    st.session_state.monthly_import_result_message = message


def _render_monthly_import_feedback():
    message = st.session_state.get("monthly_import_result_message", "")
    message_type = st.session_state.get("monthly_import_result_type", "")
    if not message or not message_type:
        return

    if message_type == "success":
        st.success(message)
    elif message_type == "warning":
        st.warning(message)
    else:
        st.info(message)

    debug_info = st.session_state.get("monthly_import_debug_info")
    if debug_info:
        st.write(
            {
                "sheet_names": debug_info.get("sheet_names", []),
                "presence_columns": debug_info.get("presence_columns", []),
                "source_columns": debug_info.get("source_columns", []),
                "distinct_uploaded_query_numbers": debug_info.get("distinct_uploaded_query_numbers", []),
                "distinct_uploaded_created_by": debug_info.get("distinct_uploaded_created_by", []),
                "distinct_uploaded_record_month": debug_info.get("distinct_uploaded_record_month", []),
                "distinct_uploaded_ai_platform": debug_info.get("distinct_uploaded_ai_platform", []),
            }
        )

        st.caption("Presence dtypes")
        st.dataframe(
            pd.DataFrame(
                [{"column": k, "dtype": v} for k, v in debug_info.get("presence_dtypes", {}).items()]
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.caption("Source dtypes")
        st.dataframe(
            pd.DataFrame(
                [{"column": k, "dtype": v} for k, v in debug_info.get("source_dtypes", {}).items()]
            ),
            use_container_width=True,
            hide_index=True,
        )

        if debug_info.get("presence_head_raw"):
            st.caption("Presence raw head()")
            st.dataframe(pd.DataFrame(debug_info["presence_head_raw"]), use_container_width=True, hide_index=True)

        if debug_info.get("presence_head_normalized"):
            st.caption("Presence normalized head()")
            st.dataframe(pd.DataFrame(debug_info["presence_head_normalized"]), use_container_width=True, hide_index=True)

        if debug_info.get("source_head_raw"):
            st.caption("Source raw head()")
            st.dataframe(pd.DataFrame(debug_info["source_head_raw"]), use_container_width=True, hide_index=True)

        if debug_info.get("source_head_normalized"):
            st.caption("Source normalized head()")
            st.dataframe(pd.DataFrame(debug_info["source_head_normalized"]), use_container_width=True, hide_index=True)

        if debug_info.get("inserted_presence_sample"):
            st.caption("Inserted presence sample")
            st.dataframe(pd.DataFrame(debug_info["inserted_presence_sample"]), use_container_width=True, hide_index=True)

        if debug_info.get("inserted_source_sample"):
            st.caption("Inserted source sample")
            st.dataframe(pd.DataFrame(debug_info["inserted_source_sample"]), use_container_width=True, hide_index=True)


def _normalize_editor_df(df: pd.DataFrame, columns: list[str], defaults: dict) -> pd.DataFrame:
    df = _safe_df(df, columns=columns)
    if df.empty:
        return pd.DataFrame([defaults], columns=columns)

    for col in columns:
        if col not in df.columns:
            df[col] = defaults.get(col, "")

    df = df[columns].copy()
    df = df.dropna(how="all")

    if df.empty:
        return pd.DataFrame([defaults], columns=columns)

    return df.reset_index(drop=True)


def _row_has_meaningful_input(row: pd.Series, columns: list[str]) -> bool:
    for col in columns:
        value = row.get(col, "")
        if pd.isna(value):
            continue
        if _norm(value) != "":
            return True
    return False


def _prepare_editor_grid(
    df: pd.DataFrame,
    columns: list[str],
    defaults: dict,
    completion_columns: list[str],
) -> pd.DataFrame:
    normalized = _normalize_editor_df(df, columns, defaults)
    records = normalized.to_dict(orient="records")

    if not records:
        records = [defaults.copy()]
    else:
        while len(records) > 1 and not _row_has_meaningful_input(pd.Series(records[-1]), columns) and not _row_has_meaningful_input(pd.Series(records[-2]), columns):
            records.pop()

        last_row = pd.Series(records[-1])
        row_is_complete = all(_norm(last_row.get(col, "")) != "" for col in completion_columns)
        if row_is_complete:
            records.append(defaults.copy())

    return pd.DataFrame(records, columns=columns).reset_index(drop=True)


def _sanitize_presence_editor_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _safe_df(df, PRESENCE_EDITOR_COLUMNS)
    if normalized.empty:
        return pd.DataFrame([PRESENCE_EDITOR_DEFAULTS.copy()], columns=PRESENCE_EDITOR_COLUMNS)

    cleaned_rows = []
    for _, row in normalized.iterrows():
        cn = _norm(row.get("entity_name_cn", ""))
        en = _norm(row.get("entity_name_en", ""))
        position = _norm(row.get("position", ""))

        if not cn and not en:
            cleaned_rows.append(PRESENCE_EDITOR_DEFAULTS.copy())
        else:
            cleaned_rows.append(
                {
                    "entity_name_cn": cn,
                    "entity_name_en": en,
                    "position": position,
                }
            )

    return pd.DataFrame(cleaned_rows, columns=PRESENCE_EDITOR_COLUMNS)


def _sanitize_source_editor_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = _safe_df(df, SOURCE_EDITOR_COLUMNS)
    if normalized.empty:
        return pd.DataFrame([SOURCE_EDITOR_DEFAULTS.copy()], columns=SOURCE_EDITOR_COLUMNS)

    cleaned_rows = []
    for _, row in normalized.iterrows():
        source_name = _norm(row.get("source_name", ""))
        source_url = _norm(row.get("source_url", ""))
        occurrence_number = _norm(row.get("occurrence_number", ""))
        quoted_or_not = _norm(row.get("quoted_or_not", ""))
        quoted_url = _norm(row.get("quoted_url", ""))

        if not source_name and not source_url and not quoted_url:
            cleaned_rows.append(SOURCE_EDITOR_DEFAULTS.copy())
        else:
            cleaned_rows.append(
                {
                    "source_name": source_name,
                    "source_url": source_url,
                    "occurrence_number": occurrence_number,
                    "quoted_or_not": quoted_or_not,
                    "quoted_url": quoted_url,
                }
            )

    return pd.DataFrame(cleaned_rows, columns=SOURCE_EDITOR_COLUMNS)


def _sync_editor_state_value(
    state_key: str,
    widget_key: str,
    df: pd.DataFrame,
    sanitizer,
    columns: list[str],
    defaults: dict,
    completion_columns: list[str],
):
    original = _safe_df(df, columns=columns)
    normalized = _prepare_editor_grid(
        sanitizer(original),
        columns,
        defaults,
        completion_columns=completion_columns,
    )
    st.session_state[state_key] = normalized


def _init_editor_state():
    defaults = {
        "presence_form_entity_name_cn": "",
        "presence_form_entity_name_en": "",
        "presence_form_position": "",
        "source_form_source_name": "",
        "source_form_source_url": "",
        "source_form_occurrence_number": "",
        "source_form_quoted_or_not": "",
        "source_form_quoted_url": "",
    }

    if st.session_state.pop("reset_presence_form", False):
        for key in [
            "presence_form_entity_name_cn",
            "presence_form_entity_name_en",
            "presence_form_position",
        ]:
            st.session_state[key] = ""

    if st.session_state.pop("reset_source_form", False):
        for key in [
            "source_form_source_name",
            "source_form_source_url",
            "source_form_occurrence_number",
            "source_form_quoted_or_not",
            "source_form_quoted_url",
        ]:
            st.session_state[key] = ""

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    if "manual_presence_records" not in st.session_state:
        st.session_state.manual_presence_records = []
    if "manual_source_records" not in st.session_state:
        st.session_state.manual_source_records = []


def _reset_presence_form():
    st.session_state.reset_presence_form = True


def _reset_source_form():
    st.session_state.reset_source_form = True


def _reset_editor_state():
    _reset_presence_form()
    _reset_source_form()
    st.session_state.manual_presence_records = []
    st.session_state.manual_source_records = []


def _autofill_presence_entity_en(df: pd.DataFrame) -> pd.DataFrame:
    df = _safe_df(df)
    if df.empty:
        return df

    project_id = _current_project_id()
    if project_id is None:
        return df

    for idx, row in df.iterrows():
        cn = _norm(row.get("entity_name_cn", ""))
        en = _norm(row.get("entity_name_en", ""))
        if cn and not en:
            mapped_en = get_entity_name_en(project_id, cn)
            if mapped_en:
                df.at[idx, "entity_name_en"] = mapped_en
    return df


def _autofill_source_url(df: pd.DataFrame) -> pd.DataFrame:
    df = _safe_df(df)
    if df.empty:
        return df

    project_id = _current_project_id()
    if project_id is None:
        return df

    for idx, row in df.iterrows():
        source_name = _norm(row.get("source_name", ""))
        source_url = _norm(row.get("source_url", ""))
        if source_name and not source_url:
            mapped_url = get_source_url(project_id, source_name)
            if mapped_url:
                df.at[idx, "source_url"] = mapped_url
    return df


def _presence_unmapped_messages(df: pd.DataFrame):
    df = _safe_df(df)
    project_id = _current_project_id()
    if project_id is None:
        return []
    messages = []
    for _, row in df.iterrows():
        cn = _norm(row.get("entity_name_cn", ""))
        if not cn:
            continue
        mapped_en = get_entity_name_en(project_id, cn)
        if not mapped_en:
            messages.append(f"Unmapped entity: {cn}")
    return list(dict.fromkeys(messages))


def _source_unmapped_messages(df: pd.DataFrame):
    df = _safe_df(df)
    project_id = _current_project_id()
    if project_id is None:
        return []
    messages = []
    for _, row in df.iterrows():
        source_name = _norm(row.get("source_name", ""))
        if not source_name:
            continue
        mapped_url = get_source_url(project_id, source_name)
        if not mapped_url:
            messages.append(f"Unmapped source: {source_name}")
    return list(dict.fromkeys(messages))


def _clean_presence_rows(df: pd.DataFrame):
    df = _safe_df(df)
    rows = []

    for _, row in df.iterrows():
        cn = _norm(row.get("entity_name_cn", ""))
        en = _norm(row.get("entity_name_en", ""))
        position = row.get("position", None)

        if not cn and not en:
            continue

        if not cn:
            raise ValueError("Entity Name CN is required in Presence Records.")
        if not en:
            raise ValueError(f"Entity Name EN is required for unmapped brand: {cn}")
        if pd.isna(position) or _norm(position) == "":
            raise ValueError("Position is required in Presence Records.")

        rows.append(
            {
                "entity_name_cn": cn,
                "entity_name_en": en,
                "position": int(position),
            }
        )

    return rows


def _clean_source_rows(df: pd.DataFrame):
    df = _safe_df(df)
    rows = []

    for _, row in df.iterrows():
        source_name = _norm(row.get("source_name", ""))
        source_url = _norm(row.get("source_url", ""))
        occurrence_number = row.get("occurrence_number", None)
        quoted_or_not = _norm(row.get("quoted_or_not", "")).upper()
        quoted_url = _norm(row.get("quoted_url", ""))

        if not source_name:
            continue

        if not source_url:
            raise ValueError(f"Source URL is required for unmapped source: {source_name}")
        if pd.isna(occurrence_number) or _norm(occurrence_number) == "":
            raise ValueError("Occurrence Number is required in Source Records.")

        rows.append(
            {
                "source_name": source_name,
                "source_url": source_url,
                "occurrence_number": int(occurrence_number),
                "quoted_or_not": quoted_or_not,
                "quoted_url": quoted_url,
            }
        )

    return rows


def _render_query_info_card(query_info: dict):
    if not query_info:
        st.warning("Selected query not found.")
        return

    c1, c2 = st.columns(2)
    with c1:
        st.text_input("Query Type", value=query_info.get("query_type", ""), disabled=True)
        st.text_input("Query Name CN", value=query_info.get("query_name_cn", ""), disabled=True)
    with c2:
        st.text_input("Query Name EN", value=query_info.get("query_name_en", ""), disabled=True)
        st.text_input("Product Category", value=query_info.get("product_category", ""), disabled=True)

def _presence_form_autofill_en():
    project_id = _current_project_id()
    if project_id is None:
        return

    cn = _norm(st.session_state.get("presence_form_entity_name_cn", ""))
    current_en = _norm(st.session_state.get("presence_form_entity_name_en", ""))
    if not cn:
        st.session_state.presence_form_entity_name_en = ""
        return

    mapped_en = get_entity_name_en(project_id, cn)
    if mapped_en and not current_en:
        st.session_state.presence_form_entity_name_en = mapped_en


def _source_form_autofill_url():
    project_id = _current_project_id()
    if project_id is None:
        return

    source_name = _norm(st.session_state.get("source_form_source_name", ""))
    current_url = _norm(st.session_state.get("source_form_source_url", ""))
    if not source_name:
        st.session_state.source_form_source_url = ""
        return

    mapped_url = get_source_url(project_id, source_name)
    if mapped_url and not current_url:
        st.session_state.source_form_source_url = mapped_url


def _render_presence_records_table():
    records = st.session_state.get("manual_presence_records", [])
    if not records:
        st.info("No presence records added yet.")
        return

    display_df = pd.DataFrame(records)
    display_df.insert(0, "selected", False)
    edited_df = st.data_editor(
        display_df,
        use_container_width=True,
        hide_index=True,
        key="manual_presence_records_editor",
        column_config={
            "selected": st.column_config.CheckboxColumn("Select"),
            "entity_name_cn": st.column_config.TextColumn("Entity Name CN"),
            "entity_name_en": st.column_config.TextColumn("Entity Name EN"),
            "position": st.column_config.TextColumn("Position"),
        },
        disabled=[col for col in display_df.columns if col != "selected"],
    )
    selected_indices = edited_df.index[edited_df["selected"] == True].tolist()
    if st.button("Delete Selected Presence Records", use_container_width=True, key="delete_manual_presence_rows"):
        if not selected_indices:
            st.error("Please select at least one presence record.")
        else:
            st.session_state.manual_presence_records = [
                row for idx, row in enumerate(records) if idx not in selected_indices
            ]
            st.rerun()


def _render_source_records_table():
    records = st.session_state.get("manual_source_records", [])
    if not records:
        st.info("No source records added yet.")
        return

    display_df = pd.DataFrame(records)
    display_df.insert(0, "selected", False)
    edited_df = st.data_editor(
        display_df,
        use_container_width=True,
        hide_index=True,
        key="manual_source_records_editor",
        column_config={
            "selected": st.column_config.CheckboxColumn("Select"),
            "source_name": st.column_config.TextColumn("Source Name"),
            "source_url": st.column_config.TextColumn("Source URL"),
            "occurrence_number": st.column_config.TextColumn("Occurrence Number"),
            "quoted_or_not": st.column_config.TextColumn("Quoted Or Not"),
            "quoted_url": st.column_config.TextColumn("Quoted URL"),
        },
        disabled=[col for col in display_df.columns if col != "selected"],
    )
    selected_indices = edited_df.index[edited_df["selected"] == True].tolist()
    if st.button("Delete Selected Source Records", use_container_width=True, key="delete_manual_source_rows"):
        if not selected_indices:
            st.error("Please select at least one source record.")
        else:
            st.session_state.manual_source_records = [
                row for idx, row in enumerate(records) if idx not in selected_indices
            ]
            st.rerun()


# =========================================================
# Data Entry Page
# =========================================================
def render_manual_entry():
    st.subheader("Manual Entry")
    st.caption("Record monthly presence and source results based on existing Query Master.")

    _init_editor_state()

    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return

    queries_df = get_all_queries(project_id=project_id, active_only=True)
    if queries_df.empty:
        st.info("No active query is available. Please create Query Master first in Data Record.")
        return

    query_numbers = queries_df["query_number"].tolist()

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        record_month = st.selectbox("Record Month", MONTHS, key="manual_record_month")
    with top2:
        ai_platform = st.selectbox("AI Platform", PLATFORMS, key="manual_ai_platform")
    with top3:
        check_date = st.date_input("Check Date", key="manual_check_date")
    with top4:
        created_by = st.text_input("Created By", key="manual_created_by", placeholder="e.g. Alice")

    selected_query = st.selectbox("Query Number", query_numbers, key="manual_query_number")
    query_info = get_query_by_number(selected_query, project_id=project_id)
    _render_query_info_card(query_info)

    st.markdown("### Presence Records")
    p1, p2, p3, p4 = st.columns([2.2, 2.2, 1.2, 1.1])
    with p1:
        st.text_input(
            "Entity Name CN",
            key="presence_form_entity_name_cn",
            on_change=_presence_form_autofill_en,
        )
    with p2:
        st.text_input("Entity Name EN", key="presence_form_entity_name_en")
    with p3:
        st.text_input("Position", key="presence_form_position")
    with p4:
        st.markdown("<div style='height: 1.75rem;'></div>", unsafe_allow_html=True)
        if st.button("Add", use_container_width=True, key="add_presence_record"):
            try:
                presence_row = _clean_presence_rows(pd.DataFrame([{
                    "entity_name_cn": st.session_state.get("presence_form_entity_name_cn", ""),
                    "entity_name_en": st.session_state.get("presence_form_entity_name_en", ""),
                    "position": st.session_state.get("presence_form_position", ""),
                }]))
                st.session_state.manual_presence_records.extend(presence_row)
                _reset_presence_form()
                st.rerun()
            except Exception as e:
                st.error(str(e))

    current_presence_cn = _norm(st.session_state.get("presence_form_entity_name_cn", ""))
    current_presence_en = _norm(st.session_state.get("presence_form_entity_name_en", ""))
    if current_presence_cn and not current_presence_en:
        st.error(f"Unmapped entity: {current_presence_cn}")

    _render_presence_records_table()

    st.markdown("### Source Records")
    s1, s2, s3, s4, s5, s6 = st.columns([2.0, 2.0, 1.3, 1.3, 1.8, 1.0])
    with s1:
        st.text_input(
            "Source Name",
            key="source_form_source_name",
            on_change=_source_form_autofill_url,
        )
    with s2:
        st.text_input("Source URL", key="source_form_source_url")
    with s3:
        st.text_input("Occurrence Number", key="source_form_occurrence_number")
    with s4:
        st.selectbox("Quoted Or Not", options=["", "Y", "N"], key="source_form_quoted_or_not")
    with s5:
        st.text_input("Quoted URL", key="source_form_quoted_url")
    with s6:
        st.markdown("<div style='height: 1.75rem;'></div>", unsafe_allow_html=True)
        if st.button("Add", use_container_width=True, key="add_source_record"):
            try:
                source_row = _clean_source_rows(pd.DataFrame([{
                    "source_name": st.session_state.get("source_form_source_name", ""),
                    "source_url": st.session_state.get("source_form_source_url", ""),
                    "occurrence_number": st.session_state.get("source_form_occurrence_number", ""),
                    "quoted_or_not": st.session_state.get("source_form_quoted_or_not", ""),
                    "quoted_url": st.session_state.get("source_form_quoted_url", ""),
                }]))
                st.session_state.manual_source_records.extend(source_row)
                _reset_source_form()
                st.rerun()
            except Exception as e:
                st.error(str(e))

    current_source_name = _norm(st.session_state.get("source_form_source_name", ""))
    current_source_url = _norm(st.session_state.get("source_form_source_url", ""))
    if current_source_name and not current_source_url:
        st.error(f"Unmapped source: {current_source_name}")

    _render_source_records_table()

    notes = st.text_area("Submission Notes", placeholder="Optional notes...")

    c1, c2 = st.columns([1, 1])
    with c1:
        save_clicked = st.button("Save Submission", type="primary", use_container_width=True)
    with c2:
        clear_clicked = st.button("Clear Tables", use_container_width=True)

    if clear_clicked:
        _reset_editor_state()
        st.success("Entry tables cleared.")
        st.rerun()

    if save_clicked:
        try:
            if not _norm(created_by):
                st.error("Created By is required.")
                return

            presence_rows = _clean_presence_rows(pd.DataFrame(st.session_state.get("manual_presence_records", [])))
            source_rows = _clean_source_rows(pd.DataFrame(st.session_state.get("manual_source_records", [])))

            submission_id = save_manual_submission(
                project_id=project_id,
                query_number=selected_query,
                record_month=record_month,
                ai_platform=ai_platform,
                check_date=str(check_date),
                created_by=_norm(created_by),
                presence_rows=presence_rows,
                source_rows=source_rows,
                notes=_norm(notes),
            )

            _reset_editor_state()
            st.success(f"Submission saved successfully: {submission_id}")

        except Exception as e:
            st.error(f"Failed to save submission: {e}")


def render_excel_upload():
    st.subheader("Excel Upload")
    st.caption("Upload monthly result template with two sheets: presence_records and source_records.")

    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return

    _download_template_button(
        "Download Monthly Result Template",
        build_monthly_results_template_bytes(),
        "monthly_result_template.xlsx",
        "download_monthly_template",
    )

    uploaded_file = st.file_uploader(
        "Upload Monthly Result Excel",
        type=["xlsx"],
        key="monthly_result_upload",
    )

    _render_monthly_import_feedback()
    st.text("EXCEL DEBUG VISIBLE")

    if uploaded_file is not None:
        if st.button("Import Monthly Result Excel", type="primary"):
            try:
                result = import_monthly_results_excel(uploaded_file, project_id=project_id)
                st.session_state.monthly_import_debug_info = result.get("debug_info", {})
                inserted_submissions = int(result.get("submissions", 0))
                inserted_presence = int(result.get("presence_records", 0))
                inserted_source = int(result.get("source_records", 0))
                updated_submissions = int(result.get("updated_submissions", 0))
                updated_presence = int(result.get("updated_presence_records", 0))
                updated_source = int(result.get("updated_source_records", 0))
                skipped_submissions = int(result.get("skipped_duplicate_submissions", 0))
                skipped_presence = int(result.get("skipped_duplicate_presence_records", 0))
                skipped_source = int(result.get("skipped_duplicate_source_records", 0))

                wrote_any_data = (inserted_submissions + inserted_presence + inserted_source + updated_submissions + updated_presence + updated_source) > 0
                skipped_any_duplicates = (skipped_submissions + skipped_presence + skipped_source) > 0

                if wrote_any_data and skipped_any_duplicates:
                    _set_monthly_import_feedback(
                        "success",
                        f"Import completed with duplicates skipped. "
                        f"Added {inserted_submissions} submissions, {inserted_presence} presence records, {inserted_source} source records. "
                        f"Skipped {skipped_submissions} duplicate submissions, {skipped_presence} duplicate presence records, {skipped_source} duplicate source records."
                    )
                elif wrote_any_data:
                    if (updated_submissions + updated_presence + updated_source) > 0 and (inserted_submissions + inserted_presence + inserted_source) == 0:
                        _set_monthly_import_feedback(
                            "success",
                            f"Import completed. "
                            f"Updated {updated_submissions} submissions, {updated_presence} presence records, {updated_source} source records."
                        )
                    else:
                        _set_monthly_import_feedback(
                            "success",
                            f"Import successful. "
                            f"Added {inserted_submissions} submissions, {inserted_presence} presence records, {inserted_source} source records."
                        )
                else:
                    _set_monthly_import_feedback(
                        "info",
                        "No new data imported. All uploaded records already exist.",
                    )
                st.rerun()
            except Exception as e:
                st.session_state.monthly_import_debug_info = {}
                _set_monthly_import_feedback("warning", f"Import failed: {e}")
                st.error(f"Import failed: {e}")


def render_data_entry_page():
    tab1, tab2 = st.tabs(["Manual Entry", "Excel Upload"])
    with tab1:
        render_manual_entry()
    with tab2:
        render_excel_upload()


# =========================================================
# Data Record - Query Master
# =========================================================
def render_query_master_manager():
    st.subheader("Query Master")
    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return

    c_upload1, c_upload2 = st.columns(2)
    with c_upload1:
        uploaded_qm = st.file_uploader("Upload Query Master Excel", type=["xlsx"], key="query_master_upload")
        if uploaded_qm is not None and st.button("Import Query Master Excel", use_container_width=True, key="import_query_master_btn"):
            try:
                result = import_query_master_excel(uploaded_qm, project_id=project_id)
                st.success(
                    f"Query Master import successful. "
                    f"Inserted: {result['query_master']}, "
                    f"Updated existing: {result['updated_existing']}"
                )
                st.rerun()
            except Exception as e:
                st.error(f"Failed to import Query Master: {e}")
    with c_upload2:
        _download_template_button(
            "Download Query Master Template",
            build_query_master_template_bytes(),
            "query_master_template.xlsx",
            "download_query_master_template",
        )

    with st.form("query_master_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            query_number = st.text_input("Query Number", placeholder="Q001")
            query_type = st.selectbox("Query Type", QUERY_TYPES)
            query_name_cn = st.text_input("Query Name CN")
        with c2:
            query_name_en = st.text_input("Query Name EN")
            product_category = st.selectbox("Product Category", CATEGORIES)
            publish_month_default = st.multiselect(
                "Publish Month Default",
                options=MONTHS,
                default=[],
            )

        active = st.selectbox("Active", options=[1, 0], format_func=lambda x: "Yes" if x == 1 else "No")
        submitted = st.form_submit_button("Save Query Master", type="primary")

        if submitted:
            try:
                if not _norm(query_number):
                    st.error("Query Number is required.")
                elif not _norm(query_name_cn):
                    st.error("Query Name CN is required.")
                else:
                    upsert_query_master(
                        project_id=project_id,
                        query_number=_norm(query_number),
                        query_type=_norm(query_type),
                        query_name_cn=_norm(query_name_cn),
                        query_name_en=_norm(query_name_en),
                        product_category=_norm(product_category),
                        publish_month_default=publish_month_default,
                        active=int(active),
                    )
                    st.success("Query Master saved.")
                    st.rerun()
            except Exception as e:
                st.error(f"Failed to save Query Master: {e}")

    st.markdown("### Current Query Master")
    queries_df = get_all_queries(project_id=project_id, active_only=False)
    if queries_df.empty:
        st.info("No query master records yet.")
    else:
        display_df = queries_df.copy()
        display_df.insert(0, "selected", False)

        edited_queries = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            key="query_master_editor",
            column_config={
                "selected": st.column_config.CheckboxColumn("Select"),
                "query_number": st.column_config.TextColumn("Query Number"),
                "query_type": st.column_config.TextColumn("Query Type"),
                "query_name_cn": st.column_config.TextColumn("Query Name CN"),
                "query_name_en": st.column_config.TextColumn("Query Name EN"),
                "product_category": st.column_config.TextColumn("Product Category"),
                "publish_month_default": st.column_config.TextColumn("Publish Month"),
                "active": st.column_config.NumberColumn("Active"),
                "updated_at": st.column_config.TextColumn("Updated At"),
            },
            disabled=[col for col in display_df.columns if col != "selected"],
        )

        selected_rows = edited_queries[edited_queries["selected"] == True].copy()
        selected_query_numbers = selected_rows["query_number"].tolist() if not selected_rows.empty else []

        if "show_delete_query_master_confirm" not in st.session_state:
            st.session_state.show_delete_query_master_confirm = False

        st.caption("勾选一行或多行后，在下面统一编辑。Publish Month 现在是多选值。")

        action_col1, action_col2 = st.columns(2)
        with action_col1:
            if st.button("Delete Selected", use_container_width=True, key="qm_delete_selected"):
                if not selected_query_numbers:
                    st.error("Please select at least one record.")
                else:
                    st.session_state.show_delete_query_master_confirm = True

            if st.session_state.show_delete_query_master_confirm:
                st.warning("Are you sure you want to delete the selected query records?")
                d1, d2 = st.columns(2)
                with d1:
                    if st.button("Confirm Delete", type="primary", use_container_width=True, key="qm_confirm_delete"):
                        if not selected_query_numbers:
                            st.session_state.show_delete_query_master_confirm = False
                            st.error("Please select at least one record.")
                        else:
                            try:
                                deleted_count = delete_query_master_batch(
                                    project_id=project_id,
                                    query_numbers=selected_query_numbers,
                                )
                                st.session_state.show_delete_query_master_confirm = False
                                st.success(f"Successfully deleted {deleted_count} query record(s).")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to delete Query Master: {e}")
                with d2:
                    if st.button("Cancel", use_container_width=True, key="qm_cancel_delete"):
                        st.session_state.show_delete_query_master_confirm = False
                        st.rerun()

        with action_col2:
            with st.expander("Edit Selected Query Master", expanded=False):
                default_query_type = selected_rows["query_type"].iloc[0] if len(selected_rows) == 1 else QUERY_TYPES[0]
                default_query_name_cn = selected_rows["query_name_cn"].iloc[0] if len(selected_rows) == 1 else ""
                default_query_name_en = selected_rows["query_name_en"].iloc[0] if len(selected_rows) == 1 else ""
                default_product_category = selected_rows["product_category"].iloc[0] if len(selected_rows) == 1 else CATEGORIES[0]
                default_publish_months = split_publish_months(selected_rows["publish_month_default"].iloc[0]) if len(selected_rows) == 1 else []
                default_active = int(selected_rows["active"].iloc[0]) if len(selected_rows) == 1 else 1

                c1, c2 = st.columns(2)

                with c1:
                    edit_query_type = st.selectbox(
                        "Edit Query Type",
                        QUERY_TYPES,
                        index=QUERY_TYPES.index(default_query_type) if default_query_type in QUERY_TYPES else 0,
                        key="qm_edit_query_type",
                    )
                    edit_query_name_cn = st.text_input(
                        "Edit Query Name CN",
                        value=default_query_name_cn,
                        key="qm_edit_query_name_cn",
                    )
                    edit_query_name_en = st.text_input(
                        "Edit Query Name EN",
                        value=default_query_name_en,
                        key="qm_edit_query_name_en",
                    )

                with c2:
                    edit_product_category = st.selectbox(
                        "Edit Product Category",
                        CATEGORIES,
                        index=CATEGORIES.index(default_product_category) if default_product_category in CATEGORIES else 0,
                        key="qm_edit_product_category",
                    )
                    edit_publish_month_default = st.multiselect(
                        "Edit Publish Month",
                        options=MONTHS,
                        default=default_publish_months,
                        key="qm_edit_publish_month",
                    )
                    edit_active = st.selectbox(
                        "Edit Active",
                        options=[1, 0],
                        index=0 if default_active == 1 else 1,
                        format_func=lambda x: "Yes" if x == 1 else "No",
                        key="qm_edit_active",
                    )

                use_query_type = st.checkbox("Update Query Type", key="qm_use_query_type")
                use_query_name_cn = st.checkbox("Update Query Name CN", key="qm_use_query_name_cn")
                use_query_name_en = st.checkbox("Update Query Name EN", key="qm_use_query_name_en")
                use_product_category = st.checkbox("Update Product Category", key="qm_use_product_category")
                use_publish_month = st.checkbox("Update Publish Month", key="qm_use_publish_month")
                use_active = st.checkbox("Update Active", key="qm_use_active")

                if st.button("Apply Edit to Selected Query Master", use_container_width=True, key="qm_apply_edit"):
                    if not selected_query_numbers:
                        st.error("Please select at least one query.")
                    else:
                        try:
                            update_fields = {}

                            if use_query_type:
                                update_fields["query_type"] = edit_query_type

                            if use_query_name_cn:
                                if not _norm(edit_query_name_cn):
                                    st.error("Query Name CN cannot be empty.")
                                    st.stop()
                                update_fields["query_name_cn"] = _norm(edit_query_name_cn)

                            if use_query_name_en:
                                update_fields["query_name_en"] = _norm(edit_query_name_en)

                            if use_product_category:
                                update_fields["product_category"] = edit_product_category

                            if use_publish_month:
                                update_fields["publish_month_default"] = edit_publish_month_default

                            if use_active:
                                update_fields["active"] = int(edit_active)

                            if not update_fields:
                                st.error("Please choose at least one field to update.")
                            else:
                                bulk_update_query_master(selected_query_numbers, update_fields, project_id=project_id)
                                st.success("Selected Query Master rows updated.")
                                st.rerun()

                        except Exception as e:
                            st.error(f"Failed to update Query Master: {e}")

    st.markdown("### Archive / Activate Query")
    if not queries_df.empty:
        selected_query = st.selectbox(
            "Select Query Number",
            queries_df["query_number"].tolist(),
            key="query_master_archive_selector",
        )
        q_info = get_query_by_number(selected_query, project_id=project_id)

        if q_info:
            current_active = q_info.get("active", 1)
            c1, c2 = st.columns(2)
            with c1:
                st.write(f"Current Status: {'Active' if current_active == 1 else 'Archived'}")
            with c2:
                target_active = st.selectbox(
                    "Set Status",
                    options=[1, 0],
                    format_func=lambda x: "Active" if x == 1 else "Archived",
                    key="query_master_set_status",
                )

            if st.button("Update Query Status", use_container_width=True):
                try:
                    set_query_active(selected_query, target_active, project_id=project_id)
                    st.success("Query status updated.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update query status: {e}")


# =========================================================
# Data Record - Entity Mapping
# =========================================================
def render_entity_mapping_manager():
    st.subheader("Entity Mapping")
    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return

    st.caption("Entity Mapping is project-level and can only be loaded from Excel.")
    c1, c2 = st.columns(2)
    with c1:
        with st.form("entity_mapping_form", clear_on_submit=True):
            entity_name_cn = st.text_input("Entity Name CN")
            entity_name_en = st.text_input("Entity Name EN")
            submitted = st.form_submit_button("Save Entity Mapping", type="primary")

            if submitted:
                try:
                    if not _norm(entity_name_cn) or not _norm(entity_name_en):
                        st.error("Both Entity Name CN and Entity Name EN are required.")
                    else:
                        upsert_entity_mapping(project_id, _norm(entity_name_cn), _norm(entity_name_en))
                        st.success("Entity mapping saved.")
                        st.rerun()
                except Exception as e:
                    st.error(f"Failed to save entity mapping: {e}")

    with c2:
        _download_template_button(
            "Download Entity Mapping Template",
            build_entity_mapping_template_bytes(),
            "entity_mapping_template.xlsx",
            "download_entity_template",
        )
        uploaded_entity = st.file_uploader("Upload Entity Mapping Excel", type=["xlsx"], key="entity_mapping_upload")
        if uploaded_entity is not None and st.button("Import Entity Mapping Excel", use_container_width=True, key="import_entity_mapping_btn"):
            success, msg = load_entity_mapping_from_excel(project_id=project_id, uploaded_file=uploaded_entity)
            if success:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

    mapping_df = get_all_entity_mappings(project_id)
    if mapping_df.empty:
        st.info("No entity mappings yet.")
    else:
        if "show_delete_entity_mapping_confirm" not in st.session_state:
            st.session_state.show_delete_entity_mapping_confirm = False

        display_df = mapping_df.copy()
        display_df.insert(0, "selected", False)

        edited_df = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            key="entity_mapping_editor",
            column_config={
                "selected": st.column_config.CheckboxColumn("Select"),
                "entity_name_cn": st.column_config.TextColumn("Entity Name CN"),
                "entity_name_en": st.column_config.TextColumn("Entity Name EN"),
                "updated_at": st.column_config.TextColumn("Updated At"),
            },
            disabled=[col for col in display_df.columns if col != "selected"],
        )

        selected_rows = edited_df[edited_df["selected"] == True].copy()
        selected_entity_names = selected_rows["entity_name_cn"].tolist() if not selected_rows.empty else []

        if st.button("Delete Selected", use_container_width=True, key="entity_mapping_delete_selected"):
            if not selected_entity_names:
                st.error("Please select at least one record.")
            else:
                st.session_state.show_delete_entity_mapping_confirm = True

        if st.session_state.show_delete_entity_mapping_confirm:
            st.warning("Are you sure you want to delete the selected entity mappings?")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm Delete", type="primary", use_container_width=True, key="entity_mapping_confirm_delete"):
                    if not selected_entity_names:
                        st.session_state.show_delete_entity_mapping_confirm = False
                        st.error("Please select at least one record.")
                    else:
                        try:
                            deleted_count = delete_entity_mapping_batch(project_id=project_id, entity_name_cns=selected_entity_names)
                            st.session_state.show_delete_entity_mapping_confirm = False
                            st.success(f"Successfully deleted {deleted_count} entity mapping record(s).")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete entity mappings: {e}")
            with c2:
                if st.button("Cancel", use_container_width=True, key="entity_mapping_cancel_delete"):
                    st.session_state.show_delete_entity_mapping_confirm = False
                    st.rerun()


# =========================================================
# Data Record - Source Mapping
# =========================================================
def render_source_mapping_manager():
    st.subheader("Source Mapping")
    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return

    st.caption("Source Mapping is project-level and can only be loaded from Excel.")
    c1, c2 = st.columns(2)
    with c1:
        with st.form("source_mapping_form", clear_on_submit=True):
            source_name = st.text_input("Source Name")
            source_url = st.text_input("Source URL", placeholder="e.g. zhihu.com")
            submitted = st.form_submit_button("Save Source Mapping", type="primary")

            if submitted:
                try:
                    if not _norm(source_name) or not _norm(source_url):
                        st.error("Both Source Name and Source URL are required.")
                    else:
                        upsert_source_mapping(project_id, _norm(source_name), _norm(source_url))
                        st.success("Source mapping saved.")
                        st.rerun()
                except Exception as e:
                    st.error(f"Failed to save source mapping: {e}")

    with c2:
        _download_template_button(
            "Download Source Mapping Template",
            build_source_mapping_template_bytes(),
            "source_mapping_template.xlsx",
            "download_source_template",
        )
        uploaded_source = st.file_uploader("Upload Source Mapping Excel", type=["xlsx"], key="source_mapping_upload")
        if uploaded_source is not None and st.button("Import Source Mapping Excel", use_container_width=True, key="import_source_mapping_btn"):
            success, msg = load_source_mapping_from_excel(project_id=project_id, uploaded_file=uploaded_source)
            if success:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

    mapping_df = get_all_source_mappings(project_id)
    if mapping_df.empty:
        st.info("No source mappings yet.")
    else:
        if "show_delete_source_mapping_confirm" not in st.session_state:
            st.session_state.show_delete_source_mapping_confirm = False

        display_df = mapping_df.copy()
        display_df.insert(0, "selected", False)

        edited_df = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            key="source_mapping_editor",
            column_config={
                "selected": st.column_config.CheckboxColumn("Select"),
                "source_name": st.column_config.TextColumn("Source Name"),
                "source_url": st.column_config.TextColumn("Source URL"),
                "updated_at": st.column_config.TextColumn("Updated At"),
            },
            disabled=[col for col in display_df.columns if col != "selected"],
        )

        selected_rows = edited_df[edited_df["selected"] == True].copy()
        selected_source_names = selected_rows["source_name"].tolist() if not selected_rows.empty else []

        if st.button("Delete Selected", use_container_width=True, key="source_mapping_delete_selected"):
            if not selected_source_names:
                st.error("Please select at least one record.")
            else:
                st.session_state.show_delete_source_mapping_confirm = True

        if st.session_state.show_delete_source_mapping_confirm:
            st.warning("Are you sure you want to delete the selected source mappings?")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm Delete", type="primary", use_container_width=True, key="source_mapping_confirm_delete"):
                    if not selected_source_names:
                        st.session_state.show_delete_source_mapping_confirm = False
                        st.error("Please select at least one record.")
                    else:
                        try:
                            deleted_count = delete_source_mapping_batch(project_id=project_id, source_names=selected_source_names)
                            st.session_state.show_delete_source_mapping_confirm = False
                            st.success(f"Successfully deleted {deleted_count} source mapping record(s).")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete source mappings: {e}")
            with c2:
                if st.button("Cancel", use_container_width=True, key="source_mapping_cancel_delete"):
                    st.session_state.show_delete_source_mapping_confirm = False
                    st.rerun()


# =========================================================
# Data Record - Submission Manager
# =========================================================
def render_submission_manager():
    st.subheader("Submission Manager")
    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return

    submissions_df = get_all_submissions(project_id=project_id)
    if submissions_df.empty:
        st.info("No submissions yet.")
        return

    display_df = submissions_df.copy()
    display_df.insert(0, "selected", False)

    edited_df = st.data_editor(
        display_df,
        use_container_width=True,
        hide_index=True,
        key="submission_manager_editor",
        column_config={
            "selected": st.column_config.CheckboxColumn("Select"),
        },
        disabled=[col for col in display_df.columns if col != "selected"],
    )

    selected_rows = edited_df[edited_df["selected"] == True].copy()
    selected_ids = selected_rows["submission_id"].tolist() if not selected_rows.empty else []

    if st.button("Delete Selected", use_container_width=True):
        if not selected_ids:
            st.error("Please select at least one submission.")
        else:
            try:
                bulk_delete_submissions(selected_ids, project_id=project_id)
                st.success("Selected submissions deleted.")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to delete submissions: {e}")


# =========================================================
# Data Record - Raw Data / Master Tables
# =========================================================
def render_raw_records():
    st.subheader("Raw Data")
    st.caption("This section is the consolidated master table of all Excel uploads and manual entries.")
    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return

    presence_df = get_all_presence_records(project_id=project_id)
    source_df = get_all_source_records(project_id=project_id)

    tab1, tab2 = st.tabs(["Presence Master Table", "Source Master Table"])

    with tab1:
        if presence_df.empty:
            st.info("No presence data yet.")
        else:
            p_display = presence_df.copy()
            p_display.insert(0, "selected", False)

            p_edited = st.data_editor(
                p_display,
                use_container_width=True,
                hide_index=True,
                key="presence_raw_editor",
                column_config={
                    "selected": st.column_config.CheckboxColumn("Select"),
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                },
                disabled=[col for col in p_display.columns if col != "selected"],
            )

            selected_rows = p_edited[p_edited["selected"] == True].copy()
            selected_ids = selected_rows["id"].tolist() if not selected_rows.empty else []

            if st.button(
                "Delete Selected Presence Rows",
                use_container_width=True,
                key="delete_presence_rows",
            ):
                if not selected_ids:
                    st.error("Please select at least one presence row.")
                else:
                    try:
                        bulk_delete_presence_records(selected_ids)
                        st.success("Selected presence rows deleted.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to delete presence rows: {e}")

    with tab2:
        if source_df.empty:
            st.info("No source data yet.")
        else:
            s_display = source_df.copy()
            s_display.insert(0, "selected", False)

            s_edited = st.data_editor(
                s_display,
                use_container_width=True,
                hide_index=True,
                key="source_raw_editor",
                column_config={
                    "selected": st.column_config.CheckboxColumn("Select"),
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                },
                disabled=[col for col in s_display.columns if col != "selected"],
            )

            selected_rows = s_edited[s_edited["selected"] == True].copy()
            selected_ids = selected_rows["id"].tolist() if not selected_rows.empty else []

            if st.button(
                "Delete Selected Source Rows",
                use_container_width=True,
                key="delete_source_rows",
            ):
                if not selected_ids:
                    st.error("Please select at least one source row.")
                else:
                    try:
                        bulk_delete_source_records(selected_ids)
                        st.success("Selected source rows deleted.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to delete source rows: {e}")


def render_data_record_page():
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "Query Master",
            "Entity Mapping",
            "Source Mapping",
            "Submission Manager",
            "Raw Data",
        ]
    )

    with tab1:
        render_query_master_manager()

    with tab2:
        render_entity_mapping_manager()

    with tab3:
        render_source_mapping_manager()

    with tab4:
        render_submission_manager()

    with tab5:
        render_raw_records()
    project_id = _current_project_id()
    if project_id is None:
        st.info("Please select a project first.")
        return
