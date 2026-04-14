import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.metrics import (
    compute_kpis,
    get_brand_ranking,
    get_channel_ranking,
    get_brand_visibility_by_category,
    get_brand_visibility_by_category_and_publish_month,
    get_brand_visibility_by_publish_month,
    get_brand_visibility_by_record_month,
    get_source_occurrence_ranking,
    get_source_platform_comparison,
    get_source_distribution_by_platform,
    get_quoted_source_ranking,
    get_query_count_by_publish_month,
)


BI_PRIMARY = "#2e1452"
BI_SECONDARY = "#4a2a7a"
BI_ACCENT = "#2bd6d9"
BI_WARM = "#5ee0e2"
BI_MUTED = "#8a9aaa"
BI_GRID = "rgba(46, 20, 82, 0.10)"
BI_PAPER = "#ffffff"
BI_PLOT = "#f8f9fc"
BI_FONT = "#2d2d2d"
BI_FONT_MUTED = "#6c7a89"
BI_HOVER_BG = "#f8f9fc"
BI_HOVER_BORDER = "rgba(43, 214, 217, 0.24)"
BI_LINE = "rgba(224, 229, 234, 0.95)"
BI_BAR_BORDER = "rgba(46, 20, 82, 0.10)"
BI_PLATFORM_SEQ = ["#2f80ed", "#f2c94c", "#56ccf2", "#f2994a", "#1c4fd7", "#b8860b"]
BI_SOURCE_BAR_SOFT = "#ecf8f9"
BI_SOURCE_BAR_HIGHLIGHT = "#ff7a59"
BI_LINE_SEQ = [
    "#1d4ed8",
    "#f59e0b",
    "#0ea5e9",
    "#10b981",
    "#ef4444",
    "#8b5cf6",
    "#f97316",
    "#14b8a6",
    "#ec4899",
    "#64748b",
    "#2563eb",
    "#ca8a04",
    "#059669",
    "#dc2626",
    "#7c3aed",
    "#0891b2",
]
BI_SEQ = [
    "#2e1452",
    "#4a2a7a",
    "#2bd6d9",
    "#5ee0e2",
    "#23b8bb",
    "#8a9aaa",
    "#3498db",
]


# =========================================================
# Helpers
# =========================================================
def _empty_fig(title: str = "No data available"):
    fig = go.Figure()
    fig.update_layout(
        title=title,
        template="plotly_white",
        height=420,
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        paper_bgcolor=BI_PAPER,
        plot_bgcolor=BI_PLOT,
        annotations=[
            dict(
                text="No data available",
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
        ],
    )
    return fig


def _apply_layout(fig, title: str = "", height: int = 420):
    fig.update_layout(
        title=title,
        template="plotly_white",
        height=height,
        margin=dict(l=18, r=18, t=58, b=18),
        legend_title_text="",
        paper_bgcolor=BI_PAPER,
        plot_bgcolor=BI_PLOT,
        font=dict(color=BI_FONT),
        colorway=BI_SEQ,
        title_font=dict(size=18, color=BI_PRIMARY),
        hoverlabel=dict(
            bgcolor=BI_HOVER_BG,
            bordercolor=BI_HOVER_BORDER,
            font=dict(color=BI_FONT),
        ),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=BI_GRID,
        zeroline=False,
        linecolor=BI_LINE,
        tickfont=dict(color=BI_FONT_MUTED),
        title_font=dict(color=BI_FONT_MUTED),
    )
    fig.update_yaxes(
        showgrid=False,
        zeroline=False,
        linecolor=BI_LINE,
        tickfont=dict(color=BI_FONT_MUTED),
        title_font=dict(color=BI_FONT_MUTED),
    )
    return fig


def _format_visibility_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "Coverage Rate" not in df.columns:
        return df

    out = df.copy()
    out["Coverage Rate"] = out["Coverage Rate"].apply(
        lambda v: f"{v * 100:.1f}%".replace(".0%", "%")
        if pd.notna(v) else ""
    )
    return out


def _parse_visibility_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "Coverage Rate" not in df.columns:
        return df

    out = df.copy()
    out["Coverage Rate"] = out["Coverage Rate"].apply(
        lambda v: float(str(v).replace("%", "").strip()) / 100
        if pd.notna(v) and str(v).strip() != "" else 0.0
    )
    return out


def _sort_month_df(df: pd.DataFrame, month_col: str):
    month_order = {
        "2026-02": 1,
        "2026-03": 2,
        "2026-04": 3,
        "2026-05": 4,
        "2026-06": 5,
        "2026-07": 6,
        "2026-08": 7,
        "2026-09": 8,
        "2026-10": 9,
        "2026-11": 10,
        "2026-12": 11,
    }
    out = df.copy()
    out["_sort"] = out[month_col].map(month_order).fillna(999)
    out = out.sort_values("_sort").drop(columns="_sort")
    return out


# =========================================================
# KPI Cards Data
# =========================================================
def get_kpi_cards_data(
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame,
    content_publish_df: pd.DataFrame | None = None,
) -> dict:
    kpis = compute_kpis(presence_df, source_df, content_publish_df)
    return {
        "Total Query": kpis.get("total_queries", 0),
        "source occurance": kpis.get("source_occurance", 0),
        "Quote Rate": f"{kpis.get('quote_rate', 0):.1%}",
    }


# =========================================================
# Brand / Channel Charts
# =========================================================
def build_brand_ranking_chart(presence_df: pd.DataFrame, top_n: int = 10):
    ranking_df = get_brand_ranking(presence_df)
    if ranking_df.empty:
        return _empty_fig("Brand Ranking")

    chart_df = ranking_df.head(top_n).copy()

    fig = px.bar(
        chart_df,
        x="Brand Mention",
        y="Brand",
        orientation="h",
        text="Brand Mention",
        color_discrete_sequence=[BI_PRIMARY],
        hover_data={
            "Brand": True,
            "Brand Mention": True,
            "Avg Position": True,
            "Best Position": True,
        },
    )

    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, "Brand Ranking")


def build_brand_ranking_chart_from_table(ranking_df: pd.DataFrame, top_n: int = 10):
    if ranking_df is None or ranking_df.empty:
        return _empty_fig("Brand Ranking")

    chart_df = ranking_df.head(top_n).copy()
    fig = px.bar(
        chart_df,
        x="Brand Mention",
        y="Brand",
        orientation="h",
        text="Brand Mention",
        color_discrete_sequence=[BI_PRIMARY],
        hover_data={
            "Brand": True,
            "Brand Mention": True,
            "Avg Position": True,
            "Best Position": True,
        },
    )
    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, "Brand Ranking")


def build_channel_ranking_chart(presence_df: pd.DataFrame, top_n: int = 10):
    ranking_df = get_channel_ranking(presence_df)
    if ranking_df.empty:
        return _empty_fig("Channel Ranking")

    chart_df = ranking_df.head(top_n).copy()

    fig = px.bar(
        chart_df,
        x="Channel Mention",
        y="Channel",
        orientation="h",
        text="Channel Mention",
        color_discrete_sequence=[BI_SECONDARY],
        hover_data={
            "Channel": True,
            "Channel Mention": True,
            "Avg Position": True,
        },
    )

    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, "Channel Ranking")


def build_channel_ranking_chart_from_table(ranking_df: pd.DataFrame, top_n: int = 10):
    if ranking_df is None or ranking_df.empty:
        return _empty_fig("Channel Ranking")

    chart_df = ranking_df.head(top_n).copy()
    fig = px.bar(
        chart_df,
        x="Channel Mention",
        y="Channel",
        orientation="h",
        text="Channel Mention",
        color_discrete_sequence=[BI_SECONDARY],
        hover_data={
            "Channel": True,
            "Channel Mention": True,
            "Avg Position": True,
        },
    )
    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, "Channel Ranking")


# =========================================================
# Source Charts
# =========================================================
def build_source_occurrence_chart(source_df: pd.DataFrame, top_n: int = 10):
    ranking_df = get_source_occurrence_ranking(source_df)
    if ranking_df.empty:
        return _empty_fig("Source Occurance")

    chart_df = ranking_df.head(top_n).copy()
    return build_source_occurrence_chart_from_table(chart_df, top_n=top_n)


def build_source_occurrence_chart_from_table(ranking_df: pd.DataFrame, top_n: int = 10):
    if ranking_df is None or ranking_df.empty:
        return _empty_fig("Source Occurance")

    chart_df = ranking_df.head(top_n).copy()
    chart_df = chart_df.sort_values("source occurance", ascending=False).reset_index(drop=True)

    highlight_idx = int(chart_df["source occurance"].astype(float).idxmax()) if not chart_df.empty else -1
    bar_colors = [
        BI_SOURCE_BAR_HIGHLIGHT if idx == highlight_idx else BI_SOURCE_BAR_SOFT
        for idx in chart_df.index
    ]

    fig = go.Figure(
        data=[
            go.Bar(
                x=chart_df["Source"],
                y=chart_df["source occurance"],
                marker=dict(
                    color=bar_colors,
                    line=dict(color=BI_BAR_BORDER, width=1.2),
                ),
                hovertemplate="Source=%{x}<br>source occurance=%{y}<extra></extra>",
            )
        ]
    )

    for idx, row in chart_df.iterrows():
        fig.add_annotation(
            x=row["Source"],
            y=row["source occurance"],
            text=str(row["source occurance"]),
            showarrow=False,
            yshift=14,
            font=dict(
                size=12,
                color="#ff7a59" if idx == highlight_idx else BI_FONT_MUTED,
                family="Arial",
            ),
        )

    fig.update_layout(
        title="Source Occurance",
        height=430,
        margin=dict(l=20, r=20, t=60, b=76),
        paper_bgcolor=BI_PAPER,
        plot_bgcolor=BI_PLOT,
        font=dict(color=BI_FONT, family="Arial"),
        hoverlabel=dict(
            bgcolor=BI_HOVER_BG,
            bordercolor=BI_HOVER_BORDER,
            font=dict(color=BI_FONT),
        ),
        showlegend=False,
        bargap=0.34,
        xaxis=dict(
            title="",
            tickfont=dict(color=BI_FONT_MUTED),
            showgrid=False,
            linecolor=BI_LINE,
            zeroline=False,
        ),
        yaxis=dict(
            title="source occurance",
            title_font=dict(color=BI_FONT_MUTED, size=13),
            tickfont=dict(color=BI_FONT_MUTED),
            gridcolor=BI_GRID,
            zeroline=False,
            linecolor=BI_LINE,
        ),
    )
    return fig


def build_quoted_source_chart(source_df: pd.DataFrame, top_n: int = 10):
    quoted_df = get_quoted_source_ranking(source_df)
    if quoted_df.empty:
        return _empty_fig("Quoted Source Number")

    chart_df = quoted_df.head(top_n).copy()

    fig = px.bar(
        chart_df,
        x="Quoted Source number",
        y="Source",
        orientation="h",
        text="Quoted Source number",
        color_discrete_sequence=[BI_WARM],
        hover_data={
            "Source": True,
            "Quoted Source number": True,
        },
    )

    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, "Quoted Source Number")


def build_quoted_source_chart_from_table(quoted_df: pd.DataFrame, top_n: int = 10):
    if quoted_df is None or quoted_df.empty:
        return _empty_fig("Quoted Source Number")

    chart_df = quoted_df.head(top_n).copy()
    fig = px.bar(
        chart_df,
        x="Quoted Source number",
        y="Source",
        orientation="h",
        text="Quoted Source number",
        color_discrete_sequence=[BI_WARM],
        hover_data={
            "Source": True,
            "Quoted Source number": True,
        },
    )
    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, "Quoted Source Number")


def build_source_distribution_by_platform_chart(source_df: pd.DataFrame, top_n: int = 8):
    distribution_df = get_source_distribution_by_platform(source_df, top_n=top_n)
    if distribution_df.empty:
        return _empty_fig("Source Distribution by Platform")

    fig = px.bar(
        distribution_df,
        x="Source",
        y="source occurance",
        color="AI Platform",
        barmode="group",
        text="source occurance",
        color_discrete_sequence=BI_PLATFORM_SEQ,
        hover_data={
            "Source": True,
            "AI Platform": True,
            "source occurance": True,
        },
    )

    fig.update_traces(
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1,
        textposition="outside",
        cliponaxis=False,
    )
    fig.update_xaxes(tickangle=-25)
    return _apply_layout(fig, "Source Distribution by Platform", height=460)


def build_source_distribution_by_platform_chart_from_table(distribution_df: pd.DataFrame):
    if distribution_df is None or distribution_df.empty:
        return _empty_fig("Source Distribution by Platform")

    fig = px.bar(
        distribution_df,
        x="Source",
        y="source occurance",
        color="AI Platform",
        barmode="group",
        text="source occurance",
        color_discrete_sequence=BI_PLATFORM_SEQ,
        hover_data={
            "Source": True,
            "AI Platform": True,
            "source occurance": True,
        },
    )
    fig.update_traces(
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1,
        textposition="outside",
        cliponaxis=False,
    )
    fig.update_xaxes(tickangle=-25)
    return _apply_layout(fig, "Source Distribution by Platform", height=460)


# =========================================================
# Visibility Charts
# =========================================================
def build_brand_visibility_by_category_chart(
    presence_df: pd.DataFrame,
    category: str,
    top_n: int = 10,
    score_col: str = "Visibility Score",
):
    visibility_df = get_brand_visibility_by_category(presence_df, category)
    if visibility_df.empty:
        return _empty_fig(f"Brand Visibility - {category}")

    chart_df = visibility_df.head(top_n).copy()

    if score_col not in chart_df.columns:
        score_col = "Visibility Score"

    fig = px.bar(
        chart_df,
        x=score_col,
        y="Brand",
        orientation="h",
        text=score_col,
        color_discrete_sequence=[BI_PRIMARY],
        hover_data={
            "Brand": True,
            "Covered Queries": True,
            "Query Pool": True,
            "Coverage Rate": ":.1%",
            "Avg Best Position": True,
            "Visibility Score": True,
        },
    )

    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, f"Brand Visibility - {category}")


def build_brand_visibility_by_category_chart_from_table(
    visibility_df: pd.DataFrame,
    title: str,
    top_n: int = 10,
    score_col: str = "Visibility Score",
):
    if visibility_df is None or visibility_df.empty:
        return _empty_fig(title)

    chart_df = _parse_visibility_table(visibility_df).head(top_n).copy()
    if score_col not in chart_df.columns:
        score_col = "Visibility Score"

    fig = px.bar(
        chart_df,
        x=score_col,
        y="Brand",
        orientation="h",
        text=score_col,
        color_discrete_sequence=[BI_PRIMARY],
        hover_data={
            "Brand": True,
            "Covered Queries": True,
            "Query Pool": True,
            "Coverage Rate": ":.1%",
            "Avg Best Position": True,
            "Visibility Score": True,
        },
    )
    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, title)


def build_brand_visibility_by_category_and_publish_month_chart(
    presence_df: pd.DataFrame,
    category: str,
    publish_month: str,
    top_n: int = 10,
):
    visibility_df = get_brand_visibility_by_category_and_publish_month(
        presence_df,
        category=category,
        publish_month=publish_month,
    )
    if visibility_df.empty:
        return _empty_fig(f"Brand Visibility - {category} / {publish_month}")

    chart_df = visibility_df.head(top_n).copy()

    fig = px.bar(
        chart_df,
        x="Visibility Score",
        y="Brand",
        orientation="h",
        text="Visibility Score",
        color_discrete_sequence=[BI_SECONDARY],
        hover_data={
            "Brand": True,
            "Covered Queries": True,
            "Query Pool": True,
            "Coverage Rate": ":.1%",
            "Avg Best Position": True,
            "Visibility Score": True,
        },
    )

    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    fig.update_yaxes(categoryorder="total ascending")
    return _apply_layout(fig, f"Brand Visibility - {category} / {publish_month}")


def build_brand_visibility_by_publish_month_chart(
    presence_df: pd.DataFrame,
    brand: str = "",
    top_n_months: int = 12,
):
    month_df = get_brand_visibility_by_publish_month(presence_df)
    if month_df.empty:
        return _empty_fig("Brand Visibility by Publish Month")

    if brand:
        month_df = month_df[month_df["Brand"].astype(str).str.strip() == str(brand).strip()].copy()

    if month_df.empty:
        return _empty_fig("Brand Visibility by Publish Month")

    month_df = _sort_month_df(month_df, "Publish Month")

    fig = px.line(
        month_df,
        x="Publish Month",
        y="Visibility Score",
        color="Brand" if not brand else None,
        markers=True,
        color_discrete_sequence=BI_SEQ,
        hover_data={
            "Publish Month": True,
            "Brand": True if "Brand" in month_df.columns else False,
            "Covered Queries": True,
            "Query Pool": True,
            "Coverage Rate": ":.1%",
            "Avg Best Position": True,
            "Visibility Score": True,
        },
    )

    fig.update_traces(
        line=dict(width=3),
        marker=dict(size=8, line=dict(width=1.5, color=BI_PAPER)),
    )
    return _apply_layout(fig, "Brand Visibility by Publish Month", height=460)


def build_brand_visibility_by_publish_month_chart_from_table(month_df: pd.DataFrame):
    if month_df is None or month_df.empty:
        return _empty_fig("Brand Visibility by Publish Month")

    chart_df = _parse_visibility_table(month_df).copy()
    chart_df = _sort_month_df(chart_df, "Publish Month")
    fig = px.line(
        chart_df,
        x="Publish Month",
        y="Visibility Score",
        color="Brand" if "Brand" in chart_df.columns else None,
        markers=True,
        color_discrete_sequence=BI_SEQ,
        hover_data={
            "Publish Month": True,
            "Brand": True if "Brand" in chart_df.columns else False,
            "Covered Queries": True,
            "Query Pool": True,
            "Coverage Rate": ":.1%",
            "Avg Best Position": True,
            "Visibility Score": True,
        },
    )
    fig.update_traces(
        line=dict(width=3),
        marker=dict(size=8, line=dict(width=1.5, color=BI_PAPER)),
    )
    return _apply_layout(fig, "Brand Visibility by Publish Month", height=460)


def build_brand_visibility_by_record_month_chart(
    presence_df: pd.DataFrame,
    brand: str = "",
):
    month_df = get_brand_visibility_by_record_month(presence_df)
    if month_df.empty:
        return _empty_fig("Brand Visibility Score by Record Month")

    if brand:
        month_df = month_df[month_df["Brand"].astype(str).str.strip() == str(brand).strip()].copy()

    if month_df.empty:
        return _empty_fig("Brand Visibility Score by Record Month")

    month_df = _sort_month_df(month_df, "Record Month")
    fig = px.line(
        month_df,
        x="Record Month",
        y="Visibility Score",
        color="Brand" if not brand else None,
        markers=True,
        color_discrete_sequence=BI_LINE_SEQ,
        hover_data={
            "Record Month": True,
            "Brand": True if "Brand" in month_df.columns else False,
            "Covered Queries": True,
            "Query Pool": True,
            "Coverage Rate": ":.1%",
            "Avg Best Position": True,
            "Visibility Score": True,
        },
    )
    fig.update_traces(
        line=dict(width=3),
        marker=dict(size=8, line=dict(width=1.5, color=BI_PAPER)),
    )
    return _apply_layout(fig, "Brand Visibility Score by Record Month", height=460)


def build_brand_visibility_by_record_month_chart_from_table(month_df: pd.DataFrame):
    if month_df is None or month_df.empty:
        return _empty_fig("Brand Visibility Score by Record Month")

    chart_df = _parse_visibility_table(month_df).copy()
    chart_df = _sort_month_df(chart_df, "Record Month")
    fig = px.line(
        chart_df,
        x="Record Month",
        y="Visibility Score",
        color="Brand" if "Brand" in chart_df.columns else None,
        markers=True,
        color_discrete_sequence=BI_LINE_SEQ,
        hover_data={
            "Record Month": True,
            "Brand": True if "Brand" in chart_df.columns else False,
            "Covered Queries": True,
            "Query Pool": True,
            "Coverage Rate": ":.1%",
            "Avg Best Position": True,
            "Visibility Score": True,
        },
    )
    fig.update_traces(
        line=dict(width=3),
        marker=dict(size=8, line=dict(width=1.5, color=BI_PAPER)),
    )
    return _apply_layout(fig, "Brand Visibility Score by Record Month", height=460)


# =========================================================
# Query / Publish Month Charts
# =========================================================
def build_query_count_by_publish_month_chart(queries_df: pd.DataFrame):
    count_df = get_query_count_by_publish_month(queries_df)
    if count_df.empty:
        return _empty_fig("Query Count by Publish Month")

    count_df = _sort_month_df(count_df, "Publish Month")

    fig = px.bar(
        count_df,
        x="Publish Month",
        y="Query Count",
        text="Query Count",
        color_discrete_sequence=[BI_PRIMARY],
        hover_data={
            "Publish Month": True,
            "Query Count": True,
        },
    )

    fig.update_traces(
        textposition="outside",
        marker_line_color=BI_BAR_BORDER,
        marker_line_width=1.2,
    )
    return _apply_layout(fig, "Query Count by Publish Month")


def build_brand_presence_ranking_figure(
    table_df: pd.DataFrame,
    title: str = "Brand Presence Ranking",
    top_n: int = 10,
) -> go.Figure:
    if table_df is None or table_df.empty:
        return _empty_fig(title)

    chart_df = table_df.head(top_n).copy().iloc[::-1].reset_index(drop=True)
    rank_values = list(range(len(chart_df), 0, -1))
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank_labels = [medals.get(rank, f"#{rank}") for rank in rank_values]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=chart_df["Brand"],
            x=chart_df["Brand Mention"],
            orientation="h",
            text=chart_df["Brand Mention"],
            textposition="outside",
            marker=dict(
                color=BI_ACCENT,
                line=dict(color=BI_PRIMARY, width=1.5),
            ),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Presence Volume: %{x}<br>"
                "Avg Position: %{customdata[0]:.2f}<br>"
                "Best Position: %{customdata[1]}<br>"
                "<extra></extra>"
            ),
            customdata=chart_df[["Avg Position", "Best Position"]].values,
        )
    )

    for brand, rank_label in zip(chart_df["Brand"], rank_labels):
        fig.add_annotation(
            x=0,
            y=brand,
            text=rank_label,
            showarrow=False,
            xanchor="right",
            xshift=-10,
            font=dict(size=16, color=BI_PRIMARY),
        )

    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color=BI_PRIMARY), x=0.5, xanchor="center"),
        template="plotly_white",
        height=500,
        margin=dict(l=95, r=60, t=80, b=50),
        paper_bgcolor=BI_PAPER,
        plot_bgcolor=BI_PLOT,
        showlegend=False,
        bargap=0.3,
        font=dict(color=BI_FONT),
    )
    fig.update_xaxes(
        title="Presence Volume",
        title_font=dict(size=14, color=BI_FONT_MUTED),
        tickfont=dict(color=BI_FONT_MUTED),
        gridcolor=BI_GRID,
        zeroline=False,
        linecolor=BI_LINE,
    )
    fig.update_yaxes(
        title="",
        tickfont=dict(size=13, color=BI_FONT),
        automargin=True,
        showgrid=False,
        zeroline=False,
        linecolor=BI_LINE,
    )
    return fig


def build_brand_visibility_ranking_figure(
    table_df: pd.DataFrame,
    title: str = "Brand Visibility Ranking",
    top_n: int = 10,
) -> go.Figure:
    if table_df is None or table_df.empty:
        return _empty_fig(title)

    chart_df = _parse_visibility_table(table_df).head(top_n).copy().iloc[::-1].reset_index(drop=True)
    rank_values = list(range(len(chart_df), 0, -1))
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank_labels = [medals.get(rank, f"#{rank}") for rank in rank_values]

    max_score = chart_df["Visibility Score"].max()
    colors = []
    for score in chart_df["Visibility Score"]:
        intensity = score / max_score if max_score and max_score > 0 else 0.5
        colors.append(f"rgba(43, 214, 217, {0.4 + intensity * 0.6:.3f})")

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=chart_df["Brand"],
            x=chart_df["Visibility Score"],
            orientation="h",
            text=chart_df["Visibility Score"].round(2),
            textposition="outside",
            marker=dict(
                color=colors,
                line=dict(color=BI_PRIMARY, width=1.5),
            ),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Visibility Score: %{x:.2f}<br>"
                "Coverage Rate: %{customdata[0]:.1%}<br>"
                "Covered Queries: %{customdata[1]} / %{customdata[2]}<br>"
                "<extra></extra>"
            ),
            customdata=chart_df[["Coverage Rate", "Covered Queries", "Query Pool"]].values,
        )
    )

    for brand, rank_label in zip(chart_df["Brand"], rank_labels):
        fig.add_annotation(
            x=0,
            y=brand,
            text=rank_label,
            showarrow=False,
            xanchor="right",
            xshift=-10,
            font=dict(size=16, color=BI_PRIMARY),
        )

    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color=BI_PRIMARY), x=0.5, xanchor="center"),
        template="plotly_white",
        height=500,
        margin=dict(l=95, r=60, t=80, b=50),
        paper_bgcolor=BI_PAPER,
        plot_bgcolor=BI_PLOT,
        showlegend=False,
        bargap=0.3,
        font=dict(color=BI_FONT),
    )
    fig.update_xaxes(
        title="Visibility Score",
        title_font=dict(size=14, color=BI_FONT_MUTED),
        tickfont=dict(color=BI_FONT_MUTED),
        gridcolor=BI_GRID,
        zeroline=False,
        linecolor=BI_LINE,
    )
    fig.update_yaxes(
        title="",
        tickfont=dict(size=13, color=BI_FONT),
        automargin=True,
        showgrid=False,
        zeroline=False,
        linecolor=BI_LINE,
    )
    return fig


def build_channel_ranking_figure(
    table_df: pd.DataFrame,
    title: str = "Channel Ranking",
    top_n: int = 10,
) -> go.Figure:
    if table_df is None or table_df.empty:
        return _empty_fig(title)

    chart_df = table_df.head(top_n).copy().iloc[::-1].reset_index(drop=True)
    rank_values = list(range(len(chart_df), 0, -1))
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    rank_labels = [medals.get(rank, f"#{rank}") for rank in rank_values]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=chart_df["Channel"],
            x=chart_df["Channel Mention"],
            orientation="h",
            text=chart_df["Channel Mention"],
            textposition="outside",
            marker=dict(
                color=BI_WARM,
                line=dict(color=BI_PRIMARY, width=1.5),
            ),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Channel Mention: %{x}<br>"
                "Avg Position: %{customdata[0]:.2f}<br>"
                "<extra></extra>"
            ),
            customdata=chart_df[["Avg Position"]].values,
        )
    )

    for channel, rank_label in zip(chart_df["Channel"], rank_labels):
        fig.add_annotation(
            x=0,
            y=channel,
            text=rank_label,
            showarrow=False,
            xanchor="right",
            xshift=-10,
            font=dict(size=16, color=BI_PRIMARY),
        )

    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color=BI_PRIMARY), x=0.5, xanchor="center"),
        template="plotly_white",
        height=450,
        margin=dict(l=95, r=60, t=80, b=50),
        paper_bgcolor=BI_PAPER,
        plot_bgcolor=BI_PLOT,
        showlegend=False,
        bargap=0.3,
        font=dict(color=BI_FONT),
    )
    fig.update_xaxes(
        title="Channel Mention",
        title_font=dict(size=14, color=BI_FONT_MUTED),
        tickfont=dict(color=BI_FONT_MUTED),
        gridcolor=BI_GRID,
        zeroline=False,
        linecolor=BI_LINE,
    )
    fig.update_yaxes(
        title="",
        tickfont=dict(size=13, color=BI_FONT),
        automargin=True,
        showgrid=False,
        zeroline=False,
        linecolor=BI_LINE,
    )
    return fig


# =========================================================
# Table Helpers for Dashboard
# =========================================================
def build_brand_ranking_table(presence_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    df = get_brand_ranking(presence_df)
    if df.empty:
        return df
    return df.head(top_n).copy()


def build_channel_ranking_table(presence_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    df = get_channel_ranking(presence_df)
    if df.empty:
        return df
    return df.head(top_n).copy()


def build_source_occurrence_table(source_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    df = get_source_occurrence_ranking(source_df)
    if df.empty:
        return df
    return df.head(top_n).copy()


def build_quoted_source_table(source_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    df = get_quoted_source_ranking(source_df)
    if df.empty:
        return df
    return df.head(top_n).copy()


def build_brand_visibility_by_category_table(
    presence_df: pd.DataFrame,
    category: str,
    top_n: int = 20,
) -> pd.DataFrame:
    df = get_brand_visibility_by_category(presence_df, category)
    if df.empty:
        return df
    return _format_visibility_table(df.head(top_n).copy())


def build_brand_visibility_by_category_and_publish_month_table(
    presence_df: pd.DataFrame,
    category: str,
    publish_month: str,
    top_n: int = 20,
) -> pd.DataFrame:
    df = get_brand_visibility_by_category_and_publish_month(
        presence_df,
        category,
        publish_month,
    )
    if df.empty:
        return df
    return _format_visibility_table(df.head(top_n).copy())


def build_source_distribution_by_platform_table(source_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    return get_source_distribution_by_platform(source_df, top_n=top_n)


def build_source_platform_comparison_tables(
    source_df: pd.DataFrame,
    primary_platform: str = "Doubao",
    secondary_platform: str = "Deepseek",
    top_n: int = 20,
) -> dict[str, pd.DataFrame]:
    return get_source_platform_comparison(
        source_df,
        primary_platform=primary_platform,
        secondary_platform=secondary_platform,
        top_n=top_n,
    )


def build_query_count_by_publish_month_table(queries_df: pd.DataFrame) -> pd.DataFrame:
    return get_query_count_by_publish_month(queries_df)


def build_brand_visibility_by_publish_month_table(presence_df: pd.DataFrame) -> pd.DataFrame:
    return _format_visibility_table(get_brand_visibility_by_publish_month(presence_df))


def build_brand_visibility_by_record_month_table(presence_df: pd.DataFrame) -> pd.DataFrame:
    return _format_visibility_table(get_brand_visibility_by_record_month(presence_df))


# =========================================================
# Combined Dashboard Builder
# =========================================================
def build_dashboard_payload(
    queries_df: pd.DataFrame,
    presence_df: pd.DataFrame,
    source_df: pd.DataFrame,
    content_publish_df: pd.DataFrame | None = None,
    selected_category: str = "",
    selected_publish_month: str = "",
):
    """
    Return a dict for app.py usage.
    """
    payload = {
        "kpis": get_kpi_cards_data(presence_df, source_df, content_publish_df),
        "brand_ranking_table": build_brand_ranking_table(presence_df),
        "channel_ranking_table": build_channel_ranking_table(presence_df),
        "source_occurrence_table": build_source_occurrence_table(source_df),
        "quoted_source_table": build_quoted_source_table(source_df),
        "source_distribution_by_platform_table": build_source_distribution_by_platform_table(source_df),
        "source_platform_comparison_tables": build_source_platform_comparison_tables(source_df),
        "query_count_by_publish_month_table": build_query_count_by_publish_month_table(queries_df),
        "brand_visibility_by_publish_month_table": build_brand_visibility_by_publish_month_table(presence_df),
        "brand_visibility_by_record_month_table": build_brand_visibility_by_record_month_table(presence_df),

        "brand_ranking_chart": build_brand_ranking_chart(presence_df),
        "channel_ranking_chart": build_channel_ranking_chart(presence_df),
        "source_occurrence_chart": build_source_occurrence_chart(source_df),
        "quoted_source_chart": build_quoted_source_chart(source_df),
        "source_distribution_by_platform_chart": build_source_distribution_by_platform_chart(source_df),
        "query_count_by_publish_month_chart": build_query_count_by_publish_month_chart(queries_df),
        "brand_visibility_by_publish_month_chart": build_brand_visibility_by_publish_month_chart(presence_df),
        "brand_visibility_by_record_month_chart": build_brand_visibility_by_record_month_chart(presence_df),
    }

    if selected_category:
        payload["brand_visibility_by_category_table"] = build_brand_visibility_by_category_table(
            presence_df,
            selected_category,
        )
        payload["brand_visibility_by_category_chart"] = build_brand_visibility_by_category_chart(
            presence_df,
            selected_category,
        )

    if selected_category and selected_publish_month:
        payload["brand_visibility_by_category_and_publish_month_table"] = (
            build_brand_visibility_by_category_and_publish_month_table(
                presence_df,
                selected_category,
                selected_publish_month,
            )
        )
        payload["brand_visibility_by_category_and_publish_month_chart"] = (
            build_brand_visibility_by_category_and_publish_month_chart(
                presence_df,
                selected_category,
                selected_publish_month,
            )
        )

    return payload
