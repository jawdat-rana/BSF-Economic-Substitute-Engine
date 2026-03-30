import os
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from supabase import create_client, Client


st.set_page_config(
    page_title="BSF Economic Substitution Engine",
    page_icon="📈",
    layout="wide",
)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]


def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_table_as_df(supabase: Client, table_name: str, chunk_size: int = 1000) -> pd.DataFrame:
    all_rows = []
    start = 0

    while True:
        end = start + chunk_size - 1
        response = (
            supabase.table(table_name)
            .select("*")
            .range(start, end)
            .execute()
        )

        batch = response.data or []
        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < chunk_size:
            break

        start += chunk_size

    return pd.DataFrame(all_rows)


@st.cache_data(ttl=300)
def load_data() -> dict[str, pd.DataFrame]:
    supabase = get_supabase()

    delta_df = fetch_table_as_df(supabase, "chart_delta_tracker_monthly")
    protein_df = fetch_table_as_df(supabase, "chart_protein_pivot_monthly")
    insights_df = fetch_table_as_df(supabase, "chart_insights_monthly")
    latest_snapshot_df = fetch_table_as_df(supabase, "chart_latest_snapshot")
    overlay_df = fetch_table_as_df(supabase, "chart_fertilizer_overlay_monthly")
    fert_fish_df = fetch_table_as_df(supabase, "chart_fertilizer_vs_fishmeal_monthly")

    for df_name in ["delta_df", "protein_df", "insights_df", "latest_snapshot_df", "overlay_df", "fert_fish_df"]:
        df = locals()[df_name]
        if not df.empty:
            for date_col in ["observed_month", "latest_month", "inserted_at"]:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            locals()[df_name] = df

    if not delta_df.empty:
        delta_df = delta_df.sort_values("observed_month")
    if not protein_df.empty:
        protein_df = protein_df.sort_values("observed_month")
    if not insights_df.empty and "observed_month" in insights_df.columns:
        insights_df = insights_df.sort_values(["observed_month", "chart_type"], ascending=[False, True])
    if not latest_snapshot_df.empty and "commodity_code" in latest_snapshot_df.columns:
        latest_snapshot_df = latest_snapshot_df.sort_values("commodity_code")

    return {
        "delta_df": delta_df,
        "protein_df": protein_df,
        "insights_df": insights_df,
        "latest_snapshot_df": latest_snapshot_df,
        "overlay_df": overlay_df,
        "fert_fish_df": fert_fish_df,
    }


def format_value(value: Optional[float], prefix: str = "$", suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{prefix}{value:,.2f}{suffix}"


def get_latest_row(df: pd.DataFrame, date_col: str = "observed_month") -> Optional[pd.Series]:
    if df.empty or date_col not in df.columns:
        return None
    valid = df.dropna(subset=[date_col]).sort_values(date_col)
    if valid.empty:
        return None
    return valid.iloc[-1]


def filter_by_date(df: pd.DataFrame, start_date, end_date, date_col: str = "observed_month") -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    out = df.copy()
    out = out[(out[date_col] >= pd.Timestamp(start_date)) & (out[date_col] <= pd.Timestamp(end_date))]
    return out


def build_delta_chart(delta_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=delta_df["observed_month"],
            y=delta_df["urea_global_usd_per_ton"],
            mode="lines+markers",
            name="Global Urea",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=delta_df["observed_month"],
            y=delta_df["urea_au_usd_per_ton"],
            mode="lines+markers",
            name="AU Urea",
        )
    )

    fig.update_layout(
        title="Delta Tracker: Global Urea vs AU Urea",
        xaxis_title="Month",
        yaxis_title="USD per ton",
        hovermode="x unified",
    )
    return fig


def build_delta_bar_chart(delta_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=delta_df["observed_month"],
            y=delta_df["delta_usd_per_ton"],
            name="Delta (AU - Global)",
        )
    )

    fig.update_layout(
        title="Monthly Urea Premium / Discount",
        xaxis_title="Month",
        yaxis_title="USD per ton",
        hovermode="x unified",
    )
    return fig


def build_protein_pivot_chart(protein_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=protein_df["observed_month"],
            y=protein_df["bsf_meal_benchmark_high"],
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=protein_df["observed_month"],
            y=protein_df["bsf_meal_benchmark_low"],
            mode="lines",
            fill="tonexty",
            name="BSF Benchmark Band",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=protein_df["observed_month"],
            y=protein_df["bsf_meal_benchmark_mid"],
            mode="lines",
            name="BSF Mid Benchmark",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=protein_df["observed_month"],
            y=protein_df["fishmeal_usd_per_ton"],
            mode="lines+markers",
            name="Fishmeal",
        )
    )

    fig.update_layout(
        title="Protein Pivot: Fishmeal vs BSF Benchmark Band",
        xaxis_title="Month",
        yaxis_title="USD per ton",
        hovermode="x unified",
    )
    return fig


def render_latest_cards(
    delta_df: pd.DataFrame,
    protein_df: pd.DataFrame,
) -> None:
    delta_latest = get_latest_row(delta_df)
    protein_latest = get_latest_row(protein_df)

    col1, col2, col3, col4, col5 = st.columns(5)

    global_urea = None
    au_urea = None
    delta_val = None
    fishmeal = None
    fishmeal_gap = None

    if delta_latest is not None:
        global_urea = delta_latest.get("urea_global_usd_per_ton")
        au_urea = delta_latest.get("urea_au_usd_per_ton")
        delta_val = delta_latest.get("delta_usd_per_ton")

    if protein_latest is not None:
        fishmeal = protein_latest.get("fishmeal_usd_per_ton")
        fishmeal_gap = protein_latest.get("premium_gap_vs_mid")

    with col1:
        st.metric("Latest Global Urea", format_value(global_urea))
        st.caption("Monthly World Bank urea benchmark for the latest available month in the selected range.")

    with col2:
        st.metric("Latest AU Urea", format_value(au_urea))
        st.caption("Monthly AU urea estimate derived from the latest GrainGrowers report in each month, converted to USD.")

    with col3:
        st.metric("Latest Urea Delta", format_value(delta_val))
        st.caption("Difference between AU urea and global urea. Positive means AU is trading at a premium.")

    with col4:
        st.metric("Latest Fishmeal", format_value(fishmeal))
        st.caption("Monthly fishmeal price from the World Bank series.")

    with col5:
        st.metric("Fishmeal vs BSF Mid", format_value(fishmeal_gap))
        st.caption("Gap between fishmeal and the BSF meal midpoint benchmark.")


def render_latest_insights(insights_df: pd.DataFrame, selected_month: Optional[pd.Timestamp]) -> None:
    st.subheader("Insights")
    st.caption("Rule-based monthly commentary generated from the current chart datasets.")

    if insights_df.empty:
        st.info("No insights available yet.")
        return

    insights_df = insights_df.copy()
    insights_df["observed_month"] = pd.to_datetime(insights_df["observed_month"], errors="coerce")

    if selected_month is None:
        selected_month = insights_df["observed_month"].max()

    latest_insights = insights_df[insights_df["observed_month"] == selected_month].copy()
    latest_insights = latest_insights.sort_values("chart_type")

    if latest_insights.empty:
        st.info("No insights available for the selected month.")
        return

    for _, row in latest_insights.iterrows():
        with st.container(border=True):
            st.markdown(f"**{row['insight_title']}**")
            st.write(row["insight_text"])
            st.caption(f"{row.get('chart_type')} • severity: {row.get('severity')}")


def main() -> None:
    st.title("BSF Economic Substitution Engine")
    st.caption("Urea, fishmeal, and benchmark substitution intelligence")

    data = load_data()
    delta_df = data["delta_df"]
    protein_df = data["protein_df"]
    insights_df = data["insights_df"]
    overlay_df = data["overlay_df"]
    fert_fish_df = data["fert_fish_df"]

    if delta_df.empty and protein_df.empty:
        st.warning("No chart data found yet. Run the Week 3 derived metrics script first.")
        return

    # Keep only months where both AU and global urea exist for delta visuals
    delta_overlap_df = delta_df.dropna(subset=["urea_global_usd_per_ton", "urea_au_usd_per_ton"]).copy()

    all_dates = []
    if not delta_overlap_df.empty:
        all_dates.extend(delta_overlap_df["observed_month"].dropna().tolist())
    if not protein_df.empty:
        all_dates.extend(protein_df["observed_month"].dropna().tolist())

    if not all_dates:
        st.warning("No dated rows available for visualization.")
        return

    min_date = min(all_dates).date()
    max_date = max(all_dates).date()

    st.sidebar.header("Filters")
    start_date, end_date = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if start_date > end_date:
        st.error("Start date must be before end date.")
        return

    selectable_months = []
    if not insights_df.empty and "observed_month" in insights_df.columns:
        selectable_months = sorted(pd.to_datetime(insights_df["observed_month"]).dropna().dt.date.unique().tolist())

    latest_month_selector = None
    if selectable_months:
        default_month = selectable_months[-1]
        latest_month_selector = st.sidebar.selectbox(
            "Insight month",
            options=selectable_months,
            index=len(selectable_months) - 1,
            format_func=lambda d: pd.Timestamp(d).strftime("%B %Y"),
        )

    filtered_delta_df = filter_by_date(delta_df, start_date, end_date)
    filtered_delta_overlap_df = filter_by_date(delta_overlap_df, start_date, end_date)
    filtered_protein_df = filter_by_date(protein_df, start_date, end_date)
    filtered_insights_df = filter_by_date(insights_df, start_date, end_date) if not insights_df.empty else insights_df
    filtered_overlay_df = filter_by_date(overlay_df, start_date, end_date) if not overlay_df.empty else overlay_df
    filtered_fert_fish_df = filter_by_date(fert_fish_df, start_date, end_date) if not fert_fish_df.empty else fert_fish_df

    st.subheader("Latest Snapshot")
    st.caption("Quick read of the most recent available values inside the selected date range.")
    render_latest_cards(filtered_delta_overlap_df, filtered_protein_df)

    st.divider()

    if not filtered_delta_overlap_df.empty:
        st.subheader("Delta Tracker")
        st.caption(
            "This chart compares global urea with AU urea for months where both values are available. "
            "It shows whether the AU market is trading above or below the global benchmark."
        )
        st.plotly_chart(build_delta_chart(filtered_delta_overlap_df), use_container_width=True)

        st.subheader("Monthly Urea Premium / Discount")
        st.caption(
            "This bar chart shows the monthly difference between AU urea and global urea. "
            "Positive bars indicate an AU premium; negative bars indicate a discount."
        )
        st.plotly_chart(build_delta_bar_chart(filtered_delta_overlap_df), use_container_width=True)
    else:
        st.info("No overlapping AU/global urea months in the selected date range.")

    st.divider()

    if not filtered_protein_df.empty:
        st.subheader("Protein Pivot")
        st.caption(
            "This chart compares fishmeal prices with the BSF meal benchmark band. "
            "It helps show when fishmeal is moving closer to or further from BSF competitiveness."
        )
        st.plotly_chart(build_protein_pivot_chart(filtered_protein_df), use_container_width=True)

    st.divider()

    selected_insight_month = pd.Timestamp(latest_month_selector) if latest_month_selector is not None else None
    render_latest_insights(filtered_insights_df, selected_insight_month)

    with st.expander("Show Delta Tracker Data"):
        st.caption("Underlying monthly dataset for the delta charts.")
        st.dataframe(filtered_delta_overlap_df, use_container_width=True)

    with st.expander("Show Protein Pivot Data"):
        st.caption("Underlying monthly dataset for the fishmeal vs BSF benchmark chart.")
        st.dataframe(filtered_protein_df, use_container_width=True)

    with st.expander("Show Insight Data"):
        st.caption("Rule-based insight rows generated by the Week 3 pipeline.")
        st.dataframe(filtered_insights_df, use_container_width=True)

    with st.expander("Show Overlay Source Data"):
        st.caption("Merged source table used to build the delta tracker.")
        st.dataframe(filtered_overlay_df, use_container_width=True)

    with st.expander("Show Fertilizer / Fishmeal Source Data"):
        st.caption("Monthly World Bank fertilizer and fishmeal source table.")
        st.dataframe(filtered_fert_fish_df, use_container_width=True)


if __name__ == "__main__":
    main()