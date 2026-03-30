import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from supabase import create_client, Client


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

SOURCE_ID = "week3_derived_metrics"
PARSER_VERSION = "week3_derived_metrics_v1"

SOURCE_REGISTRY_BASE = {
    "source_id": SOURCE_ID,
    "source_name": "Week 3 Derived Metrics Builder",
    "source_type": "derived_metrics_job",
    "source_url": "local_job",
    "frequency_expected": "manual",
    "default_currency": "USD",
    "default_unit": "metric_ton",
    "active_flag": True,
    "notes": "Builds delta tracker, protein pivot, and insight tables",
}

SOURCE_SLA_DAYS = {
    "wb_pinksheet_monthly": 40,
    "graingrowers_fertiliser_report": 21,
    "week3_derived_metrics": 7,
}

BSF_MEAL_BENCHMARK_LOW = 1200.0
BSF_MEAL_BENCHMARK_MID = 1500.0
BSF_MEAL_BENCHMARK_HIGH = 1800.0


def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def json_safe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    safe_df = df.copy()
    safe_df = safe_df.astype(object)
    safe_df = safe_df.where(pd.notnull(safe_df), None)
    return safe_df


def upsert_rows(
    supabase: Client,
    table_name: str,
    rows: list[dict],
    on_conflict: Optional[str] = None,
    chunk_size: int = 500,
) -> None:
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        supabase.table(table_name).upsert(chunk, on_conflict=on_conflict).execute()


def fetch_table_as_df(supabase: Client, table_name: str, chunk_size: int = 1000) -> pd.DataFrame:
    all_rows = []
    start = 0

    while True:
        end = start + chunk_size - 1
        response = (
            supabase
            .table(table_name)
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

    df = pd.DataFrame(all_rows)
    print(f"Fetched {len(df)} rows from {table_name}")
    return df


def ensure_source_registry_row(supabase: Client) -> None:
    supabase.table("source_registry").upsert(
        SOURCE_REGISTRY_BASE,
        on_conflict="source_id",
    ).execute()


def mark_source_checked(supabase: Client) -> None:
    payload = {
        **SOURCE_REGISTRY_BASE,
        "last_checked_at": datetime.now(timezone.utc).isoformat(),
        "parser_version": PARSER_VERSION,
        "run_status": "running",
        "last_error": None,
    }
    supabase.table("source_registry").upsert(payload, on_conflict="source_id").execute()


def mark_source_success(supabase: Client, row_count: int) -> None:
    now_utc = datetime.now(timezone.utc).isoformat()
    payload = {
        **SOURCE_REGISTRY_BASE,
        "last_checked_at": now_utc,
        "last_success_at": now_utc,
        "parser_version": PARSER_VERSION,
        "run_status": "success",
        "last_row_count": row_count,
        "last_error": None,
    }
    supabase.table("source_registry").upsert(payload, on_conflict="source_id").execute()


def mark_source_failed(supabase: Client, error_message: str) -> None:
    payload = {
        **SOURCE_REGISTRY_BASE,
        "last_checked_at": datetime.now(timezone.utc).isoformat(),
        "parser_version": PARSER_VERSION,
        "run_status": "failed",
        "last_error": error_message[:2000],
    }
    supabase.table("source_registry").upsert(payload, on_conflict="source_id").execute()


def create_run_log_start(
    supabase: Client,
    source_id: str,
    parser_version: str,
) -> int:
    payload = {
        "source_id": source_id,
        "parser_version": parser_version,
        "run_started_at": datetime.now(timezone.utc).isoformat(),
        "run_status": "running",
    }
    response = supabase.table("source_run_log").insert(payload).execute()
    return response.data[0]["id"]


def update_run_log_success(
    supabase: Client,
    run_log_id: int,
    row_count: int,
) -> None:
    payload = {
        "run_finished_at": datetime.now(timezone.utc).isoformat(),
        "run_status": "success",
        "row_count": row_count,
        "error_message": None,
    }
    supabase.table("source_run_log").update(payload).eq("id", run_log_id).execute()


def update_run_log_failed(
    supabase: Client,
    run_log_id: int,
    error_message: str,
) -> None:
    payload = {
        "run_finished_at": datetime.now(timezone.utc).isoformat(),
        "run_status": "failed",
        "error_message": error_message[:2000],
    }
    supabase.table("source_run_log").update(payload).eq("id", run_log_id).execute()


def compute_freshness_status(last_success_at, sla_days: int) -> tuple[str, Optional[int], str]:
    if last_success_at is None:
        return "failed", None, "No successful run recorded."

    now = datetime.now(timezone.utc)
    age_days = (now - last_success_at).days

    if age_days <= sla_days:
        return "fresh", age_days, f"Latest successful run is within SLA ({sla_days} days)."

    return "stale", age_days, f"Latest successful run is older than SLA ({sla_days} days)."


def upsert_freshness_status(
    supabase: Client,
    source_id: str,
    last_success_at,
) -> None:
    sla_days = SOURCE_SLA_DAYS.get(source_id, 30)
    freshness_status, age_days, note = compute_freshness_status(last_success_at, sla_days)

    payload = {
        "source_id": source_id,
        "freshness_status": freshness_status,
        "freshness_checked_at": datetime.now(timezone.utc).isoformat(),
        "last_success_at": last_success_at.isoformat() if last_success_at is not None else None,
        "sla_days": sla_days,
        "age_days": age_days,
        "note": note,
    }

    supabase.table("source_freshness_status").upsert(
        payload,
        on_conflict="source_id",
    ).execute()


def refresh_source_freshness_from_registry(supabase: Client, source_id: str) -> None:
    response = (
        supabase.table("source_registry")
        .select("source_id,last_success_at")
        .eq("source_id", source_id)
        .execute()
    )

    rows = response.data or []
    if not rows:
        raise ValueError(f"Source {source_id} not found in source_registry.")

    row = rows[0]
    last_success_at = row.get("last_success_at")

    parsed_last_success = None
    if last_success_at:
        parsed_last_success = pd.to_datetime(last_success_at).to_pydatetime()
        if parsed_last_success.tzinfo is None:
            parsed_last_success = parsed_last_success.replace(tzinfo=timezone.utc)

    upsert_freshness_status(supabase, source_id, parsed_last_success)


def classify_delta(delta_value: float) -> Optional[str]:
    if pd.isna(delta_value):
        return None
    if delta_value > 150:
        return "strong_premium"
    if delta_value > 50:
        return "premium"
    if delta_value >= -50:
        return "near_parity"
    return "discount"


def classify_protein_gap(gap_value: float) -> Optional[str]:
    if pd.isna(gap_value):
        return None
    if gap_value > 200:
        return "fishmeal_above_bsf_mid"
    if gap_value >= -100:
        return "fishmeal_near_bsf_mid"
    return "fishmeal_below_bsf_mid"


def build_delta_tracker(overlay_df: pd.DataFrame) -> pd.DataFrame:
    df = overlay_df.copy()
    df["observed_month"] = pd.to_datetime(df["observed_month"]).dt.normalize()

    numeric_cols = [
        "urea_global_usd_per_ton",
        "urea_au_usd_per_ton",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["delta_usd_per_ton"] = df["urea_au_usd_per_ton"] - df["urea_global_usd_per_ton"]
    df["delta_pct_vs_global"] = (
        df["delta_usd_per_ton"] / df["urea_global_usd_per_ton"] * 100
    )
    df["economic_state"] = df["delta_usd_per_ton"].apply(classify_delta)

    return df[
        [
            "observed_month",
            "urea_global_usd_per_ton",
            "urea_au_usd_per_ton",
            "delta_usd_per_ton",
            "delta_pct_vs_global",
            "economic_state",
        ]
    ].sort_values("observed_month")


def build_protein_pivot(chart_df: pd.DataFrame) -> pd.DataFrame:
    df = chart_df.copy()
    df["observed_month"] = pd.to_datetime(df["observed_month"]).dt.normalize()
    df["fishmeal_usd_per_ton"] = pd.to_numeric(df["fishmeal_usd_per_ton"], errors="coerce")

    df["bsf_meal_benchmark_low"] = BSF_MEAL_BENCHMARK_LOW
    df["bsf_meal_benchmark_mid"] = BSF_MEAL_BENCHMARK_MID
    df["bsf_meal_benchmark_high"] = BSF_MEAL_BENCHMARK_HIGH
    df["premium_gap_vs_mid"] = df["fishmeal_usd_per_ton"] - df["bsf_meal_benchmark_mid"]
    df["competitiveness_state"] = df["premium_gap_vs_mid"].apply(classify_protein_gap)

    return df[
        [
            "observed_month",
            "fishmeal_usd_per_ton",
            "bsf_meal_benchmark_low",
            "bsf_meal_benchmark_mid",
            "bsf_meal_benchmark_high",
            "premium_gap_vs_mid",
            "competitiveness_state",
        ]
    ].sort_values("observed_month")


def generate_monthly_insights(
    delta_df: pd.DataFrame,
    protein_df: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    delta_latest = delta_df.dropna(subset=["delta_usd_per_ton"]).copy()
    protein_latest = protein_df.dropna(subset=["fishmeal_usd_per_ton"]).copy()

    if not delta_latest.empty:
        for _, row in delta_latest.iterrows():
            month = row["observed_month"]
            delta = row["delta_usd_per_ton"]
            state = row["economic_state"]

            if state == "strong_premium":
                title = "AU urea market well above global benchmark"
                text = f"AU urea traded ${delta:.2f}/t above the global benchmark in {month.strftime('%B %Y')}."
                severity = "high"
            elif state == "premium":
                title = "AU urea premium persists"
                text = f"AU urea stayed above the global benchmark by ${delta:.2f}/t in {month.strftime('%B %Y')}."
                severity = "medium"
            elif state == "near_parity":
                title = "AU urea near global parity"
                text = f"AU urea was close to the global benchmark in {month.strftime('%B %Y')}."
                severity = "low"
            else:
                title = "AU urea below global benchmark"
                text = f"AU urea traded below the global benchmark by ${abs(delta):.2f}/t in {month.strftime('%B %Y')}."
                severity = "medium"

            rows.append(
                {
                    "observed_month": month,
                    "chart_type": "delta_tracker",
                    "insight_title": title,
                    "insight_text": text,
                    "severity": severity,
                }
            )

    if not protein_latest.empty:
        for _, row in protein_latest.iterrows():
            month = row["observed_month"]
            gap = row["premium_gap_vs_mid"]
            state = row["competitiveness_state"]

            if state == "fishmeal_above_bsf_mid":
                title = "Fishmeal above BSF mid benchmark"
                text = f"Fishmeal sat ${gap:.2f}/t above the BSF mid benchmark in {month.strftime('%B %Y')}."
                severity = "medium"
            elif state == "fishmeal_near_bsf_mid":
                title = "Fishmeal near BSF benchmark midpoint"
                text = f"Fishmeal stayed close to the BSF mid benchmark in {month.strftime('%B %Y')}."
                severity = "low"
            else:
                title = "Fishmeal below BSF mid benchmark"
                text = f"Fishmeal traded ${abs(gap):.2f}/t below the BSF mid benchmark in {month.strftime('%B %Y')}."
                severity = "medium"

            rows.append(
                {
                    "observed_month": month,
                    "chart_type": "protein_pivot",
                    "insight_title": title,
                    "insight_text": text,
                    "severity": severity,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=["observed_month", "chart_type", "insight_title", "insight_text", "severity"]
        )

    return pd.DataFrame(rows).sort_values(["observed_month", "chart_type"])


def delete_existing_insights_for_months(supabase: Client, months: list[str]) -> None:
    if not months:
        return

    for month in months:
        (
            supabase.table("chart_insights_monthly")
            .delete()
            .eq("observed_month", month)
            .execute()
        )


def main() -> None:
    supabase = get_supabase()
    ensure_source_registry_row(supabase)
    mark_source_checked(supabase)

    run_log_id = create_run_log_start(supabase, SOURCE_ID, PARSER_VERSION)

    try:
        overlay_df = fetch_table_as_df(supabase, "chart_fertilizer_overlay_monthly")
        fert_fish_df = fetch_table_as_df(supabase, "chart_fertilizer_vs_fishmeal_monthly")

        if overlay_df.empty:
            raise ValueError("chart_fertilizer_overlay_monthly is empty.")
        if fert_fish_df.empty:
            raise ValueError("chart_fertilizer_vs_fishmeal_monthly is empty.")

        delta_df = build_delta_tracker(overlay_df)
        protein_df = build_protein_pivot(fert_fish_df)
        insights_df = generate_monthly_insights(delta_df, protein_df)

        # Round display-ready fields
        for col in ["urea_global_usd_per_ton", "urea_au_usd_per_ton", "delta_usd_per_ton", "delta_pct_vs_global"]:
            delta_df[col] = pd.to_numeric(delta_df[col], errors="coerce").round(2)

        for col in [
            "fishmeal_usd_per_ton",
            "bsf_meal_benchmark_low",
            "bsf_meal_benchmark_mid",
            "bsf_meal_benchmark_high",
            "premium_gap_vs_mid",
        ]:
            protein_df[col] = pd.to_numeric(protein_df[col], errors="coerce").round(2)

        # Export delta tracker
        delta_export_df = delta_df.copy()
        delta_export_df["observed_month"] = delta_export_df["observed_month"].dt.strftime("%Y-%m-%d")
        delta_export_df = json_safe_dataframe(delta_export_df)
        delta_rows = delta_export_df.to_dict(orient="records")

        upsert_rows(
            supabase,
            "chart_delta_tracker_monthly",
            delta_rows,
            on_conflict="observed_month",
        )

        # Export protein pivot
        protein_export_df = protein_df.copy()
        protein_export_df["observed_month"] = protein_export_df["observed_month"].dt.strftime("%Y-%m-%d")
        protein_export_df = json_safe_dataframe(protein_export_df)
        protein_rows = protein_export_df.to_dict(orient="records")

        upsert_rows(
            supabase,
            "chart_protein_pivot_monthly",
            protein_rows,
            on_conflict="observed_month",
        )

        # Export insights
        insight_row_count = 0
        if not insights_df.empty:
            insight_export_df = insights_df.copy()
            insight_export_df["observed_month"] = insight_export_df["observed_month"].dt.strftime("%Y-%m-%d")
            insight_export_df = json_safe_dataframe(insight_export_df)

            insight_months = sorted(insight_export_df["observed_month"].dropna().unique().tolist())
            delete_existing_insights_for_months(supabase, insight_months)

            insight_rows = insight_export_df.to_dict(orient="records")
            upsert_rows(supabase, "chart_insights_monthly", insight_rows)
            insight_row_count = len(insight_rows)

        total_rows = len(delta_rows) + len(protein_rows) + insight_row_count

        mark_source_success(supabase, total_rows)
        update_run_log_success(supabase, run_log_id, total_rows)
        refresh_source_freshness_from_registry(supabase, SOURCE_ID)

        print(f"Loaded {len(delta_rows)} delta rows")
        print(f"Loaded {len(protein_rows)} protein rows")
        print(f"Loaded {insight_row_count} insight rows")
        print("\nLatest delta rows:")
        print(delta_df.tail(12).to_string(index=False))
        print("\nLatest protein rows:")
        print(protein_df.tail(12).to_string(index=False))

    except Exception as exc:
        mark_source_failed(supabase, str(exc))
        update_run_log_failed(supabase, run_log_id, str(exc))
        refresh_source_freshness_from_registry(supabase, SOURCE_ID)
        raise


if __name__ == "__main__":
    main()