import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from supabase import create_client, Client


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

SOURCE_ID = "graingrowers_fertiliser_report"
PARSER_VERSION = "graingrowers_csv_v1"
CSV_PATH = r"./data/graingrowers_au_prices.csv"

SOURCE_REGISTRY_BASE = {
    "source_id": SOURCE_ID,
    "source_name": "GrainGrowers Fertiliser Report",
    "source_type": "industry_report",
    "source_url": "https://www.graingrowers.com.au/",
    "frequency_expected": "fortnightly",
    "default_currency": "AUD",
    "default_unit": "metric_ton",
    "active_flag": True,
    "notes": "AU fertilizer regional overlay source",
}

SOURCE_SLA_DAYS = {
    "wb_pinksheet_monthly": 40,              # monthly source
    "graingrowers_fertiliser_report": 21,    # fortnightly source
}


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
        try:
            supabase.table(table_name).upsert(chunk, on_conflict=on_conflict).execute()
        except Exception:
            print(f"Failed upsert for table={table_name}, chunk starting at row {start}")
            print("First row in failed chunk:", chunk[0] if chunk else None)
            raise


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


def load_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=None, engine="python")
    print(df.shape)
    print(df.head().to_string())

    required_cols = [
        "observed_month",
        "report_date",
        "commodity_code",
        "commodity_name",
        "region",
        "price_low",
        "price_high",
        "price_mid",
        "currency",
        "unit",
        "quality_spec",
        "source_url",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required CSV columns: {missing}")

    df["observed_month"] = pd.to_datetime(df["observed_month"], format="%d/%m/%Y").dt.normalize()
    df["report_date"] = pd.to_datetime(df["report_date"], format="%d/%m/%Y").dt.normalize()
    df["price_low"] = pd.to_numeric(df["price_low"], errors="coerce")
    df["price_high"] = pd.to_numeric(df["price_high"], errors="coerce")
    df["price_mid"] = pd.to_numeric(df["price_mid"], errors="coerce")

    if df["observed_month"].isna().any():
        raise ValueError("Some observed_month values could not be parsed.")

    if df["report_date"].isna().any():
        raise ValueError("Some report_date values could not be parsed.")

    if df["price_mid"].isna().any():
        raise ValueError("Some price_mid values could not be parsed.")

    return df


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
    run_row = response.data[0]
    return run_row["id"]


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


def build_normalized_rows(csv_df: pd.DataFrame) -> pd.DataFrame:
    out = csv_df.copy()
    out["source_id"] = SOURCE_ID
    out["price_value"] = out["price_mid"]
    out["evidence_type"] = "market_series"

    return out[
        [
            "source_id",
            "commodity_code",
            "commodity_name",
            "region",
            "observed_month",
            "report_date",
            "price_value",
            "price_low",
            "price_high",
            "price_mid",
            "currency",
            "unit",
            "quality_spec",
            "evidence_type",
        ]
    ]


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


def build_overlay_chart(
    commodity_df: pd.DataFrame,
    fx_df: pd.DataFrame,
) -> pd.DataFrame:
    if commodity_df.empty:
        raise ValueError("commodity_prices_normalized is empty.")

    commodity_df = commodity_df.copy()
    commodity_df["observed_month"] = pd.to_datetime(commodity_df["observed_month"]).dt.normalize()

    if "report_date" in commodity_df.columns:
        commodity_df["report_date"] = pd.to_datetime(commodity_df["report_date"], errors="coerce").dt.normalize()

    if not fx_df.empty:
        fx_df = fx_df.copy()
        fx_df["observed_month"] = pd.to_datetime(fx_df["observed_month"]).dt.normalize()

    # World Bank global monthly
    global_df = commodity_df[
        commodity_df["source_id"] == "wb_pinksheet_monthly"
    ].copy()

    global_pivot = (
        global_df.pivot_table(
            index="observed_month",
            columns="commodity_code",
            values="price_value",
            aggfunc="first",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    global_pivot = global_pivot.rename(
        columns={
            "urea": "urea_global_usd_per_ton",
            "dap": "dap_global_usd_per_ton",
        }
    )

    # GrainGrowers report-level -> latest report per month
    au_df = commodity_df[
        commodity_df["source_id"] == SOURCE_ID
    ].copy()

    if au_df.empty:
        merged = global_pivot.copy()
        if "urea_au_usd_per_ton" not in merged.columns:
            merged["urea_au_usd_per_ton"] = None
        if "dap_au_usd_per_ton" not in merged.columns:
            merged["dap_au_usd_per_ton"] = None
        return merged

    au_df = au_df.sort_values(["commodity_code", "region", "observed_month", "report_date"])
    au_latest_monthly = (
        au_df.groupby(["commodity_code", "region", "observed_month"], as_index=False)
        .tail(1)
        .copy()
    )

    # FX conversion
    if not fx_df.empty:
        au_latest_monthly = au_latest_monthly.merge(
            fx_df,
            how="left",
            on="observed_month",
            suffixes=("", "_fx"),
        )

        def convert_row(row):
            if row["currency"] == "USD":
                return row["price_value"]
            if row["currency"] == "AUD":
                rate = row.get("rate")
                if pd.isna(rate):
                    return None
                return row["price_value"] * rate
            return None

        au_latest_monthly["price_value_usd"] = au_latest_monthly.apply(convert_row, axis=1)
    else:
        au_latest_monthly["price_value_usd"] = au_latest_monthly["price_value"]

    au_pivot = (
        au_latest_monthly.pivot_table(
            index="observed_month",
            columns="commodity_code",
            values="price_value_usd",
            aggfunc="first",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    au_pivot = au_pivot.rename(
        columns={
            "urea": "urea_au_usd_per_ton",
            "dap": "dap_au_usd_per_ton",
        }
    )

    merged = global_pivot.merge(au_pivot, how="outer", on="observed_month")
    merged = merged.sort_values("observed_month")

    for col in [
        "urea_global_usd_per_ton",
        "dap_global_usd_per_ton",
        "urea_au_usd_per_ton",
        "dap_au_usd_per_ton",
    ]:
        if col not in merged.columns:
            merged[col] = None

    return merged[
        [
            "observed_month",
            "urea_global_usd_per_ton",
            "dap_global_usd_per_ton",
            "urea_au_usd_per_ton",
            "dap_au_usd_per_ton",
        ]
    ]


def main() -> None:
    supabase = get_supabase()
    ensure_source_registry_row(supabase)
    mark_source_checked(supabase)

    run_log_id = create_run_log_start(supabase, SOURCE_ID, PARSER_VERSION)

    try:
        csv_df = load_csv(CSV_PATH)
        normalized_df = build_normalized_rows(csv_df)

        normalized_export_df = normalized_df.copy()
        normalized_export_df["observed_month"] = normalized_export_df["observed_month"].dt.strftime("%Y-%m-%d")
        normalized_export_df["report_date"] = normalized_export_df["report_date"].dt.strftime("%Y-%m-%d")
        normalized_export_df = json_safe_dataframe(normalized_export_df)
        normalized_rows = normalized_export_df.to_dict(orient="records")

        upsert_rows(
            supabase,
            "commodity_prices_normalized",
            normalized_rows,
            on_conflict="source_id,commodity_code,region,report_date",
        )

        commodity_df = fetch_table_as_df(supabase, "commodity_prices_normalized")
        fx_df = fetch_table_as_df(supabase, "fx_rates_monthly")

        if not fx_df.empty:
            fx_df = fx_df[
                (fx_df["from_currency"] == "AUD") &
                (fx_df["to_currency"] == "USD")
            ].copy()

        overlay_df = build_overlay_chart(commodity_df, fx_df)

        overlay_df["observed_month"] = pd.to_datetime(overlay_df["observed_month"]).dt.normalize()
        overlay_df["urea_global_usd_per_ton"] = pd.to_numeric(overlay_df["urea_global_usd_per_ton"], errors="coerce").round(2)
        overlay_df["dap_global_usd_per_ton"] = pd.to_numeric(overlay_df["dap_global_usd_per_ton"], errors="coerce").round(2)
        overlay_df["urea_au_usd_per_ton"] = pd.to_numeric(overlay_df["urea_au_usd_per_ton"], errors="coerce").round(2)
        overlay_df["dap_au_usd_per_ton"] = pd.to_numeric(overlay_df["dap_au_usd_per_ton"], errors="coerce").round(2)

        overlay_export_df = overlay_df.copy()
        overlay_export_df["observed_month"] = overlay_export_df["observed_month"].dt.strftime("%Y-%m-%d")
        overlay_export_df = json_safe_dataframe(overlay_export_df)
        overlay_rows = overlay_export_df.to_dict(orient="records")

        upsert_rows(
            supabase,
            "chart_fertilizer_overlay_monthly",
            overlay_rows,
            on_conflict="observed_month",
        )

        mark_source_success(supabase, len(normalized_rows))
        update_run_log_success(supabase, run_log_id, len(normalized_rows))
        refresh_source_freshness_from_registry(supabase, SOURCE_ID)

        print(f"Loaded {len(normalized_rows)} GrainGrowers normalized rows")
        print(f"Loaded {len(overlay_rows)} overlay chart rows")

    except Exception as exc:
        mark_source_failed(supabase, str(exc))
        update_run_log_failed(supabase, run_log_id, str(exc))
        refresh_source_freshness_from_registry(supabase, SOURCE_ID)
        raise


if __name__ == "__main__":
    main()