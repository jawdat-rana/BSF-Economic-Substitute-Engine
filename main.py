import io
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests
from supabase import create_client, Client


WORLD_BANK_XLSX_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/"
    "CMO-Historical-Data-Monthly.xlsx"
)

SOURCE_ID = "wb_pinksheet_monthly"
PARSER_VERSION = "wb_monthly_prices_v1"

TARGET_COMMODITIES = {
    "Fish meal": {
        "commodity_code": "fishmeal",
        "commodity_name": "Fishmeal",
        "region": "global",
        "quality_spec": "World Bank Pink Sheet series",
    },
    "DAP": {
        "commodity_code": "dap",
        "commodity_name": "DAP",
        "region": "global",
        "quality_spec": "World Bank Pink Sheet series",
    },
    "Urea": {
        "commodity_code": "urea",
        "commodity_name": "Urea",
        "region": "global",
        "quality_spec": "World Bank Pink Sheet series",
    },
}

SOURCE_SLA_DAYS = {
    "wb_pinksheet_monthly": 40,
    "graingrowers_fertiliser_report": 21,
}

SOURCE_REGISTRY_BASE = {
    "source_id": SOURCE_ID,
    "source_name": "World Bank Commodity Price Data - Historical Monthly",
    "source_type": "official_dataset",
    "source_url": WORLD_BANK_XLSX_URL,
    "frequency_expected": "monthly",
    "default_currency": "USD",
    "default_unit": "metric_ton",
    "active_flag": True,
    "notes": "POC source for Urea, DAP, Fishmeal from Monthly Prices sheet",
}


def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


def download_excel(url: str) -> bytes:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.content


def load_monthly_prices_sheet(excel_bytes: bytes) -> pd.DataFrame:
    return pd.read_excel(
        io.BytesIO(excel_bytes),
        sheet_name="Monthly Prices",
        header=None,
        engine="openpyxl",
    )


def json_safe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    safe_df = df.copy()
    safe_df = safe_df.astype(object)
    safe_df = safe_df.where(pd.notnull(safe_df), None)
    return safe_df


def ensure_source_registry_row(supabase: Client) -> None:
    supabase.table("source_registry").upsert(
        SOURCE_REGISTRY_BASE,
        on_conflict="source_id",
    ).execute()


def mark_source_checked(supabase: Client, source_id: str, parser_version: str) -> None:
    payload = {
        **SOURCE_REGISTRY_BASE,
        "source_id": source_id,
        "last_checked_at": datetime.now(timezone.utc).isoformat(),
        "parser_version": parser_version,
        "run_status": "running",
        "last_error": None,
    }
    supabase.table("source_registry").upsert(
        payload,
        on_conflict="source_id",
    ).execute()


def mark_source_success(supabase: Client, source_id: str, row_count: int) -> None:
    now_utc = datetime.now(timezone.utc).isoformat()
    payload = {
        **SOURCE_REGISTRY_BASE,
        "source_id": source_id,
        "last_checked_at": now_utc,
        "last_success_at": now_utc,
        "parser_version": PARSER_VERSION,
        "run_status": "success",
        "last_row_count": row_count,
        "last_error": None,
    }
    supabase.table("source_registry").upsert(
        payload,
        on_conflict="source_id",
    ).execute()


def mark_source_failed(supabase: Client, source_id: str, error_message: str) -> None:
    payload = {
        **SOURCE_REGISTRY_BASE,
        "source_id": source_id,
        "last_checked_at": datetime.now(timezone.utc).isoformat(),
        "parser_version": PARSER_VERSION,
        "run_status": "failed",
        "last_error": error_message[:2000],
    }
    supabase.table("source_registry").upsert(
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


def parse_world_bank_monthly_prices(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the 'Monthly Prices' sheet using the observed workbook layout:
    - row 4: commodity names
    - row 5: units
    - row 6+: monthly data
    - col 0: month key like 1960M01
    """
    if raw_df is None or raw_df.empty:
        raise ValueError("Monthly Prices sheet is empty.")

    if len(raw_df) < 7:
        raise ValueError("Monthly Prices sheet does not have the expected minimum number of rows.")

    header_row_idx = 4
    unit_row_idx = 5
    data_start_row_idx = 6

    headers = raw_df.iloc[header_row_idx].tolist()
    units = raw_df.iloc[unit_row_idx].tolist()
    data_df = raw_df.iloc[data_start_row_idx:].copy()

    data_df.columns = [str(x).strip() if pd.notna(x) else "" for x in headers]
    data_df = data_df.rename(columns={"": "month_key"})

    if "month_key" not in data_df.columns:
        first_col_name = data_df.columns[0]
        data_df = data_df.rename(columns={first_col_name: "month_key"})

    data_df["month_key"] = data_df["month_key"].astype(str).str.strip()
    data_df = data_df[data_df["month_key"].str.match(r"^\d{4}M\d{2}$", na=False)].copy()

    if data_df.empty:
        raise ValueError("No monthly rows found after filtering month_key pattern YYYYMmm.")

    unit_map = {}
    for idx, header in enumerate(headers):
        header_name = str(header).strip() if pd.notna(header) else ""
        unit_value = units[idx] if idx < len(units) else None
        if header_name:
            unit_map[header_name] = str(unit_value).strip() if pd.notna(unit_value) else None

    records = []

    for source_col, meta in TARGET_COMMODITIES.items():
        if source_col not in data_df.columns:
            raise ValueError(
                f"Expected commodity column '{source_col}' not found. "
                f"Available columns include: {list(data_df.columns[:15])} ..."
            )

        subset = data_df[["month_key", source_col]].copy()
        subset = subset.rename(columns={source_col: "price_value"})

        subset["price_value"] = (
            subset["price_value"]
            .replace("…", pd.NA)
            .replace("...", pd.NA)
        )
        subset["price_value"] = pd.to_numeric(subset["price_value"], errors="coerce")
        subset = subset.dropna(subset=["price_value"]).copy()

        subset["observed_month"] = pd.to_datetime(
            subset["month_key"].str.replace(r"^(\d{4})M(\d{2})$", r"\1-\2-01", regex=True)
        )
        subset["report_date"] = subset["observed_month"]

        subset["source_id"] = SOURCE_ID
        subset["commodity_code"] = meta["commodity_code"]
        subset["commodity_name"] = meta["commodity_name"]
        subset["region"] = meta["region"]
        subset["quality_spec"] = meta["quality_spec"]
        subset["currency"] = "USD"
        subset["unit"] = "metric_ton"
        subset["raw_unit_label"] = unit_map.get(source_col)
        subset["evidence_type"] = "market_series"

        records.append(
            subset[
                [
                    "source_id",
                    "commodity_code",
                    "commodity_name",
                    "region",
                    "quality_spec",
                    "observed_month",
                    "report_date",
                    "price_value",
                    "currency",
                    "unit",
                    "raw_unit_label",
                    "evidence_type",
                ]
            ]
        )

    normalized_df = pd.concat(records, ignore_index=True)
    return normalized_df.sort_values(["commodity_code", "observed_month"])


def upsert_rows(
    supabase: Client,
    table_name: str,
    rows: List[Dict],
    on_conflict: Optional[str] = None,
    chunk_size: int = 500,
) -> None:
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        supabase.table(table_name).upsert(chunk, on_conflict=on_conflict).execute()


def build_chart_monthly(df: pd.DataFrame) -> pd.DataFrame:
    pivot = (
        df.pivot_table(
            index="observed_month",
            columns="commodity_code",
            values="price_value",
            aggfunc="first",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    return pivot.rename(
        columns={
            "urea": "urea_usd_per_ton",
            "dap": "dap_usd_per_ton",
            "fishmeal": "fishmeal_usd_per_ton",
        }
    )


def build_latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    latest = (
        df.sort_values(["commodity_code", "observed_month"])
        .groupby("commodity_code", as_index=False)
        .tail(1)
        .copy()
    )

    return latest[
        ["commodity_code", "commodity_name", "observed_month", "price_value", "currency", "unit"]
    ].rename(
        columns={
            "observed_month": "latest_month",
            "price_value": "latest_price",
        }
    )


def main() -> None:
    supabase = get_supabase()
    ensure_source_registry_row(supabase)
    mark_source_checked(supabase, SOURCE_ID, PARSER_VERSION)

    run_log_id = create_run_log_start(supabase, SOURCE_ID, PARSER_VERSION)

    try:
        excel_bytes = download_excel(WORLD_BANK_XLSX_URL)
        raw_df = load_monthly_prices_sheet(excel_bytes)
        normalized_df = parse_world_bank_monthly_prices(raw_df)

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

        monthly_chart_df = build_chart_monthly(normalized_df)
        monthly_chart_export_df = monthly_chart_df.copy()
        monthly_chart_export_df["observed_month"] = monthly_chart_export_df["observed_month"].dt.strftime("%Y-%m-%d")
        monthly_chart_export_df = json_safe_dataframe(monthly_chart_export_df)
        monthly_chart_rows = monthly_chart_export_df.to_dict(orient="records")

        upsert_rows(
            supabase,
            "chart_fertilizer_vs_fishmeal_monthly",
            monthly_chart_rows,
            on_conflict="observed_month",
        )

        latest_snapshot_df = build_latest_snapshot(normalized_df)
        latest_snapshot_export_df = latest_snapshot_df.copy()
        latest_snapshot_export_df["latest_month"] = latest_snapshot_export_df["latest_month"].dt.strftime("%Y-%m-%d")
        latest_snapshot_export_df = json_safe_dataframe(latest_snapshot_export_df)
        latest_snapshot_rows = latest_snapshot_export_df.to_dict(orient="records")

        upsert_rows(
            supabase,
            "chart_latest_snapshot",
            latest_snapshot_rows,
            on_conflict="commodity_code",
        )

        mark_source_success(supabase, SOURCE_ID, len(normalized_rows))
        update_run_log_success(supabase, run_log_id, len(normalized_rows))
        refresh_source_freshness_from_registry(supabase, SOURCE_ID)

        print(f"Loaded {len(normalized_rows)} normalized rows")
        print(f"Loaded {len(monthly_chart_rows)} monthly chart rows")
        print(f"Loaded {len(latest_snapshot_rows)} latest snapshot rows")

    except Exception as exc:
        mark_source_failed(supabase, SOURCE_ID, str(exc))
        update_run_log_failed(supabase, run_log_id, str(exc))
        refresh_source_freshness_from_registry(supabase, SOURCE_ID)
        raise


if __name__ == "__main__":
    main()