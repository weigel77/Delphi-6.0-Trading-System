"""CSV/XLSX trade import helpers for the Horme journal."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .trade_store import JOURNAL_NAME_DEFAULT, normalize_trade_payload

SUPPORTED_IMPORT_SUFFIXES = {".csv", ".xlsx"}
COLUMN_ALIASES = {
    "tradetype": "option_type",
    "status": "status",
    "system": "system_name",
    "systemname": "system_name",
    "systemversion": "system_version",
    "version": "system_version",
    "journal": "journal_name",
    "journalname": "journal_name",
    "profile": "candidate_profile",
    "candidateprofile": "candidate_profile",
    "modename": "candidate_profile",
    "tradedate": "trade_date",
    "date": "trade_date",
    "entrydate": "trade_date",
    "entrydatetime": "entry_datetime",
    "entrytime": "entry_datetime",
    "entrytimestamp": "entry_datetime",
    "expiration": "expiration_date",
    "expiry": "expiration_date",
    "expirationdate": "expiration_date",
    "symbol": "underlying_symbol",
    "ticker": "underlying_symbol",
    "underlying": "underlying_symbol",
    "underlyingsymbol": "underlying_symbol",
    "spx": "spx_at_entry",
    "spxentry": "spx_at_entry",
    "spxatentry": "spx_at_entry",
    "vix": "vix_at_entry",
    "vixentry": "vix_at_entry",
    "vixatentry": "vix_at_entry",
    "structure": "structure_grade",
    "structuregrade": "structure_grade",
    "macro": "macro_grade",
    "macrograde": "macro_grade",
    "expectedmove": "expected_move",
    "short": "short_strike",
    "shortstrike": "short_strike",
    "long": "long_strike",
    "longstrike": "long_strike",
    "width": "spread_width",
    "spreadwidth": "spread_width",
    "contracts": "contracts",
    "contract": "contracts",
    "qty": "contracts",
    "quantity": "contracts",
    "size": "contracts",
    "estimatecredit": "candidate_credit_estimate",
    "creditestimate": "candidate_credit_estimate",
    "candidatecreditestimate": "candidate_credit_estimate",
    "targetcredit": "candidate_credit_estimate",
    "credit": "actual_entry_credit",
    "netcredit": "actual_entry_credit",
    "entrycredit": "actual_entry_credit",
    "actualentrycredit": "actual_entry_credit",
    "premium": "actual_entry_credit",
    "premiumreceived": "actual_entry_credit",
    "distancetoshort": "distance_to_short",
    "distance": "distance_to_short",
    "shortdelta": "short_delta",
    "delta": "short_delta",
    "entrynotes": "notes_entry",
    "notes": "notes_entry",
    "notesentry": "notes_entry",
    "exitdatetime": "exit_datetime",
    "exittime": "exit_datetime",
    "exittimestamp": "exit_datetime",
    "spxexit": "spx_at_exit",
    "spxatexit": "spx_at_exit",
    "exitvalue": "actual_exit_value",
    "closevalue": "actual_exit_value",
    "actualexitvalue": "actual_exit_value",
    "debitpaid": "actual_exit_value",
    "closemethod": "close_method",
    "closereason": "close_reason",
    "exitnotes": "notes_exit",
    "notesexit": "notes_exit",
}


def parse_trade_import(file_storage: Any, *, trade_mode: str, journal_name: str = JOURNAL_NAME_DEFAULT) -> Dict[str, Any]:
    if file_storage is None:
        raise ValueError("Choose a CSV or Excel file before previewing the import.")

    filename = str(getattr(file_storage, "filename", "") or "").strip()
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_IMPORT_SUFFIXES:
        raise ValueError("Upload a .csv or .xlsx file to preview a trade import.")

    raw_bytes = file_storage.read()
    if not raw_bytes:
        raise ValueError("Select a non-empty CSV or Excel file to import.")

    dataframe = _read_import_frame(raw_bytes, suffix)
    if dataframe.empty and len(dataframe.columns) == 0:
        raise ValueError("The import file does not contain any columns to preview.")

    column_map = {column: resolve_import_field(column) for column in dataframe.columns}
    recognized_columns = [column for column, field in column_map.items() if field]
    if not recognized_columns:
        raise ValueError("No supported trade columns were found. Use headings like Date, Expiration, Short Strike, Long Strike, Qty, Net Credit, Exit Value, or Notes.")

    preview_rows = []
    for spreadsheet_row, record in enumerate(dataframe.to_dict(orient="records"), start=2):
        raw_values = build_default_trade_values(trade_mode=trade_mode, journal_name=journal_name)
        mapped_fields = []

        for source_column, field_name in column_map.items():
            if not field_name:
                continue
            value = clean_import_value(record.get(source_column))
            if value in {None, ""}:
                continue
            raw_values[field_name] = value
            mapped_fields.append(field_name)

        if raw_values.get("actual_entry_credit") not in {None, ""} and raw_values.get("candidate_credit_estimate") in {None, ""}:
            raw_values["candidate_credit_estimate"] = raw_values["actual_entry_credit"]
        if raw_values.get("trade_date") in {None, ""} and raw_values.get("entry_datetime") not in {None, ""}:
            raw_values["trade_date"] = raw_values["entry_datetime"]
        if raw_values.get("status") in {None, "", "open"} and (
            raw_values.get("actual_exit_value") not in {None, ""} or raw_values.get("exit_datetime") not in {None, ""}
        ):
            raw_values["status"] = "closed"

        if not mapped_fields:
            preview_rows.append(
                {
                    "row_number": spreadsheet_row,
                    "status": "invalid",
                    "messages": ["No supported trade fields were mapped from this row."],
                    "mapped_fields": [],
                    "payload": None,
                }
            )
            continue

        try:
            normalized_payload = normalize_trade_payload(raw_values)
        except ValueError as exc:
            preview_rows.append(
                {
                    "row_number": spreadsheet_row,
                    "status": "invalid",
                    "messages": [str(exc)],
                    "mapped_fields": sorted(set(mapped_fields)),
                    "payload": None,
                }
            )
            continue

        preview_rows.append(
            {
                "row_number": spreadsheet_row,
                "status": "ready",
                "messages": [],
                "mapped_fields": sorted(set(mapped_fields)),
                "payload": normalized_payload,
            }
        )

    return {
        "file_name": filename,
        "recognized_columns": recognized_columns,
        "rows": preview_rows,
    }


def build_default_trade_values(*, trade_mode: str, journal_name: str) -> Dict[str, Any]:
    return {
        "trade_number": "",
        "trade_mode": trade_mode,
        "system_name": "Apollo",
        "journal_name": journal_name or JOURNAL_NAME_DEFAULT,
        "system_version": "2.0",
        "candidate_profile": "Legacy",
        "status": "open",
        "trade_date": "",
        "entry_datetime": "",
        "expiration_date": "",
        "underlying_symbol": "SPX",
        "spx_at_entry": "",
        "vix_at_entry": "",
        "structure_grade": "",
        "macro_grade": "",
        "expected_move": "",
        "option_type": "Put Credit Spread",
        "short_strike": "",
        "long_strike": "",
        "spread_width": "",
        "contracts": "",
        "candidate_credit_estimate": "",
        "actual_entry_credit": "",
        "distance_to_short": "",
        "short_delta": "",
        "notes_entry": "",
        "exit_datetime": "",
        "spx_at_exit": "",
        "actual_exit_value": "",
        "close_method": "",
        "close_reason": "",
        "notes_exit": "",
    }


def resolve_import_field(column_name: Any) -> str | None:
    normalized_name = normalize_column_name(column_name)
    return COLUMN_ALIASES.get(normalized_name)


def normalize_column_name(column_name: Any) -> str:
    return "".join(character for character in str(column_name or "").strip().lower() if character.isalnum())


def clean_import_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
            return value.date()
        return value.to_pydatetime()
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except Exception:
            return value
    if isinstance(value, str):
        return value.strip()
    return value


def _read_import_frame(raw_bytes: bytes, suffix: str) -> pd.DataFrame:
    if suffix == ".csv":
        frame = pd.read_csv(BytesIO(raw_bytes), dtype=object)
    else:
        frame = pd.read_excel(BytesIO(raw_bytes), dtype=object)
    return frame.where(pd.notna(frame), "")
