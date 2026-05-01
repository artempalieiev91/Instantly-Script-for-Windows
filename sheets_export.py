"""
Запис розбитих сегментів у Google Таблицю через Sheets API (service account).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)


def sheets_title_safe(name: str) -> str:
    t = "".join(c if c not in r':\/?*[]' else "-" for c in (name or ""))
    return (t[:100] or "Sheet").strip()


def _a1_range(sheet_title: str, cell: str = "A1") -> str:
    safe = sheets_title_safe(sheet_title)
    return "'" + safe.replace("'", "''") + f"'!{cell}"


def build_service_from_json_bytes(data: bytes):
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError as e:
        raise RuntimeError(
            "Встановіть пакети: pip install google-api-python-client google-auth"
        ) from e
    info = json.loads(data.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def build_service_from_json_path(path: str | Path):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(
        str(path), scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _list_sheet_titles(service, spreadsheet_id: str) -> list[str]:
    meta = (
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties.title",
        )
        .execute()
    )
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def ensure_sheet(service, spreadsheet_id: str, title: str) -> str:
    safe = sheets_title_safe(title)
    if safe in _list_sheet_titles(service, spreadsheet_id):
        return safe
    body = {"requests": [{"addSheet": {"properties": {"title": safe}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    return safe


def _clear_sheet_values(service, spreadsheet_id: str, sheet_title: str) -> None:
    safe = sheets_title_safe(sheet_title)
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=_a1_range(safe, "A:ZZ"),
    ).execute()


def _cell_for_sheet(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    if isinstance(v, (int, bool)):
        return v
    if isinstance(v, float):
        return v
    return str(v)


def _pad_rows(rows: list[list[Any]]) -> list[list[Any]]:
    if not rows:
        return []
    m = max(len(r) for r in rows)
    return [list(r) + [""] * (m - len(r)) for r in rows]


def dataframe_to_values(df: pd.DataFrame) -> list[list[Any]]:
    header = [str(c) for c in df.columns.tolist()]
    rows: list[list[Any]] = [header]
    if df.empty:
        return rows
    for row in df.itertuples(index=False, name=None):
        rows.append([_cell_for_sheet(v) for v in row])
    return _pad_rows(rows)


def write_dataframe_to_sheet(
    service,
    spreadsheet_id: str,
    logical_title: str,
    df: pd.DataFrame,
) -> None:
    ensure_sheet(service, spreadsheet_id, logical_title)
    safe = sheets_title_safe(logical_title)
    _clear_sheet_values(service, spreadsheet_id, safe)
    values = dataframe_to_values(df)
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=_a1_range(safe, "A1"),
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def export_split_to_spreadsheet(
    service,
    spreadsheet_id: str,
    buckets: dict[str, pd.DataFrame],
    unmatched: pd.DataFrame | None,
) -> None:
    for name, bdf in buckets.items():
        write_dataframe_to_sheet(service, spreadsheet_id, name, bdf)
    header_cols = next(iter(buckets.values())).columns
    um = (
        unmatched
        if unmatched is not None and len(unmatched) > 0
        else pd.DataFrame(columns=header_cols)
    )
    write_dataframe_to_sheet(service, spreadsheet_id, "_Unmatched_split", um)
