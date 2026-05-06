"""
Логіка розбиття рядків за email_provider_research_team × регіон (USA / Europe).
Відповідає поведінці Google Apps Script користувача.
"""

from __future__ import annotations

import io
import re
from typing import Any

import pandas as pd
import requests

OUTPUT_SHEETS = [
    "Other (gmail, etc) USA",
    "Other (gmail, etc) Europe",
    "outlook USA",
    "outlook Europe",
]

DEFAULT_PROVIDER_COLUMN = "email_provider_research_team"
DEFAULT_LOCATION_COLUMN = ""


EU_PHRASES = [
    "united kingdom",
    "great britain",
    "north macedonia",
    "czech republic",
    "bosnia and herzegovina",
]

EU_WORDS = [
    "albania",
    "andorra",
    "austria",
    "belarus",
    "belgium",
    "bulgaria",
    "croatia",
    "cyprus",
    "czechia",
    "czech",
    "denmark",
    "estonia",
    "finland",
    "france",
    "germany",
    "greece",
    "hungary",
    "iceland",
    "ireland",
    "italy",
    "latvia",
    "liechtenstein",
    "lithuania",
    "luxembourg",
    "malta",
    "moldova",
    "monaco",
    "montenegro",
    "netherlands",
    "holland",
    "norway",
    "poland",
    "portugal",
    "romania",
    "russia",
    "serbia",
    "slovakia",
    "slovenia",
    "spain",
    "sweden",
    "switzerland",
    "ukraine",
    "england",
    "scotland",
    "wales",
    "kosovo",
    "vatican",
    "uk",
    "gb",
]

EU2 = {
    "at",
    "be",
    "bg",
    "hr",
    "cy",
    "cz",
    "dk",
    "ee",
    "fi",
    "fr",
    "de",
    "gr",
    "hu",
    "ie",
    "it",
    "lv",
    "lt",
    "lu",
    "mt",
    "nl",
    "pl",
    "pt",
    "ro",
    "sk",
    "si",
    "es",
    "se",
    "gb",
    "uk",
    "no",
    "ch",
    "is",
    "li",
    "me",
    "rs",
    "ba",
    "mk",
    "al",
    "ad",
    "mc",
    "sm",
    "va",
    "by",
    "md",
    "ua",
    "ru",
}


def extract_spreadsheet_id(s: str) -> str:
    s = (s or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9-_]+", s):
        return s
    raise ValueError("Невірне посилання або ID таблиці.")


def extract_gid_from_url(url: str) -> str | None:
    if not url:
        return None
    m = re.search(r"[#&?]gid=(\d+)", url)
    if m:
        return m.group(1)
    return None


def discover_google_sheet_gids(spreadsheet_url_or_id: str, *, timeout: int = 60) -> list[str]:
    """
    Намагається витягнути gid аркушів із HTML сторінки /edit (без Google API).
    Публічна таблиця з доступом «перегляд за посиланням» зазвичай віддає gid у розмітці.
    """
    sid = extract_spreadsheet_id(spreadsheet_url_or_id)
    url = f"https://docs.google.com/spreadsheets/d/{sid}/edit"
    r = requests.get(url, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(
            f"Не вдалося завантажити HTML таблиці для пошуку аркушів (HTTP {r.status_code}). "
            "Перевірте доступ за посиланням."
        )
    text = r.text
    patterns = (
        r'"sheetId"\s*:\s*(\d+)',
        r'"gid"\s*:\s*(\d+)',
        r"[#&?]gid=(\d+)",
    )
    seen: set[str] = set()
    ordered: list[str] = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            gid = m.group(1)
            if gid not in seen:
                seen.add(gid)
                ordered.append(gid)
    return ordered


def google_sheet_csv_export_url(spreadsheet_url_or_id: str, gid: str | None = None) -> str:
    sid = extract_spreadsheet_id(spreadsheet_url_or_id)
    base = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv"
    if gid:
        base += f"&gid={gid}"
    return base


def google_sheet_xlsx_export_url(spreadsheet_url_or_id: str) -> str:
    sid = extract_spreadsheet_id(spreadsheet_url_or_id)
    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx"


def load_spreadsheet_all_tabs_via_xlsx_export(
    spreadsheet_url_or_id: str,
    *,
    timeout: int = 120,
) -> list[tuple[str, pd.DataFrame]]:
    """
    Одне завантаження книги як XLSX з docs.google.com/.../export (усі аркуші в одному файлі).
    Якщо експорт недоступний або файл не читається — порожній список (викликайте fallback на CSV/gid).
    Потребує openpyxl (через pandas.read_excel).
    """
    try:
        sid = extract_spreadsheet_id(spreadsheet_url_or_id)
    except ValueError:
        return []
    url = google_sheet_xlsx_export_url(sid)
    try:
        r = requests.get(url, timeout=timeout)
    except requests.RequestException:
        return []
    if r.status_code != 200:
        return []
    try:
        xf = pd.ExcelFile(io.BytesIO(r.content), engine="openpyxl")
    except Exception:
        return []
    out: list[tuple[str, pd.DataFrame]] = []
    for name in xf.sheet_names:
        try:
            df = pd.read_excel(xf, sheet_name=name, header=0)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        out.append((str(name), df))
    return out


def normalize_provider(cell: Any) -> str | None:
    t = str(cell or "").strip().lower()
    if not t:
        return None
    if "outlook" in t:
        return "outlook"
    if "gmail" in t or "other" in t or "yahoo" in t:
        return "other"
    return None


def find_country_column_indices(header_row: list[Any]) -> list[int]:
    indices: list[int] = []
    for i, cell in enumerate(header_row):
        h = str(cell or "").strip().lower()
        if not h:
            continue
        if h == "country" or "country" in h:
            indices.append(i)
    return indices


def find_column_index(header_row: list[Any], name: str) -> int:
    want = str(name).strip().lower()
    for i, cell in enumerate(header_row):
        if str(cell).strip().lower() == want:
            return i
    raise ValueError(f'Колонку "{name}" не знайдено в першому рядку.')


def normalize_region_from_country_value(cell: Any) -> str | None:
    raw = str(cell or "").strip()
    if not raw:
        return None
    if "@" in raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        return None
    if len(raw) > 200:
        return None

    t = raw.lower()

    if t in ("us", "usa", "u.s.", "u.s.a."):
        return "usa"
    if "united states" in t:
        return "usa"
    if t in ("america", "united states of america"):
        return "usa"
    if "usa" in t and "europe" not in t:
        return "usa"

    if t == "canada" or "canada" in t:
        return "usa"
    if t == "canadian":
        return "usa"

    if t in ("eu", "europe", "european union") or "europe" in t:
        return "europe"

    for p in EU_PHRASES:
        if p in t:
            return "europe"

    for word in EU_WORDS:
        if len(word) <= 2:
            if t == word:
                return "europe"
        elif t == word or word in t:
            return "europe"

    if re.fullmatch(r"[a-z]{2}", t):
        if t == "us":
            return "usa"
        if t == "ca":
            return "usa"
        if t in EU2:
            return "europe"

    return None


def resolve_region_for_row(
    row: list[Any], country_indices: list[int], provider_column_index: int
) -> str | None:
    tried: set[int] = set()
    for idx in country_indices:
        tried.add(idx)
        if idx < len(row):
            lv = normalize_region_from_country_value(row[idx])
            if lv:
                return lv
    for c, val in enumerate(row):
        if c == provider_column_index or c in tried:
            continue
        lv2 = normalize_region_from_country_value(val)
        if lv2:
            return lv2
    return None


def split_rows(
    rows: list[list[Any]],
    provider_column: str = DEFAULT_PROVIDER_COLUMN,
    location_column: str = DEFAULT_LOCATION_COLUMN,
) -> tuple[dict[str, list[list[Any]]], list[list[Any]], list[Any]]:
    if not rows:
        raise ValueError("Немає рядків.")
    header = list(rows[0])
    body = [list(r) for r in rows[1:]]
    if not header:
        raise ValueError("Лист порожній.")

    pi = find_column_index(header, provider_column)
    country_indices = find_country_column_indices(header)
    use_override = bool(str(location_column or "").strip())
    li_override = -1
    if use_override:
        li_override = find_column_index(header, str(location_column).strip())

    buckets: dict[str, list[list[Any]]] = {n: [] for n in OUTPUT_SHEETS}
    unmatched: list[list[Any]] = []

    max_cols = len(header)
    for row in body:
        while len(row) < max_cols:
            row.append("")
        pv = normalize_provider(row[pi] if pi < len(row) else "")
        if li_override >= 0:
            lv = normalize_region_from_country_value(
                row[li_override] if li_override < len(row) else ""
            )
        else:
            lv = resolve_region_for_row(row, country_indices, pi)

        if pv == "other" and lv == "usa":
            buckets[OUTPUT_SHEETS[0]].append(row)
        elif pv == "other" and lv == "europe":
            buckets[OUTPUT_SHEETS[1]].append(row)
        elif pv == "outlook" and lv == "usa":
            buckets[OUTPUT_SHEETS[2]].append(row)
        elif pv == "outlook" and lv == "europe":
            buckets[OUTPUT_SHEETS[3]].append(row)
        else:
            unmatched.append(row)

    return buckets, unmatched, header


def split_dataframe(
    df: pd.DataFrame,
    provider_column: str = DEFAULT_PROVIDER_COLUMN,
    location_column: str = DEFAULT_LOCATION_COLUMN,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame | None]:
    rows = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()
    buckets_rows, unmatched_rows, header = split_rows(
        rows, provider_column=provider_column, location_column=location_column
    )
    out: dict[str, pd.DataFrame] = {}
    for name, data_rows in buckets_rows.items():
        if data_rows:
            out[name] = pd.DataFrame(data_rows, columns=header)
        else:
            out[name] = pd.DataFrame(columns=header)
    unmatched_df = None
    if unmatched_rows:
        unmatched_df = pd.DataFrame(unmatched_rows, columns=header)
    return out, unmatched_df


def summary_lines(buckets: dict[str, pd.DataFrame], unmatched: pd.DataFrame | None) -> list[str]:
    lines = [f"{name}: {len(df)} рядків даних" for name, df in buckets.items()]
    if buckets:
        total = sum(len(df) for df in buckets.values())
        lines.append(f"Усього (обрана область): {total} рядків даних")
    if unmatched is not None and len(unmatched):
        lines.append(f"_Unmatched_split: {len(unmatched)} рядків")
    return lines
