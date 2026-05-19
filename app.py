"""
Streamlit: розбиття таблиці за логікою Instantly (провайдер × USA/Europe).
"""

from __future__ import annotations

import hashlib
import html
import io
import json
import os
import pickle
import re
import uuid
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st

import instantly_import_mapping as imap
from instantly_api import (
    InstantlyClient,
    parse_email_list_field,
    partition_sender_pool_lines,
)
from instantly_workflow import (
    ORDERED_GMAIL_BUCKETS,
    ORDERED_OUTLOOK_BUCKETS,
    buckets_for_provider_scope,
    run_full_pipeline,
)
from pipedrive_api import (
    PipedriveError,
    collect_excluded_person_ids,
    fetch_person_fields,
    normalize_sheet_person_id,
)
from split_engine import (
    DEFAULT_PROVIDER_COLUMN,
    LH_GEO_BUCKET_EUROPE,
    LH_GEO_BUCKET_USA_CA,
    discover_google_sheet_gids,
    extract_gid_from_url,
    extract_spreadsheet_id,
    google_sheet_csv_export_url,
    load_spreadsheet_all_tabs_via_xlsx_export,
    split_dataframe,
    split_dataframe_geo_only,
    summary_lines,
)

_IMPORT_CV_ALIASES_PATH = Path(__file__).resolve().parent / imap.USER_IMPORT_CV_ALIASES_FILENAME
_SENDER_POOLS_SIDEBAR_PATH = Path(__file__).resolve().parent / "instantly_sender_pools_sidebar.json"
# Персона в Pipedrive: кастомне поле з цією назвою (як у UI CRM) для правила «Replied at = …».
_LH_PIPEDRIVE_REPLIED_AT_FIELD = "Replied at"
_CRM_SAVED_RUNS_PATH = Path(__file__).resolve().parent / "crm_saved_runs.json"
_CRM_TEMPLATE_VARS_PATH = Path(__file__).resolve().parent / "crm_template_variables.json"

_CRM_TEMPLATE_VARS_DEFAULTS: dict[str, str] = {
    "person_sdr_responsible": "Maryna",
    "activity_due_date": "2026-04-29",
    "activity_done": "Done",
    "activity_type": "email campaign",
    "activity_assigned_to_user": "Maryna Chut",
    "person_target_event_append_sheet_activity_subject": "1",
    "person_target_event_extra_suffix": "",
}

_CRM_TEMPLATE_BOOL_KEYS: frozenset[str] = frozenset({"person_target_event_append_sheet_activity_subject"})

_CRM_TEMPLATE_VAR_FIELDS: tuple[tuple[str, str], ...] = (
    ("person_sdr_responsible", "Person - SDR Responsible"),
    ("activity_due_date", "Activity - Due date"),
    ("activity_done", "Activity - Done"),
    ("activity_type", "Activity type"),
    ("activity_assigned_to_user", "Activity assigned to user"),
)

_CRM_TPL_PLACEHOLDER_MIGRATION = "_crm_tpl_placeholder_migrated_v6"

_MATCH_LOAD_MODE_ONE_ALL = "one_book_all_tabs"
_MATCH_LOAD_MODE_MANY_FIRST = "many_books_first_tab"

# Якщо в назві запису CRM є слово Event/Events — Activity - Subject = «назва» + «-» + колонка з таблиці.
_CRM_EVENT_TITLE_SUBJECT_RE = re.compile(r"(?i)\bevents?\b")


_CRM_ACTIVITY_EXPORT_COLUMNS: tuple[str, ...] = (
    "Person - SDR Responsible",
    "Activity - Subject",
    "Activity - Due date",
    "Activity - ID",
    "Person - ID",
    "Activity - Done",
    "Activity type",
    "Activity assigned to user",
    "Person - TargetEvent",
)

_CRM_SPLIT_ONLY_LOG: list[dict[str, str]] = [
    {"примітка": "Збережено після розбиття таблиці без запуску Instantly API."},
]

# Вибір у блоці «CRM після розбиття»: один запис на обидва регіони провайдера (USA + Europe).
_CRM_SPLIT_COMBINED_GMAIL_KEY = "__crm_split_combined_gmail__"
_CRM_SPLIT_COMBINED_OUTLOOK_KEY = "__crm_split_combined_outlook__"
_CRM_SPLIT_COMBINED_ALL_KEY = "__crm_split_combined_gmail_outlook__"


def _crm_json_safe(obj: Any) -> Any:
    """Серіалізація журналу (int64, Timestamp тощо) у JSON-сумісний вигляд."""
    return json.loads(json.dumps(obj, default=str))


def _dataframe_records_json_safe(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Рядки таблиці → список dict для JSON (NaN → null, дати ISO)."""
    if df is None or df.empty:
        return []
    raw = df.to_json(orient="records", date_format="iso", default_handler=str)
    return json.loads(raw)


def _contacts_snapshot_from_buckets(
    work_buckets: dict[str, pd.DataFrame],
    unmatched: pd.DataFrame | None,
    provider_scope: str,
) -> dict[str, Any]:
    contacts_by_bucket = {
        name: _dataframe_records_json_safe(bdf)
        for name, bdf in work_buckets.items()
        if bdf is not None and not bdf.empty
    }
    out: dict[str, Any] = {
        "provider_scope": provider_scope,
        "contacts_by_bucket": contacts_by_bucket,
        "unmatched_rows": (
            _dataframe_records_json_safe(unmatched)
            if unmatched is not None and len(unmatched)
            else None
        ),
    }
    return _crm_json_safe(out)


def _contacts_snapshot_single_bucket(
    bucket_name: str,
    df: pd.DataFrame,
    provider_scope: str,
) -> dict[str, Any]:
    rows = _dataframe_records_json_safe(df)
    out: dict[str, Any] = {
        "provider_scope": provider_scope,
        "contacts_by_bucket": {bucket_name: rows} if rows else {},
        "unmatched_rows": None,
        "saved_from": "split_only",
    }
    return _crm_json_safe(out)


def _contacts_snapshot_split_only_multi_buckets(
    bucket_name_to_df: dict[str, pd.DataFrame],
    provider_scope: str,
) -> dict[str, Any]:
    """Як split_only, але кілька сегментів у contacts_by_bucket (наприклад Gmail USA + EU)."""
    contacts_by_bucket: dict[str, list[dict[str, Any]]] = {}
    for name, bdf in bucket_name_to_df.items():
        if bdf is None or bdf.empty:
            continue
        contacts_by_bucket[name] = _dataframe_records_json_safe(bdf)
    out: dict[str, Any] = {
        "provider_scope": provider_scope,
        "contacts_by_bucket": contacts_by_bucket,
        "unmatched_rows": None,
        "saved_from": "split_only",
    }
    return _crm_json_safe(out)


def _contacts_snapshot_has_rows(snap: dict[str, Any] | None) -> bool:
    if not snap or not isinstance(snap, dict):
        return False
    cb = snap.get("contacts_by_bucket")
    if isinstance(cb, dict):
        for v in cb.values():
            if isinstance(v, list) and len(v) > 0:
                return True
    um = snap.get("unmatched_rows")
    return isinstance(um, list) and len(um) > 0


def _crm_snapshot_gmail_outlook_row_counts(snap: dict[str, Any] | None) -> tuple[int, int]:
    """Суми рядків по кошиках Gmail/Other (USA+EU) та Outlook (USA+EU) у contacts_snapshot."""
    if not snap or not isinstance(snap, dict):
        return 0, 0
    cb = snap.get("contacts_by_bucket")
    if not isinstance(cb, dict):
        return 0, 0

    def _sum(keys: tuple[str, ...]) -> int:
        n = 0
        for k in keys:
            rows = cb.get(k)
            if isinstance(rows, list):
                n += len(rows)
        return n

    return _sum(ORDERED_GMAIL_BUCKETS), _sum(ORDERED_OUTLOOK_BUCKETS)


def _combined_contacts_df(contacts_by_bucket: dict[str, Any]) -> pd.DataFrame | None:
    """Один DataFrame: усі сегменти + колонка _segment."""
    frames: list[pd.DataFrame] = []
    if not isinstance(contacts_by_bucket, dict):
        return None
    for seg, rows in contacts_by_bucket.items():
        if not isinstance(rows, list) or not rows:
            continue
        sub = pd.DataFrame(rows)
        sub["_segment"] = seg
        frames.append(sub)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _load_crm_saved_runs() -> list[dict[str, Any]]:
    if not _CRM_SAVED_RUNS_PATH.is_file():
        return []
    try:
        raw = json.loads(_CRM_SAVED_RUNS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            out.append(item)
    return out


def _save_crm_saved_runs(entries: list[dict[str, Any]]) -> tuple[bool, str | None]:
    try:
        _CRM_SAVED_RUNS_PATH.write_text(
            json.dumps(entries, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return True, None
    except OSError as ex:
        return False, str(ex)


def _append_crm_saved_run(
    campaign_name_prefix: str,
    log: list[Any] | None,
    *,
    contacts_snapshot: dict[str, Any] | None = None,
    entry_kind: str = "instantly_pipeline",
) -> tuple[bool, str | None]:
    prefix = (campaign_name_prefix or "").strip()
    if not prefix:
        return False, "Порожній префікс."
    if entry_kind == "split_only":
        if not _contacts_snapshot_has_rows(contacts_snapshot):
            return False, "Немає рядків контактів для збереження."
        log_out: list[Any] = list(_CRM_SPLIT_ONLY_LOG)
    else:
        if not log:
            return False, "Журнал порожній."
        log_out = log
    entry: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "campaign_name_prefix": prefix,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "entry_kind": entry_kind,
        "log": _crm_json_safe(log_out),
    }
    if contacts_snapshot:
        entry["contacts_snapshot"] = _crm_json_safe(contacts_snapshot)
    entries = _load_crm_saved_runs()
    entries.append(entry)
    return _save_crm_saved_runs(entries)


def _load_sender_pools_sidebar_file() -> dict[str, str]:
    if not _SENDER_POOLS_SIDEBAR_PATH.is_file():
        return {}
    try:
        raw = json.loads(_SENDER_POOLS_SIDEBAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for key in ("gmail_other", "outlook"):
        v = raw.get(key, "")
        out[key] = v if isinstance(v, str) else str(v or "")
    return out


def _ensure_sender_pools_widget_state() -> None:
    if (
        "instantly_pool_gmail" in st.session_state
        and "instantly_pool_outlook" in st.session_state
    ):
        return
    disk = _load_sender_pools_sidebar_file()
    st.session_state.setdefault(
        "instantly_pool_gmail", str(disk.get("gmail_other", "") or "")
    )
    st.session_state.setdefault(
        "instantly_pool_outlook", str(disk.get("outlook", "") or "")
    )


def _persist_sender_pools_sidebar_from_session() -> None:
    try:
        payload = {
            "gmail_other": str(st.session_state.get("instantly_pool_gmail", "")),
            "outlook": str(st.session_state.get("instantly_pool_outlook", "")),
        }
        _SENDER_POOLS_SIDEBAR_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError:
        pass


def _write_empty_sender_pools_sidebar_file() -> None:
    try:
        _SENDER_POOLS_SIDEBAR_PATH.write_text(
            json.dumps({"gmail_other": "", "outlook": ""}, ensure_ascii=False, indent=2)
            + "\n",
            encoding="utf-8",
        )
    except OSError:
        pass


def _ensure_import_cv_aliases_text_state() -> None:
    if "import_cv_aliases_text" in st.session_state:
        return
    if _IMPORT_CV_ALIASES_PATH.is_file():
        try:
            st.session_state.import_cv_aliases_text = _IMPORT_CV_ALIASES_PATH.read_text(
                encoding="utf-8"
            )
        except OSError:
            st.session_state.import_cv_aliases_text = ""
    else:
        st.session_state.import_cv_aliases_text = ""


def _persist_import_cv_aliases_to_disk() -> tuple[bool, str | None]:
    """
    Зберігає поточний текст поля аліасів у user_import_column_aliases.txt.
    Повертає (успіх, текст помилки при невдачі).
    """
    try:
        _IMPORT_CV_ALIASES_PATH.write_text(
            str(st.session_state.get("import_cv_aliases_text") or ""),
            encoding="utf-8",
        )
        return True, None
    except OSError as ex:
        return False, str(ex)


def _render_import_cv_aliases_panel() -> None:
    """Окремий блок на головній сторінці (не всередині вкладених expander — інакше легко не помітити)."""
    with st.expander(
        "Маппінг колонок CSV → Instantly: повний перелік + ваші доповнення",
        expanded=False,
    ):
        st.caption(
            f"Нижче — **усі вбудовані відповідності** і **ваші рядки з поля** (якщо є). "
            f"Власні правила: кожен рядок — **назва стовпця**, потім **`,`** або **`=>`**, потім **ключ** (`email_01`, `subject_line`…). "
            f"Регістр ігнорується; `#` — коментар. Файл: `{imap.USER_IMPORT_CV_ALIASES_FILENAME}` (підтягується при старті; "
            "також записується при «Виконати API…» або кнопці збереження). "
            "**Інший ПК / колега:** передати разом із папкою проєкту цей `.txt` або закомітити його в git — сам по собі код твої дописи з платформи не несе; вони лише в цьому файлі на диску."
        )
        _ref_rows = imap.reference_mapping_rows_for_ui()
        _, _alias_notes, _alias_preview = imap.parse_user_cv_alias_text(
            str(st.session_state.get("import_cv_aliases_text") or "")
        )
        _user_rows: list[dict[str, str]] = [
            {
                "Група": "Ваші додаткові правила (мають пріоритет при збігу назви)",
                "Джерело (CSV)": src,
                "Куди в API": f"custom_variables → {tgt}",
            }
            for src, tgt in _alias_preview
        ]
        _all_preview = _ref_rows + _user_rows
        st.dataframe(
            pd.DataFrame(_all_preview),
            use_container_width=True,
            height=min(520, 60 + 26 * len(_all_preview)),
        )
        st.text_area(
            "Додаткові відповідності (редактор)",
            key="import_cv_aliases_text",
            height=140,
            placeholder=(
                "# приклад:\n"
                "my_export_subject, subject_line\n"
                "long_prefix_email_01_body => email_01\n"
            ),
        )
        ac1, ac2 = st.columns(2)
        with ac1:
            if st.button(
                "Зберегти список у файл",
                key="btn_save_cv_aliases",
                help=f"Запис у {_IMPORT_CV_ALIASES_PATH.name}",
            ):
                ok, err_wr = _persist_import_cv_aliases_to_disk()
                if ok:
                    st.success(f"Збережено: {_IMPORT_CV_ALIASES_PATH.name}")
                else:
                    st.error(err_wr or "Не вдалося записати файл.")
        with ac2:
            if st.button("Перечитати з файлу", key="btn_reload_cv_aliases"):
                try:
                    if _IMPORT_CV_ALIASES_PATH.is_file():
                        st.session_state.import_cv_aliases_text = (
                            _IMPORT_CV_ALIASES_PATH.read_text(encoding="utf-8")
                        )
                    else:
                        st.session_state.import_cv_aliases_text = ""
                    st.rerun()
                except OSError as ex:
                    st.error(str(ex))
        if _alias_notes:
            st.warning("\n".join(_alias_notes[:12]))
            if len(_alias_notes) > 12:
                st.caption(f"… ще повідомлень: {len(_alias_notes) - 12}")


def load_csv_from_bytes(data: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(data))


def load_sheet_from_url(url: str) -> pd.DataFrame:
    gid = extract_gid_from_url(url)
    export_url = google_sheet_csv_export_url(url, gid=gid)
    r = requests.get(export_url, timeout=60)
    if r.status_code == 403:
        raise RuntimeError(
            "Google повернув 403 (таблиця недоступна за посиланням). "
            "Відкрийте доступ «будь-хто за посиланням» (перегляд) або завантажте CSV з комп’ютера."
        )
    if r.status_code != 200:
        raise RuntimeError(f"Не вдалося завантажити таблицю (HTTP {r.status_code}).")
    return load_csv_from_bytes(r.content)


def load_sheet_from_url_first_tab(url: str) -> pd.DataFrame:
    """Перший аркуш книги; **`gid` / `#gid=` у посиланні ігнорується** (для метчу LH: завжди 1-й лист)."""
    return load_sheet_from_url(_spreadsheet_edit_url_first_sheet(url))


def _spreadsheet_edit_url_first_sheet(url_or_id: str) -> str:
    sid = extract_spreadsheet_id(url_or_id.strip())
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit"


def load_google_workbook_all_tabs_csv_frames(base_url: str) -> list[tuple[str, pd.DataFrame]]:
    """Одна таблиця: усі аркуші — спершу один XLSX export усієї книги; якщо не вийшло — gid із HTML /edit + CSV."""
    u = base_url.strip()
    pairs_xlsx = load_spreadsheet_all_tabs_via_xlsx_export(u)
    if pairs_xlsx:
        return pairs_xlsx

    sid = extract_spreadsheet_id(u)
    try:
        gids = discover_google_sheet_gids(u)
    except Exception:
        gids = []
    if not gids:
        df = load_sheet_from_url(_spreadsheet_edit_url_first_sheet(u))
        return [("(перший аркуш)", df)]
    out: list[tuple[str, pd.DataFrame]] = []
    for gid in gids:
        href = f"https://docs.google.com/spreadsheets/d/{sid}/edit#gid={gid}"
        try:
            df = load_sheet_from_url(href)
        except Exception:
            continue
        if df is not None and not df.empty:
            out.append((f"gid {gid}", df))
    if not out:
        df = load_sheet_from_url(_spreadsheet_edit_url_first_sheet(u))
        return [("(fallback перший аркуш)", df)]
    return out


def load_google_many_first_tabs_csv_frames(lines_block: str) -> list[tuple[str, pd.DataFrame]]:
    """Багато таблиць: по першому аркушу кожної (ігнорується gid у посиланні).

    Якщо в списку кілька рядків вказують на одну й ту саму книгу (той самий spreadsheet ID),
    завантаження виконується лише раз — збережено порядок, враховується перше посилання.
    """
    lines = [ln.strip() for ln in lines_block.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("Додайте хоча б один рядок із посиланням або ID таблиці.")
    out: list[tuple[str, pd.DataFrame]] = []
    seen_ids: set[str] = set()
    for ln in lines:
        sid = extract_spreadsheet_id(ln)
        if sid in seen_ids:
            continue
        seen_ids.add(sid)
        df = load_sheet_from_url(_spreadsheet_edit_url_first_sheet(ln))
        out.append((sid[:14] + "…", df))
    return out


def load_google_one_linked_tab_per_line(lines_block: str) -> list[tuple[str, pd.DataFrame]]:
    """
    Linked Helper: кожен рядок — одне посилання на Google Таблицю.
    Береться **лише той аркуш**, що зашитий у посиланні (`#gid=` / `?gid=` у URL);
    якщо gid немає — CSV-експорт без gid (зазвичай перший аркуш книги).

    Той самий рядок-посилання двічі не завантажується.
    """
    lines_raw = [ln.strip() for ln in (lines_block or "").splitlines() if ln.strip()]
    if not lines_raw:
        raise ValueError("Додайте хоча б один рядок із посиланням.")
    seen_url: set[str] = set()
    lines: list[str] = []
    for ln in lines_raw:
        if ln in seen_url:
            continue
        seen_url.add(ln)
        lines.append(ln)

    out: list[tuple[str, pd.DataFrame]] = []
    for i, ln in enumerate(lines, start=1):
        sid = extract_spreadsheet_id(ln)
        gid = extract_gid_from_url(ln)
        tag = f"gid{gid}" if gid else "no_gid"
        label = f"{i}:{sid[:10]}…_{tag}"
        df = load_sheet_from_url(ln)
        if df is None or df.empty:
            continue
        out.append((label, df))
    if not out:
        raise RuntimeError("Не вдалося завантажити жодного непорожнього аркуша за посиланнями.")
    return out


def _concat_sheet_frames_with_label(pairs: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for label, df in pairs:
        if df is None or df.empty:
            continue
        d = df.copy()
        d["_sheet_label"] = label
        parts.append(d)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


_LH_CRM_ZIP_FILENAME = "CRM_LinkedHelper.csv"
_LH_CRM_EXPORT_COLUMNS: tuple[str, ...] = (
    "Person ID",
    "Activity - Subject",
    "Person - TargetEvent",
)


def _lh_resolve_sheet_column(df: pd.DataFrame, *candidates: str) -> str | None:
    if df is None or df.empty or not candidates:
        return None
    lowered = {str(c).strip().casefold(): c for c in df.columns}
    for cand in candidates:
        cf = str(cand).strip().casefold()
        if cf in lowered:
            return lowered[cf]
    return None


_LH_GENERATION_SHEET_NAMES_CF: frozenset[str] = frozenset(
    {"ln_generation", "linkedin_generation"}
)


def _lh_resolve_join_column(df: pd.DataFrame, key_hint: str) -> str:
    hint = (key_hint or "").strip()
    if not hint:
        raise ValueError("Порожня назва колонки для з’єднання аркушів.")
    if hint in df.columns:
        return hint
    res = _lh_resolve_sheet_column(df, hint)
    if res:
        return res
    raise ValueError(f"Немає колонки «{hint}».")


def _lh_find_generation_sheet_via_xlsx(spreadsheet_url: str) -> tuple[str, pd.DataFrame]:
    """Аркуш ln_generation або linkedin_generation у книзі (потрібен XLSX-експорт)."""
    pairs = load_spreadsheet_all_tabs_via_xlsx_export(spreadsheet_url.strip())
    if not pairs:
        return "", pd.DataFrame()
    for name, df in pairs:
        if df is None or df.empty:
            continue
        if str(name).strip().casefold() in _LH_GENERATION_SHEET_NAMES_CF:
            return str(name), df.copy()
    return "", pd.DataFrame()


def _lh_strip_df_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """BOM і зайві пробіли в заголовках колонок (Google Sheets / експорт)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    out.columns = [re.sub(r"\s+", " ", str(c).replace("\ufeff", "").strip()) for c in out.columns]
    return out


def _lh_normalize_header_token(name: str) -> str:
    s = str(name).strip().replace("\ufeff", "").casefold()
    return re.sub(r"[\s\-_]+", "", s)


def _lh_find_person_id_column_on_master(df_master: pd.DataFrame) -> str | None:
    """
    Колонка **Person ID** на 1-му аркуші — **список id для CRM** (звірка з **pipedrive_contact_id** поділу),
    незалежно від того, чи залишилась окрема колонка Person ID у злитій таблиці.
    """
    if df_master is None or df_master.empty:
        return None
    for cand in (
        "Person ID",
        "Person - ID",
        "Person_ID",
        "person_id",
        "PersonID",
        "Pipedrive Person ID",
    ):
        r = _lh_resolve_sheet_column(df_master, cand)
        if r:
            return r
    best_score = (-1, -1)
    best_col: str | None = None
    for c in df_master.columns:
        tok = _lh_normalize_header_token(c)
        score = 0
        if tok == "personid":
            score = 100
        elif tok.endswith("personid") and "activity" not in tok and "target" not in tok and "subject" not in tok:
            score = 70
        if score <= 0:
            continue
        ser = df_master[c]
        try:
            n_non_empty = int(
                (
                    ser.notna()
                    & (ser.astype(str).str.strip() != "")
                    & (ser.astype(str).str.lower() != "nan")
                ).sum()
            )
        except Exception:
            n_non_empty = int(ser.notna().sum())
        key = (score, n_non_empty)
        if key > best_score:
            best_score = key
            best_col = str(c)
    return best_col


def _lh_person_id_lookup_key(val: Any) -> str:
    """Нормалізація id для однакового порівняння поділ ↔ метч (пробіли, коми, non‑breaking space)."""
    s = normalize_sheet_person_id(val)
    if not s:
        return ""
    return re.sub(r"[,\s\u00a0]", "", s)


def _lh_match_sheet_id_set(df_master: pd.DataFrame, k_m: str) -> frozenset[str]:
    """Нормалізовані id з колонки метчу — для CRM звірки з **pipedrive_contact_id** поділу."""
    if df_master is None or df_master.empty or not (k_m or "").strip():
        return frozenset()
    if k_m not in df_master.columns:
        return frozenset()
    out: set[str] = set()
    for val in df_master[k_m].tolist():
        k = _lh_person_id_lookup_key(val)
        if k:
            out.add(k)
    return frozenset(out)


def _lh_resolve_gen_master_join_keys(
    df_gen: pd.DataFrame,
    df_master: pd.DataFrame,
    join_key_gen: str,
) -> tuple[str, str]:
    """
    Злиття: **pipedrive_contact_id** із поділу (`join_key_gen`) **=** **Person ID** на 1-му аркуші, якщо така колонка є.
    Інакше — та сама назва колонки, що на поділі.
    """
    k_g = _lh_resolve_join_column(df_gen, join_key_gen)
    pid_m = _lh_find_person_id_column_on_master(df_master)
    if pid_m:
        return k_g, pid_m
    k_m = _lh_resolve_join_column(df_master, join_key_gen)
    return k_g, k_m


_LH_JOIN_NORM_COL = "__join_key_norm__"


def _lh_merge_gen_left_master(
    df_gen: pd.DataFrame,
    df_master: pd.DataFrame,
    join_key: str,
    *,
    prefer_master_column_hints: tuple[str, ...] = (),
) -> tuple[pd.DataFrame, str, str]:
    """
    Left join: рядки **поділу** зліва, **метч** справа.
    Join виконується по нормалізованому ключу (int-str, float→int, пробіли) — щоб збігались
    '359142', 359142, 359142.0 тощо. Тимчасова колонка __join_key_norm__ прибирається після merge.
    Повертає (merged, key_на_поділі, key_на_метчі).
    """
    if df_gen is None or df_gen.empty:
        raise ValueError("Аркуш поділу (ln_generation / linkedin_generation) порожній.")
    if df_master is None or df_master.empty:
        raise ValueError("Аркуш метчу (перше посилання) порожній.")
    k_g, k_m = _lh_resolve_gen_master_join_keys(df_gen, df_master, join_key)
    g = df_gen.copy()
    for hint in prefer_master_column_hints:
        h = (hint or "").strip()
        if not h:
            continue
        c_drop = _lh_resolve_sheet_column(g, h)
        if c_drop and c_drop != k_g:
            g = g.drop(columns=[c_drop], errors="ignore")
    m = df_master.copy()

    # Нормалізуємо ключі в тимчасову колонку для точного збігу незалежно від типу
    norm_col = _LH_JOIN_NORM_COL
    g[norm_col] = g[k_g].apply(_lh_person_id_lookup_key)
    m[norm_col] = m[k_m].apply(_lh_person_id_lookup_key)

    drop_cols = [c for c in m.columns if c not in (k_m, norm_col) and c in g.columns]
    m = m.drop(columns=drop_cols, errors="ignore")

    merged = g.merge(m, on=norm_col, how="left", suffixes=("", "_master_dup"))
    # Прибираємо тимчасову колонку та дублікати ключа з метчу
    merged = merged.drop(columns=[norm_col], errors="ignore")
    if k_m != k_g and k_m in merged.columns:
        merged = merged.drop(columns=[k_m], errors="ignore")
    dup_cols = [c for c in merged.columns if c.endswith("_master_dup")]
    if dup_cols:
        merged = merged.drop(columns=dup_cols, errors="ignore")

    return merged, k_g, k_m


def _lh_load_match_and_generation_merged(
    lines_block: str,
    join_key: str,
    *,
    prefer_master_column_hints: tuple[str, ...] = (),
) -> tuple[pd.DataFrame, str, frozenset[str], list[str]]:
    """
    Рядок 1 URL — **будь-яке посилання на книгу**; **метч** завжди з **першого аркуша** (gid у цьому URL не використовується).
    Поділ → рядок 2 або аркуш ln_generation / linkedin_generation у тій самій або іншій книзі (**pipedrive_contact_id**); тут **gid** у посиланні враховується.
    **Умова злиття:** **pipedrive_contact_id** (поділ) **=** **Person ID** (метч); якщо назви збігаються — одна колонка в обох.
    Повертає ще **набір id з метч-аркуша** (для CRM: лише рядки поділу з id з цього списку) та **список колонок поділу до merge** (для ZIP без зайвих полів).
    Поля CRM (`prefer_master_column_hints`) з аркуша **поділу** видаляються перед join, щоб не затирати дані **метчу** порожніми однойменними колонками.
    """
    seen: set[str] = set()
    lines: list[str] = []
    for ln in (lines_block or "").splitlines():
        s = ln.strip()
        if not s or s in seen:
            continue
        seen.add(s)
        lines.append(s)
    if not lines:
        raise ValueError("Додайте хоча б один рядок із посиланням.")

    url_master = lines[0]
    df_master0 = load_sheet_from_url_first_tab(url_master)
    if df_master0 is None or df_master0.empty:
        raise RuntimeError(
            "Перше посилання не дало даних (**перший аркуш** книги для метчу). Перевірте доступ до таблиці."
        )
    df_master0 = _lh_strip_df_column_names(df_master0)

    # Якщо є кілька посилань — завантажуємо перші аркуші всіх і об'єднуємо в один метч.
    master_frames: list[pd.DataFrame] = [df_master0]
    for extra_url in lines[1:]:
        if extract_gid_from_url(extra_url):
            try:
                df_ex = load_sheet_from_url_first_tab(extra_url)
                if df_ex is not None and not df_ex.empty:
                    master_frames.append(_lh_strip_df_column_names(df_ex))
            except Exception:
                pass
    if len(master_frames) > 1:
        df_master = pd.concat(master_frames, ignore_index=True)
    else:
        df_master = df_master0

    # Збираємо поділи з усіх посилань, де вказано gid.
    # Рядок 1: перший аркуш = метч; якщо в URL є gid — той аркуш також є поділом.
    # Рядки 2+: тільки gid-аркуш = поділ (перший аркуш ігнорується).
    gen_frames: list[pd.DataFrame] = []
    gen_descs: list[str] = []
    for gi, url_g in enumerate(lines, start=1):
        gid_i = extract_gid_from_url(url_g)
        if not gid_i:
            continue  # немає gid — тільки метч (рядок 1), або пропускаємо
        df_g = load_sheet_from_url(url_g)
        if df_g is None or df_g.empty:
            raise RuntimeError(
                f"Рядок {gi}: посилання (gid={gid_i}) не дало даних. "
                "Перевірте доступ і правильність gid."
            )
        df_g = _lh_strip_df_column_names(df_g)
        gen_frames.append(df_g)
        gen_descs.append(f"рядок {gi} (gid={gid_i}): **{len(df_g)}** ряд")

    gen_desc = ""
    if gen_frames:
        df_gen = pd.concat(gen_frames, ignore_index=True) if len(gen_frames) > 1 else gen_frames[0]
        gen_desc = "поділ: " + "; ".join(gen_descs) + f"; разом **{len(df_gen)}** ряд"
    else:
        # Жоден URL не містить gid — шукаємо ln_generation у книзі першого рядка
        gname, df_gen = _lh_find_generation_sheet_via_xlsx(url_master)
        if df_gen is None or df_gen.empty:
            raise RuntimeError(
                "У жодному посиланні не знайдено **gid=** (аркуш поділу). "
                "Вкажіть **gid** конкретного аркуша в посиланні (відкрийте потрібний аркуш у браузері — gid є в URL). "
                "Або переконайтеся, що в книзі є аркуш **ln_generation** / **linkedin_generation** "
                "з доступним **XLSX**-експортом."
            )
        df_gen = _lh_strip_df_column_names(df_gen)
        gen_desc = f"поділ: аркуш **«{gname}»** (у книзі 1-го рядка, XLSX), **{len(df_gen)}** ряд"

    # Зберігаємо колонки поділу ДО merge — щоб у ZIP лишати тільки їх
    gen_columns_orig: list[str] = list(df_gen.columns)

    merged, k_g, k_m = _lh_merge_gen_left_master(
        df_gen,
        df_master,
        join_key,
        prefer_master_column_hints=prefer_master_column_hints,
    )

    # Whitelist для CRM = Person ID з першого аркуша КОЖНОГО наданого посилання.
    # URL1 вже завантажено як df_master; для URL2+ завантажуємо перші аркуші окремо.
    all_match_ids: set[str] = set()
    def _collect_ids_from_master(df: pd.DataFrame) -> None:
        col = _lh_find_person_id_column_on_master(df)
        ids = _lh_match_sheet_id_set(df, col or k_m)
        all_match_ids.update(ids)

    _collect_ids_from_master(df_master)
    for extra_url in lines[1:]:
        if extract_gid_from_url(extra_url):
            try:
                df_extra = load_sheet_from_url_first_tab(extra_url)
                if df_extra is not None and not df_extra.empty:
                    df_extra = _lh_strip_df_column_names(df_extra)
                    _collect_ids_from_master(df_extra)
            except Exception:
                pass  # якщо не вдалось завантажити — не критично
    match_ids = frozenset(all_match_ids)

    # Скільки рядків реально зіматчилось (k_m не NaN після left join)
    if k_m in merged.columns:
        n_matched = int(merged[k_m].notna().sum())
    else:
        n_matched = 0

    # Зразки id з кожного боку для діагностики
    def _sample_ids(df: pd.DataFrame, col: str) -> str:
        if col not in df.columns:
            return "—"
        vals = df[col].dropna().astype(str).unique().tolist()
        sample = [v for v in vals[:5] if v.strip() and v.lower() != "nan"]
        return ", ".join(sample) or "—"

    gen_id_sample = _sample_ids(df_gen, k_g)
    master_id_sample = _sample_ids(df_master, k_m)

    sid0 = extract_spreadsheet_id(url_master)
    pref = sid0[:10] + "…" if len(sid0) > 10 else sid0
    master_src = (
        f"**перші аркуші {len(master_frames)} книг**, разом **{len(df_master)}** ряд"
        if len(master_frames) > 1
        else f"**перший аркуш** книги **{pref}**, **{len(df_master)}** ряд"
    )
    meta = (
        f"Метч: {master_src} "
        f"(колонки: {', '.join(str(c) for c in df_master.columns[:8])}…); "
        f"{gen_desc}; "
        f"**join:** **{k_g}** (поділ) = **{k_m}** (метч); "
        f"**зіматчено:** {n_matched} з {len(df_gen)} рядків. "
        f"Зразки: поділ [{gen_id_sample}] ↔ метч [{master_id_sample}]"
    )
    return merged, meta, match_ids, gen_columns_orig


def _lh_df_cell_str(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    if isinstance(val, bool):
        return ""
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        s = str(val).strip()
        return s if s and s.lower() != "nan" else ""
    if isinstance(val, int):
        return str(val)
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    return s


def _lh_compose_crm_target_event(te_from_sheet: str, activity_subject: str) -> str:
    """Person - TargetEvent у CRM CSV: поле з таблиці + через кому Activity - Subject."""
    te = (te_from_sheet or "").strip()
    act = (activity_subject or "").strip()
    if te and act:
        return f"{te}, {act}"
    return te or act


def _lh_resolve_crm_column_fuzzy(df: pd.DataFrame, *candidates: str) -> str | None:
    """Резолв колонки CRM: точні імена + токен заголовка (Google / дублікати назв)."""
    if df is None or df.empty:
        return None
    r = _lh_resolve_sheet_column(df, *candidates)
    if r:
        return r
    for cand in candidates:
        ct = _lh_normalize_header_token(cand)
        if not ct:
            continue
        for c in df.columns:
            if _lh_normalize_header_token(c) == ct:
                return str(c)
    return None


def _lh_build_crm_lh_export_df(
    work_by_reg: dict[str, pd.DataFrame],
    *,
    person_col_gen: str,
    crm_person_id_column: str,
    activity_col_name: str,
    target_col_name: str,
    match_sheet_ids: frozenset[str] | None,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Файл для CRM після гео та правил Pipedrive.
    У файл потрапляють рядки поділу, де **pipedrive_contact_id** **збігається** з id з колонки **Person ID** на **1-му посиланні** (список будується з сирого аркуша метчу, навіть якщо у злитті лишилась лише одна колонка id).
    Activity / TargetEvent можуть бути порожніми.
    У колонці **Person ID** у CSV — значення з окремої колонки Person ID у злитті, якщо вона є; інакше **pipedrive_contact_id**.
    """
    notes: list[str] = []
    frames: list[pd.DataFrame] = []
    for key in (LH_GEO_BUCKET_USA_CA, LH_GEO_BUCKET_EUROPE):
        d = work_by_reg.get(key)
        if d is not None and len(d):
            frames.append(d)
    if not frames:
        notes.append("CRM LinkedHelper: немає рядків після фільтра — файл не додається.")
        return pd.DataFrame(columns=list(_LH_CRM_EXPORT_COLUMNS)), notes

    combo = pd.concat(frames, ignore_index=True)
    pc_gen = (person_col_gen or "").strip()
    pc_gen_use = pc_gen if pc_gen in combo.columns else _lh_resolve_sheet_column(combo, pc_gen)
    if not pc_gen_use:
        notes.append(
            f"CRM LinkedHelper: немає колонки «{pc_gen}» (поділ) — файл не додається."
        )
        return pd.DataFrame(columns=list(_LH_CRM_EXPORT_COLUMNS)), notes

    crm_id_hint = (crm_person_id_column or "").strip()
    crm_id_use = _lh_resolve_crm_column_fuzzy(
        combo,
        crm_id_hint,
        "Person ID",
        "Person - ID",
        "Person_ID",
        "PersonID",
    )
    if not crm_id_use:
        for c in combo.columns:
            t = _lh_normalize_header_token(str(c))
            if t == "personid" or (t.endswith("personid") and "activity" not in t and "target" not in t):
                crm_id_use = str(c)
                break
    if not crm_id_use:
        notes.append(
            "CRM: у злитті немає окремої колонки **Person ID** (зазвичай лише **pipedrive_contact_id**) — це очікувано: "
            "звуження йде за списком **Person ID** з **1-го посилання**; у CSV підставляється **pipedrive_contact_id**."
        )

    act_c = _lh_resolve_crm_column_fuzzy(
        combo,
        activity_col_name,
        "Activity - Subject",
        "Activity – Subject",
    )
    if act_c is None:
        for c in combo.columns:
            t = _lh_normalize_header_token(str(c))
            if "activity" in t and "subject" in t:
                act_c = str(c)
                break
    te_c = _lh_resolve_crm_column_fuzzy(
        combo,
        target_col_name,
        "Person - TargetEvent",
        "Person – TargetEvent",
    )
    if te_c is None:
        for c in combo.columns:
            t = _lh_normalize_header_token(str(c))
            if "target" in t and "event" in t:
                te_c = str(c)
                break
    if act_c is None:
        notes.append(
            f"CRM LinkedHelper: колонку **{activity_col_name}** не знайдено — «Activity - Subject» у файлі буде порожнім."
        )
    if te_c is None:
        notes.append(
            f"CRM LinkedHelper: колонку **{target_col_name}** не знайдено — «Person - TargetEvent» = лише **Activity - Subject** (якщо він є)."
        )

    mis = match_sheet_ids
    if mis:
        n0 = len(combo)

        def _pid_in_match(v: Any) -> bool:
            return _lh_person_id_lookup_key(v) in mis

        filtered = combo[combo[pc_gen_use].map(_pid_in_match)].copy()
        if filtered.empty:
            notes.append(
                f"CRM LinkedHelper: жоден **pipedrive_contact_id** (поділ) не знайдено серед **Person ID** 1-го аркуша "
                f"(можливо, join не дав збігів або id у різних форматах). "
                f"Взято **всі {n0}** рядки без фільтра — перевірте, чи правильні аркуші вказано."
            )
        else:
            combo = filtered
            if len(combo) < n0:
                notes.append(
                    f"CRM LinkedHelper: за списком **Person ID** з 1-го аркуша залишено **{len(combo)}** з **{n0}** рядків."
                )

    rows_out: list[dict[str, str]] = []
    for _, row in combo.iterrows():
        pid = ""
        if crm_id_use:
            pid = normalize_sheet_person_id(row.get(crm_id_use))
        if not pid:
            pid = normalize_sheet_person_id(row.get(pc_gen_use))
        if not pid:
            continue
        act = _lh_df_cell_str(row.get(act_c)) if act_c else ""
        te_raw = _lh_df_cell_str(row.get(te_c)) if te_c else ""
        rows_out.append(
            {
                "Person ID": pid,
                "Activity - Subject": act,
                "Person - TargetEvent": _lh_compose_crm_target_event(te_raw, act),
            }
        )

    if not rows_out:
        notes.append("CRM LinkedHelper: усі рядки без Person ID у колонці **поділу** / злиття — файл не додається.")
        return pd.DataFrame(columns=list(_LH_CRM_EXPORT_COLUMNS)), notes

    out = pd.DataFrame(rows_out)
    for c in _LH_CRM_EXPORT_COLUMNS:
        if c not in out.columns:
            out[c] = ""
    out = out[list(_LH_CRM_EXPORT_COLUMNS)]
    notes.append(f"CRM LinkedHelper: **{len(out)}** рядків → **`{_LH_CRM_ZIP_FILENAME}`** (окреме завантаження).")
    return out, notes


def _normalize_id_for_join(val: Any) -> str:
    """Ключ зіставлення CRM pipedrive_contact_id ↔ Person - ID у таблиці (без урахування регістру пробілів, float→int)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    if isinstance(val, bool):
        return ""
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        s = str(val).strip()
        return s if s and s.lower() != "nan" else ""
    if isinstance(val, int):
        return str(val)
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    if re.fullmatch(r"-?\d+\.0+", s):
        try:
            return str(int(float(s)))
        except ValueError:
            pass
    return s


def _flatten_saved_crm_contact_rows(entries: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        eid = str(e.get("id") or "")
        prefix = str(e.get("campaign_name_prefix") or "")
        snap = e.get("contacts_snapshot")
        if not isinstance(snap, dict):
            continue
        cb = snap.get("contacts_by_bucket")
        if isinstance(cb, dict):
            for seg, lst in cb.items():
                if not isinstance(lst, list):
                    continue
                for rec in lst:
                    if isinstance(rec, dict):
                        r = dict(rec)
                        r["_crm_saved_entry_id"] = eid
                        r["_crm_record_title"] = prefix
                        r["_crm_segment"] = seg
                        rows.append(r)
        um = snap.get("unmatched_rows")
        if isinstance(um, list):
            for rec in um:
                if isinstance(rec, dict):
                    r = dict(rec)
                    r["_crm_saved_entry_id"] = eid
                    r["_crm_record_title"] = prefix
                    r["_crm_segment"] = "_Unmatched_split"
                    rows.append(r)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _merged_cell_str(rd: dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k not in rd:
            continue
        v = rd[k]
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except TypeError:
            pass
        s = str(v).strip()
        if s:
            return s
    return ""


def _truthy_crm_template_flag(val: str | None) -> bool:
    return str(val or "").strip().lower() in ("1", "true", "yes", "on")


def _compose_activity_export_person_target_event(
    rd: dict[str, Any],
    template_vals: dict[str, str],
    *,
    activity_subject_export: str,
) -> str:
    """
    Колонка експорту Person - TargetEvent: значення з таблиці + через кому **повний**
    текст колонки експорту «Activity - Subject» для цього ж рядка (за прапором шаблону)
    + необовʼязковий суфікс з шаблону CRM.

    Якщо поле з таблиці порожнє, а допис увімкнено — результат починається з «, …»
    (кома й пробіл перед текстом Activity - Subject).
    """
    base = _merged_cell_str(rd, "Person - TargetEvent_sheet", "Person - TargetEvent").strip()
    parts: list[str] = []
    if base:
        parts.append(base)
    if _truthy_crm_template_flag(
        template_vals.get("person_target_event_append_sheet_activity_subject")
    ):
        subj = (activity_subject_export or "").strip()
        if subj:
            if not parts:
                parts.append("")
            parts.append(subj)
    extra = str(template_vals.get("person_target_event_extra_suffix") or "").strip()
    if extra:
        parts.append(extra)
    return ", ".join(parts)


def _crm_activity_export_subject(rd: dict[str, Any], record_title: str) -> str:
    rt = record_title.strip()
    if not rt:
        return ""
    if not _CRM_EVENT_TITLE_SUBJECT_RE.search(rt):
        return rt
    sheet_sub = _merged_cell_str(
        rd, "Activity - Subject_sheet", "Activity - Subject"
    ).strip()
    if not sheet_sub:
        return rt
    return f"{rt}-{sheet_sub}"


def build_crm_activity_export_df(
    sheet_df: pd.DataFrame,
    crm_entries: list[dict[str, Any]],
    *,
    sheet_id_col: str,
    crm_id_col: str,
    template_vals: dict[str, str],
) -> tuple[pd.DataFrame, str | None, dict[str, int] | None, pd.DataFrame | None]:
    """
    Зіставлення CRM ↔ таблиця за ID (CRM: pipedrive_contact_id, таблиця: Person - ID за замовчуванням).
    Постійні поля — з шаблону CRM; Activity - Subject — зазвичай назва запису CRM;
    якщо в назві є Event/Events — «назва» + «-» + колонка Activity - Subject з таблиці (за наявності).
    Колонка **Person - TargetEvent** у експорті: значення з таблиці + через кому **той самий текст**,
    що колонка **Activity - Subject** у цьому експорті (за прапором шаблону)
    + необовʼязковий текст із шаблону CRM.

    У результат потрапляють лише рядки **inner join** за нормалізованим ID. Словник статистики (третій елемент)
    пояснює розбіжність кількостей CRM / таблиця / експорт. Четвертий елемент — рядки CRM з придатним ID,
    для яких **немає** відповідного Person - ID у таблиці (для окремого CSV); при помилці валідації — None.
    """
    crm_flat = _flatten_saved_crm_contact_rows(crm_entries)
    if crm_flat.empty:
        return pd.DataFrame(), "У CRM немає збережених рядків контактів (contacts_snapshot).", None, None
    sc = sheet_id_col.strip()
    cc = crm_id_col.strip()
    if not sc or not cc:
        return pd.DataFrame(), "Вкажіть назви колонок для зіставлення.", None, None
    if sc not in sheet_df.columns:
        return pd.DataFrame(), f"У таблиці з посилання немає колонки «{sc}».", None, None
    if cc not in crm_flat.columns:
        return pd.DataFrame(), f"У CRM немає колонки «{cc}». Перевірте CSV контактів.", None, None

    n_sheet_total = len(sheet_df)
    n_crm_total = len(crm_flat)

    L = sheet_df.copy()
    R = crm_flat.copy()
    L["_join_id"] = L[sc].map(_normalize_id_for_join)
    R["_join_id"] = R[cc].map(_normalize_id_for_join)

    sheet_missing_join = int((L["_join_id"].str.len() == 0).sum())
    crm_missing_join = int((R["_join_id"].str.len() == 0).sum())

    L = L[L["_join_id"].str.len() > 0]
    R = R[R["_join_id"].str.len() > 0]

    sheet_keys = set(L["_join_id"].unique())
    crm_keys = set(R["_join_id"].unique())

    crm_without_sheet = int((~R["_join_id"].isin(sheet_keys)).sum())
    sheet_without_crm = int((~L["_join_id"].isin(crm_keys)).sum())

    crm_only_no_sheet = R[~R["_join_id"].isin(sheet_keys)].copy()
    crm_only_no_sheet.drop(columns=["_join_id"], inplace=True, errors="ignore")
    audit_front = [
        c for c in ("_crm_record_title", "_crm_segment", "_crm_saved_entry_id", cc) if c in crm_only_no_sheet.columns
    ]
    audit_rest = [c for c in crm_only_no_sheet.columns if c not in audit_front]
    if audit_front:
        crm_only_no_sheet = crm_only_no_sheet[audit_front + audit_rest]

    merged = L.merge(R, on="_join_id", how="inner", suffixes=("_sheet", "_crm"))
    merged = merged.drop(columns=["_join_id"], errors="ignore")

    stats: dict[str, int] = {
        "crm_rows_flat_total": n_crm_total,
        "crm_rows_missing_join_id": crm_missing_join,
        "crm_rows_usable_join_id": len(R),
        "sheet_rows_flat_total": n_sheet_total,
        "sheet_rows_missing_join_id": sheet_missing_join,
        "sheet_rows_usable_join_id": len(L),
        "crm_rows_without_sheet_match": crm_without_sheet,
        "sheet_rows_without_crm_match": sheet_without_crm,
        "merged_output_rows": len(merged),
    }

    if merged.empty:
        return (
            pd.DataFrame(),
            "Немає збігів за ID (перевірте pipedrive_contact_id та Person - ID).",
            stats,
            crm_only_no_sheet,
        )

    out_rows: list[dict[str, str]] = []
    for _, row in merged.iterrows():
        rd = row.to_dict()
        record_title = _merged_cell_str(
            rd, "_crm_record_title_crm", "_crm_record_title_sheet", "_crm_record_title"
        )
        subject = _crm_activity_export_subject(rd, record_title)
        out_rows.append(
            {
                "Person - SDR Responsible": template_vals.get("person_sdr_responsible", ""),
                "Activity - Subject": subject,
                "Activity - Due date": template_vals.get("activity_due_date", ""),
                "Activity - ID": _merged_cell_str(
                    rd, "Activity - ID_sheet", "Activity - ID"
                ),
                "Person - ID": _merged_cell_str(rd, "Person - ID_sheet", "Person - ID"),
                "Activity - Done": template_vals.get("activity_done", ""),
                "Activity type": template_vals.get("activity_type", ""),
                "Activity assigned to user": template_vals.get(
                    "activity_assigned_to_user", ""
                ),
                "Person - TargetEvent": _compose_activity_export_person_target_event(
                    rd,
                    template_vals,
                    activity_subject_export=subject,
                ),
            }
        )

    out = pd.DataFrame(out_rows)
    for c in _CRM_ACTIVITY_EXPORT_COLUMNS:
        if c not in out.columns:
            out[c] = ""
    out = out[list(_CRM_ACTIVITY_EXPORT_COLUMNS)]
    return out, None, stats, crm_only_no_sheet


def _crm_activity_export_stats_caption(st: dict[str, int]) -> str:
    """Людський опис розбіжностей між CRM, таблицею та inner join."""
    merged = int(st.get("merged_output_rows", 0))
    crm_tot = int(st.get("crm_rows_flat_total", 0))
    cr_no_id = int(st.get("crm_rows_missing_join_id", 0))
    cr_ok = int(st.get("crm_rows_usable_join_id", 0))
    cr_no_sheet = int(st.get("crm_rows_without_sheet_match", 0))
    sh_tot = int(st.get("sheet_rows_flat_total", 0))
    sh_no_id = int(st.get("sheet_rows_missing_join_id", 0))
    sh_ok = int(st.get("sheet_rows_usable_join_id", 0))
    sh_no_crm = int(st.get("sheet_rows_without_crm_match", 0))
    cr_matched_rows = cr_ok - cr_no_sheet
    dup_note = ""
    if merged > cr_matched_rows:
        extra = merged - cr_matched_rows
        dup_note = (
            f" Рядків експорту **більше**, ніж контактів CRM із збігом (**+{extra}**): "
            "зазвичай у таблиці кілька рядків на один і той самий Person - ID "
            "або в CRM кілька рядків з однаковим ID (усі зливаються з кожним відповідником у таблиці)."
        )
    return (
        f"**Чому кількості відрізняються:** експорт — це **лише перетин** CRM і таблиці за ID (inner join). "
        f"**CRM:** усього **{crm_tot}** рядків контактів; без придатного ID для злиття — **{cr_no_id}**; "
        f"з ID — **{cr_ok}**, з них **немає відповідного рядка в таблиці** — **{cr_no_sheet}**. "
        f"**Таблиця:** усього **{sh_tot}** рядків; без придатного Person - ID — **{sh_no_id}**; "
        f"з ID — **{sh_ok}**, з них **немає в CRM** — **{sh_no_crm}**. "
        f"**У файлі експорту:** **{merged}** рядків.{dup_note} "
        "**Рядки CRM без рядка в таблиці** можна викачати окремим CSV (кнопка під блоком, якщо такі є)."
    )


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def _safe_filename(title: str) -> str:
    t = "".join(c if c not in ':\\/?*[]' else "-" for c in title)
    return (t[:100] or "export").strip()


def _excel_sheet_name(title: str) -> str:
    """Excel: макс. 31 символ; заборонені : \\ / ? * [ ]."""
    t = "".join(c if c not in r':\\/?*[]' else "-" for c in (title or ""))
    t = (t[:31] or "Sheet").strip()
    return t


def build_split_export_zip(
    source_df: pd.DataFrame,
    buckets: dict[str, pd.DataFrame],
    unmatched: pd.DataFrame | None,
) -> bytes:
    """Один ZIP: головний список + CSV по кожному сегменту (+ unmatched за потреби)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("source.csv", df_to_csv_bytes(source_df))
        for name, bdf in buckets.items():
            zf.writestr(_safe_filename(name) + ".csv", df_to_csv_bytes(bdf))
        if unmatched is not None and len(unmatched) > 0:
            zf.writestr("_Unmatched_split.csv", df_to_csv_bytes(unmatched))
    return buf.getvalue()


def build_split_export_xlsx(
    source_df: pd.DataFrame,
    buckets: dict[str, pd.DataFrame],
    unmatched: pd.DataFrame | None,
) -> bytes:
    """Один .xlsx: вкладка source + по вкладці на сегмент (+ unmatched за потреби)."""
    try:
        import openpyxl  # noqa: F401
    except ImportError as e:
        raise RuntimeError("Встановіть openpyxl: py -m pip install openpyxl") from e
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        source_df.to_excel(writer, sheet_name="source", index=False)
        used_names: set[str] = {"source"}
        for i, (name, bdf) in enumerate(buckets.items()):
            sn = _excel_sheet_name(name)
            if sn in used_names:
                sn = _excel_sheet_name(f"{i}_{name}")
            used_names.add(sn)
            bdf.to_excel(writer, sheet_name=sn, index=False)
        if unmatched is not None and len(unmatched) > 0:
            um = "_Unmatched_split"
            if um in used_names:
                um = "Unmatched"
            unmatched.to_excel(writer, sheet_name=um[:31], index=False)
    buf.seek(0)
    return buf.getvalue()


_BROWSER_SS_INSTANTLY_KEY = "instantly_sdr_api_key_v1"
_BROWSER_SS_PIPEDRIVE_KEY = "instantly_sdr_pipedrive_token_v1"


_CLEAR_INSTANTLY_API_KEY_FLAG = "_request_clear_instantly_api_key"
_USER_CLEARED_INSTANTLY_KEY = "_instantly_api_key_cleared_by_user"
_CLEAR_PIPEDRIVE_API_KEY_FLAG = "_request_clear_pipedrive_api_key"
_USER_CLEARED_PIPEDRIVE_KEY = "_pipedrive_api_key_cleared_by_user"
_PENDING_CLEAR_SENDER_POOLS_SIDEBAR = "_pending_clear_sender_pools_sidebar"


def _ensure_instantly_api_key_widget_state() -> None:
    if "instantly_api_key_input" not in st.session_state:
        st.session_state.instantly_api_key_input = ""


def _disk_instantly_api_key_bootstrap() -> str:
    k = (os.environ.get("INSTANTLY_API_KEY") or "").strip()
    if k:
        return k
    try:
        if "INSTANTLY_API_KEY" in st.secrets:
            return str(st.secrets["INSTANTLY_API_KEY"]).strip()
    except FileNotFoundError:
        pass
    return ""


def _hydrate_instantly_api_key_session_storage() -> None:
    """Відновити ключ із sessionStorage вкладки (переживає F5, поки вкладка відкрита)."""
    if str(st.session_state.get("instantly_api_key_input") or "").strip():
        return
    try:
        from streamlit_js_eval import streamlit_js_eval
    except ImportError:
        return
    raw = streamlit_js_eval(
        js_expressions=f"sessionStorage.getItem('{_BROWSER_SS_INSTANTLY_KEY}')",
        key="instantly_key_ss_read",
    )
    if raw and isinstance(raw, str) and raw.strip():
        st.session_state.instantly_api_key_input = raw.strip()
        st.session_state.pop(_USER_CLEARED_INSTANTLY_KEY, None)


def _persist_instantly_api_key_session_storage(value: str) -> None:
    try:
        from streamlit_js_eval import streamlit_js_eval
    except ImportError:
        return
    v = str(value or "").strip()
    if not v:
        streamlit_js_eval(
            js_expressions=f"sessionStorage.removeItem('{_BROWSER_SS_INSTANTLY_KEY}')",
            key="instantly_key_ss_remove",
        )
        return
    payload = json.dumps(v)
    streamlit_js_eval(
        js_expressions=f"sessionStorage.setItem('{_BROWSER_SS_INSTANTLY_KEY}', {payload})",
        key="instantly_key_ss_write",
    )


def _on_instantly_api_key_commit() -> None:
    st.session_state.pop(_USER_CLEARED_INSTANTLY_KEY, None)
    _persist_instantly_api_key_session_storage(
        str(st.session_state.get("instantly_api_key_input") or "")
    )


def get_instantly_api_key() -> str | None:
    """Пріоритет: поле в UI → змінна оточення → secrets.toml (деплой / локально)."""
    try:
        ui = str(st.session_state.get("instantly_api_key_input") or "").strip()
        if ui:
            return ui
    except Exception:
        pass
    k = (os.environ.get("INSTANTLY_API_KEY") or "").strip()
    if k:
        return k
    try:
        if "INSTANTLY_API_KEY" in st.secrets:
            return str(st.secrets["INSTANTLY_API_KEY"]).strip() or None
    except FileNotFoundError:
        pass
    return None


def _toml_escape_value(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def persist_instantly_key_to_secrets_toml(api_key: str) -> Path:
    """Додає або оновлює INSTANTLY_API_KEY у .streamlit/secrets.toml поруч із app.py."""
    root = Path(__file__).resolve().parent
    d = root / ".streamlit"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "secrets.toml"
    prefix_re = re.compile(r"^\s*INSTANTLY_API_KEY\s*=", re.IGNORECASE)
    line = f'INSTANTLY_API_KEY = "{_toml_escape_value(api_key)}"'
    if p.exists():
        text = p.read_text(encoding="utf-8")
        lines = text.splitlines()
        out: list[str] = []
        found = False
        for ln in lines:
            if prefix_re.match(ln):
                if not found:
                    out.append(line)
                    found = True
            else:
                out.append(ln)
        if not found:
            if out and out[-1].strip():
                out.append("")
            out.append(line)
        p.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")
    else:
        p.write_text(line + "\n", encoding="utf-8")
    return p


def persist_pipedrive_key_to_secrets_toml(api_token: str) -> Path:
    """Додає або оновлює PIPEDRIVE_API_TOKEN у .streamlit/secrets.toml поруч із app.py."""
    root = Path(__file__).resolve().parent
    d = root / ".streamlit"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "secrets.toml"
    prefix_re = re.compile(r"^\s*PIPEDRIVE_API_TOKEN\s*=", re.IGNORECASE)
    line = f'PIPEDRIVE_API_TOKEN = "{_toml_escape_value(api_token)}"'
    if p.exists():
        text = p.read_text(encoding="utf-8")
        lines = text.splitlines()
        out: list[str] = []
        found = False
        for ln in lines:
            if prefix_re.match(ln):
                if not found:
                    out.append(line)
                    found = True
            else:
                out.append(ln)
        if not found:
            if out and out[-1].strip():
                out.append("")
            out.append(line)
        p.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")
    else:
        p.write_text(line + "\n", encoding="utf-8")
    return p


def _ensure_pipedrive_api_key_widget_state() -> None:
    if "lh_pipedrive_token_input" in st.session_state and "pipedrive_api_token_input" not in st.session_state:
        st.session_state.pipedrive_api_token_input = str(
            st.session_state.pop("lh_pipedrive_token_input") or ""
        )
    if "pipedrive_api_token_input" not in st.session_state:
        st.session_state.pipedrive_api_token_input = ""


def _disk_pipedrive_api_key_bootstrap() -> str:
    for env_k in ("PIPEDRIVE_API_TOKEN", "PIPEDRIVE_API_KEY"):
        v = (os.environ.get(env_k) or "").strip()
        if v:
            return v
    try:
        if "PIPEDRIVE_API_TOKEN" in st.secrets:
            return str(st.secrets["PIPEDRIVE_API_TOKEN"]).strip()
    except FileNotFoundError:
        pass
    return ""


def _hydrate_pipedrive_api_key_session_storage() -> None:
    if str(st.session_state.get("pipedrive_api_token_input") or "").strip():
        return
    try:
        from streamlit_js_eval import streamlit_js_eval
    except ImportError:
        return
    raw = streamlit_js_eval(
        js_expressions=f"sessionStorage.getItem('{_BROWSER_SS_PIPEDRIVE_KEY}')",
        key="pipedrive_key_ss_read",
    )
    if raw and isinstance(raw, str) and raw.strip():
        st.session_state.pipedrive_api_token_input = raw.strip()
        st.session_state.pop(_USER_CLEARED_PIPEDRIVE_KEY, None)


def _persist_pipedrive_api_key_session_storage(value: str) -> None:
    try:
        from streamlit_js_eval import streamlit_js_eval
    except ImportError:
        return
    v = str(value or "").strip()
    if not v:
        streamlit_js_eval(
            js_expressions=f"sessionStorage.removeItem('{_BROWSER_SS_PIPEDRIVE_KEY}')",
            key="pipedrive_key_ss_remove",
        )
        return
    payload = json.dumps(v)
    streamlit_js_eval(
        js_expressions=f"sessionStorage.setItem('{_BROWSER_SS_PIPEDRIVE_KEY}', {payload})",
        key="pipedrive_key_ss_write",
    )


def _on_pipedrive_api_key_commit() -> None:
    st.session_state.pop(_USER_CLEARED_PIPEDRIVE_KEY, None)
    _persist_pipedrive_api_key_session_storage(
        str(st.session_state.get("pipedrive_api_token_input") or "")
    )


def get_pipedrive_api_key() -> str | None:
    """Пріоритет: поле в UI (Linked Helper) → змінна оточення → secrets.toml."""
    try:
        ui = str(st.session_state.get("pipedrive_api_token_input") or "").strip()
        if ui:
            return ui
    except Exception:
        pass
    for env_k in ("PIPEDRIVE_API_TOKEN", "PIPEDRIVE_API_KEY"):
        v = (os.environ.get(env_k) or "").strip()
        if v:
            return v
    try:
        if "PIPEDRIVE_API_TOKEN" in st.secrets:
            return str(st.secrets["PIPEDRIVE_API_TOKEN"]).strip() or None
    except FileNotFoundError:
        pass
    return None


def _parse_iso_date_yyyy_mm_dd(s: str) -> date | None:
    raw = (s or "").strip()[:10]
    if len(raw) < 10 or raw[4] != "-" or raw[7] != "-":
        return None
    try:
        y, mo, d = (int(raw[0:4]), int(raw[5:7]), int(raw[8:10]))
        return date(y, mo, d)
    except ValueError:
        return None


def _coerce_session_value_to_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return _parse_iso_date_yyyy_mm_dd(str(val))


def _load_crm_template_vars_merged() -> dict[str, str]:
    """Значення з диска поверх константних дефолтів."""
    base = dict(_CRM_TEMPLATE_VARS_DEFAULTS)
    if not _CRM_TEMPLATE_VARS_PATH.is_file():
        return base
    try:
        raw = json.loads(_CRM_TEMPLATE_VARS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return base
    if not isinstance(raw, dict):
        return base
    for k in _CRM_TEMPLATE_VARS_DEFAULTS:
        if k not in raw:
            continue
        v = raw[k]
        if v is None:
            continue
        base[k] = str(v).strip() if isinstance(v, str) else str(v)
    return base


def _save_crm_template_vars_disk(vals: dict[str, str]) -> tuple[bool, str | None]:
    try:
        to_store = {k: str(vals.get(k, "") or "") for k in _CRM_TEMPLATE_VARS_DEFAULTS}
        _CRM_TEMPLATE_VARS_PATH.write_text(
            json.dumps(to_store, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return True, None
    except OSError as ex:
        return False, str(ex)


def _persist_crm_template_vars_callback() -> None:
    """Зберігає на диск: якщо поле порожнє — лишаємо попереднє значення з файлу для цього ключа."""
    merged = _load_crm_template_vars_merged()
    vals: dict[str, str] = {}
    for k in _CRM_TEMPLATE_VARS_DEFAULTS:
        if k == "activity_due_date":
            picked = _coerce_session_value_to_date(st.session_state.get(f"crm_tpl_{k}"))
            vals[k] = (
                picked.strftime("%Y-%m-%d")
                if picked
                else date.today().strftime("%Y-%m-%d")
            )
        elif k in _CRM_TEMPLATE_BOOL_KEYS:
            vals[k] = "1" if bool(st.session_state.get(f"crm_tpl_{k}")) else ""
        else:
            typed = str(st.session_state.get(f"crm_tpl_{k}", "") or "").strip()
            vals[k] = typed if typed else merged[k]
    ok, err = _save_crm_template_vars_disk(vals)
    if not ok:
        st.session_state["_crm_tpl_save_err"] = err


def _crm_template_effective_values() -> dict[str, str]:
    """Фактичне значення кожного поля: введене в інпуті або шаблон із файлу (як у placeholder)."""
    merged = _load_crm_template_vars_merged()
    out: dict[str, str] = {}
    for k in _CRM_TEMPLATE_VARS_DEFAULTS:
        if k == "activity_due_date":
            picked = _coerce_session_value_to_date(st.session_state.get(f"crm_tpl_{k}"))
            out[k] = (
                picked.strftime("%Y-%m-%d")
                if picked
                else date.today().strftime("%Y-%m-%d")
            )
        elif k in _CRM_TEMPLATE_BOOL_KEYS:
            sk = f"crm_tpl_{k}"
            out[k] = (
                ("1" if st.session_state.get(sk) else "")
                if sk in st.session_state
                else merged[k]
            )
        else:
            typed = str(st.session_state.get(f"crm_tpl_{k}", "") or "").strip()
            out[k] = typed if typed else merged[k]
    return out


def _migrate_crm_tpl_widgets_to_placeholders() -> None:
    """Один раз за сесію: прибрати старі заповнені значення, щоб працювали placeholders."""
    if st.session_state.get(_CRM_TPL_PLACEHOLDER_MIGRATION):
        return
    for k in _CRM_TEMPLATE_VARS_DEFAULTS:
        st.session_state.pop(f"crm_tpl_{k}", None)
    st.session_state[_CRM_TPL_PLACEHOLDER_MIGRATION] = True


def _render_crm_template_variables_row() -> None:
    """Два горизонтальні рядки: підписи + TargetEvent; унизу — усі поля введення в одному рядку."""
    _migrate_crm_tpl_widgets_to_placeholders()
    merged = _load_crm_template_vars_merged()
    st.markdown(
        """
<style>
/*
 * Рядок полів шаблону CRM: другий горизонтальний блок містить date_input у 2-й колонці —
 * за цим знаходимо лише рядок інпутів і вирівнюємо шрифт / placeholder з іншими полями.
 */
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(2) div[data-testid="stDateInput"]
) div[data-testid="stTextInput"] input,
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(2) div[data-testid="stDateInput"]
) div[data-testid="stDateInput"] input {
    font-size: 1rem !important;
    line-height: 1.5 !important;
    min-height: 2.375rem !important;
}
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(2) div[data-testid="stDateInput"]
) div[data-testid="stTextInput"] input::placeholder {
    font-size: 1rem !important;
    font-weight: 400 !important;
}
/*
 * Рядок підписів CRM: вирівнюємо всі колонки по нижньому краю рядка — текст 1–5 і чекбокс + підпис у 6-й на одній лінії.
 */
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(5) div[data-testid="stMarkdown"]
):has(
    div[data-testid="column"]:nth-child(6) input[type="checkbox"]
):not(:has(div[data-testid="stDateInput"])) {
    align-items: flex-end !important;
}
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(5) div[data-testid="stMarkdown"]
):has(
    div[data-testid="column"]:nth-child(6) input[type="checkbox"]
):not(:has(div[data-testid="stDateInput"]))
div[data-testid="column"]:nth-child(-n+5) div[data-testid="stMarkdown"] p {
    margin: 0 0 0.35rem 0 !important;
    padding: 0 !important;
    font-size: 1rem !important;
    font-weight: 400 !important;
    line-height: 1.5 !important;
}
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(5) div[data-testid="stMarkdown"]
):has(
    div[data-testid="column"]:nth-child(6) input[type="checkbox"]
):not(:has(div[data-testid="stDateInput"]))
div[data-testid="column"]:nth-child(6) [data-testid="stCheckbox"] {
    align-items: center !important;
}
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(5) div[data-testid="stMarkdown"]
):has(
    div[data-testid="column"]:nth-child(6) input[type="checkbox"]
):not(:has(div[data-testid="stDateInput"]))
div[data-testid="column"]:nth-child(6) [data-testid="stCheckbox"] > div {
    align-items: center !important;
}
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(5) div[data-testid="stMarkdown"]
):has(
    div[data-testid="column"]:nth-child(6) input[type="checkbox"]
):not(:has(div[data-testid="stDateInput"]))
div[data-testid="column"]:nth-child(6) label[data-testid="stWidgetLabel"] {
    padding-left: 0.35rem !important;
}
div[data-testid="stHorizontalBlock"]:has(
    div[data-testid="column"]:nth-child(5) div[data-testid="stMarkdown"]
):has(
    div[data-testid="column"]:nth-child(6) input[type="checkbox"]
):not(:has(div[data-testid="stDateInput"]))
div[data-testid="column"]:nth-child(6) div[data-testid="element-container"] {
    margin-bottom: 0.35rem !important;
}
</style>
""",
        unsafe_allow_html=True,
    )
    st.caption(
        f"Шаблон CRM: у текстових полях **сірий текст** — підказка за замовчуванням (`{_CRM_TEMPLATE_VARS_PATH.name}`). "
        "**Activity - Due date** — календар по кліку (можна швидко перейти до поточної дати). Після зміни будь-якого поля шаблон зберігається на диск. "
        "Два рядки: зверху підписи та **Add to Person - TargetEvent**, знизу — усі поля введення в одному рядку (суфікс TargetEvent праворуч)."
    )
    chk_default = _truthy_crm_template_flag(
        merged.get("person_target_event_append_sheet_activity_subject")
    )
    suf_disk = (merged.get("person_target_event_extra_suffix") or "").strip()
    suf_placeholder = suf_disk if suf_disk else "Необовʼязково — текст через кому в кінці TargetEvent"

    col_weights = [1.02, 1.02, 1.02, 1.02, 1.02, 1.88]

    def _crm_tpl_label_markdown(label: str) -> None:
        # Стилі задає блок <style> у цьому ж компоненті — узгоджено з колонкою чекбокса TargetEvent.
        st.markdown(f"<p>{html.escape(label)}</p>", unsafe_allow_html=True)

    row_labels = st.columns(col_weights, gap="small")
    for i, (_field_key, label) in enumerate(_CRM_TEMPLATE_VAR_FIELDS):
        with row_labels[i]:
            _crm_tpl_label_markdown(label)
    _target_event_chk_label = "Add to Person - TargetEvent"
    with row_labels[5]:
        st.checkbox(
            _target_event_chk_label,
            value=chk_default,
            key="crm_tpl_person_target_event_append_sheet_activity_subject",
            help=(
                "Якщо увімкнено: до значення колонки **Person - TargetEvent** з таблиці додається через кому "
                "**повний** текст колонки **Activity - Subject** у згенерованому експорті (той самий рядок). "
                "Якщо з таблиці TargetEvent порожньо — рядок починається з «, …». Приклад: "
                "«EMTE45, LNTE36, 2026-04-09 Softdev … [GENERAL]»."
            ),
            on_change=_persist_crm_template_vars_callback,
        )

    row_inputs = st.columns(col_weights, gap="small")
    for i, (field_key, label) in enumerate(_CRM_TEMPLATE_VAR_FIELDS):
        with row_inputs[i]:
            if field_key == "activity_due_date":
                st.date_input(
                    label,
                    value=date.today(),
                    format="YYYY-MM-DD",
                    key=f"crm_tpl_{field_key}",
                    label_visibility="collapsed",
                    help="У полі та в файлі дата у форматі рррр-мм-дд (наприклад 2026-04-29).",
                    on_change=_persist_crm_template_vars_callback,
                )
            else:
                st.text_input(
                    label,
                    key=f"crm_tpl_{field_key}",
                    label_visibility="collapsed",
                    placeholder=merged[field_key],
                    on_change=_persist_crm_template_vars_callback,
                )
    with row_inputs[5]:
        st.text_input(
            "Optional suffix (Person - TargetEvent)",
            key="crm_tpl_person_target_event_extra_suffix",
            label_visibility="collapsed",
            placeholder=suf_placeholder,
            on_change=_persist_crm_template_vars_callback,
            help="Необовʼязковий текст із шаблону CRM — останній фрагмент через кому після допису Activity - Subject (експорт).",
        )


def _pool_preview_caption(raw: str) -> str:
    em, tg = partition_sender_pool_lines(parse_email_list_field(str(raw or "")))
    parts: list[str] = []
    if em:
        parts.append(f"email: **{len(em)}** шт.")
    if tg:
        parts.append("теги: **" + "**, **".join(tg) + "**")
    return " · ".join(parts) if parts else "— *порожньо (відправників скинуть для сегментів цього провайдера)*"


_SESSION_AFTER_CRM_DISK_CLEAR_KEYS: tuple[str, ...] = (
    "crm_match_activity_export_df",
    "crm_match_activity_export_stats",
    "crm_match_activity_export_crm_only_no_sheet_df",
)


def _pop_session_after_cleared_crm_store_on_disk() -> None:
    """Після запису порожнього CRM у файл — скинути похідний стан у session_state."""
    for k in _SESSION_AFTER_CRM_DISK_CLEAR_KEYS:
        st.session_state.pop(k, None)
    st.session_state.pop("crm_delete_all_confirm", None)
    st.session_state.pop("hdr_strip_crm_clear_chk", None)


def _render_header_status_strip() -> None:
    """Компактний рядок під заголовком сторінки: відправники (лише перегляд) і стан CRM."""
    g_raw = str(st.session_state.get("instantly_pool_gmail") or "").strip()
    o_raw = str(st.session_state.get("instantly_pool_outlook") or "").strip()
    g_txt = _pool_preview_caption(g_raw) if g_raw else "— порожньо"
    o_txt = _pool_preview_caption(o_raw) if o_raw else "— порожньо"

    crm_n = len(_load_crm_saved_runs())
    clean = crm_n == 0

    row = st.columns([3.25, 3.25, 3.5])
    with row[0]:
        st.caption(f"**Зараз Gmail/Other:** {g_txt}")
    with row[1]:
        st.caption(f"**Зараз Outlook:** {o_txt}")
    with row[2]:
        if clean:
            st.caption("**CRM:** чисто")
        else:
            sub = st.columns([2.35, 1.65])
            with sub[0]:
                st.caption(f"**CRM:** **{crm_n}** записів")
            with sub[1]:
                with st.popover("Очистити CRM"):
                    st.caption(
                        f"Файл **`{_CRM_SAVED_RUNS_PATH.name}`**: буде видалено **усі** **{crm_n}** записів."
                    )
                    chk = st.checkbox(
                        "Підтверджую безповоротне видалення всіх записів CRM",
                        key="hdr_strip_crm_clear_chk",
                    )
                    if st.button(
                        "Очистити все з CRM",
                        type="primary",
                        disabled=not chk,
                        key="hdr_strip_crm_clear_btn",
                    ):
                        ok_cl, err_cl = _save_crm_saved_runs([])
                        if ok_cl:
                            _pop_session_after_cleared_crm_store_on_disk()
                            st.rerun()
                        else:
                            st.error(err_cl or "Не вдалося оновити файл.")


def _lh_split_df_into_parts(df: pd.DataFrame, n_accounts: int) -> list[pd.DataFrame]:
    """Рівно `n_accounts` частин; порожній df дає n порожніх таблиць з тими ж колонками."""
    n = max(1, int(n_accounts))
    if df is None:
        return [pd.DataFrame() for _ in range(n)]
    if df.empty:
        return [pd.DataFrame(columns=df.columns).copy() for _ in range(n)]
    if n <= 1:
        return [df.copy()]
    L = len(df)
    base, rem = divmod(L, n)
    parts: list[pd.DataFrame] = []
    start = 0
    for i in range(n):
        sz = base + (1 if i < rem else 0)
        parts.append(df.iloc[start : start + sz].copy())
        start += sz
    return parts


# Дозволені країни для Europe.csv — інші потрапляють у _Other_Countries.csv
_LH_EUROPE_ALLOWED_COUNTRIES: frozenset[str] = frozenset({
    "austria", "belgium", "denmark", "finland", "france", "germany",
    "luxembourg", "netherlands", "norway", "sweden", "switzerland",
    "iceland", "ireland", "united kingdom",
    # поширені скорочення / коди
    "at", "be", "dk", "fi", "fr", "de", "lu", "nl", "no", "se", "ch", "is", "ie",
    "gb", "uk", "great britain", "england", "scotland", "wales",
})


# Перейменування колонок у ZIP-файлах для LinkedHelper
def _lh_build_column_rename() -> dict[str, str]:
    """Генерує маппінг message_MM_paragraph_PP → cs_mMpP для повідомлень 1–8, параграфів 1–8."""
    m: dict[str, str] = {
        "First Name":          "cs_name",
        "Email":               "cs_mail",
        "Person Linkedin Url": "profile_url",
    }
    for msg in range(1, 9):
        for par in range(1, 9):
            m[f"message_{msg:02d}_paragraph_{par:02d}"] = f"cs_m{msg}p{par}"
    return m


_LH_ZIP_COLUMN_RENAME: dict[str, str] = _lh_build_column_rename()

# Колонки контактних даних, які завжди лишаємо у ZIP (регістронезалежно)
_LH_ZIP_CONTACT_COLS: tuple[str, ...] = (
    "pipedrive_contact_id",
    "first name", "last name", "title", "company", "email",
    "person linkedin url", "website", "city", "state", "country",
)


def _lh_select_zip_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Лишає лише колонки для LinkedHelper:
    - pipedrive_contact_id + базові контактні поля
    - message_XX_paragraph_YY (будь-яка кількість)
    Все інше (id, UUID, ln_generation_text, CRM-поля тощо) прибирається.
    Після фільтру застосовує перейменування (_LH_ZIP_COLUMN_RENAME).
    """
    if df is None or df.empty:
        return df

    contact_set = set(_LH_ZIP_CONTACT_COLS)
    keep: list[str] = []
    for col in df.columns:
        norm = col.strip().lower()
        if norm in contact_set:
            keep.append(col)
        elif re.match(r"message_\d+_paragraph_\d+", norm):
            keep.append(col)
    result = df[keep].copy() if keep else df.copy()

    # Якщо в LinkedIn URL є кілька посилань — лишаємо лише перше
    linkedin_col: str | None = None
    for col in result.columns:
        if col.strip().lower() == "person linkedin url":
            linkedin_col = col
            break
    if linkedin_col:
        def _first_linkedin_url(val: Any) -> str:
            s = str(val or "").strip()
            if not s or s.lower() == "nan":
                return ""
            # Розбиваємо по найпоширеніших роздільниках
            parts = re.split(r"[\n\r,;|\s]+", s)
            for p in parts:
                p = p.strip()
                if p and ("linkedin" in p.lower() or p.startswith("http")):
                    return p
            return parts[0].strip() if parts else s
        result[linkedin_col] = result[linkedin_col].apply(_first_linkedin_url)

    result = result.rename(columns=_LH_ZIP_COLUMN_RENAME)

    # Сортуємо колонки у правильному порядку після rename
    # 1) фіксований порядок контактних полів
    _contact_order = [
        "pipedrive_contact_id",
        "cs_name", "Last Name", "Title", "Company",
        "cs_mail", "profile_url", "Website", "City", "State", "Country",
    ]
    # 2) message-колонки: cs_mXpY сортуємо (X, Y) чисельно; решта message_* — теж по (X, Y)
    def _msg_sort_key(col: str) -> tuple[int, int]:
        m = re.match(r"(?:cs_m|message_0*)(\d+)(?:p|_paragraph_0*)(\d+)$", col.strip().lower())
        if m:
            return int(m.group(1)), int(m.group(2))
        return (999, 999)

    contact_present = [c for c in _contact_order if c in result.columns]
    msg_cols = sorted(
        [c for c in result.columns if c not in contact_present],
        key=_msg_sort_key,
    )
    result = result[contact_present + msg_cols]

    return result


def _lh_slugify_activity(name: str) -> str:
    """Безпечна назва файлу з 'Activity - Subject' (до 60 символів).
    Видаляємо лише справді заборонені символи Windows: \\ / : * ? \" < > |
    Пробіли та решта символів ([], -, цифри, літери) — зберігаються як є.
    """
    s = re.sub(r'[\\/:*?"<>|\r\n]', "", str(name or "Unknown")).strip()
    return s[:60] or "Unknown"


def _lh_filter_europe_allowed(
    eu_df: pd.DataFrame,
    location_col: str = "Country",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Розділяє Europe-рядки на дозволені країни та «інші»."""
    if eu_df is None or eu_df.empty:
        return eu_df if eu_df is not None else pd.DataFrame(), pd.DataFrame()

    # Знаходимо колонку з країною
    col_use: str | None = None
    for c in eu_df.columns:
        if str(c).strip().lower() == (location_col or "country").strip().lower():
            col_use = str(c)
            break
    if col_use is None:
        for c in eu_df.columns:
            if "country" in str(c).lower():
                col_use = str(c)
                break

    if col_use is None:
        # Не знайшли колонку — всі рядки лишаємо
        return eu_df.copy(), pd.DataFrame(columns=eu_df.columns)

    def _is_allowed(val: Any) -> bool:
        s = str(val or "").strip().lower()
        return s in _LH_EUROPE_ALLOWED_COUNTRIES

    mask = eu_df[col_use].apply(_is_allowed)
    return eu_df[mask].copy(), eu_df[~mask].copy()


def _lh_build_linked_helper_export_zip(
    *,
    n_account_folders: int,
    work_by_reg: dict[str, pd.DataFrame],
    excluded_df: pd.DataFrame | None,
    unmatched_df: pd.DataFrame | None,
    location_col: str = "Country",
    gen_columns: list[str] | None = None,
) -> tuple[bytes, pd.DataFrame]:
    """
    ZIP: `account_NN/{Activity_Subject}_{Geo}.csv` (N = кількість акаунтів).
    Ліди спочатку діляться по Activity - Subject, потім по гео, потім рівномірно між акаунтами.
    Якщо Activity - Subject відсутній — файли `{Geo}.csv` як раніше.
    Колонки: LinkedHelper-формат (pipedrive_contact_id + контактні + message_XX_paragraph_YY).
    У корені: pipedrive_excluded.csv / _Unmatched_geo.csv / _Other_Countries.csv (якщо є).
    Повертає (zip_bytes, other_countries_df).
    """
    _ACT_COL = "Activity - Subject"
    n = max(1, int(n_account_folders))

    usa = work_by_reg.get(LH_GEO_BUCKET_USA_CA)
    if usa is None:
        usa = pd.DataFrame()
    eu_raw = work_by_reg.get(LH_GEO_BUCKET_EUROPE)
    if eu_raw is None:
        eu_raw = pd.DataFrame()
    eu, other_eu = _lh_filter_europe_allowed(eu_raw, location_col=location_col)

    # account_files[account_idx][filename] = df_part (вже з правильними колонками)
    account_files: dict[int, dict[str, pd.DataFrame]] = {i: {} for i in range(n)}

    def _add_geo_bucket(geo_label: str, geo_df: pd.DataFrame) -> None:
        """Ділить один гео-бакет по activity, потім між акаунтами."""
        if geo_df is None or geo_df.empty:
            return
        if _ACT_COL in geo_df.columns:
            activities = geo_df[_ACT_COL].fillna("Unknown").unique()
            for act in activities:
                act_slug = _lh_slugify_activity(str(act))
                act_df = geo_df[geo_df[_ACT_COL].fillna("Unknown") == act].copy()
                fname = f"{act_slug}_{geo_label}.csv"
                parts = _lh_split_df_into_parts(act_df, n)
                for i in range(n):
                    part = parts[i] if i < len(parts) else pd.DataFrame(columns=act_df.columns)
                    account_files[i][fname] = _lh_select_zip_columns(part)
        else:
            fname = f"{geo_label}.csv"
            parts = _lh_split_df_into_parts(geo_df, n)
            for i in range(n):
                part = parts[i] if i < len(parts) else pd.DataFrame(columns=geo_df.columns)
                account_files[i][fname] = _lh_select_zip_columns(part)

    _add_geo_bucket(LH_GEO_BUCKET_USA_CA, usa)
    _add_geo_bucket(LH_GEO_BUCKET_EUROPE, eu)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i in range(n):
            folder = f"account_{i + 1:02d}"
            for fname, df_part in account_files[i].items():
                zf.writestr(f"{folder}/{fname}", df_to_csv_bytes(df_part))
        if excluded_df is not None and len(excluded_df):
            zf.writestr("pipedrive_excluded.csv", df_to_csv_bytes(excluded_df))
        if unmatched_df is not None and len(unmatched_df):
            zf.writestr("_Unmatched_geo.csv", df_to_csv_bytes(_lh_select_zip_columns(unmatched_df)))
        if other_eu is not None and len(other_eu):
            zf.writestr("_Other_Countries.csv", df_to_csv_bytes(_lh_select_zip_columns(other_eu)))
    return buf.getvalue(), other_eu if other_eu is not None else pd.DataFrame()


def _lh_apply_pipedrive_exclusions(
    df: pd.DataFrame,
    person_col: str,
    api_token: str,
    mql_names_csv: str,
    replied_values_csv: str,
    replied_field_api_key: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    За вказаною колонкою (за замовчуванням **pipedrive_contact_id** = id персони в Pipedrive) витягує контакти з API й виключає рядки,
    якщо label = MQL (налаштовується) або поле **Replied at** (назва поля фіксована) дорівнює одному з налаштованих значень.
    """
    notes: list[str] = []
    pc = person_col.strip()
    if pc not in df.columns:
        return (
            df.copy(),
            pd.DataFrame(),
            [
                f"У таблиці немає колонки «{pc}» — фільтр за Pipedrive **не запускався** (це не помилка з'єднання з API). "
                f"Вкажіть точну назву стовпця з id контакту, наприклад **pipedrive_contact_id**."
            ],
        )

    mql_list = [x.strip() for x in (mql_names_csv or "").split(",") if x.strip()]
    replied_vals = [x.strip() for x in (replied_values_csv or "").split(",") if x.strip()]
    if not mql_list:
        mql_list = ["MQL"]
    if not replied_vals:
        replied_vals = ["Email"]

    ids_unique: list[int] = []
    seen: set[int] = set()
    for _, row in df.iterrows():
        nid = normalize_sheet_person_id(row.get(pc))
        if not nid:
            continue
        try:
            xi = int(nid)
        except ValueError:
            continue
        if xi not in seen:
            seen.add(xi)
            ids_unique.append(xi)

    try:
        excluded_set, reasons, pd_diag = collect_excluded_person_ids(
            api_token,
            ids_unique,
            mql_label_names=mql_list,
            replied_at_field_name=_LH_PIPEDRIVE_REPLIED_AT_FIELD,
            replied_at_values=replied_vals,
            replied_at_field_api_key=(replied_field_api_key or "").strip(),
        )
        # Показуємо лише повідомлення про помилки/попередження; решта — в деталях
        _diag_keywords = ("не знайдено", "помилк", "недоступн", "порожн", "увага", "жоден", "перевірте", "не повернул")
        for msg in pd_diag:
            ml = msg.lower()
            if any(k in ml for k in _diag_keywords):
                notes.append(msg)
    except PipedriveError as e:
        return df.copy(), pd.DataFrame(), [str(e)]

    kept_idx: list[Any] = []
    excluded_records: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        nid = normalize_sheet_person_id(row.get(pc))
        pid_i: int | None = None
        if nid:
            try:
                pid_i = int(nid)
            except ValueError:
                pid_i = None
        if pid_i is not None and pid_i in excluded_set:
            d = row.to_dict()
            d["_pipedrive_person_id"] = pid_i
            d["_pipedrive_exclude_reason"] = reasons.get(pid_i, "")
            excluded_records.append(d)
        else:
            kept_idx.append(idx)

    kept = df.loc[kept_idx].copy() if kept_idx else pd.DataFrame(columns=df.columns)
    excl_df = pd.DataFrame(excluded_records) if excluded_records else pd.DataFrame()
    if ids_unique:
        n_excl = len(excl_df)
        mql_cnt = sum(1 for r in reasons.values() if "mql" in r.lower())
        rep_cnt = sum(1 for r in reasons.values() if "replied" in r.lower())
        parts_stat = []
        if mql_cnt:
            parts_stat.append(f"MQL: {mql_cnt}")
        if rep_cnt:
            parts_stat.append(f"Replied at: {rep_cnt}")
        detail = f" ({', '.join(parts_stat)})" if parts_stat else ""
        notes.append(
            f"Pipedrive: перевірено **{len(ids_unique)}** id → виключено **{n_excl}**{detail}."
        )
    return kept, excl_df, notes


def _lh_work_by_reg_after_geo_and_pipedrive(
    concat_df: pd.DataFrame,
    *,
    location_column: str,
    person_col: str,
    api_token: str,
    mql_names_csv: str,
    replied_values_csv: str,
    replied_field_api_key: str = "",
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, list[pd.DataFrame], list[str]]:
    """
    Гео з Country, далі за наявності токена — виключення Pipedrive по **person_col**.
    Ті самі кроки, що для ZIP по акаунтах і для окремого **CRM_LinkedHelper.csv**.
    """
    notes: list[str] = []
    token_now = (api_token or "").strip()
    if not token_now:
        notes.append("Токен Pipedrive не задано — фільтр CRM пропущено.")

    geo_buckets, unmatched_geo = split_dataframe_geo_only(
        concat_df,
        location_column=location_column,
    )
    excluded_all: list[pd.DataFrame] = []
    work_by_reg: dict[str, pd.DataFrame] = {}
    for reg_name, bdf in geo_buckets.items():
        if bdf is None or bdf.empty:
            work_by_reg[reg_name] = bdf.copy() if bdf is not None else pd.DataFrame()
            continue
        if token_now:
            kept, exc, n = _lh_apply_pipedrive_exclusions(
                bdf,
                person_col,
                token_now,
                mql_names_csv,
                replied_values_csv,
                replied_field_api_key=replied_field_api_key,
            )
            notes.extend(n)
            if len(exc):
                excluded_all.append(exc)
            work_by_reg[reg_name] = kept
        else:
            work_by_reg[reg_name] = bdf.copy()
    return work_by_reg, unmatched_geo, excluded_all, notes


def _lh_frozen_excluded_person_ids(excluded_df: pd.DataFrame | None) -> frozenset[int]:
    """ID з колонки `_pipedrive_person_id` у `pipedrive_excluded.csv` (як після `_lh_apply_pipedrive_exclusions`)."""
    if excluded_df is None or excluded_df.empty:
        return frozenset()
    col = "_pipedrive_person_id"
    if col not in excluded_df.columns:
        return frozenset()
    out: set[int] = set()
    for v in excluded_df[col].tolist():
        try:
            out.add(int(v))
        except (TypeError, ValueError):
            continue
    return frozenset(out)


def _lh_work_by_reg_after_geo_minus_person_ids(
    concat_df: pd.DataFrame,
    *,
    location_column: str,
    person_col: str,
    excluded_person_ids: set[int] | frozenset[int],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Той самий гео-поділ, далі рядки з person_col прибираються, якщо id ∈ excluded_person_ids
    (як після Pipedrive, але лише по вже відомому списку — без API).
    """
    geo_buckets, unmatched_geo = split_dataframe_geo_only(
        concat_df,
        location_column=location_column,
    )
    excl = excluded_person_ids or frozenset()
    pc = person_col.strip()
    work_by_reg: dict[str, pd.DataFrame] = {}
    for reg_name, bdf in geo_buckets.items():
        if bdf is None or bdf.empty:
            work_by_reg[reg_name] = bdf.copy() if bdf is not None else pd.DataFrame()
            continue
        if pc not in bdf.columns:
            work_by_reg[reg_name] = bdf.copy()
            continue
        kept_idx: list[Any] = []
        for idx, row in bdf.iterrows():
            nid = normalize_sheet_person_id(row.get(pc))
            pid_i: int | None = None
            if nid:
                try:
                    pid_i = int(nid)
                except ValueError:
                    pid_i = None
            if pid_i is not None and pid_i in excl:
                continue
            kept_idx.append(idx)
        work_by_reg[reg_name] = (
            bdf.loc[kept_idx].copy() if kept_idx else pd.DataFrame(columns=bdf.columns)
        )
    return work_by_reg, unmatched_geo


def _lh_pipedrive_work_cache_key(
    concat_df: pd.DataFrame,
    *,
    location_column: str,
    person_col: str,
    api_token: str,
    mql_names_csv: str,
    replied_values_csv: str,
    replied_field_api_key: str = "",
) -> str:
    try:
        row_h = pd.util.hash_pandas_object(concat_df, index=True)
        core = row_h.values.tobytes()
    except Exception:
        core = pickle.dumps(concat_df, protocol=4)
    tail = "|".join(
        [
            location_column,
            person_col,
            api_token,
            mql_names_csv,
            replied_values_csv,
            replied_field_api_key or "",
        ]
    ).encode("utf-8")
    return hashlib.sha256(core + tail).hexdigest()


def _lh_get_work_by_reg_with_session_cache(
    concat_df: pd.DataFrame,
    *,
    location_column: str,
    person_col: str,
    api_token: str,
    mql_names_csv: str,
    replied_values_csv: str,
    replied_field_api_key: str = "",
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, list[pd.DataFrame], list[str], bool]:
    """Один запит до Pipedrive на той самий зміст таблиці й параметри; повторні ZIP/CRM беруть з кешу."""
    ck = _lh_pipedrive_work_cache_key(
        concat_df,
        location_column=location_column,
        person_col=person_col,
        api_token=api_token,
        mql_names_csv=mql_names_csv,
        replied_values_csv=replied_values_csv,
        replied_field_api_key=replied_field_api_key or "",
    )
    cached = st.session_state.get("lh_pd_work_cache")
    if isinstance(cached, dict) and cached.get("key") == ck:
        wbr = cached.get("work_by_reg") or {}
        umg = cached.get("unmatched_geo")
        exa = cached.get("excluded_all") or []
        pnr = list(cached.get("pipe_notes") or [])
        if not isinstance(umg, pd.DataFrame):
            umg = pd.DataFrame()
        return wbr, umg, exa, pnr, True

    work_by_reg, unmatched_geo, excluded_all, pipe_notes = _lh_work_by_reg_after_geo_and_pipedrive(
        concat_df,
        location_column=location_column,
        person_col=person_col,
        api_token=api_token,
        mql_names_csv=mql_names_csv,
        replied_values_csv=replied_values_csv,
        replied_field_api_key=replied_field_api_key or "",
    )
    st.session_state["lh_pd_work_cache"] = {
        "key": ck,
        "work_by_reg": work_by_reg,
        "unmatched_geo": unmatched_geo,
        "excluded_all": excluded_all,
        "pipe_notes": pipe_notes,
    }
    return work_by_reg, unmatched_geo, excluded_all, pipe_notes, False


def _lh_url_lines_non_empty(raw: str) -> list[str]:
    return [ln.strip() for ln in (raw or "").splitlines() if ln.strip()]


def _render_linked_helper_tab() -> None:
    """LinkedHelper²: злиття таблиць; гео з Country; Pipedrive для виключень; ZIP = N папок account_XX; CRM CSV — окремо."""
    st.caption(
        "Завантаження **лише читання** з Google: таблиці **не змінюються**. "
        "**1-й рядок** — посилання на **книгу** Google; **метч** (Person ID тощо) **завжди з першого аркуша** цієї книги — **`gid` у цьому посиланні не використовується**. "
        "**2-й рядок** — **поділ** (**ln_generation** / **linkedin_generation**); тут **нормально вказати `gid=`** для потрібного листа. "
        "**Умова join:** **pipedrive_contact_id** (поділ) **=** **Person ID** (метч). "
        "**Гео (USA+Canada vs Європа)** береться з колонки Country у злитих рядках поділу; **Pipedrive не для регіону**. "
        "Pipedrive потрібен лише щоб за **pipedrive_contact_id** **прибрати з основних списків** контакти з label **MQL** "
        "або з полем **Replied at** = **Email** (які значення вважати виключенням — нижче; назва поля в CRM — **Replied at**). Токен API — у **боковій панелі**. Виключені — **pipedrive_excluded.csv** в ZIP. "
        f"Файл **`{_LH_CRM_ZIP_FILENAME}`** для імпорту в CRM збирається **окремою кнопкою** і **не** потрапляє в архів."
    )

    st.caption(
        "**Рядок 1** — будь-яке посилання на книгу; **метч** = **перший аркуш** (ігнор `gid`). "
        "**Поділ:** **рядок 2** з **pipedrive_contact_id** (**gid** у 2-му рядку враховується) або один рядок + **XLSX** (**openpyxl**). "
        "**Join:** **pipedrive_contact_id** (поділ) **=** **Person ID** (метч); поле **ID контакту на поділі** нижче — саме **pipedrive_contact_id**. "
        f"Колонки для **`CRM_LinkedHelper.csv`** (Person ID, Activity, TargetEvent) — **внизу** вкладки, біля кнопки збірки. "
        f"CRM-файл (**{_LH_CRM_ZIP_FILENAME}**) не входить до **linked_helper_export.zip**."
    )
    st.text_area(
        "LH посилання на Google Таблицю (список, по одному в рядку)",
        height=120,
        key="lh_sheet_match_urls",
        label_visibility="visible",
        placeholder=(
            "# Рядок 1 — будь-яке посилання на книгу (метч = перший аркуш, gid ігнорується)\n"
            "# Рядок 2 — поділ з pipedrive_contact_id (бажано вказати gid потрібного листа)\n"
            "https://docs.google.com/…/edit?gid=…\n"
            "https://docs.google.com/…/edit?gid=…\n"
        ),
    )

    _lh_prev_df = st.session_state.get("lh_concat_df")
    _lh_prev_meta = st.session_state.get("lh_concat_meta")

    _parts_help = (
        "Скільки папок **`account_01`, `account_02`, …** буде в ZIP. У кожній — **USA_Canada.csv** та **Europe.csv** "
        "(рядки кожного регіону **окремо** діляться між папками максимально рівномірно). У **корені ZIP**: "
        "**`pipedrive_excluded.csv`**, **`_Unmatched_geo.csv`** (лише якщо є дані)."
    )
    if isinstance(_lh_prev_df, pd.DataFrame) and len(_lh_prev_df):
        _parts_help = f"Зараз у пам’яті **{len(_lh_prev_df)}** рядків після злиття. {_parts_help}"

    r1, r2 = st.columns(2)
    with r1:
        st.text_input(
            "ID контакту на поділі (join + Pipedrive)",
            value="pipedrive_contact_id",
            key="lh_person_id_col",
            help="**Гео — лише Country** поруч. Колонка **pipedrive_contact_id** на **ln_generation / linkedin_generation**: нею задається, **з яким значенням порівнюється Person ID** на 1-му аркуші (**pipedrive_contact_id = Person ID**). "
            "Також нею **Pipedrive** (MQL / Replied) і вивід **CRM_LinkedHelper.csv**. **Person ID** для метчу — з **першого аркуша** книги з 1-го рядка посилань. Окремі імена колонок для CRM — внизу вкладки.",
        )
    with r2:
        st.text_input(
            "Country",
            value="Country",
            key="lh_location_col",
            help="Стовпець країни на **поділі** (ln_generation / linkedin_generation). USA+Canada vs Європа як у Instantly. "
            "Порожньо — авто: «country» у назві або скан рядка.",
        )

    if isinstance(_lh_prev_df, pd.DataFrame) and len(_lh_prev_df):
        mrow1, mrow2 = st.columns(2)
        with mrow1:
            st.metric("Рядків після злиття", len(_lh_prev_df))
        with mrow2:
            useen: set[str] = set()
            nuniq = 0
            for ln in _lh_url_lines_non_empty(str(st.session_state.get("lh_sheet_match_urls") or "")):
                if ln not in useen:
                    useen.add(ln)
                    nuniq += 1
            st.metric("Унікальних посилань", nuniq)
        st.caption(f"**Злиття (метч + поділ):** {_lh_prev_meta or '—'}")
    else:
        st.caption(
            "Перевірте **ID контакту на поділі** та **Country** вище, потім натисніть кнопку. "
            "Після злиття тут з’явиться **кількість рядків**; **акаунтів** для ZIP оберіть у блоці **«Зібрати ZIP»** (поле під **Акаунтів (ZIP)**)."
        )

    load_lh = st.button(
        "Завантажити та з’єднати таблиці",
        type="secondary",
        key="lh_sheet_match_load",
        width="content",
        help="Збирає злиття: **метч** — завжди **перший аркуш** книги з **1-го рядка** (gid у URL не використовується); **поділ** — **2-й рядок** (за бажанням з gid) або **XLSX**.",
    )

    if load_lh:
        raw = str(st.session_state.get("lh_sheet_match_urls") or "").strip()
        lines = _lh_url_lines_non_empty(raw)
        if not lines:
            st.warning("Вставте посилання.")
        else:
            try:
                with st.spinner("Завантаження з Google…"):
                    join_key = str(st.session_state.get("lh_person_id_col") or "pipedrive_contact_id").strip()
                    crm_act = str(st.session_state.get("lh_crm_col_activity") or "Activity - Subject").strip()
                    crm_te = str(st.session_state.get("lh_crm_col_target") or "Person - TargetEvent").strip()
                    crm_pid = str(st.session_state.get("lh_crm_person_id_source_col") or "Person ID").strip()
                    prefer_m = tuple(dict.fromkeys(x for x in (crm_act, crm_te, crm_pid) if x))
                    concat_df, merge_meta, match_sheet_ids, gen_cols = _lh_load_match_and_generation_merged(
                        raw,
                        join_key,
                        prefer_master_column_hints=prefer_m,
                    )
                if concat_df.empty:
                    st.session_state.pop("lh_concat_df", None)
                    st.session_state.pop("lh_concat_meta", None)
                    st.session_state.pop("lh_pd_work_cache", None)
                    st.session_state.pop("lh_zip_excluded_key", None)
                    st.session_state.pop("lh_zip_excluded_ids", None)
                    st.session_state.pop("lh_match_sheet_id_set", None)
                    st.session_state.pop("lh_gen_columns", None)
                    st.session_state.pop("lh_last_zip", None)
                    st.session_state.pop("lh_last_notes", None)
                    st.session_state.pop("lh_last_zip_success", None)
                    st.error("Не вдалося зібрати злиту таблицю.")
                else:
                    st.session_state.pop("lh_pd_work_cache", None)
                    st.session_state.pop("lh_zip_excluded_key", None)
                    st.session_state.pop("lh_zip_excluded_ids", None)
                    st.session_state.pop("lh_last_zip", None)
                    st.session_state.pop("lh_last_notes", None)
                    st.session_state.pop("lh_last_zip_success", None)
                    st.session_state["lh_concat_df"] = concat_df
                    st.session_state["lh_concat_meta"] = merge_meta
                    st.session_state["lh_match_sheet_id_set"] = match_sheet_ids
                    st.session_state["lh_gen_columns"] = gen_cols
                    seen_u: set[str] = set()
                    uniq_ln: list[str] = []
                    for ln in lines:
                        if ln in seen_u:
                            continue
                        seen_u.add(ln)
                        uniq_ln.append(ln)
                    extra = ""
                    if len(uniq_ln) >= 2 and not extract_gid_from_url(uniq_ln[1]):
                        extra = (
                            " Увага: **2-й рядок** без **`gid=`** — переконайтеся, що для **поділу** відкривається потрібний аркуш."
                        )
                    st.success(
                        f"У памʼять завантажено **{len(concat_df)}** рядків (join по **{join_key}**).{extra}"
                    )
                    st.caption(merge_meta)
                    if match_sheet_ids:
                        st.caption(
                            f"**Person ID** з усіх метч-аркушів: **{len(match_sheet_ids)}** унікальних id — "
                            "CRM файл буде містити рядки, де **pipedrive_contact_id** ∈ цьому списку."
                        )
                    else:
                        st.warning(
                            "На 1-му аркуші **не знайдено** колонки **Person ID** / **Person - ID** — "
                            "CRM файл міститиме всі рядки (без фільтра по першому аркушу). "
                            "Перевірте зразки id у рядку вище."
                        )
            except Exception as ex:
                st.session_state.pop("lh_concat_df", None)
                st.session_state.pop("lh_concat_meta", None)
                st.session_state.pop("lh_pd_work_cache", None)
                st.session_state.pop("lh_zip_excluded_key", None)
                st.session_state.pop("lh_zip_excluded_ids", None)
                st.session_state.pop("lh_match_sheet_id_set", None)
                st.error(str(ex))

    st.markdown("###### Правила виключення (Pipedrive)")
    st.caption(
        "Застосовуються при **«Зібрати ZIP»** та при **окремій збірці CRM** — за тими самими правилами, якщо в боковій панелі задано токен. "
        "Повторне натискання з **тими ж** злитими даними й полями **не дублює** запити до Pipedrive (кеш після першої збірки). "
        "Після **«Зібрати ZIP»** збірка CRM може йти лише по **`pipedrive_excluded`** у пам’яті сесії — теж **без** API."
    )
    pc_a, pc_b = st.columns(2)
    with pc_a:
        st.text_input(
            "Label-и для виключення (через кому, текст як у Pipedrive)",
            value="MQL",
            key="lh_pipedrive_mql_labels",
        )
    with pc_b:
        st.text_input(
            "Значення Replied at для виключення (через кому)",
            value="Email",
            key="lh_pipedrive_replied_values",
            help="Поле персони в Pipedrive має називатися **Replied at** (як у типовому сетапі). "
            "Порівняння з відображеним текстом поля (для enum — підпис опції, напр. Email).",
        )

    if "lh_pipedrive_replied_field_api_key" not in st.session_state:
        st.session_state["lh_pipedrive_replied_field_api_key"] = "229c1c2bb15b87994a9ee73ba83a816d869a0586"

    st.divider()

    concat_df = st.session_state.get("lh_concat_df")
    meta = st.session_state.get("lh_concat_meta")

    if st.session_state.get("lh_zip_building"):
        # Режим обробки: кнопка не рендериться, лише статус
        st.info("Збираємо ZIP (Pipedrive + гео)…")
        try:
            loc_col = str(st.session_state.get("lh_location_col") or "").strip()
            person_col = str(st.session_state.get("lh_person_id_col") or "pipedrive_contact_id").strip()
            n_acc = int(st.session_state.get("lh_account_parts") or 1)
            token_now = (get_pipedrive_api_key() or "").strip()
            mql_csv = str(st.session_state.get("lh_pipedrive_mql_labels") or "")
            rep_vals = str(st.session_state.get("lh_pipedrive_replied_values") or "")
            rep_field_key = str(st.session_state.get("lh_pipedrive_replied_field_api_key") or "").strip()
            work_by_reg, unmatched_geo, excluded_all, pipe_notes, _pd_cached = (
                _lh_get_work_by_reg_with_session_cache(
                    concat_df,
                    location_column=loc_col,
                    person_col=person_col,
                    api_token=token_now,
                    mql_names_csv=mql_csv,
                    replied_values_csv=rep_vals,
                    replied_field_api_key=rep_field_key,
                )
            )
            notes: list[str] = []
            if _pd_cached:
                notes.append("Pipedrive: використано кеш (без нових запитів).")
            else:
                notes.extend(pipe_notes)
            excluded_df = (
                pd.concat(excluded_all, ignore_index=True) if excluded_all else pd.DataFrame()
            )
            eu_raw_preview = work_by_reg.get(LH_GEO_BUCKET_EUROPE)
            eu_allowed_preview, eu_other_preview = _lh_filter_europe_allowed(
                eu_raw_preview if eu_raw_preview is not None else pd.DataFrame(),
                location_col=loc_col or "Country",
            )
            if work_by_reg:
                geo_stat: dict[str, int] = {}
                for k, v in work_by_reg.items():
                    if k == LH_GEO_BUCKET_EUROPE:
                        geo_stat[k] = len(eu_allowed_preview)
                    else:
                        geo_stat[k] = len(v)
                if len(eu_other_preview):
                    geo_stat["Other_Countries"] = len(eu_other_preview)
                geo_parts = ", ".join(
                    f"**{k}** — {v}" for k, v in sorted(geo_stat.items())
                )
                total_rows = sum(geo_stat.values())
                total_excl = len(excluded_df) if len(excluded_df) else 0
                folders = f"**{n_acc}** папок" if n_acc > 1 else "**1** папка"
                notes.append(
                    f"ZIP ({folders}): {geo_parts} — всього **{total_rows}**"
                    + (f"; виключено всього **{total_excl}**." if total_excl else ".")
                )
            zip_ck = _lh_pipedrive_work_cache_key(
                concat_df, location_column=loc_col, person_col=person_col,
                api_token=token_now, mql_names_csv=mql_csv,
                replied_values_csv=rep_vals, replied_field_api_key=rep_field_key,
            )
            st.session_state["lh_zip_excluded_key"] = zip_ck
            st.session_state["lh_zip_excluded_ids"] = _lh_frozen_excluded_person_ids(
                excluded_df if len(excluded_df) else None
            )
            zbytes, other_eu_df = _lh_build_linked_helper_export_zip(
                n_account_folders=n_acc,
                work_by_reg=work_by_reg,
                excluded_df=excluded_df if len(excluded_df) else None,
                unmatched_df=unmatched_geo,
                location_col=loc_col or "Country",
                gen_columns=st.session_state.get("lh_gen_columns"),
            )
            if other_eu_df is not None and len(other_eu_df):
                notes.append(
                    "Контакти поза дозволеним списком Europe → **`_Other_Countries.csv`** у корені ZIP."
                )
            other_eu_note = " / **`_Other_Countries.csv`**" if (other_eu_df is not None and len(other_eu_df)) else ""
            st.session_state["lh_last_zip"] = zbytes
            st.session_state["lh_last_notes"] = notes
            st.session_state["lh_last_zip_success"] = (
                f"ZIP готовий: **{n_acc}** папок (акаунтів); у кожній — файли **{{Activity}}_{{Geo}}.csv** "
                f"(по одному на кожен таргет × регіон). "
                f"У корені: **`pipedrive_excluded.csv`** / **`_Unmatched_geo.csv`**{other_eu_note} (лише якщо є дані). "
                f"**`{_LH_CRM_ZIP_FILENAME}`** не в архіві — окремо внизу вкладки."
            )
        except Exception as ex:
            st.session_state["lh_last_zip_success"] = None
            st.session_state["lh_last_zip_error"] = str(ex)
        finally:
            st.session_state["lh_zip_building"] = False
        st.rerun()
    else:
        # Звичайний режим: показуємо контролі
        c_zip_left, c_zip_right = st.columns(2, vertical_alignment="bottom")
        with c_zip_left:
            st.caption("Акаунтів (ZIP)")
            st.number_input(
                " ", min_value=1, max_value=500, value=1, step=1,
                key="lh_account_parts", help=_parts_help,
                label_visibility="collapsed", width="stretch",
            )
        with c_zip_right:
            run_lh = st.button(
                "Зібрати ZIP (папки + виключення)",
                type="primary", key="lh_run_pipeline", width="content",
                help="Архів **linked_helper_export.zip** — без **CRM_LinkedHelper.csv**.",
            )
        if isinstance(concat_df, pd.DataFrame) and len(concat_df):
            with st.expander("Попередній перегляд з'єднаної таблиці", expanded=False):
                st.caption(f"Усього **{len(concat_df)}** рядків. Джерела: {meta or chr(8212)}")
                st.dataframe(concat_df.head(40), use_container_width=True)
        if run_lh:
            if not isinstance(concat_df, pd.DataFrame) or concat_df.empty:
                st.error("Спочатку завантажте таблиці кнопкою вище.")
            else:
                st.session_state["lh_zip_building"] = True
                st.rerun()

    zip_error_msg = st.session_state.pop("lh_last_zip_error", None)
    if zip_error_msg:
        st.error(f"Помилка при збірці ZIP: {zip_error_msg}")
    zip_success_msg = st.session_state.get("lh_last_zip_success")
    if zip_success_msg:
        st.success(zip_success_msg)
    notes_last = st.session_state.get("lh_last_notes")
    if notes_last:
        for ln in notes_last:
            st.caption(ln)
    zip_blob = st.session_state.get("lh_last_zip")
    if zip_blob:
        st.download_button(
            label="Завантажити linked_helper_export.zip",
            data=zip_blob,
            file_name="linked_helper_export.zip",
            mime="application/zip",
            key="lh_dl_zip",
        )

    st.markdown(f"###### Файл для CRM (`{_LH_CRM_ZIP_FILENAME}`) — окремо від ZIP")
    st.caption(
        "У **`CRM_LinkedHelper.csv`** лише рядки поділу, де **pipedrive_contact_id** **дорівнює** якомусь **Person ID** з 1-го аркуша; "
        "Activity / TargetEvent з метчу підставляються, якщо заповнені (можуть бути порожні). Назви колонок нижче."
    )
    crm_ca, crm_cb, crm_cc = st.columns(3)
    with crm_ca:
        st.text_input(
            "Activity → CRM",
            value="Activity - Subject",
            key="lh_crm_col_activity",
            help="Після злиття — з **метчу**; порожня однойменна колонка на поділі перед join прибирається. Поле **Activity - Subject** у **`CRM_LinkedHelper.csv`**.",
        )
    with crm_cb:
        st.text_input(
            "TargetEvent → CRM",
            value="Person - TargetEvent",
            key="lh_crm_col_target",
            help="З **метчу**; у CSV до значення додається через кому **Activity - Subject**.",
        )
    with crm_cc:
        st.text_input(
            "Person ID → CRM",
            value="Person ID",
            key="lh_crm_person_id_source_col",
            help="Резерв для **Person ID** у CSV, якщо немає id у колонці **поділу** (`ID злиття`). Типово колонка **Person ID** з 1-го аркуша; після join з **pipedrive_contact_id** вона може зливатися в одну колонку — тоді береться id з поділу.",
        )
    build_crm = st.button(
        f"Зібрати {_LH_CRM_ZIP_FILENAME}",
        type="secondary",
        key="lh_build_crm_csv",
        width="content",
        help="Після **«Зібрати ZIP»** з тими ж даними — виключення з **`pipedrive_excluded`** без повторного Pipedrive. Якщо ZIP ще не збирали — один прохід API / кеш як для ZIP.",
    )
    if build_crm:
        if not isinstance(concat_df, pd.DataFrame) or concat_df.empty:
            st.error("Спочатку завантажте таблиці кнопкою вище.")
        else:
            try:
                st.session_state.pop("lh_last_crm_csv", None)
                st.session_state.pop("lh_last_crm_notes", None)
                loc_col = str(st.session_state.get("lh_location_col") or "").strip()
                person_col = str(st.session_state.get("lh_person_id_col") or "pipedrive_contact_id").strip()
                token_now = (get_pipedrive_api_key() or "").strip()
                mql_csv = str(st.session_state.get("lh_pipedrive_mql_labels") or "")
                rep_vals = str(st.session_state.get("lh_pipedrive_replied_values") or "")
                rep_field_key = str(st.session_state.get("lh_pipedrive_replied_field_api_key") or "").strip()

                ck = _lh_pipedrive_work_cache_key(
                    concat_df,
                    location_column=loc_col,
                    person_col=person_col,
                    api_token=token_now,
                    mql_names_csv=mql_csv,
                    replied_values_csv=rep_vals,
                    replied_field_api_key=rep_field_key,
                )
                zip_ex_key = st.session_state.get("lh_zip_excluded_key")
                zip_ex_ids = st.session_state.get("lh_zip_excluded_ids")
                use_zip_excluded = (
                    bool(st.session_state.get("lh_last_zip"))
                    and zip_ex_key == ck
                    and zip_ex_ids is not None
                )
                if use_zip_excluded:
                    work_by_reg, _ = _lh_work_by_reg_after_geo_minus_person_ids(
                        concat_df,
                        location_column=loc_col,
                        person_col=person_col,
                        excluded_person_ids=zip_ex_ids,
                    )
                    pd_cached = False
                    crm_from_zip_excl = True
                else:
                    work_by_reg, _, _, pipe_notes, pd_cached = _lh_get_work_by_reg_with_session_cache(
                        concat_df,
                        location_column=loc_col,
                        person_col=person_col,
                        api_token=token_now,
                        mql_names_csv=mql_csv,
                        replied_values_csv=rep_vals,
                        replied_field_api_key=rep_field_key,
                    )
                    crm_from_zip_excl = False
                crm_act = str(st.session_state.get("lh_crm_col_activity") or "Activity - Subject").strip()
                crm_te = str(st.session_state.get("lh_crm_col_target") or "Person - TargetEvent").strip()
                crm_lh_df, crm_notes = _lh_build_crm_lh_export_df(
                    work_by_reg,
                    person_col_gen=person_col,
                    crm_person_id_column=str(st.session_state.get("lh_crm_person_id_source_col") or "Person ID").strip(),
                    activity_col_name=crm_act,
                    target_col_name=crm_te,
                    match_sheet_ids=st.session_state.get("lh_match_sheet_id_set"),
                )
                if crm_from_zip_excl:
                    all_crm_notes = [
                        "CRM: виключення за **останнім ZIP** (`pipedrive_excluded`) — **без запитів до Pipedrive**."
                    ] + list(crm_notes)
                elif pd_cached:
                    all_crm_notes = [
                        "Pipedrive: **без повторних запитів** — використано **кеш** (ті самі таблиця та правила виключення)."
                    ] + list(crm_notes)
                else:
                    all_crm_notes = list(pipe_notes) + list(crm_notes)
                st.session_state["lh_last_crm_notes"] = all_crm_notes
                if crm_lh_df.empty:
                    st.warning(f"Файл **`{_LH_CRM_ZIP_FILENAME}`** порожній — перевірте колонки та фільтр Pipedrive.")
                else:
                    st.session_state["lh_last_crm_csv"] = df_to_csv_bytes(crm_lh_df)
                    st.success(
                        f"**`{_LH_CRM_ZIP_FILENAME}`** готовий (**{len(crm_lh_df)}** рядків) — завантажте файл нижче."
                    )
            except Exception as ex:
                st.error(str(ex))

    crm_notes_last = st.session_state.get("lh_last_crm_notes")
    if crm_notes_last:
        for ln in crm_notes_last:
            st.caption(ln)
    crm_blob = st.session_state.get("lh_last_crm_csv")
    if crm_blob:
        st.download_button(
            label=f"Завантажити {_LH_CRM_ZIP_FILENAME}",
            data=crm_blob,
            file_name=_LH_CRM_ZIP_FILENAME,
            mime="text/csv",
            key="lh_dl_crm",
        )


def _render_crm_tab() -> None:
    st.caption(
        f"Накопичувальні збереження: журнал Instantly API і/або **рядки з розбиття**. "
        f"Файл на цьому ПК: **`{_CRM_SAVED_RUNS_PATH.name}`** — дані лишаються після оновлення сторінки."
    )
    _render_crm_template_variables_row()
    save_err = st.session_state.pop("_crm_tpl_save_err", None)
    if save_err:
        st.warning(f"Не вдалося зберегти шаблон CRM на диск: {save_err}")

    entries = _load_crm_saved_runs()
    if not entries:
        st.info(
            "Поки порожньо. На цій же вкладці після розбиття таблиці використайте блок **«CRM після розбиття (без API)»**, "
            "або після запуску Instantly API — **Додати журнал у CRM**."
        )
    else:
        st.caption(f"Записів: **{len(entries)}** (новіші зверху).")
        da_cols = st.columns([5, 4])
        with da_cols[0]:
            confirm_del_all = st.checkbox(
                "Підтверджую безповоротне видалення **усіх** записів із файлу",
                key="crm_delete_all_confirm",
                help=f"Очистить `{_CRM_SAVED_RUNS_PATH.name}`; окремі записи не потрібно відкривати.",
            )
        with da_cols[1]:
            if st.button(
                "Видалити всі записи",
                type="secondary",
                disabled=not confirm_del_all,
                key="crm_delete_all_records_btn",
            ):
                ok_clear, err_clear = _save_crm_saved_runs([])
                if ok_clear:
                    _pop_session_after_cleared_crm_store_on_disk()
                    st.rerun()
                else:
                    st.error(err_clear or "Не вдалося оновити файл.")
        for entry in reversed(entries):
            eid = str(entry.get("id") or "")
            prefix = str(entry.get("campaign_name_prefix") or "").strip() or "—"
            saved_at = str(entry.get("saved_at") or "")
            log_rows = entry.get("log")
            kind = str(entry.get("entry_kind") or "instantly_pipeline")
            snap_raw = entry.get("contacts_snapshot")
            g_cnt, o_cnt = _crm_snapshot_gmail_outlook_row_counts(
                snap_raw if isinstance(snap_raw, dict) else None
            )
            title = (
                f"{prefix[:80]}{'…' if len(prefix) > 80 else ''} · "
                f"{saved_at[:19].replace('T', ' ')} · Gmail: {g_cnt} · Outlook: {o_cnt}"
            )
            with st.expander(title, expanded=False):
                if kind == "split_only":
                    st.caption("**Назва запису:** " + prefix)
                else:
                    st.caption("**Префікс назви кампаній:** " + prefix)
                if isinstance(log_rows, list) and log_rows:
                    if kind == "split_only":
                        note = ""
                        if isinstance(log_rows[0], dict):
                            note = str(log_rows[0].get("примітка") or "")
                        st.info(note or "Запис без журналу Instantly API.")
                    else:
                        st.markdown("**Журнал API**")
                        st.dataframe(
                            pd.DataFrame(log_rows),
                            use_container_width=True,
                            height=min(420, 80 + 28 * len(log_rows)),
                        )
                else:
                    if kind == "split_only":
                        st.caption("Запис без журналу Instantly API.")
                    else:
                        st.warning("У записі немає рядків журналу (пошкоджені дані). Можна видалити запис.")
                snap = entry.get("contacts_snapshot")
                if isinstance(snap, dict):
                    cb = snap.get("contacts_by_bucket")
                    um = snap.get("unmatched_rows")
                    scope_lbl = snap.get("provider_scope")
                    total_seg = 0
                    if isinstance(cb, dict):
                        total_seg = sum(len(v) for v in cb.values() if isinstance(v, list))
                    total_um = len(um) if isinstance(um, list) else 0
                    if total_seg or total_um:
                        if str(entry.get("entry_kind") or "instantly_pipeline") == "split_only":
                            st.markdown(
                                "**Контакти** — знімок **обраного** сегмента після розбиття "
                                f"(`provider_scope`: `{scope_lbl}`)."
                            )
                        else:
                            st.markdown(
                                "**Контакти** — повний знімок таблиць для обраної області сегментів "
                                f"на момент запуску API (`provider_scope`: `{scope_lbl}`)."
                            )
                        comb = _combined_contacts_df(cb) if isinstance(cb, dict) else None
                        if comb is not None and len(comb):
                            st.download_button(
                                label="Завантажити всі сегменти одним CSV (колонка _segment)",
                                data=df_to_csv_bytes(comb),
                                file_name=_safe_filename(f"{prefix}_all_segments") + ".csv",
                                mime="text/csv",
                                key=f"crm_allcsv_{eid}",
                            )
                        if isinstance(cb, dict):
                            for si, (seg_name, rows) in enumerate(cb.items()):
                                if not isinstance(rows, list) or not rows:
                                    continue
                                with st.expander(f"{seg_name} — {len(rows)} рядків", expanded=False):
                                    st.dataframe(
                                        pd.DataFrame(rows),
                                        use_container_width=True,
                                        height=min(400, 80 + 24 * min(len(rows), 15)),
                                    )
                                    st.download_button(
                                        label=f"CSV: {seg_name}",
                                        data=df_to_csv_bytes(pd.DataFrame(rows)),
                                        file_name=_safe_filename(f"{prefix}_{seg_name}") + ".csv",
                                        mime="text/csv",
                                        key=f"crm_seg_{eid}_{si}",
                                    )
                        if isinstance(um, list) and um:
                            with st.expander(f"_Unmatched_split — {len(um)} рядків", expanded=False):
                                st.dataframe(
                                    pd.DataFrame(um),
                                    use_container_width=True,
                                    height=min(400, 80 + 24 * min(len(um), 15)),
                                )
                                st.download_button(
                                    label="CSV: unmatched",
                                    data=df_to_csv_bytes(pd.DataFrame(um)),
                                    file_name=_safe_filename(f"{prefix}_Unmatched_split") + ".csv",
                                    mime="text/csv",
                                    key=f"crm_um_{eid}",
                                )
                    else:
                        st.caption("*У цьому записі немає збережених рядків контактів (старий запис або порожні кошики).*")
                else:
                    st.caption("*Знімку контактів немає — запис створено до оновлення або пайплайн без таблиці.*")
                if st.button("Видалити цей запис", key=f"crm_del_{eid}"):
                    rest = [e for e in entries if str(e.get("id")) != eid]
                    ok, err_wr = _save_crm_saved_runs(rest)
                    if ok:
                        st.rerun()
                    else:
                        st.error(err_wr or "Не вдалося оновити файл.")

    st.divider()
    _render_crm_sheet_links_section()


def _render_crm_sheet_links_section() -> None:
    """Завантаження таблиць за посиланнями + зведення з CRM за pipedrive_contact_id ↔ Person - ID."""
    st.markdown("##### Дані за посиланням для зіставлення з CRM")
    st.caption(
        "Таблиця має бути доступна для **перегляду за посиланням** (експорт Google). "
        "У режимі «усі аркуші» спочатку береться один **XLSX-експорт усієї книги** (усі вкладки); "
        "якщо недоступно — спроба gid у HTML /edit і окремі CSV по кожному gid. "
        "Експорт активності зводиться за **pipedrive_contact_id** у CRM та **Person - ID** у таблиці з посилання."
    )

    row_l, row_r = st.columns((5, 7))
    with row_l:
        st.caption("**Режим**")
        st.radio(
            "Режим завантаження таблиць",
            options=[_MATCH_LOAD_MODE_ONE_ALL, _MATCH_LOAD_MODE_MANY_FIRST],
            format_func=lambda x: {
                _MATCH_LOAD_MODE_ONE_ALL: "Одна таблиця — усі аркуші",
                _MATCH_LOAD_MODE_MANY_FIRST: "Багато таблиць — перший аркуш кожної",
            }[x],
            horizontal=False,
            label_visibility="collapsed",
            key="crm_sheet_match_mode",
        )
    with row_r:
        st.caption("**Посилання**")
        st.text_area(
            "Посилання на Google Таблицю (ї)",
            height=96,
            key="crm_sheet_match_urls",
            label_visibility="collapsed",
            placeholder=(
                "Режим 1: один рядок — посилання на одну книгу.\n"
                "Режим 2: кожен рядок — окрема книга (береться лише перший аркуш)."
            ),
        )

    col_ld, _sp = st.columns(2)
    with col_ld:
        load_clicked = st.button(
            "Завантажити дані з посилань",
            type="secondary",
            key="crm_sheet_match_load",
        )

    if load_clicked:
        raw = str(st.session_state.get("crm_sheet_match_urls") or "").strip()
        if not raw:
            st.warning("Вставте посилання.")
        else:
            try:
                mode_now = str(st.session_state.get("crm_sheet_match_mode") or _MATCH_LOAD_MODE_ONE_ALL)
                with st.spinner("Завантаження CSV з Google…"):
                    if mode_now == _MATCH_LOAD_MODE_ONE_ALL:
                        line = raw.splitlines()[0].strip()
                        if not line:
                            raise ValueError("Порожній перший рядок посилання.")
                        pairs = load_google_workbook_all_tabs_csv_frames(line)
                    else:
                        pairs = load_google_many_first_tabs_csv_frames(raw)
                    concat_df = _concat_sheet_frames_with_label(pairs)
                if concat_df.empty:
                    st.session_state.pop("crm_match_sheet_concat_df", None)
                    st.session_state.pop("crm_match_sheet_meta", None)
                    st.session_state.pop("crm_match_activity_export_df", None)
                    st.session_state.pop("crm_match_activity_export_stats", None)
                    st.session_state.pop("crm_match_activity_export_crm_only_no_sheet_df", None)
                    st.error("Не вдалося зібрати жодного рядка з таблиць.")
                else:
                    st.session_state.pop("crm_match_activity_export_df", None)
                    st.session_state.pop("crm_match_activity_export_stats", None)
                    st.session_state.pop("crm_match_activity_export_crm_only_no_sheet_df", None)
                    st.session_state["crm_match_sheet_concat_df"] = concat_df
                    st.session_state["crm_match_sheet_meta"] = ", ".join(
                        f"{lab}:{len(df)}" for lab, df in pairs if df is not None and len(df)
                    )
                    mode_label = (
                        "одна книга, усі знайдені аркуші"
                        if mode_now == _MATCH_LOAD_MODE_ONE_ALL
                        else "кілька книг, перший аркуш кожної"
                    )
                    st.success(f"У памʼять завантажено **{len(concat_df)}** рядків ({mode_label}).")
            except Exception as ex:
                st.session_state.pop("crm_match_sheet_concat_df", None)
                st.session_state.pop("crm_match_sheet_meta", None)
                st.session_state.pop("crm_match_activity_export_df", None)
                st.session_state.pop("crm_match_activity_export_stats", None)
                st.session_state.pop("crm_match_activity_export_crm_only_no_sheet_df", None)
                st.error(str(ex))

    concat_df = st.session_state.get("crm_match_sheet_concat_df")
    meta = st.session_state.get("crm_match_sheet_meta")
    if isinstance(concat_df, pd.DataFrame) and len(concat_df):
        st.caption(f"Таблиця в памʼяті: **{len(concat_df)}** рядків. Джерела: {meta or '—'}")
        with st.expander("Попередній перегляд вантажу з посилань", expanded=False):
            st.dataframe(concat_df.head(40), use_container_width=True)
        st.download_button(
            label="Завантажити CSV вантажу з посилань (_sheet_label)",
            data=df_to_csv_bytes(concat_df),
            file_name="crm_sheet_links_concat.csv",
            mime="text/csv",
            key="crm_sheet_links_concat_dl",
        )

        st.markdown("###### Експорт активності (CRM + таблиця за ID)")
        st.caption(
            "Експорт — **inner join** за ID: у файлі лише контакти, які є **і** в CRM (поле зліва), **і** в таблиці з посилання (**Person - ID**). "
            "Рядки CRM без такого ID у таблиці **не потрапляють** у вихід. Після формування з’явиться розбивка лічильників. "
            "Ідентифікатор контакту в результаті — **Person - ID**. Поля **Person - SDR Responsible**, **Activity - Due date**, "
            "**Activity - Done**, **Activity type**, **Activity assigned to user** — як у шаблоні на початку вкладки CRM. "
            "**Activity - Subject** у колонці експорту — **назва запису** CRM; якщо в назві є **Event**/**Events**, додається "
            "«-» і значення колонки **Activity - Subject** з таблиці (наприклад … Events … `-Advanced Engineering`). "
            "**Activity - ID** та **Person - ID** беруться з таблиці. **Person - TargetEvent** у файлі збирається за правилами "
            "на вкладці CRM (колонки таблиці + опційне дописування **повного** тексту колонки експорту **Activity - Subject** через кому та довільний суфікс)."
        )
        lc1, lc2 = st.columns(2)
        with lc1:
            st.text_input(
                "Колонка ID у даних CRM",
                value="pipedrive_contact_id",
                key="crm_export_crm_id_col",
                help="Зазвичай pipedrive_contact_id у CSV контактів у збережених записах CRM.",
            )
        with lc2:
            st.text_input(
                "Колонка Person - ID у таблиці з посилання",
                value="Person - ID",
                key="crm_export_sheet_id_col",
            )

        if st.button(
            "Сформувати експорт активності (CRM + таблиця)",
            type="primary",
            key="crm_build_activity_export",
        ):
            tmpl = _crm_template_effective_values()
            entries_now = _load_crm_saved_runs()
            cc = str(st.session_state.get("crm_export_crm_id_col") or "").strip()
            sc = str(st.session_state.get("crm_export_sheet_id_col") or "").strip()
            df_exp, err_exp, stats_exp, df_crm_no_sheet = build_crm_activity_export_df(
                concat_df,
                entries_now,
                sheet_id_col=sc,
                crm_id_col=cc,
                template_vals=tmpl,
            )
            if stats_exp is not None:
                st.session_state["crm_match_activity_export_stats"] = stats_exp
            else:
                st.session_state.pop("crm_match_activity_export_stats", None)
            if df_crm_no_sheet is not None and not df_crm_no_sheet.empty:
                st.session_state["crm_match_activity_export_crm_only_no_sheet_df"] = df_crm_no_sheet
            else:
                st.session_state.pop("crm_match_activity_export_crm_only_no_sheet_df", None)
            if err_exp:
                st.session_state.pop("crm_match_activity_export_df", None)
                st.error(err_exp)
            else:
                st.session_state["crm_match_activity_export_df"] = df_exp
                st.success(f"Зібрано рядків для експорту: **{len(df_exp)}**.")

        stats_sess = st.session_state.get("crm_match_activity_export_stats")
        if isinstance(stats_sess, dict):
            st.info(_crm_activity_export_stats_caption(stats_sess))
        nom_df = st.session_state.get("crm_match_activity_export_crm_only_no_sheet_df")
        if isinstance(nom_df, pd.DataFrame) and len(nom_df):
            with st.expander(
                f"CRM без збігу в таблиці ({len(nom_df)} рядків) — для перевірки",
                expanded=False,
            ):
                st.caption(
                    "Рядки з CRM із заповненим полем ID для злиття, для яких **немає** такого **Person - ID** "
                    "у завантаженій таблиці з посилання."
                )
                st.dataframe(nom_df.head(80), use_container_width=True, height=min(400, 80 + 22 * min(len(nom_df), 25)))
            st.download_button(
                label=f"Завантажити CSV: CRM без збігу в таблиці ({len(nom_df)})",
                data=df_to_csv_bytes(nom_df),
                file_name="crm_activity_export_crm_not_in_sheet.csv",
                mime="text/csv",
                key="crm_activity_export_crm_nomatch_dl",
            )
    exp_df = st.session_state.get("crm_match_activity_export_df")
    if isinstance(exp_df, pd.DataFrame) and len(exp_df):
        st.dataframe(
            exp_df.head(100),
            use_container_width=True,
            height=min(480, 80 + 22 * min(len(exp_df), 35)),
        )
        st.download_button(
            label="Завантажити CSV експорту активності",
            data=df_to_csv_bytes(exp_df),
            file_name="crm_activity_export.csv",
            mime="text/csv",
            key="crm_activity_export_dl",
        )


def _render_split_table_api_tab() -> None:
    """Вміст вкладки «Instantly»."""
    st.caption(
        "Спочатку завантажте таблицю. **Повний маппінг колонок** (вбудований + ваші правила) — у згорнутому блоці "
        "«Маппінг колонок CSV → Instantly: повний перелік + ваші доповнення» нижче. Далі — Instantly API: дублікати, "
        "акаунти, bulk leads. Без автозапуску кампаній."
    )

    if "loaded_df" not in st.session_state:
        st.session_state.loaded_df = None
    if "loaded_src" not in st.session_state:
        st.session_state.loaded_src = None

    src = st.radio("Джерело даних", ["Посилання на Google Таблицю", "Файл CSV на комп’ютері"], horizontal=True)

    df: pd.DataFrame | None = None
    err: str | None = None

    if src == "Посилання на Google Таблицю":
        st.caption(
            "Завантаження йде через **публічний CSV-експорт** Google. Таблиця має бути доступна за посиланням "
            "(хоча б для перегляду), інакше використайте **Файл CSV на комп’ютері**."
        )
        url = st.text_input(
            "URL або ID таблиці",
            placeholder="https://docs.google.com/spreadsheets/d/.../edit#gid=0",
            help="Якщо в URL є gid= — буде взято потрібний лист, інакше — перший аркуш.",
        )
        if st.button("Завантажити й розбити", type="primary"):
            if not (url or "").strip():
                st.warning("Вставте посилання або ID.")
            else:
                try:
                    st.session_state.loaded_df = load_sheet_from_url(url.strip())
                    st.session_state.loaded_src = "url"
                    st.session_state.last_sheet_url = url.strip()
                except Exception as e:
                    err = str(e)
        if st.session_state.loaded_src == "url":
            df = st.session_state.loaded_df
    else:
        up = st.file_uploader("CSV (UTF-8)", type=["csv"])
        if up is not None:
            try:
                st.session_state.loaded_df = load_csv_from_bytes(up.getvalue())
                st.session_state.loaded_src = "csv"
            except Exception as e:
                err = str(e)
        else:
            if st.session_state.loaded_src == "csv":
                st.session_state.loaded_df = None
                st.session_state.loaded_src = None
        if st.session_state.loaded_src == "csv":
            df = st.session_state.loaded_df

    st.divider()
    col_a, col_b = st.columns(2)
    with col_a:
        provider_col = st.text_input("Стовпець провайдера", value=DEFAULT_PROVIDER_COLUMN)
    with col_b:
        location_col = st.text_input(
            "Стовпець країни (опційно)",
            value="",
            help="Порожньо — авто: колонки з «country» у заголовку, інакше скан рядка.",
        )

    _render_import_cv_aliases_panel()

    if err:
        st.error(err)

    if df is None or df.empty:
        if df is not None and df.empty:
            st.warning("Таблиця порожня.")
        return

    st.success(f"Завантажено рядків даних: {len(df)}")

    with st.expander("Маппінг колонок CSV → Instantly (як на ваших скрінах)"):
        comb = {**imap.SCREEN1_IMPORT_TYPE_BY_COLUMN, **imap.SCREEN2_IMPORT_TYPE_BY_COLUMN}
        st.dataframe(
            pd.DataFrame(
                [
                    {"Колонка": k, "Тип / поле API": v}
                    for k, v in sorted(comb.items(), key=lambda x: str(x[0]).casefold())
                ]
            ),
            use_container_width=True,
        )
        st.caption(
            "Колонка **Country** — як на скріні: тип **Location** → у API в `custom_variables` "
            "записується **Country** і **location** (для плейсхолдерів залежно від того, як названо змінну в кампанії). "
            "Колонки **email_01_body**, **email_02_body** … при відправці через API перейменовуються у **email_01**, **email_02** "
            "у `custom_variables`, щоб збігалось із плейсхолдерами в листі. **Events**: **full_email_1** тощо → **email_01**…; "
            "**subject** → **subject_line**. З генератора: **email_generation_json_email_01_body** … **email_generation_json_subject_line** "
            "→ ті самі **email_01**… та **subject_line**."
        )

    try:
        buckets, unmatched = split_dataframe(
            df,
            provider_column=provider_col.strip(),
            location_column=location_col.strip(),
        )
    except Exception as e:
        st.error(str(e))
        return

    scope_opt = st.radio(
        "Далі працювати з сегментами",
        ["all", "gmail", "outlook"],
        format_func=lambda x: {
            "all": "Все (Gmail і Outlook)",
            "gmail": "Gmail",
            "outlook": "Outlook",
        }[x],
        horizontal=True,
        key="provider_export_scope",
        help="Експорт ZIP/Excel та Instantly API — лише для обраних сегментів; "
        "«Все» включає всі чотири кошики.",
    )
    work_buckets = buckets_for_provider_scope(buckets, scope_opt)

    lines = summary_lines(work_buckets, unmatched)
    st.subheader("Підсумок")
    st.text("\n".join(lines))

    st.download_button(
        "Завантажити пакет ZIP (окремі CSV для обраної області)",
        data=build_split_export_zip(df, work_buckets, unmatched),
        file_name="instantly_split_export.zip",
        mime="application/zip",
        key="dl_zip_all",
        help="Для ручного імпорту в Instantly або Google: у ZIP окремі .csv по кожному сегменту.",
    )
    try:
        _xlsx = build_split_export_xlsx(df, work_buckets, unmatched)
        _xlsx_err = None
    except Exception as e:
        _xlsx = None
        _xlsx_err = str(e)
    if _xlsx is not None:
        st.download_button(
            "Завантажити Excel (.xlsx, вкладки для обраної області)",
            data=_xlsx,
            file_name="instantly_split_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_xlsx_all",
            help="Зручно для Google / перегляду. Для ручного імпорту в Instantly краще ZIP або CSV з вкладок.",
        )
    elif _xlsx_err:
        st.warning(f"Excel недоступний: {_xlsx_err}")

    st.caption(
        "**Instantly:** якщо ви натискаєте тут «Виконати API» — ліди йдуть з пам’яті додатку, окремий файл не потрібен. "
        "Якщо завантажуєте ліди в Інстантлі **вручну**, беріть **CSV** (кнопки по вкладках або ZIP). З Excel: відкрийте потрібну вкладку "
        "і **Файл → Зберегти як → CSV** (рідко знадобиться, якщо вже є ZIP)."
    )

    names = list(work_buckets.keys())
    tabs = st.tabs(names + (["_Unmatched_split"] if unmatched is not None and len(unmatched) else []))

    for i, name in enumerate(names):
        with tabs[i]:
            st.dataframe(work_buckets[name], use_container_width=True, height=400)
            st.download_button(
                label=f"Завантажити «{name}».csv",
                data=df_to_csv_bytes(work_buckets[name]),
                file_name=_safe_filename(name) + ".csv",
                mime="text/csv",
                key=f"dl_{name}",
            )

    if unmatched is not None and len(unmatched):
        with tabs[len(names)]:
            st.dataframe(unmatched, use_container_width=True, height=400)
            st.download_button(
                label="Завантажити _Unmatched_split.csv",
                data=df_to_csv_bytes(unmatched),
                file_name="_Unmatched_split.csv",
                mime="text/csv",
                key="dl_unmatched",
            )

    seg_choices_for_crm: list[str] = []
    if scope_opt in ("all", "gmail") and any(
        work_buckets.get(k) is not None and not work_buckets[k].empty
        for k in ORDERED_GMAIL_BUCKETS
    ):
        seg_choices_for_crm.append(_CRM_SPLIT_COMBINED_GMAIL_KEY)
    if scope_opt in ("all", "outlook") and any(
        work_buckets.get(k) is not None and not work_buckets[k].empty
        for k in ORDERED_OUTLOOK_BUCKETS
    ):
        seg_choices_for_crm.append(_CRM_SPLIT_COMBINED_OUTLOOK_KEY)
    for _nm in names:
        _bdf = work_buckets.get(_nm)
        if _bdf is not None and not _bdf.empty:
            seg_choices_for_crm.append(_nm)
    if unmatched is not None and len(unmatched):
        seg_choices_for_crm.append("_Unmatched_split")
    if (
        _CRM_SPLIT_COMBINED_GMAIL_KEY in seg_choices_for_crm
        and _CRM_SPLIT_COMBINED_OUTLOOK_KEY in seg_choices_for_crm
    ):
        seg_choices_for_crm.insert(0, _CRM_SPLIT_COMBINED_ALL_KEY)
    if seg_choices_for_crm:
        st.divider()
        st.subheader("CRM після розбиття (без Instantly API)")
        st.caption(
            "Якщо повний цикл з API не потрібен: оберіть **один регіон** або **увесь провайдер (USA + Europe)** для Gmail/Outlook, "
            "задайте **назву запису** (як підпис у вкладці CRM) і збережіть рядки в **`crm_saved_runs.json`** — той самий файл, що й для журналу API."
        )

        def _fmt_crm_split_seg_pick(opt: str) -> str:
            if opt == _CRM_SPLIT_COMBINED_ALL_KEY:
                return "Все (Gmail і Outlook)"
            if opt == _CRM_SPLIT_COMBINED_GMAIL_KEY:
                return "Gmail / Other — USA + Europe (увесь провайдер)"
            if opt == _CRM_SPLIT_COMBINED_OUTLOOK_KEY:
                return "Outlook — USA + Europe (увесь провайдер)"
            if opt == "_Unmatched_split":
                return "_Unmatched_split (без провайдера/регіону)"
            return opt

        crm_pick_seg = st.selectbox(
            "Сегмент для збереження в CRM",
            seg_choices_for_crm,
            format_func=_fmt_crm_split_seg_pick,
            key="crm_split_segment_choice",
        )
        crm_split_name = st.text_input(
            "Назва запису в CRM",
            key="crm_split_entry_display_name",
            placeholder="наприклад: Softdev USA gmail — експорт 2026-05-05",
        )
        if st.button("Додати обраний сегмент у CRM", type="secondary", key="btn_crm_add_split_segment"):
            label_stripped = str(crm_split_name or "").strip()
            if not label_stripped:
                st.warning("Введіть назву запису.")
            else:
                df_save: pd.DataFrame | None = None
                snap: dict[str, Any] | None = None
                seg_label_for_msg = _fmt_crm_split_seg_pick(crm_pick_seg)
                if crm_pick_seg == "_Unmatched_split":
                    df_save = unmatched
                    if df_save is not None and not df_save.empty:
                        snap = _contacts_snapshot_single_bucket(crm_pick_seg, df_save, scope_opt)
                elif crm_pick_seg == _CRM_SPLIT_COMBINED_ALL_KEY:
                    subset = {}
                    for k in ORDERED_GMAIL_BUCKETS:
                        if (
                            k in work_buckets
                            and work_buckets[k] is not None
                            and not work_buckets[k].empty
                        ):
                            subset[k] = work_buckets[k]
                    for k in ORDERED_OUTLOOK_BUCKETS:
                        if (
                            k in work_buckets
                            and work_buckets[k] is not None
                            and not work_buckets[k].empty
                        ):
                            subset[k] = work_buckets[k]
                    if subset:
                        df_save = pd.concat(list(subset.values()), ignore_index=True)
                        snap = _contacts_snapshot_split_only_multi_buckets(subset, scope_opt)
                elif crm_pick_seg == _CRM_SPLIT_COMBINED_GMAIL_KEY:
                    subset = {
                        k: work_buckets[k]
                        for k in ORDERED_GMAIL_BUCKETS
                        if k in work_buckets
                        and work_buckets[k] is not None
                        and not work_buckets[k].empty
                    }
                    if subset:
                        df_save = pd.concat(list(subset.values()), ignore_index=True)
                        snap = _contacts_snapshot_split_only_multi_buckets(subset, scope_opt)
                elif crm_pick_seg == _CRM_SPLIT_COMBINED_OUTLOOK_KEY:
                    subset = {
                        k: work_buckets[k]
                        for k in ORDERED_OUTLOOK_BUCKETS
                        if k in work_buckets
                        and work_buckets[k] is not None
                        and not work_buckets[k].empty
                    }
                    if subset:
                        df_save = pd.concat(list(subset.values()), ignore_index=True)
                        snap = _contacts_snapshot_split_only_multi_buckets(subset, scope_opt)
                else:
                    df_save = work_buckets.get(crm_pick_seg)
                    if df_save is not None and not df_save.empty:
                        snap = _contacts_snapshot_single_bucket(crm_pick_seg, df_save, scope_opt)

                if df_save is None or df_save.empty or snap is None:
                    st.warning("Обраний сегмент порожній.")
                else:
                    ok_wr, err_msg = _append_crm_saved_run(
                        label_stripped,
                        None,
                        contacts_snapshot=snap,
                        entry_kind="split_only",
                    )
                    if ok_wr:
                        st.success(
                            f"Збережено у CRM ({_CRM_SAVED_RUNS_PATH.name}) під назвою {label_stripped!r} "
                            f"(джерело: {seg_label_for_msg}). Перегляньте вкладку **CRM**."
                        )
                    else:
                        st.error(err_msg or "Не вдалося записати файл.")

    st.divider()
    st.subheader("Instantly API: дублікати, акаунти, ліди")
    _gmail_pool = str(st.session_state.get("instantly_pool_gmail") or "")
    _outlook_pool = str(st.session_state.get("instantly_pool_outlook") or "")
    if _gmail_pool.strip() or _outlook_pool.strip():
        pc1, pc2 = st.columns(2)
        with pc1:
            st.markdown("**Gmail / Other** (USA gmail, EU gmail)")
            st.markdown(_pool_preview_caption(_gmail_pool))
        with pc2:
            st.markdown("**Outlook** (USA outlk, EU outlk)")
            st.markdown(_pool_preview_caption(_outlook_pool))
    else:
        st.warning(
            "У сайдбарі **порожні обидва** поля відправників Gmail і Outlook — для API це «скинути відправників» "
            "у всіх сегментах. Instantly часто не додає лідів або дає 0. Введіть тег або email хоча б для одного провайдера."
        )
    st.caption(
        "Для кожного **непорожнього** сегмента: у списку кампаній (пошук як у Instantly) "
        "шукається шаблон, у назві якого є і ваш текст, і маркер (**USA gmail**, **EU gmail**, "
        "**USA outlk**, **EU outlk**). Потім Duplicate → відправники з сайдбару за провайдером (або "
        "`instantly_account_tags.py` для окремого сегмента) → імпорт лидів. **Activate не викликається.** "
        "Той самий тег додатково **кріпиться до кампанії** (бейдж у таблиці) через `toggle-resource` — це не те саме, що поле `email_tag_list`."
    )
    c1, c2 = st.columns(2)
    with c1:
        template_q = st.text_input(
            "Пошук шаблону (фрагмент назви)",
            key="instantly_template_search",
            placeholder="наприклад: Softdev",
        )
    with c2:
        name_prefix = st.text_input(
            "Префікс назви нових кампаній",
            key="instantly_new_name_prefix",
            placeholder="наприклад: 2026-05-01 Softdev —",
            help="До префікса автоматично додається пробіл і маркер сегмента (USA gmail тощо).",
        )
    if st.button("Виконати API: дублікати + акаунти + ліди", type="primary", key="btn_instantly_pipeline"):
        _persist_instantly_api_key_session_storage(
            str(st.session_state.get("instantly_api_key_input") or "")
        )
        api_k = get_instantly_api_key()
        if not api_k:
            st.error("Додайте API ключ Instantly у сайдбарі.")
        elif not str(template_q or "").strip() or not str(name_prefix or "").strip():
            st.warning("Заповніть пошук шаблону та префікс назви.")
        else:
            with st.spinner("Запити до api.instantly.ai…"):
                try:
                    ok_disk, err_disk = _persist_import_cv_aliases_to_disk()
                    if not ok_disk and err_disk:
                        st.warning(
                            f"Список аліасів не вдалося записати на диск: {err_disk}. "
                            "Пайплайн виконується; після перезапуску app дописи можуть не відновитись."
                        )
                    cli = InstantlyClient(api_k)
                    alias_dict, _alias_parse_notes, _ = imap.parse_user_cv_alias_text(
                        str(st.session_state.get("import_cv_aliases_text") or "")
                    )
                    log = run_full_pipeline(
                        cli,
                        work_buckets,
                        template_search=str(template_q).strip(),
                        new_name_prefix=str(name_prefix).strip(),
                        pool_gmail_raw=str(
                            st.session_state.get("instantly_pool_gmail") or ""
                        ),
                        pool_outlook_raw=str(
                            st.session_state.get("instantly_pool_outlook") or ""
                        ),
                        user_cv_aliases=alias_dict or None,
                    )
                    st.session_state["instantly_pipeline_log"] = log
                    st.session_state["instantly_crm_contacts_snapshot"] = (
                        _contacts_snapshot_from_buckets(
                            work_buckets, unmatched, scope_opt
                        )
                    )
                except Exception as ex:
                    st.session_state.pop("instantly_pipeline_log", None)
                    st.session_state.pop("instantly_crm_contacts_snapshot", None)
                    st.error(str(ex))
    if st.session_state.get("instantly_pipeline_log"):
        st.subheader("Журнал виконання")
        st.dataframe(
            pd.DataFrame(st.session_state["instantly_pipeline_log"]),
            use_container_width=True,
            height=min(400, 80 + 28 * len(st.session_state["instantly_pipeline_log"])),
        )
        st.caption(
            "Запис у CRM прив’язується до поля **«Префікс назви нових кампаній»** вище (той самий текст, "
            "що використовувався для запуску API). Зберігається журнал API і **усі рядки контактів** для обраної області "
            "сегментів на момент цього запуску (по кошиках + unmatched). Можна зберігати багато разів — кожне натискання додає окремий запис."
        )
        if st.button("Додати журнал у CRM", type="secondary", key="btn_add_pipeline_log_to_crm"):
            prefix = str(st.session_state.get("instantly_new_name_prefix") or "").strip()
            raw_log = st.session_state.get("instantly_pipeline_log")
            snap = st.session_state.get("instantly_crm_contacts_snapshot")
            contacts_snap = snap if isinstance(snap, dict) else None
            if not isinstance(raw_log, list) or not raw_log:
                st.warning("Немає даних журналу для збереження.")
            elif not prefix:
                st.warning(
                    "Заповніть **«Префікс назви нових кампаній»** — за цим текстом запис ідентифікується у вкладці CRM."
                )
            else:
                ok, err_wr = _append_crm_saved_run(
                    prefix, raw_log, contacts_snapshot=contacts_snap
                )
                if ok:
                    st.success(f"Збережено у CRM ({_CRM_SAVED_RUNS_PATH.name}) під префіксом: {prefix!r}.")
                else:
                    st.error(err_wr or "Не вдалося записати файл.")


def main() -> None:
    st.set_page_config(page_title="SDR Platform", layout="wide")
    _ensure_import_cv_aliases_text_state()
    if st.session_state.pop(_PENDING_CLEAR_SENDER_POOLS_SIDEBAR, False):
        _write_empty_sender_pools_sidebar_file()
        st.session_state.pop("instantly_pool_gmail", None)
        st.session_state.pop("instantly_pool_outlook", None)
    _ensure_sender_pools_widget_state()
    if st.session_state.pop(_CLEAR_INSTANTLY_API_KEY_FLAG, False):
        _persist_instantly_api_key_session_storage("")
        st.session_state[_USER_CLEARED_INSTANTLY_KEY] = True
        st.session_state.instantly_api_key_input = ""
    _ensure_instantly_api_key_widget_state()
    _hydrate_instantly_api_key_session_storage()
    if not str(st.session_state.get("instantly_api_key_input") or "").strip():
        if not st.session_state.get(_USER_CLEARED_INSTANTLY_KEY):
            boot = _disk_instantly_api_key_bootstrap()
            if boot:
                st.session_state.instantly_api_key_input = boot
                _persist_instantly_api_key_session_storage(boot)
    if st.session_state.pop(_CLEAR_PIPEDRIVE_API_KEY_FLAG, False):
        _persist_pipedrive_api_key_session_storage("")
        st.session_state[_USER_CLEARED_PIPEDRIVE_KEY] = True
        st.session_state.pipedrive_api_token_input = ""
    _ensure_pipedrive_api_key_widget_state()
    _hydrate_pipedrive_api_key_session_storage()
    if not str(st.session_state.get("pipedrive_api_token_input") or "").strip():
        if not st.session_state.get(_USER_CLEARED_PIPEDRIVE_KEY):
            bpd = _disk_pipedrive_api_key_bootstrap()
            if bpd:
                st.session_state.pipedrive_api_token_input = bpd
                _persist_pipedrive_api_key_session_storage(bpd)
    try:
        import streamlit_js_eval  # noqa: F401
    except ImportError:
        if not st.session_state.get("_warned_js_eval_missing"):
            st.session_state._warned_js_eval_missing = True
            st.sidebar.caption(
                "Щоб ключі Instantly / Pipedrive зберігались після F5 у цій вкладці: `pip install streamlit-js-eval` "
                "або `pip install -r requirements.txt`."
            )
    with st.sidebar:
        st.subheader("Instantly API")
        st.text_input(
            "Ключ API (v2)",
            type="password",
            key="instantly_api_key_input",
            autocomplete="off",
            on_change=_on_instantly_api_key_commit,
            help="Зберігається в браузері (**sessionStorage**) для цієї вкладки: не треба вводити щоразу після "
            "оновлення сторінки. Після **закриття вкладки** введіть знову або скористайтеся «Зберегти у файл». "
            "Пакет: **streamlit-js-eval** (pip install -r requirements.txt).",
        )
        api_key = get_instantly_api_key()
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Зберегти у файл", help="Запис у .streamlit/secrets.toml на цьому ПК"):
                raw = str(st.session_state.get("instantly_api_key_input") or "").strip()
                if not raw:
                    st.warning("Спочатку введіть ключ у поле вище.")
                else:
                    try:
                        path = persist_instantly_key_to_secrets_toml(raw)
                        _persist_instantly_api_key_session_storage(raw)
                        st.success(f"Збережено: {path.name}")
                    except Exception as e:
                        st.error(str(e))
        with c2:
            if st.button("Очистити поле"):
                st.session_state[_CLEAR_INSTANTLY_API_KEY_FLAG] = True
                st.rerun()
        if api_key:
            if str(st.session_state.get("instantly_api_key_input") or "").strip():
                cap = "поле вводу"
            elif (os.environ.get("INSTANTLY_API_KEY") or "").strip():
                cap = "змінна оточення"
            else:
                cap = "secrets.toml"
            st.caption(f"Ключ активний (джерело: {cap}).")
        else:
            st.caption("Ключ не задано — введіть вище або secrets.toml / INSTANTLY_API_KEY.")
        st.divider()
        st.subheader("Pipedrive API")
        st.caption(
            "**Linked Helper²** — токен як у Instantly (sessionStorage, secrets). "
            "[Документація Pipedrive](https://developers.pipedrive.com/)."
        )
        st.text_input(
            "Токен Pipedrive",
            type="password",
            key="pipedrive_api_token_input",
            autocomplete="off",
            on_change=_on_pipedrive_api_key_commit,
            help="Після введення зберігається в sessionStorage (streamlit-js-eval). "
            "«Зберегти у файл» записує PIPEDRIVE_API_TOKEN у .streamlit/secrets.toml.",
        )
        pd_key = get_pipedrive_api_key()
        pdc1, pdc2 = st.columns(2)
        with pdc1:
            if st.button(
                "Зберегти у файл",
                key="sidebar_pipedrive_save_secrets",
                help="PIPEDRIVE_API_TOKEN у .streamlit/secrets.toml",
            ):
                raw_pd = str(st.session_state.get("pipedrive_api_token_input") or "").strip()
                if not raw_pd:
                    st.warning("Спочатку введіть токен у поле вище.")
                else:
                    try:
                        path_pd = persist_pipedrive_key_to_secrets_toml(raw_pd)
                        _persist_pipedrive_api_key_session_storage(raw_pd)
                        st.success(f"Збережено: {path_pd.name}")
                    except Exception as e:
                        st.error(str(e))
        with pdc2:
            if st.button("Очистити поле", key="sidebar_pipedrive_clear_token"):
                st.session_state[_CLEAR_PIPEDRIVE_API_KEY_FLAG] = True
                st.rerun()
        if pd_key:
            if str(st.session_state.get("pipedrive_api_token_input") or "").strip():
                pd_cap = "поле вводу"
            elif (os.environ.get("PIPEDRIVE_API_TOKEN") or os.environ.get("PIPEDRIVE_API_KEY") or "").strip():
                pd_cap = "змінна оточення"
            else:
                pd_cap = "secrets.toml"
            st.caption(f"Активно (джерело: {pd_cap}).")
        else:
            st.caption("Не задано — фільтр Pipedrive на Linked Helper пропускається.")
        st.divider()
        st.subheader("Відправники за провайдером")
        st.caption(
            "**Gmail/Other** і **Outlook** для кошиків Instantly. Рядок **з @** — акаунт, **без @** — label тега. "
            f"Файл: `{_SENDER_POOLS_SIDEBAR_PATH.name}`."
        )
        st.text_area(
            "Gmail / Other",
            key="instantly_pool_gmail",
            height=64,
            label_visibility="visible",
            placeholder="gmail_gtm_group_05 або sender@gmail.com",
            help="USA gmail + EU gmail. Тонке USA/EU: instantly_account_tags.py (ACCOUNT_TAG_LABEL_BY_BUCKET).",
        )
        st.text_area(
            "Outlook",
            key="instantly_pool_outlook",
            height=64,
            label_visibility="visible",
            placeholder="outlook_hs_group_02 або sender@outlk.com",
            help="USA outlk + EU outlk.",
        )
        if st.button(
            "Очистити відправників + файл",
            key="btn_clear_sender_pools_sidebar",
            help=f"Очистить поля та {_SENDER_POOLS_SIDEBAR_PATH.name}.",
        ):
            st.session_state[_PENDING_CLEAR_SENDER_POOLS_SIDEBAR] = True
            st.rerun()
        _persist_sender_pools_sidebar_from_session()
        with st.expander("ℹ️ Деплой і повні пояснення", expanded=False):
            st.markdown(
                "**Хмарний Streamlit:** надійніше вказати ключі в **Settings → Secrets** "
                "(`INSTANTLY_API_KEY`, `PIPEDRIVE_API_TOKEN`), ніж комітити secrets.toml.\n\n"
                "**Instantly API ключ:** sessionStorage (F5 у межах вкладки); локально — «Зберегти у файл» у secrets.toml. "
                "Потрібен пакет **streamlit-js-eval** (`pip install -r requirements.txt`).\n\n"
                "**Відправники (детально):** два поля — **Gmail/Other** для сегментів *USA gmail* і *EU gmail*; "
                "**Outlook** — для *USA outlk* і *EU outlk*. У кожному: **@** = конкретний акаунт, **без @** = label тега (як multiselect). "
                f"Після оновлення сторінки підтягується **`{_SENDER_POOLS_SIDEBAR_PATH.name}`** на цьому ПК. "
                "Щоб прибрати збереження — кнопка «Очистити відправників» або вручну видалити файл."
            )
    st.title("SDR Platform")
    _render_header_status_strip()
    tab_split, tab_linked_helper, tab_crm = st.tabs(["Instantly", "LinkedHelper²", "CRM"])
    with tab_split:
        _render_split_table_api_tab()
    with tab_linked_helper:
        _render_linked_helper_tab()
    with tab_crm:
        st.subheader("CRM")
        _render_crm_tab()


if __name__ == "__main__":
    import subprocess
    import sys
    from pathlib import Path

    from streamlit.runtime.scriptrunner_utils.script_run_context import (
        get_script_run_ctx,
    )

    # IDE часто запускає `python app.py` — без `streamlit run` немає сесії й з’являються попередження.
    if get_script_run_ctx(suppress_warning=True) is not None:
        main()
    else:
        here = Path(__file__).resolve()
        raise SystemExit(
            subprocess.call(
                [sys.executable, "-m", "streamlit", "run", str(here), *sys.argv[1:]]
            )
        )
