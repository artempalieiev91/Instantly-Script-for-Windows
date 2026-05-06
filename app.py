"""
Streamlit: розбиття таблиці за логікою Instantly (провайдер × USA/Europe).
"""

from __future__ import annotations

import html
import io
import json
import os
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
from split_engine import (
    DEFAULT_PROVIDER_COLUMN,
    discover_google_sheet_gids,
    extract_gid_from_url,
    extract_spreadsheet_id,
    google_sheet_csv_export_url,
    load_spreadsheet_all_tabs_via_xlsx_export,
    split_dataframe,
    summary_lines,
)

_IMPORT_CV_ALIASES_PATH = Path(__file__).resolve().parent / imap.USER_IMPORT_CV_ALIASES_FILENAME
_SENDER_POOLS_SIDEBAR_PATH = Path(__file__).resolve().parent / "instantly_sender_pools_sidebar.json"
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
    """Багато таблиць: по першому аркушу кожної (ігнорується gid у посиланні)."""
    lines = [ln.strip() for ln in lines_block.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("Додайте хоча б один рядок із посиланням або ID таблиці.")
    out: list[tuple[str, pd.DataFrame]] = []
    for ln in lines:
        sid = extract_spreadsheet_id(ln)
        df = load_sheet_from_url(_spreadsheet_edit_url_first_sheet(ln))
        out.append((sid[:14] + "…", df))
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


_CLEAR_INSTANTLY_API_KEY_FLAG = "_request_clear_instantly_api_key"
_USER_CLEARED_INSTANTLY_KEY = "_instantly_api_key_cleared_by_user"
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
    """Вміст вкладки «Розбиття таблиці + API»."""
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
    if seg_choices_for_crm:
        st.divider()
        st.subheader("CRM після розбиття (без Instantly API)")
        st.caption(
            "Якщо повний цикл з API не потрібен: оберіть **один регіон** або **увесь провайдер (USA + Europe)** для Gmail/Outlook, "
            "задайте **назву запису** (як підпис у вкладці CRM) і збережіть рядки в **`crm_saved_runs.json`** — той самий файл, що й для журналу API."
        )

        def _fmt_crm_split_seg_pick(opt: str) -> str:
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
    st.set_page_config(page_title="Instantly SDR", layout="wide")
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
    try:
        import streamlit_js_eval  # noqa: F401
    except ImportError:
        if not st.session_state.get("_warned_js_eval_missing"):
            st.session_state._warned_js_eval_missing = True
            st.sidebar.caption(
                "Щоб ключ зберігався після F5 у цій вкладці: `pip install streamlit-js-eval` "
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
        st.caption(
            "Хмарний Streamlit: ключ надійніше ввести в **Settings → Secrets**, не в файл."
        )
        st.divider()
        st.subheader("Відправники за провайдером")
        st.caption(
            "Два поля замість одного: **Gmail/Other** — для сегментів *USA gmail* і *EU gmail*; **Outlook** — для *USA outlk* і *EU outlk*. "
            "У кожному: рядки з **@** = конкретні акаунти, **без @** = label тега (як у multiselect). "
            "Необов’язкове тонке регулювання USA/EU — у файлі **`instantly_account_tags.py`** (`ACCOUNT_TAG_LABEL_BY_BUCKET`). "
            f"Після кожного оновлення сторінки значення підтягуються з **`{_SENDER_POOLS_SIDEBAR_PATH.name}`** на цьому ПК; "
            "щоб прибрати збереження — очистіть поля або натисніть кнопку нижче."
        )
        st.text_area(
            "Gmail / Other — USA + Europe",
            key="instantly_pool_gmail",
            height=88,
            placeholder="напр. gmail_gtm_group_05 або sender@gmail.com",
            help="Застосовується до обох кошиків «Other (gmail, etc) USA|Europe», якщо там немає override у instantly_account_tags.py.",
        )
        st.text_area(
            "Outlook — USA + Europe",
            key="instantly_pool_outlook",
            height=88,
            placeholder="напр. outlook_hs_group_02 або sender@outlook.com",
            help="Застосовується до «outlook USA» та «outlook Europe», якщо немає override у instantly_account_tags.py.",
        )
        if st.button(
            "Очистити поля відправників і файл збереження",
            key="btn_clear_sender_pools_sidebar",
            help=f"Очистить обидва поля та перезапише {_SENDER_POOLS_SIDEBAR_PATH.name} порожніми рядками.",
        ):
            st.session_state[_PENDING_CLEAR_SENDER_POOLS_SIDEBAR] = True
            st.rerun()
        _persist_sender_pools_sidebar_from_session()
    st.title("Instantly SDR")
    _render_header_status_strip()
    tab_split, tab_crm = st.tabs(["Розбиття таблиці + API", "CRM"])
    with tab_split:
        _render_split_table_api_tab()
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
