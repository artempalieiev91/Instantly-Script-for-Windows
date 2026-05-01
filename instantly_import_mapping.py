"""
Маппінг колонок CSV → тип поля Instantly при імпорті лидів (UI «Column Name / Select Type»).

Скрін 1–3 узгоджені з користувачем. Значення API — див. bulk add leads.
"""

from __future__ import annotations

import re

SCREEN1_IMPORT_TYPE_BY_COLUMN: dict[str, str] = {
    "pipedrive_contact_id": "custom_variable",
    "First Name": "first_name",
    "Last Name": "last_name",
    "Title": "job_title",
    "Company Name": "company_name",
    "Company Name for Emails": "company_name",
    "Email": "email",
    "Website": "website",
    "City": "do_not_import",
    "State": "do_not_import",
    # Скрін: колонка «Country» → тип у UI «Location» (пін на карті).
    "Country": "location_field",
}

SCREEN2_IMPORT_TYPE_BY_COLUMN: dict[str, str] = {
    "subject_line": "custom_variable",
    # CSV Events: тема в колонці subject → той самий ключ, що subject_line.
    "subject": "custom_variable",
    "email_01": "custom_variable",
    "email_02": "custom_variable",
    "email_03": "custom_variable",
    # У джерелі часто *_body, у шаблоні Instantly — {{email_01}} тощо.
    "email_01_body": "custom_variable",
    "email_02_body": "custom_variable",
    "email_03_body": "custom_variable",
}

# Заголовок CSV → ім’я ключа в API custom_variables (плейсхолдер у листі).
CUSTOM_VARIABLE_API_KEY_BY_COLUMN: dict[str, str] = {
    "subject": "subject_line",
    "email_01_body": "email_01",
    "email_02_body": "email_02",
    "email_03_body": "email_03",
}

BULK_IMPORT_VERIFY_LEADS_ON_IMPORT: bool = False
BULK_IMPORT_SKIP_IF_IN_CAMPAIGN: bool = False
BULK_IMPORT_SKIP_IF_IN_LIST: bool = False
BULK_IMPORT_SKIP_IF_IN_WORKSPACE: bool = False

# Файл поруч із app.py: користувач може не чіпати код, лише дописувати рядки в UI / у цьому файлі.
USER_IMPORT_CV_ALIASES_FILENAME: str = "user_import_column_aliases.txt"

# Підписи для таблиці «всі відповідності» в UI (поля верхнього рівня ліда).
_REFERENCE_LEAD_FIELD_LABELS: dict[str, str] = {
    "first_name": "поле ліда: first_name",
    "last_name": "поле ліда: last_name",
    "job_title": "поле ліда: job_title",
    "company_name": "поле ліда: company_name",
    "email": "поле ліда: email",
    "website": "поле ліда: website",
}


def _reference_destination_for_column(col: str, typ: str) -> str:
    if typ == "do_not_import":
        return "не імпортується"
    if typ == "location_field":
        return "custom_variables → Country та location (одне значення з CSV)"
    if typ == "custom_variable":
        k = custom_variable_api_key(col, user_cv_aliases=None)
        return f"custom_variables → {k}"
    if typ in _REFERENCE_LEAD_FIELD_LABELS:
        return _REFERENCE_LEAD_FIELD_LABELS[typ]
    return typ


def reference_mapping_rows_for_ui() -> list[dict[str, str]]:
    """
    Усі вбудовані відповідності + рядки-шаблони — для однієї таблиці в Streamlit.
    Ключі словників — стабільні для DataFrame.
    """
    rows: list[dict[str, str]] = []
    for col in sorted(SCREEN1_IMPORT_TYPE_BY_COLUMN.keys(), key=lambda x: str(x).casefold()):
        typ = SCREEN1_IMPORT_TYPE_BY_COLUMN[col]
        rows.append(
            {
                "Група": "Вбудовано (основні поля)",
                "Джерело (CSV)": col,
                "Куди в API": _reference_destination_for_column(col, typ),
            }
        )
    for col in sorted(SCREEN2_IMPORT_TYPE_BY_COLUMN.keys(), key=lambda x: str(x).casefold()):
        typ = SCREEN2_IMPORT_TYPE_BY_COLUMN[col]
        rows.append(
            {
                "Група": "Вбудовано (тіло листа / subject)",
                "Джерело (CSV)": col,
                "Куди в API": _reference_destination_for_column(col, typ),
            }
        )
    pattern_group = "За шаблоном назви колонки"
    rows.extend(
        [
            {
                "Група": pattern_group,
                "Джерело (CSV)": "full_email_1, full_email_2, … (цифра в суфіксі)",
                "Куди в API": "custom_variables → email_01, email_02, …",
            },
            {
                "Група": pattern_group,
                "Джерело (CSV)": "email_1_body, email_2_body, … (регулярний вираз email_<N>_body)",
                "Куди в API": "custom_variables → email_<N> (як у числі, напр. email_1)",
            },
            {
                "Група": pattern_group,
                "Джерело (CSV)": "email_generation_json_email_<N>_body",
                "Куди в API": "custom_variables → email_01 … (N з двозначним форматом)",
            },
            {
                "Група": pattern_group,
                "Джерело (CSV)": "email_generation_json_subject_line",
                "Куди в API": "custom_variables → subject_line",
            },
            {
                "Група": pattern_group,
                "Джерело (CSV)": "будь-який інший заголовок стовпця",
                "Куди в API": "custom_variables → ключ = сам заголовок (як у CSV)",
            },
        ]
    )
    return rows


def bulk_lead_import_guard_flags() -> dict[str, bool]:
    """Параметри bulk import: дублікати/верифікація вимкнені (скрін 3)."""
    return {
        "verify_leads_on_import": BULK_IMPORT_VERIFY_LEADS_ON_IMPORT,
        "skip_if_in_campaign": BULK_IMPORT_SKIP_IF_IN_CAMPAIGN,
        "skip_if_in_list": BULK_IMPORT_SKIP_IF_IN_LIST,
        "skip_if_in_workspace": BULK_IMPORT_SKIP_IF_IN_WORKSPACE,
    }


def normalize_header(h: object) -> str:
    return str(h or "").strip()


def parse_user_cv_alias_text(
    text: str,
) -> tuple[dict[str, str], list[str], list[tuple[str, str]]]:
    """
    Текст із UI / файлу: кожен рядок «джерело» => «ключ у custom_variables».
    Ключі словника — column_header.casefold() (для пошуку без урахування регістру).

    Роздільник: кома (перша), або =>, або ->. Рядки # — коментар.

    Третій елемент — прев’ю (назва як у файлі, ключ), дублікати згорнуто за останнім рядком.
    """
    out: dict[str, str] = {}
    notes: list[str] = []
    preview_order: dict[str, tuple[str, str]] = {}
    for lineno, line in enumerate(text.splitlines(), start=1):
        s = str(line or "").strip()
        if not s or s.startswith("#"):
            continue
        if "=>" in s:
            left, _, right = s.partition("=>")
        elif "->" in s:
            left, _, right = s.partition("->")
        elif "," in s:
            left, _, right = s.partition(",")
        else:
            notes.append(f"Рядок {lineno}: додайте роздільник «,» або «=>» між назвою колонки та ключем.")
            continue
        src = normalize_header(left)
        tgt = normalize_header(right)
        if not src:
            notes.append(f"Рядок {lineno}: порожня назва колонки.")
            continue
        if not tgt:
            notes.append(f"Рядок {lineno}: порожній ключ custom_variables для «{src}».")
            continue
        ck = src.casefold()
        if ck in out and out[ck] != tgt:
            notes.append(f"Рядок {lineno}: колонка «{src}» уже мапиться на «{out[ck]}», перезаписано на «{tgt}».")
        out[ck] = tgt
        preview_order[ck] = (src, tgt)
    preview = sorted(preview_order.values(), key=lambda t: t[0].casefold())
    return out, notes, preview


def resolve_import_type(
    column_header: object, user_cv_aliases: dict[str, str] | None = None
) -> str | None:
    raw = normalize_header(column_header)
    if user_cv_aliases:
        low = raw.casefold()
        if low in user_cv_aliases:
            return "custom_variable"
    for mapping in (SCREEN1_IMPORT_TYPE_BY_COLUMN, SCREEN2_IMPORT_TYPE_BY_COLUMN):
        if raw in mapping:
            return mapping[raw]
        low = raw.casefold()
        for name, typ in mapping.items():
            if name.casefold() == low:
                return typ
    # CSV Events: full_email_1, full_email_2, … → ті самі custom_variables, що email_01…
    if re.fullmatch(r"full_email_\d+", raw, re.IGNORECASE):
        return "custom_variable"
    # Експорт з генератора: префікс email_generation_json_…
    if re.fullmatch(r"email_generation_json_email_\d+_body", raw, re.IGNORECASE):
        return "custom_variable"
    if re.fullmatch(r"email_generation_json_subject_line", raw, re.IGNORECASE):
        return "custom_variable"
    return None


def custom_variable_api_key(
    column_header: object, user_cv_aliases: dict[str, str] | None = None
) -> str:
    """Ключ у custom_variables для Instantly (може відрізнятися від назви колонки CSV)."""
    raw = normalize_header(column_header)
    if user_cv_aliases:
        alt = user_cv_aliases.get(raw.casefold())
        if alt is not None:
            return alt
    if raw in CUSTOM_VARIABLE_API_KEY_BY_COLUMN:
        return CUSTOM_VARIABLE_API_KEY_BY_COLUMN[raw]
    low = raw.casefold()
    for name, key in CUSTOM_VARIABLE_API_KEY_BY_COLUMN.items():
        if name.casefold() == low:
            return key
    m = re.fullmatch(r"email_(\d+)_body", raw, re.IGNORECASE)
    if m:
        return f"email_{m.group(1)}"
    m = re.fullmatch(r"full_email_(\d+)", raw, re.IGNORECASE)
    if m:
        return f"email_{int(m.group(1)):02d}"
    m = re.fullmatch(r"email_generation_json_email_(\d+)_body", raw, re.IGNORECASE)
    if m:
        return f"email_{int(m.group(1)):02d}"
    if re.fullmatch(r"email_generation_json_subject_line", raw, re.IGNORECASE):
        return "subject_line"
    return raw
