"""
Повний сценарій Instantly API після розбиття таблиці: пошук шаблону, дублікат, акаунти, ліди.
Без activate — лише збереження / pause за потреби.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from instantly_account_tags import ACCOUNT_TAG_LABEL_BY_BUCKET
from instantly_api import InstantlyApiError, InstantlyClient
from instantly_import_mapping import custom_variable_api_key, resolve_import_type
from split_engine import OUTPUT_SHEETS

# У назві шаблон-кампанії мають бути і пошуковий рядок (напр. Softdev), і маркер сегмента.
BUCKET_TEMPLATE_MARKERS: dict[str, str] = {
    "Other (gmail, etc) USA": "USA gmail",
    "Other (gmail, etc) Europe": "EU gmail",
    "outlook USA": "USA outlk",
    "outlook Europe": "EU outlk",
}

# Кошики для полів сайдбара «Gmail/Other» та «Outlook» (по два регіони на провайдера).
ORDERED_GMAIL_BUCKETS: tuple[str, ...] = (
    "Other (gmail, etc) USA",
    "Other (gmail, etc) Europe",
)
ORDERED_OUTLOOK_BUCKETS: tuple[str, ...] = ("outlook USA", "outlook Europe")
BUCKETS_GMAIL_OTHER: frozenset[str] = frozenset(ORDERED_GMAIL_BUCKETS)
BUCKETS_OUTLOOK: frozenset[str] = frozenset(ORDERED_OUTLOOK_BUCKETS)


def buckets_for_provider_scope(
    buckets: dict[str, pd.DataFrame],
    scope: str,
) -> dict[str, pd.DataFrame]:
    """
    scope: «all» — усі сегменти; «gmail» — лише Other (gmail…) USA/Europe;
    «outlook» — лише outlook USA/Europe.
    """
    s = (scope or "all").strip().casefold()
    if s in ("", "all"):
        return dict(buckets)
    if s == "gmail":
        keep = BUCKETS_GMAIL_OTHER
    elif s in ("outlook",):
        keep = BUCKETS_OUTLOOK
    else:
        return dict(buckets)
    return {k: v for k, v in buckets.items() if k in keep}


API_FIELD_BY_IMPORT_TYPE: dict[str, str] = {
    "first_name": "first_name",
    "last_name": "last_name",
    "job_title": "job_title",
    "company_name": "company_name",
    "email": "email",
    "website": "website",
}


def _cell_str(val: Any) -> str | None:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except TypeError:
        pass
    s = str(val).strip()
    return s if s else None


def dataframe_to_leads(
    df: pd.DataFrame, user_cv_aliases: dict[str, str] | None = None
) -> list[dict[str, Any]]:
    """Рядки DataFrame → об'єкти lead для POST /api/v2/leads/add."""
    leads: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        lead: dict[str, Any] = {}
        cv: dict[str, Any] = {}
        for col in df.columns:
            typ = resolve_import_type(col, user_cv_aliases=user_cv_aliases)
            if typ == "do_not_import":
                continue
            val = _cell_str(row.get(col))
            if val is None:
                continue
            if typ in API_FIELD_BY_IMPORT_TYPE:
                lead[API_FIELD_BY_IMPORT_TYPE[typ]] = val
            elif typ == "location_field":
                # Instantly UI: Country → Location; у API v2 — через custom_variables
                # (див. bulk add: лише custom_variables для довільних полів).
                cv["Country"] = val
                cv["location"] = val
            elif typ == "custom_variable" or typ is None:
                ck = custom_variable_api_key(col, user_cv_aliases=user_cv_aliases)
                cv[ck] = val
        if cv:
            lead["custom_variables"] = cv
        if not lead.get("email"):
            continue
        leads.append(lead)
    return leads


def _pick_template(
    campaigns: list[dict[str, Any]], base: str, marker: str
) -> dict[str, Any] | None:
    b = base.casefold()
    m = marker.casefold()
    matches = [
        c
        for c in campaigns
        if b in str(c.get("name", "")).casefold() and m in str(c.get("name", "")).casefold()
    ]
    if not matches:
        return None
    matches.sort(
        key=lambda c: str(c.get("timestamp_updated") or c.get("timestamp_created") or ""),
        reverse=True,
    )
    return matches[0]


def _chunks(xs: list[Any], n: int) -> list[list[Any]]:
    return [xs[i : i + n] for i in range(0, len(xs), n)]


def pool_raw_for_bucket(
    bucket_name: str,
    pool_gmail_raw: str,
    pool_outlook_raw: str,
) -> str:
    """
    Текст «одного поля» для кошика: провайдер gmail/other vs outlook.
    Якщо в `instantly_account_tags.ACCOUNT_TAG_LABEL_BY_BUCKET` для цього кошика
    задано непорожній рядок — він **замінює** значення з сайдбара (тонке регулювання USA vs EU).
    """
    override = (ACCOUNT_TAG_LABEL_BY_BUCKET.get(bucket_name) or "").strip()
    if override:
        return override
    if bucket_name in BUCKETS_GMAIL_OTHER:
        return str(pool_gmail_raw or "")
    if bucket_name in BUCKETS_OUTLOOK:
        return str(pool_outlook_raw or "")
    return ""


def run_full_pipeline(
    client: InstantlyClient,
    buckets: dict[str, pd.DataFrame],
    *,
    template_search: str,
    new_name_prefix: str,
    pool_gmail_raw: str,
    pool_outlook_raw: str,
    user_cv_aliases: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """
    Для кожного непорожнього кошика: знайти шаблон за назвою, duplicate,
    apply_accounts_to_use_if_provided (пул Gmail/Other або Outlook з сайдбара; опційно override у `instantly_account_tags.py`),
    bulk leads батчами по 1000.
    """
    base = template_search.strip()
    prefix = new_name_prefix.strip()
    if not base:
        raise ValueError("Вкажіть пошук шаблон-кампанії (фрагмент назви).")
    if not prefix:
        raise ValueError("Вкажіть префікс назви для нових кампаній.")

    all_campaigns = client.iter_campaigns(search=base)
    log: list[dict[str, Any]] = []

    for bucket_name in OUTPUT_SHEETS:
        df = buckets.get(bucket_name)
        if df is None or df.empty:
            log.append({"bucket": bucket_name, "skipped": True, "reason": "немає рядків"})
            continue

        marker = BUCKET_TEMPLATE_MARKERS.get(bucket_name)
        if not marker:
            log.append({"bucket": bucket_name, "error": "немає маркера в BUCKET_TEMPLATE_MARKERS"})
            continue

        tmpl = _pick_template(all_campaigns, base, marker)
        if not tmpl:
            log.append(
                {
                    "bucket": bucket_name,
                    "error": (
                        f"немає кампанії, де в назві є «{base}» і «{marker}». "
                        "Перевірте шаблони в Instantly."
                    ),
                }
            )
            continue

        tid = str(tmpl.get("id"))
        new_name = f"{prefix} {marker}".strip()

        try:
            dup = client.duplicate_campaign(tid, name=new_name)
            new_id = str(dup.get("id"))
        except InstantlyApiError as e:
            log.append({"bucket": bucket_name, "error": str(e)})
            continue

        tag_bucket = (ACCOUNT_TAG_LABEL_BY_BUCKET.get(bucket_name) or "").strip()
        pool_raw = pool_raw_for_bucket(bucket_name, pool_gmail_raw, pool_outlook_raw)

        try:
            client.apply_accounts_to_use_if_provided(
                new_id,
                email_list_raw=pool_raw,
                tag_label_raw="",
                pause_if_active=True,
            )
        except InstantlyApiError as e:
            log.append(
                {
                    "bucket": bucket_name,
                    "campaign_id": new_id,
                    "error": f"accounts: {e}",
                    "pool_source": "override instantly_account_tags"
                    if tag_bucket
                    else ("gmail" if bucket_name in BUCKETS_GMAIL_OTHER else "outlook"),
                }
            )
            continue

        leads = dataframe_to_leads(df, user_cv_aliases=user_cv_aliases)
        if not leads:
            log.append(
                {
                    "bucket": bucket_name,
                    "campaign_id": new_id,
                    "name": new_name,
                    "warning": "немає лидів із полем email після маппінгу",
                }
            )
            continue

        uploaded_total = 0
        last_res: dict[str, Any] = {}
        try:
            for batch in _chunks(leads, 1000):
                last_res = client.bulk_add_leads(new_id, batch)
                uploaded_total += int(last_res.get("leads_uploaded") or 0)
            row: dict[str, Any] = {
                "bucket": bucket_name,
                "campaign_id": new_id,
                "name": new_name,
                "template_id": tid,
                "leads_in_file": len(leads),
                "leads_uploaded_sum": uploaded_total,
                "last_batch_status": last_res.get("status"),
                "ok": True,
            }
            if uploaded_total < len(leads):
                row["warning_partial_upload"] = (
                    f"завантажено лише {uploaded_total} з {len(leads)} — часто немає активних "
                    "відправників (перевірте поле Gmail/Outlook у сайдбарі або override у instantly_account_tags) "
                    "або lead відсіюється фільтрами skip."
                )
                row["last_batch_api"] = last_res
            log.append(row)
        except InstantlyApiError as e:
            log.append({"bucket": bucket_name, "campaign_id": new_id, "error": f"leads: {e}"})

    return log
