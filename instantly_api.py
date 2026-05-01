"""
Instantly API v2: зміна Options («Accounts to use») без запуску кампанії.

Документація: https://developer.instantly.ai/

Важливо:
- PATCH /api/v2/campaigns/{id}:
  - `email_list` — лише **адреси** підключених акаунтів (з символом `@`);
  - `email_tag_list` — масив **UUID** кастомних тегів. Label тега (наприклад `gmail_gtm_group_01`)
    не можна класти в `email_list` — це ламає кампанію. У полі Streamlit «відправники» рядки **без `@`**
    автоматично трактуються як label тегів і резолвляться в UUID.
  - **Бейдж тега** в списку кампаній Instantly — це **призначення тега на кампанію** (`POST .../custom-tags/toggle-resource`,
    `resource_type: campaign`), окремо від `email_tag_list`. Після PATCH акаунтів ті самі UUID підв’язуються до кампанії.
- PATCH має містити **обидва** поля `email_list` і `email_tag_list`, коли змінюєте один із них:
  інакше з дубліката шаблону в кампанії лишиться попереднє значення іншого поля
  (наприклад, старий тег Outlook_* разом із новим, або старі конкретні акаунти).
- Якщо користувач не ввів ні email_list, ні тег у Streamlit — виконується **PATCH з порожніми**
  `email_list` та `email_tag_list`, щоб **очистити** «Accounts to use» (не копіювати з шаблону).
  Інші кроки сценарію виконуються до кінця; за потреби — pause (див. `apply_accounts_to_use_if_provided`).
"""

from __future__ import annotations

from typing import Any

import requests

BASE_URL = "https://api.instantly.ai"

# Статуси кампанії (див. схему Campaign у OpenAPI)
STATUS_DRAFT = 0
STATUS_ACTIVE = 1
STATUS_PAUSED = 2
STATUS_COMPLETED = 3
STATUS_RUNNING_SUBSEQUENCES = 4


class InstantlyApiError(Exception):
    def __init__(self, message: str, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class InstantlyClient:
    def __init__(self, api_key: str, timeout: int = 120):
        self._timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {api_key.strip()}",
            "Content-Type": "application/json",
        }

    def _req(self, method: str, path: str, json: dict | None = None) -> requests.Response:
        url = f"{BASE_URL}{path}"
        r = requests.request(
            method, url, headers=self._headers, json=json, timeout=self._timeout
        )
        return r

    def ensure_paused_if_active(self, campaign_id: str) -> dict[str, Any]:
        """Якщо кампанія активна або гонить subsequence — pause. Повертає актуальний стан."""
        camp = self.get_campaign(campaign_id)
        try:
            st_int = int(camp.get("status")) if camp.get("status") is not None else None
        except (TypeError, ValueError):
            st_int = None
        if st_int in (STATUS_ACTIVE, STATUS_RUNNING_SUBSEQUENCES):
            self.pause_campaign(campaign_id)
            return self.get_campaign(campaign_id)
        return camp

    RESOURCE_TAG_ACCOUNT = 1
    RESOURCE_TAG_CAMPAIGN = 2

    def toggle_tags_on_resources(
        self,
        tag_ids: list[str],
        resource_type: int,
        resource_ids: list[str],
        *,
        assign: bool = True,
    ) -> dict[str, Any]:
        """
        POST /api/v2/custom-tags/toggle-resource — тег у списку кампаній / на акаунтах.
        resource_type: 1 = account, 2 = campaign (див. OpenAPI Instantly).
        """
        body: dict[str, Any] = {
            "tag_ids": [str(t).strip() for t in tag_ids if str(t).strip()],
            "resource_type": int(resource_type),
            "resource_ids": [str(r).strip() for r in resource_ids if str(r).strip()],
            "assign": bool(assign),
            "selected_all": False,
        }
        r = self._req("POST", "/api/v2/custom-tags/toggle-resource", json=body)
        if not r.ok:
            raise InstantlyApiError(
                _error_message(r, "toggle custom-tags on resource"),
                status_code=r.status_code,
                body=_safe_json(r),
            )
        try:
            return r.json()
        except Exception:
            return {"success": True}

    def apply_accounts_to_use_if_provided(
        self,
        campaign_id: str,
        *,
        email_list_raw: str = "",
        tag_label_raw: str = "",
        pause_if_active: bool = True,
    ) -> dict[str, Any]:
        """
        Політика Streamlit: якщо акаунти та тег не введені — «Accounts to use» **очищаються**
        (PATCH: порожні `email_list` і `email_tag_list`), а не лишаються з шаблону-дубліката.

        Якщо введено лише тег або лише список email — **обидва** поля PATCH все одно
        відправляються: незаповнене стає `[]`, щоб з шаблону не лишився старий тег чи старі акаунти.

        Після PATCH, якщо є теги акаунтів — ті самі UUID **додаються як теги кампанії**
        (бейдж у списку кампаній у Instantly; це не те саме, що лише `email_tag_list`).

        Після цього, якщо pause_if_active — зупинка активної кампанії (без запуску через API).
        """
        raw_lines = parse_email_list_field(email_list_raw or "")
        emails, tag_labels_area = partition_sender_pool_lines(raw_lines)
        tag_explicit = str(tag_label_raw or "").strip()
        ordered_tag_labels = ordered_unique_tag_labels(tag_explicit, tag_labels_area)
        tag_uuids: list[str] = []
        if ordered_tag_labels:
            for tl in ordered_tag_labels:
                uid = self.resolve_tag_label_to_uuid(tl)
                if not uid:
                    raise InstantlyApiError(f'Тег «{tl}» не знайдено в workspace')
                tag_uuids.append(uid)
        if emails or ordered_tag_labels:
            payload: dict[str, Any] = {}
            # Завжди обидва поля, щоб з дубліката шаблону не лишились старі акаунти/теги
            payload["email_list"] = emails if emails else []
            payload["email_tag_list"] = tag_uuids if tag_uuids else []
            self.patch_campaign(campaign_id, payload)
            if tag_uuids:
                self.toggle_tags_on_resources(
                    tag_uuids,
                    self.RESOURCE_TAG_CAMPAIGN,
                    [campaign_id],
                    assign=True,
                )
        else:
            self.patch_campaign(
                campaign_id,
                {
                    "email_list": [],
                    "email_tag_list": [],
                },
            )
        if pause_if_active:
            return self.ensure_paused_if_active(campaign_id)
        return self.get_campaign(campaign_id)

    def duplicate_campaign(self, campaign_id: str, name: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if name is not None and str(name).strip():
            body["name"] = str(name).strip()
        r = self._req("POST", f"/api/v2/campaigns/{campaign_id}/duplicate", json=body or None)
        if not r.ok:
            raise InstantlyApiError(
                _error_message(r, "duplicate campaign"),
                status_code=r.status_code,
                body=_safe_json(r),
            )
        return r.json()

    def get_campaign(self, campaign_id: str) -> dict[str, Any]:
        r = self._req("GET", f"/api/v2/campaigns/{campaign_id}")
        if not r.ok:
            raise InstantlyApiError(
                _error_message(r, "get campaign"),
                status_code=r.status_code,
                body=_safe_json(r),
            )
        return r.json()

    def patch_campaign(self, campaign_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self._req("PATCH", f"/api/v2/campaigns/{campaign_id}", json=payload)
        if not r.ok:
            raise InstantlyApiError(
                _error_message(r, "patch campaign"),
                status_code=r.status_code,
                body=_safe_json(r),
            )
        return r.json()

    def pause_campaign(self, campaign_id: str) -> None:
        r = self._req("POST", f"/api/v2/campaigns/{campaign_id}/pause", json={})
        if r.status_code not in (200, 201, 204):
            raise InstantlyApiError(
                _error_message(r, "pause campaign"),
                status_code=r.status_code,
                body=_safe_json(r),
            )

    def list_custom_tags(
        self,
        *,
        search: str | None = None,
        limit: int = 100,
        starting_after: str | None = None,
    ) -> dict[str, Any]:
        """GET /api/v2/custom-tags (пагінація через next_starting_after у відповіді)."""
        params: dict[str, str] = {"limit": str(min(max(limit, 1), 100))}
        if search is not None and str(search).strip():
            params["search"] = str(search).strip()
        if starting_after:
            params["starting_after"] = starting_after
        r = requests.get(
            f"{BASE_URL}/api/v2/custom-tags",
            headers=self._headers,
            params=params,
            timeout=self._timeout,
        )
        if not r.ok:
            raise InstantlyApiError(
                _error_message(r, "list custom tags"),
                status_code=r.status_code,
                body=_safe_json(r),
            )
        return r.json()

    def iter_custom_tags(self, search: str | None = None) -> list[dict[str, Any]]:
        """Збирає всі теги (сторінки), опційно з фільтром search."""
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = self.list_custom_tags(search=search, limit=100, starting_after=cursor)
            items = page.get("items") or []
            out.extend(items)
            cursor = page.get("next_starting_after")
            if not cursor or not items:
                break
        return out

    def resolve_tag_label_to_uuid(self, label: str) -> str | None:
        """Знаходить перший тег з точним label (без урахування регістру)."""
        want = str(label or "").strip()
        if not want:
            return None
        low = want.casefold()
        # Спочатку вузький search, потім повний список при необхідності
        candidates = self.iter_custom_tags(search=want)
        for tag in candidates:
            if str(tag.get("label") or "").strip().casefold() == low:
                tid = tag.get("id")
                return str(tid) if tid else None
        for tag in self.iter_custom_tags(search=None):
            if str(tag.get("label") or "").strip().casefold() == low:
                tid = tag.get("id")
                return str(tid) if tid else None
        return None

    def list_campaigns(
        self,
        *,
        search: str | None = None,
        limit: int = 100,
        starting_after: str | None = None,
        status: int | None = None,
    ) -> dict[str, Any]:
        """GET /api/v2/campaigns — пошук за назвою (search), пагінація."""
        params: dict[str, str] = {"limit": str(min(max(limit, 1), 100))}
        if search is not None and str(search).strip():
            params["search"] = str(search).strip()
        if starting_after:
            params["starting_after"] = starting_after
        if status is not None:
            params["status"] = str(int(status))
        r = requests.get(
            f"{BASE_URL}/api/v2/campaigns",
            headers=self._headers,
            params=params,
            timeout=self._timeout,
        )
        if not r.ok:
            raise InstantlyApiError(
                _error_message(r, "list campaigns"),
                status_code=r.status_code,
                body=_safe_json(r),
            )
        return r.json()

    def iter_campaigns(self, search: str | None = None) -> list[dict[str, Any]]:
        """Усі сторінки списку кампаній."""
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = self.list_campaigns(search=search, limit=100, starting_after=cursor)
            items = page.get("items") or []
            out.extend(items)
            cursor = page.get("next_starting_after")
            if not cursor or not items:
                break
        return out

    def bulk_add_leads(
        self,
        campaign_id: str,
        leads: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """POST /api/v2/leads/add — до 1000 лидів за раз."""
        from instantly_import_mapping import bulk_lead_import_guard_flags

        body: dict[str, Any] = {
            "campaign_id": campaign_id,
            "leads": leads,
        }
        body.update(bulk_lead_import_guard_flags())
        r = self._req("POST", "/api/v2/leads/add", json=body)
        if not r.ok:
            raise InstantlyApiError(
                _error_message(r, "bulk add leads"),
                status_code=r.status_code,
                body=_safe_json(r),
            )
        return r.json()

    def set_accounts_to_use(
        self,
        campaign_id: str,
        email_list: list[str] | None = None,
        *,
        email_tag_uuids: list[str] | None = None,
        pause_if_active: bool = True,
    ) -> dict[str, Any]:
        """
        Оновлює «Accounts to use»: email_list і/або email_tag_list.
        Хоча б одне з полів має бути передано (непорожній список).
        Не запускає кампанію; за потреби pause.

        Якщо задано лише тег — додатково передається email_list: [], щоб скинути конкретні
        акаунти з дубліката шаблону. Якщо лише список email — передається email_tag_list: [].
        """
        payload: dict[str, Any] = {}
        if email_list is not None:
            payload["email_list"] = [x.strip() for x in email_list if str(x).strip()]
        if email_tag_uuids is not None:
            payload["email_tag_list"] = [str(x).strip() for x in email_tag_uuids if str(x).strip()]
        if not payload:
            raise ValueError("Передайте email_list і/або email_tag_uuids для PATCH кампанії.")
        self.patch_campaign(campaign_id, payload)
        if pause_if_active:
            return self.ensure_paused_if_active(campaign_id)
        return self.get_campaign(campaign_id)


def parse_email_list_field(text: str) -> list[str]:
    """Розбір поля з UI: кома або новий рядок."""
    out: list[str] = []
    for part in text.replace(",", "\n").splitlines():
        t = str(part).strip()
        if t:
            out.append(t)
    return out


def partition_sender_pool_lines(raw_lines: list[str]) -> tuple[list[str], list[str]]:
    """
    Рядки з поля «відправники»: з `@` → справжні email для `email_list` у API;
    без `@` → label кастомного тега (як у multiselect Instantly), для `email_tag_list`.
    """
    emails: list[str] = []
    tag_labels: list[str] = []
    for line in raw_lines:
        s = str(line).strip()
        if not s:
            continue
        if "@" in s:
            emails.append(s)
        else:
            tag_labels.append(s)
    return emails, tag_labels


def ordered_unique_tag_labels(explicit: str, from_area: list[str]) -> list[str]:
    """Порядок: спочатку окреме поле тега, потім рядки з текстової області; без дублікатів."""
    out: list[str] = []
    seen: set[str] = set()
    for t in ([explicit] if str(explicit or "").strip() else []) + list(from_area):
        t = str(t).strip()
        if not t:
            continue
        k = t.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def _safe_json(r: requests.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return r.text


def _error_message(r: requests.Response, action: str) -> str:
    j = _safe_json(r)
    if isinstance(j, dict) and j.get("message"):
        return f"Instantly API ({action}): {j['message']}"
    return f"Instantly API ({action}): HTTP {r.status_code}"
