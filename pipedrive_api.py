"""
Допоміжні виклики Pipedrive API v1 для фільтрації рядків за Person (label MQL, поле Replied at).
Документація: https://developers.pipedrive.com/docs/api/v1
"""

from __future__ import annotations

import re
from typing import Any

import requests

PD_V1 = "https://api.pipedrive.com/v1"
PD_V2 = "https://api.pipedrive.com/api/v2"


class PipedriveError(RuntimeError):
    pass


def _pd_get(api_token: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    q = dict(params or {})
    q["api_token"] = api_token
    url = path if path.startswith("http") else (f"{PD_V1}{path}" if path.startswith("/") else f"{PD_V1}/{path}")
    r = requests.get(url, params=q, timeout=60)
    try:
        body = r.json()
    except Exception as exc:
        raise PipedriveError(f"Pipedrive HTTP {r.status_code}: не JSON") from exc
    if r.status_code != 200:
        err = body.get("error") or body.get("error_info") or r.text[:500]
        raise PipedriveError(f"Pipedrive HTTP {r.status_code}: {err}")
    if not body.get("success"):
        err = body.get("error") or body.get("error_info") or str(body)
        raise PipedriveError(f"Pipedrive: {err}")
    return body


def _pd_get_v2(api_token: str, endpoint_path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """GET api/v2/… — у відповіді поле `success` інколи відсутнє; помилка лише при явному success=false або HTTP ≠ 200."""
    q = dict(params or {})
    q["api_token"] = api_token
    ep = endpoint_path if endpoint_path.startswith("/") else f"/{endpoint_path}"
    r = requests.get(f"{PD_V2}{ep}", params=q, timeout=60)
    try:
        body = r.json()
    except Exception as exc:
        raise PipedriveError(f"Pipedrive v2 HTTP {r.status_code}: не JSON") from exc
    if r.status_code != 200:
        err = body.get("error") or body.get("error_info") or r.text[:500]
        raise PipedriveError(f"Pipedrive v2 HTTP {r.status_code}: {err}")
    if body.get("success") is False:
        err = body.get("error") or body.get("error_info") or str(body)
        raise PipedriveError(f"Pipedrive v2: {err}")
    return body


def fetch_person_fields(api_token: str) -> list[dict[str, Any]]:
    body = _pd_get(api_token, "/personFields", {})
    data = body.get("data")
    return list(data) if isinstance(data, list) else []


def _person_records_from_persons_data(data: Any) -> list[dict[str, Any]]:
    """
    GET /persons?ids=… може повертати:
    - масив персон;
    - один об'єкт персони;
    - об'єкт «id → person» (або з домішкою null / службових ключів).

    Раніше для dict вимагалось len(values)==len(keys) — якщо API додавав null або зайві ключі,
    **другий чанк після 100 id повністю губився** (лишалось рівно 100 персон).
    """
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    persons: list[dict[str, Any]] = []
    for v in data.values():
        if not isinstance(v, dict):
            continue
        pid = v.get("id")
        if pid is None:
            continue
        try:
            int(pid)
        except (TypeError, ValueError):
            continue
        persons.append(v)
    if persons:
        return persons
    if data.get("id") is not None:
        try:
            int(data["id"])
        except (TypeError, ValueError):
            return []
        return [data]
    return []


def fetch_persons_by_ids(
    api_token: str,
    ids: list[int],
    *,
    custom_field_keys: list[str] | None = None,
) -> dict[int, dict[str, Any]]:
    """
    Мапа id → person з GET /v1/persons?ids=… (до 100 id за запит).

    Для **кастомних** полей (зокрема «Replied at») треба передати їхні **hash-ключі** в **custom_fields**,
    інакше Pipedrive часто **не включає** ці поля в «легку» відповідь списку — правила виключення ніколи не спрацьовують.
    """
    cf_csv: str | None = None
    if custom_field_keys:
        parts = [str(k).strip() for k in custom_field_keys if str(k).strip()]
        if parts:
            cf_csv = ",".join(parts[:15])

    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i : i + 100]
        base: dict[str, Any] = {"ids": ",".join(str(x) for x in chunk)}
        params = dict(base)
        if cf_csv:
            params["custom_fields"] = cf_csv
            params["include_option_labels"] = "1"
        try:
            body = _pd_get(api_token, "/persons", params)
        except PipedriveError:
            # Старі / обмежені клієнти: спроба без include_option_labels, потім без custom_fields.
            if not cf_csv:
                raise
            try:
                params2 = dict(base)
                params2["custom_fields"] = cf_csv
                body = _pd_get(api_token, "/persons", params2)
            except PipedriveError:
                body = _pd_get(api_token, "/persons", base)
        for p in _person_records_from_persons_data(body.get("data")):
            pid = p.get("id")
            try:
                pid_i = int(pid)
            except (TypeError, ValueError):
                continue
            out[pid_i] = p
    return out


def fetch_persons_v2_by_ids(
    api_token: str,
    ids: list[int],
    *,
    custom_field_keys: list[str] | None = None,
    field_numeric_id: int | None = None,
) -> dict[int, dict[str, Any]]:
    """
    GET /api/v2/persons?ids=…&include_fields=custom_fields — batch-запит v2 з кастомними полями.
    На відміну від v1-batch, тут `include_fields=custom_fields` гарантовано включає кастомні поля в відповідь.
    """
    parts: list[str] = []
    if custom_field_keys:
        parts.extend([str(k).strip() for k in custom_field_keys if str(k).strip()])
    if field_numeric_id is not None:
        sid = str(int(field_numeric_id))
        if sid not in parts:
            parts.append(sid)
    cf_csv: str | None = ",".join(parts[:15]) if parts else None

    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i : i + 100]
        ids_csv = ",".join(str(x) for x in chunk)
        # Для LIST-endpoint /api/v2/persons `include_fields=custom_fields` не підтримується —
        # лише параметр `custom_fields=<hash>` (і `include_option_labels`).
        attempts_v2: list[dict[str, Any]] = []
        if cf_csv:
            attempts_v2.append({"ids": ids_csv, "custom_fields": cf_csv, "include_option_labels": "true"})
            attempts_v2.append({"ids": ids_csv, "custom_fields": cf_csv})
        attempts_v2.append({"ids": ids_csv})

        body: dict[str, Any] | None = None
        for attempt in attempts_v2:
            try:
                body = _pd_get_v2(api_token, "/persons", attempt)
                break
            except PipedriveError:
                continue
        if body is None:
            continue

        data = body.get("data")
        for p in _person_records_from_persons_data(data):
            pid = p.get("id")
            try:
                pid_i = int(pid)
            except (TypeError, ValueError):
                continue
            out[pid_i] = p
    return out


def fetch_person_detail(
    api_token: str,
    person_id: int,
    *,
    custom_field_keys: list[str] | None = None,
) -> dict[str, Any] | None:
    """
    GET /v1/persons/{id} — повна картка контакту; кастомні поля часто **є тут**, навіть коли /persons?ids=… дає порожнє поле.
    """
    cf_csv: str | None = None
    if custom_field_keys:
        parts = [str(k).strip() for k in custom_field_keys if str(k).strip()]
        if parts:
            cf_csv = ",".join(parts[:15])
    path = f"/persons/{int(person_id)}"

    def _call(params: dict[str, Any]) -> dict[str, Any]:
        return _pd_get(api_token, path, params)

    try:
        if cf_csv:
            try:
                body = _call({"custom_fields": cf_csv, "include_option_labels": "1"})
            except PipedriveError:
                try:
                    body = _call({"custom_fields": cf_csv})
                except PipedriveError:
                    body = _call({})
        else:
            body = _call({})
    except PipedriveError:
        return None
    data = body.get("data")
    if isinstance(data, dict) and data.get("id") is not None:
        return data
    return None


def _pipedrive_person_custom_fields_non_empty(person: dict[str, Any]) -> bool:
    """Чи API реально повернув блок кастомних полів (не None і не порожня колекція)."""
    cf = person.get("custom_fields")
    if isinstance(cf, dict):
        return len(cf) > 0
    if isinstance(cf, list):
        return len(cf) > 0
    return False


def fetch_person_v2_detail(
    api_token: str,
    person_id: int,
    *,
    custom_field_keys: list[str] | None = None,
    field_numeric_id: int | None = None,
) -> dict[str, Any] | None:
    """
    GET /api/v2/persons/{id}.

    У документації v2 для «Get details of a person» окремо зазначено **include_fields=custom_fields**:
    без цього параметра об'єкт **custom_fields** у JSON часто **відсутній або порожній**, навіть коли в UI
    поле заповнене (користувач бачить те саме, що на person/358164).
    Додатково можна передати **custom_fields** (до 15 hash-ключів) та **include_option_labels**.
    У частини акаунтів у параметрі **custom_fields** допустимий також **числовий id** поля з personFields (разом з hash).
    """
    parts: list[str] = []
    if custom_field_keys:
        parts.extend([str(k).strip() for k in custom_field_keys if str(k).strip()])
    if field_numeric_id is not None:
        sid = str(int(field_numeric_id))
        if sid not in parts:
            parts.append(sid)
    parts = parts[:15]
    cf_csv: str | None = ",".join(parts) if parts else None
    path = f"/persons/{int(person_id)}"

    if cf_csv:
        attempts: list[dict[str, Any]] = [
            {
                "include_fields": "custom_fields",
                "custom_fields": cf_csv,
                "include_option_labels": "true",
            },
            {"include_fields": "custom_fields", "custom_fields": cf_csv},
            {"include_fields": "custom_fields"},
            {"custom_fields": cf_csv, "include_option_labels": "true"},
            {"custom_fields": cf_csv},
            {},
        ]
    else:
        attempts = [
            {"include_fields": "custom_fields"},
            {},
        ]

    last_ok: dict[str, Any] | None = None
    for params in attempts:
        try:
            body = _pd_get_v2(api_token, path, params)
        except PipedriveError:
            continue
        data = body.get("data")
        if not isinstance(data, dict) or data.get("id") is None:
            continue
        last_ok = data
        if cf_csv and not _pipedrive_person_custom_fields_non_empty(data):
            continue
        return data
    return last_ok


def _label_option_id_to_text(fields: list[dict[str, Any]]) -> dict[int, str]:
    """ID опції поля Label → видимий текст (напр. MQL)."""
    id_to_name: dict[int, str] = {}
    for f in fields:
        if not isinstance(f, dict):
            continue
        key = str(f.get("key") or "")
        ftype = str(f.get("field_type") or "").lower()
        if key != "label" and ftype != "label":
            continue
        opts = f.get("options")
        if not isinstance(opts, list):
            continue
        for o in opts:
            if not isinstance(o, dict):
                continue
            try:
                oid = int(o["id"])
            except (KeyError, TypeError, ValueError):
                continue
            lab = str(o.get("label") or o.get("name") or "").strip()
            if lab:
                id_to_name[oid] = lab
    return id_to_name


def _find_person_field_by_name(fields: list[dict[str, Any]], want: str) -> dict[str, Any] | None:
    if not want.strip():
        return None
    w = want.strip().casefold()
    for f in fields:
        if not isinstance(f, dict):
            continue
        nm = str(f.get("name") or "").strip().casefold()
        if nm == w:
            return f
    for f in fields:
        if not isinstance(f, dict):
            continue
        nm = str(f.get("name") or "").strip().casefold()
        if w in nm or nm in w:
            return f
    return None


def _find_person_field_by_api_key(fields: list[dict[str, Any]], api_key: str) -> dict[str, Any] | None:
    if not str(api_key or "").strip():
        return None
    want = str(api_key).strip()
    for f in fields:
        if not isinstance(f, dict):
            continue
        if str(f.get("key") or "") == want:
            return f
    return None


def _person_fields_replied_name_candidates(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f in fields:
        if not isinstance(f, dict):
            continue
        if "replied" in str(f.get("name") or "").casefold():
            out.append(f)
    return out


def _unwrap_pipedrive_option_raw(raw: Any) -> Any:
    """
    У відповідях Pipedrive enum/set інколи приходить об'єкт {id, label} замість голого id.
    Якщо є label — це найнадійніший текст для зіставлення з UI.
    """
    if isinstance(raw, dict):
        lab = raw.get("label")
        if lab is not None and str(lab).strip():
            return str(lab).strip()
        if "id" in raw:
            return raw.get("id")
    return raw


def _normalize_replied_label(s: str) -> str:
    """Підписи опцій у CRM / API часто відрізняюсь: «E-mail» vs «Email», зайві пробіли."""
    t = (s or "").strip().casefold()
    t = t.replace("e-mail", "email")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _display_field_value(field_def: dict[str, Any], raw: Any) -> str:
    if raw is None:
        return ""
    raw = _unwrap_pipedrive_option_raw(raw)
    if isinstance(raw, dict):
        return str(raw).strip()
    ftype = str(field_def.get("field_type") or "").lower()
    opts = field_def.get("options")
    opt_map: dict[int, str] = {}
    if isinstance(opts, list):
        for o in opts:
            if not isinstance(o, dict):
                continue
            try:
                oid = int(o["id"])
            except (KeyError, TypeError, ValueError):
                continue
            lab = str(o.get("label") or o.get("name") or "").strip()
            opt_map[oid] = lab or str(oid)

    # API v2 повертає enum/set як список [{id, label}] навіть для одиночного вибору.
    if ftype in ("enum", "set") and isinstance(raw, list):
        parts: list[str] = []
        for x in raw:
            xu = _unwrap_pipedrive_option_raw(x)
            # xu може бути рядком-підписом (label) або числом/рядком id
            if isinstance(xu, str) and not xu.isdigit():
                parts.append(xu)
            else:
                try:
                    parts.append(opt_map.get(int(xu), str(xu)))
                except (TypeError, ValueError):
                    parts.append(str(xu))
        return ", ".join(parts)
    if ftype in ("enum", "set") and opt_map:
        try:
            return str(opt_map.get(int(raw), raw))
        except (TypeError, ValueError):
            return str(raw)
    if isinstance(raw, list):
        # Для будь-якого типу — якщо список [{id,label}], витягуємо label; інакше через кому.
        parts_gen: list[str] = []
        for x in raw:
            xu = _unwrap_pipedrive_option_raw(x)
            parts_gen.append(str(xu).strip())
        return ", ".join(p for p in parts_gen if p)
    if isinstance(raw, dict):
        return str(raw)
    return str(raw).strip()


def _unwrap_nested_custom_value(val: Any) -> Any:
    """Обгортки v2/внутрішні: {\"value\": x} без label/id знімається до x для enum/числа."""
    if isinstance(val, dict):
        if val.get("label") is not None and str(val.get("label")).strip():
            return val
        if "id" in val and ("label" in val or "name" in val):
            return val
        if "currency" in val and "value" in val:
            return val
        if "value" in val and len(val) == 1:
            return val.get("value")
    return val


def _person_field_numeric_id(field_def: dict[str, Any] | None) -> int | None:
    """У GET /personFields у кожного поля є числовий `id`; у v2 custom_fields часто ключується **лише ним**, не hash `key`."""
    if not isinstance(field_def, dict):
        return None
    try:
        return int(field_def.get("id"))
    except (TypeError, ValueError):
        return None


def _person_field_raw_from_custom_fields(
    cf: Any, fk: str, *, field_numeric_id: int | None = None
) -> Any | None:
    """Значення з `person.custom_fields`: dict (v1/v2) або list of {{field_id, value}} (рідкісні відповіді)."""
    if cf is None or not str(fk or "").strip():
        return None
    fkl = fk.strip().lower()
    fnid_s = str(int(field_numeric_id)) if field_numeric_id is not None else None

    def _list_item_value(item: dict[str, Any]) -> Any | None:
        val = item.get("value")
        if val is None and "values" in item:
            val = item.get("values")
        return val

    if isinstance(cf, dict):
        if fk in cf:
            return _unwrap_nested_custom_value(cf[fk])
        for ck, cv in cf.items():
            if str(ck).lower() == fkl:
                return _unwrap_nested_custom_value(cv)
        if fnid_s is not None:
            if fnid_s in cf:
                return _unwrap_nested_custom_value(cf[fnid_s])
            for ck, cv in cf.items():
                if str(ck).strip() == fnid_s:
                    return _unwrap_nested_custom_value(cv)
        return None
    if isinstance(cf, list):
        for item in cf:
            if not isinstance(item, dict):
                continue
            cand_k = (
                item.get("field_id")
                or item.get("field_code")
                or item.get("id")
                or item.get("key")
                or item.get("hash")
            )
            if cand_k is not None and str(cand_k).strip().lower() == fkl:
                val = _list_item_value(item)
                if val is not None:
                    return _unwrap_nested_custom_value(val)
                return None
            if field_numeric_id is not None and cand_k is not None:
                try:
                    if int(cand_k) == int(field_numeric_id):
                        val = _list_item_value(item)
                        if val is not None:
                            return _unwrap_nested_custom_value(val)
                        return None
                except (TypeError, ValueError):
                    if str(cand_k).strip() == fnid_s:
                        val = _list_item_value(item)
                        if val is not None:
                            return _unwrap_nested_custom_value(val)
                        return None
        return None
    return None


def _person_field_raw(
    person: dict[str, Any], field_key: str, *, field_numeric_id: int | None = None
) -> Any:
    if not str(field_key or "").strip():
        return None
    fk = str(field_key).strip()
    got = _person_field_raw_from_custom_fields(
        person.get("custom_fields"), fk, field_numeric_id=field_numeric_id
    )
    if got is not None:
        return got
    v = person.get(fk)
    if v is not None:
        return _unwrap_nested_custom_value(v)
    if field_numeric_id is not None:
        sid = str(int(field_numeric_id))
        v2 = person.get(sid)
        if v2 is not None:
            return _unwrap_nested_custom_value(v2)
    return None


def person_exclude_flags(
    person: dict[str, Any],
    *,
    label_id_to_text: dict[int, str],
    mql_names: set[str],
    replied_field_def: dict[str, Any] | None,
    replied_match: set[str],
) -> tuple[bool, list[str]]:
    """
    Повертає (виключити?, причини), якщо:
    - текст будь-якого label_ids / legacy label збігаєть з одним із mql_names (casefold), або
    - текст поля Replied at (або іншого) збігається з одним із replied_match.
    """
    ids: list[int] = []
    label_ids = person.get("label_ids")
    if isinstance(label_ids, list):
        for lid in label_ids:
            try:
                ids.append(int(lid))
            except (TypeError, ValueError):
                pass
    legacy = person.get("label")
    if legacy not in (None, ""):
        try:
            ids.append(int(legacy))
        except (TypeError, ValueError):
            pass

    mql_hit = False
    for li in ids:
        txt = label_id_to_text.get(li, "").casefold()
        for m in mql_names:
            if m and txt == m:
                mql_hit = True
                break
        if mql_hit:
            break

    replied_hit = False
    if replied_field_def and replied_match:
        key = str(replied_field_def.get("key") or "")
        if key:
            fnid = _person_field_numeric_id(replied_field_def)
            raw = _person_field_raw(person, key, field_numeric_id=fnid)
            disp = _display_field_value(replied_field_def, raw)
            disp_n = _normalize_replied_label(disp)
            for rm in replied_match:
                if rm and disp_n == _normalize_replied_label(rm):
                    replied_hit = True
                    break

    final: list[str] = []
    if mql_hit:
        final.append("mql_label")
    if replied_hit:
        final.append("replied_at_match")
    return (mql_hit or replied_hit), final


def normalize_sheet_person_id(val: Any) -> str:
    """Аналог нормалізації ID у app.py для злиття з Pipedrive person id."""
    if val is None:
        return ""
    try:
        import pandas as pd

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


def collect_excluded_person_ids(
    api_token: str,
    person_ids: list[int],
    *,
    mql_label_names: list[str],
    replied_at_field_name: str,
    replied_at_values: list[str],
    replied_at_field_api_key: str = "",
) -> tuple[set[int], dict[int, str], list[str]]:
    """Множина person id для виключення, id → причина, підказки для UI (діагностика)."""
    diag: list[str] = []
    fields = fetch_person_fields(api_token)
    label_map = _label_option_id_to_text(fields)
    mql_set = {x.strip().casefold() for x in mql_label_names if x.strip()}
    replied_names = {x.strip().casefold() for x in replied_at_values if x.strip()}
    # person_exclude_flags порівнює через _normalize_replied_label; сирий match лишаємо casefold для стабільності.
    replied_norm_tokens = {_normalize_replied_label(x) for x in replied_names if x}
    manual_key = (replied_at_field_api_key or "").strip()
    if manual_key:
        hex_ok = bool(re.fullmatch(r"[0-9a-fA-F]+", manual_key))
        if len(manual_key) != 40 or not hex_ok:
            diag.append(
                f"CRM: **Увага:** введений API key має довжину **{len(manual_key)}**"
                + ("" if hex_ok else " і містить не-hex символи")
                + " — у **Pipedrive** ключ кастомного поля персони майже завжди **40** символів **0–9, a–f**. "
                "Якщо з UI скопійовано **неповний** ключ, у **personFields** може «знайтись» **інше** поле, "
                "а в **GET persons** значення **Replied at** лишаться порожніми. "
                "Скопіюйте key ще раз (або з таблиці **personFields** у цьому додатку) **без обрізання**."
            )
    replied_def: dict[str, Any] | None = None
    if manual_key:
        replied_def = _find_person_field_by_api_key(fields, manual_key)
        if replied_def:
            fid_msg = ""
            pfn = _person_field_numeric_id(replied_def)
            if pfn is not None:
                fid_msg = f" Числовий **id** поля в personFields: **{pfn}** (у відповідях API `custom_fields` часом ключується лише ним, не hash)."
            rk = str(replied_def.get("key") or "")
            diag.append(
                f"CRM: «Replied at» — поле **{replied_def.get('name') or '?'}**; **API key** у personFields: `{rk}` "
                f"(**{len(rk)}** симв.). Порівняйте з полем у додатку — має збігатися **символ у символ**.{fid_msg}"
            )
        else:
            diag.append(
                f"CRM: ручний API key `{manual_key}` **не знайдено** у **GET /v1/personFields** "
                "(скопіюйте ключ у **Pipedrive → Налаштування → Поля даних → Person** → поле → API)."
            )
            c_re = _person_fields_replied_name_candidates(fields)
            if c_re:
                diag.append(
                    "CRM: поля Person з **replied** у назві: "
                    + "; ".join(f"**{c.get('name')}** → `{c.get('key')}`" for c in c_re[:8])
                )
    elif (replied_at_field_name or "").strip():
        replied_def = _find_person_field_by_name(fields, replied_at_field_name)
        if replied_names and not replied_def:
            diag.append(
                f"CRM: поле персони «{replied_at_field_name.strip()}» **не знайдено** у Pipedrive **personFields** "
                "(перевірте точну назву поля в налаштуваннях CRM; без цього правило Replied at не спрацює). "
                "Або вкажіть **API key** поля в полі нижче в додатку."
            )
            c_re = _person_fields_replied_name_candidates(fields)
            if c_re:
                diag.append(
                    "CRM: можливі поля з **replied** у назві: "
                    + "; ".join(f"**{c.get('name')}** → `{c.get('key')}`" for c in c_re[:8])
                )

    cf_keys: list[str] = []
    if replied_def:
        rk = str(replied_def.get("key") or "").strip()
        if rk:
            cf_keys.append(rk)
    field_label = (
        str(replied_def.get("name") or "").strip()
        if replied_def
        else (replied_at_field_name.strip() if (replied_at_field_name or "").strip() else "Replied at")
    )
    if cf_keys:
        diag.append(
            f"CRM: запит **GET /v1/persons** доповнено **custom_fields** (`{cf_keys[0]}`) для **{field_label}**, "
            "щоб значення потрапили в відповідь (без параметра Pipedrive часто **не повертає** кастомні поля в списку)."
        )

    uniq: list[int] = []
    seen: set[int] = set()
    for x in person_ids:
        try:
            xi = int(x)
        except (TypeError, ValueError):
            continue
        if xi not in seen:
            seen.add(xi)
            uniq.append(xi)

    # 1) Спочатку batch-запит v1 (іноді повертає поле на верхньому рівні)
    persons = fetch_persons_by_ids(api_token, uniq, custom_field_keys=cf_keys if cf_keys else None)

    replied_fnid = _person_field_numeric_id(replied_def)

    # 2) Визначаємо id, для яких поле досі порожнє
    def _val_from_person(p: dict[str, Any] | None, fk: str) -> str:
        if not p or not fk:
            return ""
        return _display_field_value(
            replied_def, _person_field_raw(p, fk, field_numeric_id=replied_fnid)
        ).strip() if replied_def else ""

    missing_ids: list[int] = []
    if replied_def:
        fk = str(replied_def.get("key") or "").strip()
        if fk:
            for pid in uniq:
                if not _val_from_person(persons.get(pid), fk):
                    missing_ids.append(pid)

    n_v2_batch_fill = 0
    # 3) Один batch-запит v2 для тих, хто не отримав значення з v1
    if missing_ids and replied_def:
        v2_persons = fetch_persons_v2_by_ids(
            api_token,
            missing_ids,
            custom_field_keys=cf_keys if cf_keys else None,
            field_numeric_id=replied_fnid,
        )
        fk = str(replied_def.get("key") or "").strip()
        for pid, pdata in v2_persons.items():
            if _val_from_person(pdata, fk):
                persons[pid] = pdata
                n_v2_batch_fill += 1
            elif pid not in persons and pdata:
                persons[pid] = pdata  # зберігаємо хоча б для діагностики

    if missing_ids:
        diag.append(
            f"CRM: для **{len(missing_ids)}** персон **{field_label}** не було у v1-відповіді — "
            f"зроблено **1 batch-запит GET /api/v2/persons?ids=…&include_fields=custom_fields**; "
            f"**{n_v2_batch_fill}** персон отримали значення."
        )
    if missing_ids and n_v2_batch_fill == 0:
        diag.append(
            f"CRM: v2 batch теж не повернув значення для **{field_label}** — "
            "поле може бути недоступне через цей токен (Outfunnel або інша інтеграція). "
            "Скористайтесь кнопкою **«Отримати JSON»** в expander нижче."
        )

    n_got = len(persons)
    if uniq and not n_got:
        diag.append(
            "CRM: за запитом id **жодної персони не повернулося** "
            "(перевірте, що в стовпці саме **Person id** з Pipedrive, а не Deal id / інший об'єкт)."
        )
    elif uniq and n_got < len(uniq):
        diag.append(
            f"CRM: API **повернуло дані лише для {n_got}** з **{len(uniq)}** унікальних id "
            "(інші id відсутні в акаунті, не персони або недоступні цьому токену)."
        )

    excluded: set[int] = set()
    reasons: dict[int, str] = {}

    for pid in uniq:
        p = persons.get(pid)
        if not p:
            continue
        ex, rlist = person_exclude_flags(
            p,
            label_id_to_text=label_map,
            mql_names=mql_set,
            replied_field_def=replied_def,
            replied_match=replied_norm_tokens,
        )
        if ex:
            reasons[pid] = "|".join(rlist) if rlist else "rule"
            excluded.add(pid)

    if replied_def and replied_norm_tokens and uniq and n_got and not excluded:
        # Показати реальні підписи Replied at з кількох персон — щоб зіставити з полем у UI.
        key = str(replied_def.get("key") or "")
        samples: list[str] = []
        if key:
            for p in list(persons.values())[:12]:
                raw = _person_field_raw(p, key, field_numeric_id=replied_fnid)
                disp = _display_field_value(replied_def, raw).strip()
                if disp and disp not in samples:
                    samples.append(disp)
                if len(samples) >= 5:
                    break
        if samples:
            want = ", ".join(sorted(replied_at_values))
            diag.append(
                f"CRM: за вашими id **ніхто не потрапив під виключення**; приклади значень **{field_label}** з API: "
                f"**{'; '.join(samples)}**. У полі «Значення Replied at» зараз: **{want}**. "
                "Якщо підпис у CRM інший — додайте його в список (порівняння нормалізує *E-mail* / *Email*)."
            )
        elif key:
            extra_parts: list[str] = []
            probe = next((persons.get(pid) for pid in uniq if persons.get(pid)), None)
            if isinstance(probe, dict):
                cf = probe.get("custom_fields")
                raw_probe = _person_field_raw(probe, key, field_numeric_id=replied_fnid)
                pid0 = probe.get("id")
                rv = repr(raw_probe)
                if len(rv) > 120:
                    rv = rv[:117] + "..."
                extra_parts.append(
                    f"Перша персона в відповіді API: **id {pid0}**; тип `custom_fields` = `{type(cf).__name__}`; "
                    f"**repr(сире поле)** = `{rv}`."
                )
                if isinstance(cf, dict) and key in cf:
                    vdot = repr(cf.get(key))
                    if len(vdot) > 80:
                        vdot = vdot[:77] + "..."
                    extra_parts.append(f"У `custom_fields` ключ **є**; значення (repr): `{vdot}`.")
                elif (
                    isinstance(cf, dict)
                    and replied_fnid is not None
                    and str(replied_fnid) in cf
                ):
                    vdot = repr(cf.get(str(replied_fnid)))
                    if len(vdot) > 80:
                        vdot = vdot[:77] + "..."
                    extra_parts.append(
                        f"У `custom_fields` ключ **числовий id** `{replied_fnid}`; значення (repr): `{vdot}`."
                    )
                elif isinstance(cf, dict) and cf:
                    kk = [str(x) for x in list(cf.keys())[:10]]
                    extra_parts.append(
                        f"Зразок інших ключів **custom_fields**: `{', '.join(kk)}` (очікуваний hash: `{key}`"
                        + (f" або числовий id: `{replied_fnid}`" if replied_fnid is not None else "")
                        + ")."
                    )
                elif isinstance(cf, list) and cf:
                    first = cf[0] if cf and isinstance(cf[0], dict) else {}
                    kprev = list(first.keys())[:14] if isinstance(first, dict) else []
                    extra_parts.append(
                        f"`custom_fields` — **список** ({len(cf)} ел.); ключі **першого** елемента: `{kprev}`. "
                        f"Шукаємо **field_id** = hash `{key}` або числовий **{replied_fnid}** з personFields."
                    )
                elif cf is None or (isinstance(cf, dict) and not cf):
                    extra_parts.append(
                        "**custom_fields** відсутній або порожній — для цього токена Pipedrive часто **не віддає** кастомні значення "
                        "на картці персони (обмеження видимості поля / тип сутності **Lead** замість **Person** / інший продукт). "
                        "Перевірте в браузері той самий id: чи це саме **Person**, а не **Lead**."
                    )
            if not manual_key:
                c_re = _person_fields_replied_name_candidates(fields)
                c_line = [c for c in c_re if str(c.get("key") or "") != key][:6]
                if c_line:
                    extra_parts.append(
                        "Інші поля Person з **replied** у назві (якщо в UI інше поле): "
                        + "; ".join(f"**{c.get('name')}** → `{c.get('key')}`" for c in c_line)
                    )
            suffix = (" " + " ".join(extra_parts)) if extra_parts else ""
            diag.append(
                f"CRM: поле **{field_label}** (`{key}`) у відповіді по ваших персонах **без значення (порожнє)** — "
                "перевірте в UI, що **Replied at** стоїть на **контакті (Person)** і заповнене для цих id."
                f"{suffix}"
            )

    return excluded, reasons, diag
