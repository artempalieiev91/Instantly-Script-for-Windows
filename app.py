"""
Streamlit: розбиття таблиці за логікою Instantly (провайдер × USA/Europe).
"""

from __future__ import annotations

import io
import json
import os
import re
import zipfile
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

import instantly_import_mapping as imap
from instantly_api import (
    InstantlyClient,
    parse_email_list_field,
    partition_sender_pool_lines,
)
from instantly_workflow import run_full_pipeline
import sheets_export
from split_engine import (
    DEFAULT_PROVIDER_COLUMN,
    extract_gid_from_url,
    extract_spreadsheet_id,
    google_sheet_csv_export_url,
    split_dataframe,
    summary_lines,
)

_IMPORT_CV_ALIASES_PATH = Path(__file__).resolve().parent / imap.USER_IMPORT_CV_ALIASES_FILENAME


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
            "Google повернув 403. Зробіть таблицю доступною «Будь-хто за посиланням може переглядати», "
            "або завантажте CSV вручну (Файл → Завантажити → значення, розділені комами)."
        )
    if r.status_code != 200:
        raise RuntimeError(f"Не вдалося завантажити таблицю (HTTP {r.status_code}).")
    return load_csv_from_bytes(r.content)


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


def _pool_preview_caption(raw: str) -> str:
    em, tg = partition_sender_pool_lines(parse_email_list_field(str(raw or "")))
    parts: list[str] = []
    if em:
        parts.append(f"email: **{len(em)}** шт.")
    if tg:
        parts.append("теги: **" + "**, **".join(tg) + "**")
    return " · ".join(parts) if parts else "— *порожньо (відправників скинуть для сегментів цього провайдера)*"


def main() -> None:
    st.set_page_config(page_title="Розбиття листа — Instantly SDR", layout="wide")
    _ensure_import_cv_aliases_text_state()
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
            "Необов’язкове тонке регулювання USA/EU — у файлі **`instantly_account_tags.py`** (`ACCOUNT_TAG_LABEL_BY_BUCKET`)."
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
    st.title("Instantly SDR: розбиття таблиці + API")
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
        url = st.text_input(
            "URL або ID таблиці",
            placeholder="https://docs.google.com/spreadsheets/d/.../edit#gid=0",
            help="Якщо в URL є gid= — буде взято потрібний лист. Інакше експортується перший лист.",
        )
        if st.button("Завантажити й розбити", type="primary"):
            if not (url or "").strip():
                st.warning("Вставте посилання або ID.")
            else:
                try:
                    st.session_state.loaded_df = load_sheet_from_url(url.strip())
                    st.session_state.loaded_src = "url"
                    st.session_state.last_sheet_url = url.strip()
                    if not str(st.session_state.get("gs_export_target") or "").strip():
                        st.session_state.gs_export_target = url.strip()
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

    lines = summary_lines(buckets, unmatched)
    st.subheader("Підсумок")
    st.text("\n".join(lines))

    st.download_button(
        "Завантажити пакет ZIP (усі сегменти як окремі CSV)",
        data=build_split_export_zip(df, buckets, unmatched),
        file_name="instantly_split_export.zip",
        mime="application/zip",
        key="dl_zip_all",
        help="Для ручного імпорту в Instantly або Google: у ZIP окремі .csv по кожному сегменту.",
    )
    try:
        _xlsx = build_split_export_xlsx(df, buckets, unmatched)
        _xlsx_err = None
    except Exception as e:
        _xlsx = None
        _xlsx_err = str(e)
    if _xlsx is not None:
        st.download_button(
            "Завантажити Excel (.xlsx, усі вкладки в одному файлі)",
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

    names = list(buckets.keys())
    tabs = st.tabs(names + (["_Unmatched_split"] if unmatched is not None and len(unmatched) else []))

    for i, name in enumerate(names):
        with tabs[i]:
            st.dataframe(buckets[name], use_container_width=True, height=400)
            st.download_button(
                label=f"Завантажити «{name}».csv",
                data=df_to_csv_bytes(buckets[name]),
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

    st.divider()
    st.subheader("Назад у Google Таблицю")
    st.info(
        "Якщо ви просто зробили таблицю **доступною для редагування** — це означає, що **люди** можуть "
        "міняти її в браузері. Програма на вашому ПК **не може** сама записувати туди дані: Google для цього "
        "вимагає окремі ключі доступу (технічний акаунт або вхід через Google). "
        "**Без них** використовуйте кнопки «Завантажити … csv» вище й у Google Таблиці: **Файл → Імпорт** "
        "на потрібний аркуш (або створіть аркуші з тими ж назвами, що й сегменти, і імпортуйте в кожен)."
    )
    with st.expander(
        "Опційно: автоматичний запис аркушів через Google API (потрібен service account у Google Cloud)",
        expanded=False,
    ):
        st.caption(
            "Лише якщо готові створити service account, дати йому доступ до таблиці й завантажити JSON. "
            "Інакше цей блок можна ігнорувати — достатньо імпорту CSV."
        )
        st.file_uploader(
            "JSON service account",
            type=["json"],
            key="gs_sa_upload",
            help="Поділіться таблицею з email …@….iam.gserviceaccount.com (роль: редактор). "
            "Або задайте змінну GOOGLE_APPLICATION_CREDENTIALS — шлях до того ж файлу.",
        )
        gcol1, gcol2 = st.columns((3, 1))
        with gcol1:
            st.text_input(
                "URL або ID таблиці для запису",
                key="gs_export_target",
                placeholder="https://docs.google.com/spreadsheets/d/…",
                help="Очищаються й заповнюються аркуші сегментів + _Unmatched_split.",
            )
        with gcol2:
            st.write("")
            st.write("")
            if st.button("Записати в таблицю", type="secondary", key="btn_gs_export"):
                target = str(st.session_state.get("gs_export_target") or "").strip()
                up_sa = st.session_state.get("gs_sa_upload")
                svc = None
                try:
                    if up_sa is not None:
                        svc = sheets_export.build_service_from_json_bytes(up_sa.getvalue())
                    else:
                        gac = (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
                        if gac and Path(gac).is_file():
                            svc = sheets_export.build_service_from_json_path(gac)
                    if svc is None:
                        st.error(
                            "Завантажте JSON service account вище або задайте GOOGLE_APPLICATION_CREDENTIALS."
                        )
                    elif not target:
                        st.warning("Вкажіть URL або ID таблиці.")
                    else:
                        sid = extract_spreadsheet_id(target)
                        with st.spinner("Оновлення Google Sheets…"):
                            sheets_export.export_split_to_spreadsheet(svc, sid, buckets, unmatched)
                        st.success("Аркуші оновлено.")
                except Exception as ex:
                    st.error(str(ex))

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
                        buckets,
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
                except Exception as ex:
                    st.session_state.pop("instantly_pipeline_log", None)
                    st.error(str(ex))
    if st.session_state.get("instantly_pipeline_log"):
        st.subheader("Журнал виконання")
        st.dataframe(
            pd.DataFrame(st.session_state["instantly_pipeline_log"]),
            use_container_width=True,
            height=min(400, 80 + 28 * len(st.session_state["instantly_pipeline_log"])),
        )


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
