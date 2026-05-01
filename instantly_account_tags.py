"""
Теги в «Accounts to use» (Instantly Options).

У вебі можна вибрати не лише поштові акаунти, а й теги (на кшталт gmail_gtm_group_05).
Правило: тег, який ви обираєте тут, має бути **тим самим**, що й пул акаунтів для надсилання
(той самий label, що показується в multiselect).

У API v2 для кампанії використовується PATCH з полем `email_tag_list` — масив **UUID** тегів.
Текстовий label спершу резолвиться через GET /api/v2/custom-tags?search=...
"""

from __future__ import annotations

from split_engine import OUTPUT_SHEETS

# Необов’язково: якщо для кошика вказано непорожній рядок, він **замінює** значення
# з поля провайдера в сайдбарі лише для цього сегмента (тонко: USA vs Europe).
# Приклад: Gmail USA — один тег, Gmail Europe — інший, не змінюючи основне поле «Gmail/Other» для обох.
ACCOUNT_TAG_LABEL_BY_BUCKET: dict[str, str] = {name: "" for name in OUTPUT_SHEETS}
