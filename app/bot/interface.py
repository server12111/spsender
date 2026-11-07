import asyncio
import json
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from zoneinfo import ZoneInfo

from app.services.core import RUNTIME_LIMITS, DomainError, dump, row
from app.services.greetings import greeting_pool

from .labels import PERIODS, SECTIONS, label

RUNTIME_TITLES = {
    "batch_size": "Размер пакета",
    "concurrency": "Параллельных заданий",
    "batch_interval": "Пауза между пакетами, с",
    "planning_batch_size": "Пар за цикл планирования",
    "max_pending_jobs": "Максимум ожидающих заданий",
    "max_retries": "Повторных попыток",
    "retry_base": "Начальная задержка повтора, с",
    "retry_max": "Максимальная задержка повтора, с",
}


def grid(buttons):
    """Two readable columns; callers keep navigation and destructive actions separate."""
    return [buttons[index : index + 2] for index in range(0, len(buttons), 2)]


@dataclass
class View:
    text: str
    buttons: list[list[tuple[str, str]]] = field(default_factory=list)
    media: str | None = None
    media_kind: str | None = None

    def __post_init__(self):
        self.buttons = [part for group in self.buttons for part in grid(group)]


def serialized(function):
    @wraps(function)
    async def guarded(self, user, *args, **kwargs):
        self.e.authorize(user)
        if function.__name__ == "callback" and args and str(args[0]).startswith("u:"):
            action = await self.e.db.run(
                self.e.db.one, "SELECT action FROM ui_actions WHERE token=? AND admin_id=?", (str(args[0])[2:], user)
            )
            if action and action["action"] in {
                "campaign_status",
                "account_action",
                "issue",
                "account_remove",
                "resume_chat",
                "archive_chat",
                "cancel",
                "main",
            }:
                return await function(self, user, *args, **kwargs)
        async with self._user_locks[user]:
            return await function(self, user, *args, **kwargs)

    return guarded


class Interface:
    """UI operations return serializable views; both aiogram and tests use this code."""

    def __init__(self, engine, transport=None, authorizer=None):
        self.e, self.transport, self.auth = engine, transport, authorizer
        self._user_locks = {user: asyncio.Lock() for user in engine.config.admin_ids}

    def button(self, user, text, action, data=None, once=False, ttl=86400):
        data = dict(data or {})
        if action.startswith("wizard_") or action in {"import_confirm", "import_flag"}:
            state = self.state(user)
            if state:
                data["_form_revision"] = state["data"].get("_revision")
        token = secrets.token_urlsafe(12)
        with self.e.db.tx() as c:
            c.execute(
                "INSERT INTO ui_actions VALUES (?,?,?,?,?,?,NULL)",
                (token, user, action, dump(data or {}), self.e.clock() + ttl, once),
            )
        return text, "u:" + token

    def nav(self, user, section=None):
        buttons = []
        if section:
            buttons.append(self.button(user, "◀️ Назад", "list", {"section": section}))
        buttons.append(self.button(user, "🏠 Главное меню", "main"))
        return [buttons]

    def cancel(self, user):
        return [[self.button(user, "❌ Отмена", "cancel", once=True)]]

    def set_state(self, user, state, data=None):
        data = {**(data or {}), "_revision": secrets.token_hex(8)}
        with self.e.db.tx() as c:
            c.execute(
                "INSERT INTO ui_states VALUES (?,?,?,?) ON CONFLICT(admin_id) DO UPDATE SET state=excluded.state,data=excluded.data,expires_at=excluded.expires_at",
                (user, state, dump(data or {}), self.e.clock() + 1800),
            )

    def state(self, user):
        record = self.e.db.one("SELECT * FROM ui_states WHERE admin_id=? AND expires_at>?", (user, self.e.clock()))
        if record:
            record["data"] = json.loads(record["data"])
        return record

    def clear(self, user, expected_revision=None):
        with self.e.db.tx() as c:
            if expected_revision is not None:
                return c.execute(
                    "DELETE FROM ui_states WHERE admin_id=? AND json_extract(data,'$._revision')=?",
                    (user, expected_revision),
                ).rowcount
            return c.execute("DELETE FROM ui_states WHERE admin_id=?", (user,)).rowcount

    def time(self, timestamp):
        if timestamp is None:
            return "—"
        return (
            datetime.fromtimestamp(timestamp, timezone.utc)
            .astimezone(ZoneInfo(self.e.get_setting("timezone")))
            .strftime("%d.%m %H:%M")
        )

    def main(self, user, notice=""):
        self.e.authorize(user)
        return View(
            (notice + "\n\n" if notice else "")
            + "🤖 FE AutoSender 2.0\n\n"
            + ("🧪 Тестовый режим · без реальных отправок\n\n" if self.e.config.mode == "dry-run" else "")
            + "Выберите раздел ↓",
            grid([self.button(user, title, "list", {"section": section}) for section, title in SECTIONS.items()]),
        )

    def queue_settings(self, user):
        self.e.authorize(user)
        options = self.e.runtime_options()
        titles = {
            "batch_size": "Размер пакета",
            "concurrency": "Параллельно",
            "batch_interval": "Пауза пакетов",
            "planning_batch_size": "Пар за цикл",
            "max_pending_jobs": "Лимит очереди",
            "max_retries": "Повторов",
            "retry_base": "Задержка от",
            "retry_max": "Задержка до",
        }
        return View(
            "📦 Очередь и повторы\n\nНажмите параметр, чтобы изменить. Задержки — в секундах.",
            grid(
                [self.button(user, f"{titles[key]}: {value}", "form", {"field": key}) for key, value in options.items()]
            )
            + self.nav(user, "settings"),
        )

    def listing(self, user, section, page=0, search="", status="", period="today"):
        self.e.authorize(user)
        page = max(0, int(page))
        if section == "preparation":
            return self.preparation(user, page)
        if section == "stats":
            return self.stats(user, period)
        if section == "settings":
            return View(
                "⚙️ Настройки\n\n🌐 "
                + self.e.get_setting("timezone")
                + "\n⏱ Пауза подключений: "
                + self.e.get_setting("join_pause")
                + " с\n\nСмены по местному времени\nA · 00–06 и 12–18\nB · 06–12 и 18–00",
                [
                    [
                        self.button(user, "🌐 Часовой пояс", "form", {"field": "timezone"}),
                        self.button(user, "⏱ Пауза 50–100 с", "form", {"field": "join_pause"}),
                    ]
                ]
                + [[self.button(user, "📦 Очередь и повторы", "queue_settings")]]
                + self.nav(user),
            )
        if section == "errors":
            return self.errors(user, period, page)
        if section in {"greetings", "auto"}:
            records = self.e.db.all("SELECT id,name FROM campaigns WHERE status<>'REMOVED' ORDER BY id")
            buttons = grid(
                [
                    self.button(
                        user, f"№{r['id']} {r['name'][:22]}", "templates_hub", {"id": r["id"], "section": section}
                    )
                    for r in records[page * 8 : page * 8 + 8]
                ]
            )
            if section == "greetings":
                buttons.append([self.button(user, "👋 Пул 30 эмодзи / 100 фраз", "pool")])
            self.pagination(buttons, user, "list", {"section": section}, page, len(records) > page * 8 + 8)
            return View(
                SECTIONS[section] + "\n\n" + ("Выберите рассылку." if records else "Сначала создайте рассылку."),
                buttons + self.nav(user),
            )
        tables = {
            "campaigns": ("campaigns", "name", "status"),
            "accounts": ("accounts", "name", "account_health"),
            "chats": ("chat_pool", "title", "status"),
            "pairs": ("pairs", "id", "status"),
        }
        if section not in tables:
            raise DomainError("Раздел не найден")
        table, name, status_column = tables[section]
        where = f" WHERE CAST({name} AS TEXT) LIKE ?"
        args = ["%" + search + "%"]
        if status:
            where += f" AND {status_column}=?"
            args.append(status)
        if section == "campaigns":
            where += " AND status<>'REMOVED'"
        if section == "accounts":
            where += " AND account_health<>'REMOVED'"
        if section == "pairs":
            where += " AND account_a_id IS NOT NULL AND EXISTS(SELECT 1 FROM campaigns ca "
            where += "WHERE ca.id=pairs.campaign_id AND ca.status<>'REMOVED')"
        records = self.e.db.all(f"SELECT * FROM {table}" + where + " ORDER BY id LIMIT 9 OFFSET ?", (*args, page * 8))
        count = self.e.db.scalar(f"SELECT count(*) FROM {table}" + where, args)
        buttons = []
        for r in records[:8]:
            title = (
                str(r.get(name, r["id"]))
                if section != "pairs"
                else f"A №{r['account_a_id']} / B №{r['account_b_id'] or '—'}"
            )
            status_icon = {
                "RUNNING": "🟢",
                "READY": "🟢",
                "OK": "🟢",
                "FREE": "🟢",
                "DRAFT": "📝",
                "PAUSED": "⏸",
                "STOPPED": "⏹",
                "PREPARING": "⏳",
                "WAITING_PARTNER": "⏳",
                "WAITING_CHATS": "⏳",
                "WAITING_REPLACEMENT": "⏳",
                "ASSIGNED": "🔗",
                "DISCONNECTED": "🔌",
                "FROZEN": "🔴",
                "RESTRICTED": "🔴",
            }.get(r[status_column], "⚠️")
            if section == "accounts" and r["paused"]:
                status_icon = "⏸"
            buttons.append(
                [
                    self.button(
                        user,
                        f"{status_icon} №{r['id']} {title[:20]}",
                        "card",
                        {"section": section, "id": r["id"], "period": period},
                    )
                ]
            )
        buttons = grid([button for group in buttons for button in group])
        if section in {"accounts", "campaigns", "chats"}:
            buttons.append(
                [
                    self.button(
                        user,
                        "➕ "
                        + {"accounts": "Добавить", "campaigns": "Создать", "chats": "Импорт ссылок / TXT"}[section],
                        "add",
                        {"section": section},
                        once=True,
                    )
                ]
            )
        if section == "pairs":
            buttons.append([self.button(user, "⚙️ Настройка пар", "pair_tools")])
        if section == "accounts":
            buttons.append([self.button(user, "📁 .session / tdata", "account_import", once=True)])
        buttons.append(
            [
                self.button(user, "🔎 Поиск", "search", {"section": section}),
                self.button(user, "🔽 Статус", "filters", {"section": section}),
            ]
        )
        self.pagination(
            buttons,
            user,
            "list",
            {"section": section, "search": search, "status": status, "period": period},
            page,
            len(records) > 8,
        )
        extra = ""
        if section == "accounts":
            stats = self.e.account_overview()
            extra = f"\nГотово: {stats['ready']} · Подготовка: {stats['preparing']} · Пауза: {stats['paused']}\n"
            extra += " · ".join(f"{label(k)}: {v}" for k, v in stats["health"].items())
        if section == "pairs":
            extra = "\n\n🔄 Аккаунты одной рассылки объединяются в пары автоматически."
        if not records:
            extra += "\n\n" + (
                "По этому фильтру ничего не найдено."
                if search or status
                else {
                    "accounts": "Добавьте первый аккаунт кнопкой ниже.",
                    "campaigns": "Создайте первую рассылку кнопкой ниже.",
                    "chats": "Импортируйте чаты ссылками или TXT-файлом.",
                    "pairs": "Добавьте два аккаунта в одну рассылку — пара появится здесь.",
                }[section]
            )
        heading = f"{SECTIONS[section]} · {count}"
        if page or count > 8:
            heading += f"\nСтраница {page + 1}"
        return View(heading + extra, buttons + self.nav(user))

    def preparation_hint(self, account):
        now = self.e.clock()
        if account["account_health"] != "OK":
            return label(account["account_health"])
        if account["paused"]:
            return "Аккаунт на паузе"
        if account["flood_until"] > now:
            return "Лимит Telegram до " + self.time(account["flood_until"])
        quiet_until = account["onboarding_started_at"] + 86400
        if now < quiet_until:
            return "Тихий период до " + self.time(quiet_until)
        if not account["campaign_id"]:
            return "Назначьте аккаунту рассылку"
        if account["campaign_status"] != "RUNNING":
            return "Рассылка: " + label(account["campaign_status"])
        if account["uncertain"]:
            return "Нужно сверить неизвестный результат"
        if not account["prepared_at"]:
            return "Ожидает проверки подключения"
        if not account["account_b_id"]:
            return "Ожидает второго аккаунта в рассылке"
        if account["manual_required"]:
            return "Нужна ручная проверка чата"
        if account["onboarding_stage"] == "READY":
            return "Подготовка завершена"
        if account["onboarding_stage"] == "SILENT_24H":
            return "Тихий период завершён · ожидает обработки"
        return label(account["onboarding_stage"])

    def preparation(self, user, page=0):
        self.e.authorize(user)
        page = max(0, int(page))
        summary = self.e.account_overview()
        accounts = self.e.db.all(
            "SELECT a.*,m.campaign_id,ca.status AS campaign_status,p.account_b_id,"
            "EXISTS(SELECT 1 FROM dispatch_jobs j WHERE j.pair_id=p.id AND j.status='UNKNOWN') AS uncertain,"
            "EXISTS(SELECT 1 FROM account_chat_access ac JOIN pair_chats pc ON pc.chat_pool_id=ac.chat_pool_id "
            "WHERE ac.account_id=a.id AND pc.pair_id=p.id AND pc.removed_at IS NULL AND ac.manual_required=1) AS manual_required "
            "FROM accounts a LEFT JOIN campaign_members m ON m.account_id=a.id "
            "LEFT JOIN campaigns ca ON ca.id=m.campaign_id "
            "LEFT JOIN pairs p ON p.account_a_id=a.id OR p.account_b_id=a.id "
            "WHERE a.account_health<>'REMOVED' ORDER BY a.id LIMIT 9 OFFSET ?",
            (page * 8,),
        )
        text = (
            "⏳ Подготовка аккаунтов\n\nЗапускается автоматически после добавления.\n"
            "Внешние действия — только у запущенной рассылки.\n\n"
            f"Готово: {summary['ready']} · Подготовка: {summary['preparing']} · Пауза: {summary['paused']}"
        )
        for account in accounts[:8]:
            text += f"\n\n№{account['id']} · {account['name'][:40]}\n{self.preparation_hint(account)}"
        if not accounts:
            text += "\n\n" + ("На этой странице аккаунтов нет." if page else "Добавьте первый аккаунт.")
        buttons = grid(
            [
                self.button(
                    user,
                    f"👤 №{account['id']} {account['name'][:20]}",
                    "card",
                    {"section": "accounts", "id": account["id"]},
                )
                for account in accounts[:8]
            ]
        )
        self.pagination(buttons, user, "list", {"section": "preparation"}, page, len(accounts) > 8)
        buttons += [
            [
                self.button(user, "🤝 Пул прогрева", "warmup_pool"),
                self.button(user, "🔥 Каналы прогрева", "warmup_channels"),
            ],
            [
                self.button(user, "💬 Пул чатов", "list", {"section": "chats"}),
                self.button(user, "👋 Приветствия", "list", {"section": "greetings"}),
            ],
        ]
        return View(text, buttons + self.nav(user))

    def warmup_pool_view(self, user, page=0):
        self.e.authorize(user)
        accounts = self.e.warmup_pool(page)
        text = "🤝 Пул прогрева\n\nВыбери свои подключённые аккаунты, которые будут собеседниками. Остальные аккаунты прогреваются автоматически и могут общаться друг с другом.\n"
        buttons = []
        for account in accounts[:8]:
            buttons.append(
                [
                    self.button(
                        user,
                        ("✅ " if account["selected"] else "➕ ") + account["name"][:28],
                        "warmup_partner",
                        {
                            "id": account["id"],
                            "enabled": not account["selected"],
                            "expected": bool(account["selected"]),
                            "page": page,
                        },
                        True,
                    )
                ]
            )
        self.pagination(buttons, user, "warmup_pool", {}, page, len(accounts) > 8)
        buttons.append([self.button(user, "◀️ Подготовка", "list", {"section": "preparation"})])
        return View(text if accounts else text + "Добавьте аккаунты в бота.", buttons)

    def warmup_channels_view(self, user, page=0):
        self.e.authorize(user)
        channels = self.e.db.all("SELECT * FROM warmup_channels ORDER BY id LIMIT 9 OFFSET ?", (max(0, page) * 8,))
        text = "🔥 Каналы прогрева\n\nОтдельный пул каналов для старого механизма прогрева. Заполни его своими разрешёнными целями.\n"
        buttons = [[self.button(user, "➕ Добавить", "warmup_channels_add", once=True)]]
        for channel in channels[:8]:
            text += "\n" + channel["username"]
            buttons.append(
                [
                    self.button(
                        user, "🗑 " + channel["username"][:35], "warmup_channel_remove", {"id": channel["id"]}, True
                    )
                ]
            )
        self.pagination(buttons, user, "warmup_channels", {}, page, len(channels) > 8)
        buttons.append([self.button(user, "◀️ Подготовка", "list", {"section": "preparation"})])
        return View(text[:3900], buttons)

    def legacy_unknown_view(self, user):
        self.e.authorize(user)
        records = self.e.db.all(
            "SELECT id,account_id,method FROM legacy_rpc_journal WHERE status='UNKNOWN' ORDER BY id LIMIT 8"
        )
        text = "❓ Сверка операций прогрева\nПроверь историю Telegram перед решением. Операция автоматически не повторяется.\n"
        buttons = []
        for item in records:
            text += f"\n№{item['id']} · аккаунт №{item['account_id']} · {item['method']}"
            buttons.append(
                [
                    self.button(
                        user, f"№{item['id']} выполнена", "legacy_resolve", {"id": item["id"], "performed": True}, True
                    ),
                    self.button(
                        user,
                        f"№{item['id']} не выполнена",
                        "legacy_resolve",
                        {"id": item["id"], "performed": False},
                        True,
                    ),
                ]
            )
        return View(text if records else text + "Неопределённых операций нет.", buttons + self.nav(user, "errors"))

    def pagination(self, buttons, user, action, data, page, more):
        nav = []
        if page > 0:
            nav.append(self.button(user, "◀️", action, {**data, "page": page - 1}))
        if more:
            nav.append(self.button(user, "▶️", action, {**data, "page": page + 1}))
        if nav:
            buttons.append(nav)

    def card(self, user, section, item_id, period="today"):
        self.e.authorize(user)
        stats = self.e.statistics(period, section, item_id)
        metric = f"\n📨 Подтверждено: {stats['confirmed']} · Ошибки: {stats['errors']} · Неизвестно: {stats['unknown']}\nПериод: {PERIODS[period]}"
        metric += "\nПоследняя активность: " + self.time(stats["last_activity"])

        def b(title, action, data=None, once=False):
            return self.button(user, title, action, {"id": item_id, "section": section, **(data or {})}, once=once)

        if section == "campaigns":
            r = self.e.db.one("SELECT * FROM campaigns WHERE id=?", (item_id,))
            if not r or r["status"] == "REMOVED":
                raise DomainError("Рассылка не найдена")
            count = self.e.db.scalar(
                "SELECT count(*) FROM pairs WHERE campaign_id=? AND account_a_id IS NOT NULL", (item_id,)
            )
            text = (
                f"📣 {r['name']} · №{item_id}\n{label(r['status'])}\n\nПар: {count} · Чатов на пару: {r['chat_limit']}\nИнтервал в чат: {r['interval_seconds']} с · Между чатами: {r['chat_delay_seconds']} с"
                + metric
            )
            template_count = self.e.db.scalar(
                "SELECT count(*) FROM campaign_templates WHERE campaign_id=? AND version=?", (item_id, r["version"])
            )
            pauses = self.e.db.scalar(
                "SELECT count(*) FROM audit_events WHERE campaign_id=? AND event IN ('CAMPAIGN_PAUSED','CAMPAIGN_STOPPED')",
                (item_id,),
            )
            text += f"\nШаблонов: {template_count} · Пауз / остановок: {pauses}"
            text += f"\nЖдать новых сообщений в чате: {'да' if r['skip_unseen'] else 'нет'}"
            buttons = [
                [
                    b(
                        "▶️ Запустить",
                        "confirm",
                        {"do": "campaign_status", "status": "RUNNING", "expected": r["status"], "v": r["version"]},
                    ),
                    b(
                        "⏸ Приостановить",
                        "campaign_status",
                        {"status": "PAUSED", "expected": r["status"], "v": r["version"]},
                        True,
                    ),
                ],
                [
                    b(
                        "⛔ Остановить рассылку",
                        "campaign_status",
                        {"status": "STOPPED", "expected": r["status"], "v": r["version"]},
                        True,
                    )
                ],
                [
                    b("✏️ Название", "form", {"field": "campaign_name"}),
                    b("⏱ Интервал в чат", "form", {"field": "interval"}),
                    b("⏳ Между чатами", "form", {"field": "chat_delay"}),
                ],
                [
                    b("💬 Чатов на пару", "form", {"field": "limit"}),
                    b(
                        "🔀 Случайно" if r["selection"] == "sequential" else "🔢 По порядку",
                        "option",
                        {
                            "field": "selection",
                            "value": "random" if r["selection"] == "sequential" else "sequential",
                            "v": r["version"],
                        },
                        True,
                    ),
                ],
                [
                    b("📝 Тексты и медиа", "templates", {"kind": "mailing"}),
                    b("👁 Предпросмотр", "preview", {"kind": "mailing"}),
                ],
                [
                    b("👥 Аккаунты", "choose_campaign_accounts"),
                    b("👋 Приветствия", "templates_hub", {"section": "greetings"}),
                ],
                [
                    b("💬 Автоответы", "templates_hub", {"section": "auto"}),
                    b("🔄 Обмен чатов", "rotation"),
                ],
                [
                    b(
                        "🗑 Удалить",
                        "confirm",
                        {"do": "campaign_status", "status": "REMOVED", "expected": r["status"], "v": r["version"]},
                    ),
                ],
            ]
            buttons.append(
                [
                    b(
                        "👁 Ждать новых сообщений: " + ("вкл" if r["skip_unseen"] else "выкл"),
                        "option",
                        {"field": "skip_unseen", "value": not r["skip_unseen"], "v": r["version"]},
                        True,
                    )
                ]
            )
            # Only show the state transition that makes sense on this screen.
            start, pause = buttons[0]
            stop = buttons[1][0]
            buttons[:2] = (
                [[pause, stop]]
                if r["status"] == "RUNNING"
                else [[start, stop]]
                if r["status"] == "PAUSED"
                else [[start]]
            )
        elif section == "accounts":
            r = self.e.db.one("SELECT * FROM accounts WHERE id=?", (item_id,))
            if not r:
                raise DomainError("Аккаунт не найден")
            member = self.e.db.one(
                "SELECT ca.* FROM campaigns ca JOIN campaign_members m ON m.campaign_id=ca.id WHERE m.account_id=?",
                (item_id,),
            )
            pair = self.e.db.one(
                "SELECT p.* FROM pairs p JOIN campaigns ca ON ca.id=p.campaign_id "
                "WHERE ca.status<>'REMOVED' AND (p.account_a_id=? OR p.account_b_id=?)",
                (item_id, item_id),
            )
            shift = "Ожидает партнёра"
            n = 0
            if pair:
                card = self.e.pair_card(pair["id"])
                shift = (
                    ("На смене" if card["active_account_id"] == item_id else "Вне смены")
                    if pair["account_b_id"]
                    else "Ожидает партнёра"
                )
                n = len(card["chats"])
            text = f"👤 №{item_id} · {r['name']}\n{r['phone_mask']} · Telegram ID: {r['telegram_id'] or '—'}\n"
            text += f"Рассылка: {member['name'] if member else 'Свободный пул'} · Пара: №{pair['id'] if pair else '—'}\n{shift} · Общих чатов: {n}\n"
            text += f"Подготовка: {label(r['onboarding_stage'])}\nНа этапе: {max(0, int((self.e.clock() - r['stage_started_at']) // 3600))} ч\nСостояние: {label(r['account_health'])} · {'На паузе' if r['paused'] else 'Включён'}"
            if r["flood_until"] > self.e.clock():
                text += "\nЛимит до " + self.time(r["flood_until"])
            text += metric
            buttons = [
                [
                    b("✏️ Переименовать", "form", {"field": "account_name"}),
                    b("📣 Рассылка", "account_campaign"),
                ],
                [
                    b(
                        "▶️ Снять паузу" if r["paused"] else "⏸ Пауза",
                        "account_action",
                        {"do": "resume" if r["paused"] else "pause"},
                        True,
                    )
                ],
                [
                    b("🔑 Переподключить", "reconnect", once=True),
                    b("🔄 Проверить", "check_account", once=True),
                ],
                [
                    b("🔌 Отключить", "account_action", {"do": "disconnect"}, True),
                    b("🗑 Удалить", "confirm", {"do": "account_remove"}),
                ],
            ]
            if r["account_health"] in {"FROZEN", "RESTRICTED"}:
                buttons.append([b("✅ Ограничение снято: подтвердить вручную", "confirm", {"do": "account_health_ok"})])
            if not member and not self.e.db.scalar("SELECT count(*) FROM campaigns WHERE status<>'REMOVED'"):
                buttons.append([self.button(user, "➕ Создать рассылку", "add", {"section": "campaigns"}, True)])
            if pair:
                partner_id = pair["account_b_id"] if pair["account_a_id"] == item_id else pair["account_a_id"]
                partner = self.e.db.one("SELECT name FROM accounts WHERE id=?", (partner_id,)) if partner_id else None
                text += "\n\n🔄 " + (
                    f"Партнёр: {partner['name']}" if partner else "Партнёр добавится автоматически из этой рассылки."
                )
                buttons[1].insert(
                    0, self.button(user, "🔄 Открыть пару", "card", {"section": "pairs", "id": pair["id"]})
                )
            else:
                text += "\n\n🔄 Назначьте рассылку — бот сам сформирует пару."
        elif section == "pairs":
            r = self.e.pair_card(item_id)
            members = self.e.db.all(
                "SELECT id,name FROM accounts WHERE id IN (?,?)", (r["account_a_id"], r["account_b_id"])
            )
            names = {account["id"]: account["name"] for account in members}
            text = (
                f"🔄 Пара №{item_id} · {label(r['status'])}\n\n"
                f"A · {names.get(r['account_a_id'], '—')}\n"
                f"B · {names.get(r['account_b_id'], 'Ожидает второго аккаунта')}\n"
                f"\nСейчас смена {r['role']}\n"
            )
            text += (
                f"Общих чатов: {len(r['chats'])}/{r['chat_limit']} · Активно: {r['active_count']}\nАктивное время: {r['elapsed'] / 3600:.1f} ч"
                + metric
            )
            text += "\nЧаты: " + ", ".join(f"№{chat['chat_pool_id']}" for chat in r["chats"][:30])
            replacements = self.e.db.scalar(
                "SELECT count(*) FROM chat_issue_decisions d JOIN chat_issues i ON i.id=d.issue_id WHERE i.pair_id=? AND d.replacement_chat_id IS NOT NULL",
                (item_id,),
            )
            text += f"\nЗамен: {replacements}"
            buttons = [[b("💬 Общие чаты", "pair_chats", {"page": 0})]]
            members_buttons = [
                self.button(user, f"👤 Аккаунт {role}", "card", {"section": "accounts", "id": account_id})
                for role, account_id in (("A", r["account_a_id"]), ("B", r["account_b_id"]))
                if account_id
            ]
            buttons += grid(members_buttons)
        elif section == "chats":
            r = self.e.db.one("SELECT * FROM chat_pool WHERE id=?", (item_id,))
            if not r:
                raise DomainError("Чат не найден")
            assignment = self.e.db.one(
                "SELECT * FROM pair_chats WHERE chat_pool_id=? AND removed_at IS NULL", (item_id,)
            )
            text = f"💬 №{item_id} · {r['title']}\n{r['link']}\nID {r['chat_id']} · Тема {r['topic_id'] or '—'}\n{label(r['status'])}\n"
            text += f"Право публикации: {'Подтверждено' if r['approved'] else 'Не подтверждено'}\nВступление: {'Разрешено' if r['join_allowed'] else 'Только проверка членства'}\n"
            if assignment:
                text += f"Пара №{assignment['pair_id']} · Назначен {self.time(assignment['assigned_at'])}\nПоследняя отправка: {self.time(assignment['last_sent_at'])}\nCooldown до {self.time(assignment['cooldown_until'])}\nПоследняя ошибка: {label(assignment['last_error'])}\n"
                text += (
                    "Интервал: "
                    + (
                        f"{assignment['interval_seconds']} с"
                        if assignment["interval_seconds"]
                        else "общий интервал рассылки"
                    )
                    + "\n"
                )
            text += metric
            access_rows = self.e.db.all(
                "SELECT aca.*,a.name FROM account_chat_access aca JOIN accounts a ON a.id=aca.account_id WHERE aca.chat_pool_id=? ORDER BY a.id",
                (item_id,),
            )
            text += "\n\nПрава аккаунтов:\n" + "\n".join(
                f"{a['name']}: {'запись разрешена' if a['can_write'] else 'нет подтверждения записи'} · {self.time(a['last_checked_at'])}"
                for a in access_rows[:10]
            )
            buttons = [
                [b("✅ Проверено вручную: возобновить", "confirm", {"do": "resume_chat"})],
                [b("✏️ Права и правила", "chat_permissions"), b("🗑 Удалить из пула", "confirm", {"do": "archive_chat"})],
                [b("📜 История решений", "chat_history")],
            ]
            if assignment:
                buttons.append([b("⏱ Интервал этого чата", "form", {"field": "chat_interval"})])
        else:
            raise DomainError("Карточка не найдена")
        buttons += [
            [
                b("📅 Период", "periods", {"return": "card", "period": period}),
                b("⚠️ Ошибки", "entity_errors", {"period": period}),
            ]
        ]
        return View(text, buttons + self.nav(user, section))

    def stats(self, user, period="today", details=False):
        s = self.e.statistics(period)
        text = f"📊 Статистика · {PERIODS[period]}\n\n👤 Аккаунтов: {s['totals']['accounts']} · На смене: {s['on_shift']}\n"
        text += f"Готово: {s['ready']} · Подготовка: {s['preparing']} · Пауза: {s['paused_accounts']}\n🔄 Пар: {s['totals']['pairs']}\n"
        text += f"💬 Чатов: {s['totals']['chat_pool']} · Назначено: {s['assigned_chats']} · Активная доля: {s['active_chats']}\n"
        text += f"📣 Рассылок: {s['totals']['campaigns']}\n\n📨 Подтверждено: {s['confirmed']}\nПопыток: {s['attempts']} · Ошибок: {s['errors']}\nНужна сверка: {s['unknown']} · Проблемных чатов: {s['pending_issues']}"
        if details:
            for table, title in (
                ("accounts", "Аккаунты"),
                ("pairs", "Пары"),
                ("chat_pool", "Чаты"),
                ("campaigns", "Рассылки"),
            ):
                if s["statuses"][table]:
                    text += (
                        "\n\n" + title + "\n" + " · ".join(f"{label(k)}: {v}" for k, v in s["statuses"][table].items())
                    )
            text += "\n\nОтправки / ошибки по периодам\n" + "\n".join(
                f"{PERIODS[key]}: {values['confirmed']} / {values['errors']}"
                for key, values in self.e.period_metrics().items()
            )
            text += "\n\nПо дням\n" + (
                "\n".join(f"{day}: {value} {'▰' * min(20, value)}" for day, value in s["days"].items())
                or "Нет отправок"
            )
            text += "\n\nПоследние события\n" + "\n".join(
                f"{self.time(r['created_at'])} · {label(r['event'])}" for r in s["last_events"][:5]
            )
        controls = [
            self.button(user, SECTIONS[section], "list", {"section": section, "period": period})
            for section in ("accounts", "pairs", "chats", "campaigns", "errors")
        ]
        controls.append(
            self.button(
                user,
                "📊 Кратко" if details else "📈 Подробности",
                "stats_details",
                {"period": period, "details": not details},
            )
        )
        return View(
            text[:3900],
            grid(controls) + [[self.button(user, "📅 Период", "periods", {"return": "stats"})]] + self.nav(user),
        )

    def errors(self, user, period="all", page=0, section=None, item_id=None):
        records = self.e.error_history(period, section, item_id, page)
        text = f"⚠️ История ошибок · {PERIODS[period]}\n" + "\n".join(
            f"№{r['id']} {self.time(r['created_at'])} · {label(r['event'])}\nАкк. {r['account_id'] or '—'} · Пара {r['pair_id'] or '—'} · Чат {r['chat_pool_id'] or '—'}"
            for r in records[:8]
        )
        buttons = [
            [
                self.button(user, "🕐 Проблемные чаты", "issues", {"page": 0}),
                self.button(user, "❓ Нужна сверка", "unknown", {"page": 0}),
            ],
            [self.button(user, "📅 Период", "periods", {"return": "errors", "section": section, "id": item_id})],
            [self.button(user, "📬 Уведомления", "outbox"), self.button(user, "🩺 Диагностика", "diagnostics")],
        ]
        if self.e.db.scalar("SELECT count(*) FROM legacy_rpc_journal WHERE status='UNKNOWN'"):
            buttons.append([self.button(user, "❓ Сверка прогрева", "legacy_unknown")])
        self.pagination(
            buttons,
            user,
            "entity_errors" if section else "list",
            {"section": section or "errors", "id": item_id, "period": period},
            page,
            len(records) > 8,
        )
        return View(text if records else text + "Нет ошибок за период", buttons + self.nav(user))

    def diagnostics(self, user):
        self.e.authorize(user)
        jobs = self.e.db.all("SELECT status,count(*) AS value FROM dispatch_jobs GROUP BY status")
        logs = self.e.db.all("SELECT * FROM service_diagnostics ORDER BY id DESC LIMIT 12")
        text = "🩺 Диагностика\n" + "\n".join(f"{label(r['status'])}: {r['value']}" for r in jobs)
        text += "\n\nПоследние сбои служб:\n" + "\n".join(
            f"{self.time(r['created_at'])} · {r['component']} · {r['error_code']}" for r in logs
        )
        text += (
            "\n\nФайловый журнал: "
            + str(Path(self.e.config.database_path).parent / "logs" / "diagnostics.log")
            + " (ротация 3 × 5 МБ)."
        )
        return View(text[:3900], self.nav(user, "errors"))

    def rotation_view(self, user, campaign_id, page=0):
        self.e.authorize(user)
        record = self.e.rotation_card(campaign_id, page)
        campaign = record["campaign"]
        enabled = bool(campaign["rotation_enabled"])
        text = (
            f"🔄 Обмен чатов · {campaign['name']}\n{'Включён' if enabled else 'Выключен'} · раз в 7 дней\n"
            f"Следующий: {self.time(campaign['rotation_next_at']) if enabled else '—'}\n"
            f"Подготовка за {campaign['rotation_lead_days']} дня · сообщений на аккаунт в чат: {campaign['rotation_messages']}\n\n"
            "Аккаунты в парах, тексты и смены A/B сохраняются. Меняются только наборы чатов внутри этой рассылки. "
            "Непарный остаток не участвует. Неготовый обмен пропускается.\n\n"
            f"Автоподписки по требованиям ботов группы: {'включены' if campaign['auto_subscribe'] else 'выключены'}. "
            "CAPTCHA и опросы — вручную."
        )
        buttons = [
            [
                self.button(
                    user,
                    "⏸ Выключить обмен" if enabled else "▶️ Включить обмен",
                    "rotation_toggle",
                    {"id": campaign_id, "enabled": not enabled, "expected": enabled},
                    True,
                )
            ],
            [
                self.button(user, "⏳ Срок подготовки", "form", {"id": campaign_id, "field": "rotation_lead"}),
                self.button(user, "📝 Сообщений 0–2", "form", {"id": campaign_id, "field": "rotation_messages"}),
            ],
            [
                self.button(
                    user,
                    "🔗 Автоподписки: " + ("вкл" if campaign["auto_subscribe"] else "выкл"),
                    "option",
                    {
                        "id": campaign_id,
                        "field": "auto_subscribe",
                        "value": not campaign["auto_subscribe"],
                        "v": campaign["version"],
                        "return": "rotation",
                    },
                    True,
                )
            ],
        ]
        failed_memberships = self.e.db.scalar(
            "SELECT count(*) FROM membership_requests WHERE campaign_id=? AND status='FAILED'", (campaign_id,)
        )
        if failed_memberships:
            buttons.append(
                [
                    self.button(
                        user,
                        f"🔁 Повторить подписки ({failed_memberships})",
                        "membership_retry",
                        {"id": campaign_id},
                        True,
                    )
                ]
            )
        for exchange in record["exchanges"][:6]:
            text += f"\n\n№{exchange['id']} · Пары {exchange['left_pair_id']} ↔ {exchange['right_pair_id']} · {label(exchange['status'])}"
            buttons.append(
                [self.button(user, f"🔎 Обмен №{exchange['id']}", "rotation_detail", {"id": exchange["id"]})]
            )
        self.pagination(buttons, user, "rotation", {"id": campaign_id}, page, len(record["exchanges"]) > 6)
        return View(
            text[:3900],
            buttons + [[self.button(user, "📣 Рассылка", "card", {"id": campaign_id, "section": "campaigns"})]],
        )

    def rotation_detail(self, user, exchange_id, page=0):
        self.e.authorize(user)
        exchange = self.e.db.one(
            "SELECT x.*,r.campaign_id,r.due_at FROM rotation_exchanges x JOIN rotation_cycles r "
            "ON r.id=x.cycle_id WHERE x.id=?",
            (exchange_id,),
        )
        if not exchange:
            raise DomainError("Обмен не найден")
        progress = self.e.db.all(
            "SELECT p.*,t.chat_pool_id,cp.title FROM rotation_progress p JOIN rotation_targets t ON t.id=p.target_id "
            "JOIN chat_pool cp ON cp.id=t.chat_pool_id WHERE t.exchange_id=? ORDER BY p.id LIMIT 9 OFFSET ?",
            (exchange_id, max(0, page) * 8),
        )
        text = f"🔄 Обмен №{exchange_id} · {label(exchange['status'])}\nДата: {self.time(exchange['due_at'])}\n"
        for item in progress[:8]:
            text += (
                f"\nАккаунт №{item['account_id']} → {item['title'][:45]}\n"
                f"Вступил: {'да' if item['joined'] else 'нет'} · Права: {'да' if item['can_write'] else 'ожидание'} · "
                f"Сообщений: {item['message_index']}"
                + (f" · {label(item['last_error'])}" if item["last_error"] else "")
                + "\n"
            )
        buttons = []
        if exchange["status"] in {"PREPARING", "READY"}:
            buttons.append(
                [
                    self.button(
                        user, "✅ Проверено вручную: повторить подготовку", "rotation_retry", {"id": exchange_id}, True
                    )
                ]
            )
        self.pagination(buttons, user, "rotation_detail", {"id": exchange_id}, page, len(progress) > 8)
        buttons.append([self.button(user, "◀️ Обмен чатов", "rotation", {"id": exchange["campaign_id"]})])
        return View(text[:3900], buttons)

    def issues(self, user, page=0, records=None):
        if records is None:
            issues = self.e.db.all(
                "SELECT i.*,cp.title,cp.link FROM chat_issues i JOIN chat_pool cp ON cp.id=i.chat_pool_id WHERE i.status='OPEN' ORDER BY i.id LIMIT 7 OFFSET ?",
                (max(0, page) * 6,),
            )
        else:
            issues = records[page * 6 : page * 6 + 7]
        text, buttons = "🕐 Проблемные чаты · часовая сводка\n", []
        for issue in issues[:6]:
            text += f"\n№{issue['id']} · {issue['title'][:80]}\n{issue['link']}\nПара №{issue['pair_id']} · {label(issue['reason'])}\nПервое: {self.time(issue['first_at'])} · Последнее: {self.time(issue['last_at'])}\n"
            buttons.append(
                [
                    self.button(
                        user,
                        f"🗑 Убрать №{issue['id']}",
                        "issue",
                        {"id": issue["id"], "do": "REMOVE"},
                        True,
                        ttl=30 * 86400,
                    ),
                    self.button(
                        user,
                        f"✅ Оставить №{issue['id']}",
                        "issue",
                        {"id": issue["id"], "do": "KEEP"},
                        True,
                        ttl=30 * 86400,
                    ),
                ]
            )
        self.pagination(buttons, user, "issues", {}, page, len(issues) > 6)
        return View(text if issues else text + "Проблем нет", buttons + self.nav(user, "errors"))

    def templates_hub(self, user, campaign_id, section):
        r = self.e.db.one("SELECT * FROM campaigns WHERE id=? AND status<>'REMOVED'", (campaign_id,))
        if not r:
            raise DomainError("Рассылка не найдена")

        def b(title, action, data=None, once=False):
            return self.button(user, title, action, {"id": campaign_id, **(data or {})}, once)

        buttons = []
        kinds = (
            {"greeting": "Основные приветствия"}
            if section == "greetings"
            else {
                "dm_first": "Первое входящее ЛС",
                "dm_always": "Каждое входящее ЛС",
            }
        )
        options = {
            "greeting": "greeting_enabled",
            "dm_first": "dm_first_enabled",
            "dm_always": "dm_always_enabled",
        }
        for kind, title in kinds.items():
            enabled = r[options[kind]]
            buttons.append(
                [
                    b(
                        ("✅ " if enabled else "⭕ ") + title,
                        "option",
                        {"field": options[kind], "value": not enabled, "v": r["version"], "return": section},
                        True,
                    )
                ]
            )
            buttons.append(
                [b("📝 Тексты", "templates", {"kind": kind}), b("👁 Предпросмотр", "preview", {"kind": kind})]
            )
        if section == "greetings":
            buttons.append(
                [
                    b(
                        "Формат: " + ("3 отдельных" if r["greeting_split"] else "1 объединённое"),
                        "option",
                        {
                            "field": "greeting_split",
                            "value": not r["greeting_split"],
                            "v": r["version"],
                            "return": section,
                        },
                        True,
                    )
                ]
            )
            buttons.append([b("👋 Пул 30 / 100", "pool")])
        else:
            buttons.append(
                [
                    b(
                        "🔔 Уведомления ЛС: " + ("Вкл" if r["notify_dm"] else "Выкл"),
                        "option",
                        {"field": "notify_dm", "value": not r["notify_dm"], "v": r["version"], "return": section},
                        True,
                    )
                ]
            )
            buttons.append([b("⏱ Cooldown ответов", "form", {"field": "reply_cooldown"})])
            buttons.append(
                [
                    b("👋 Привет в ЛС", "form", {"field": "dm_greeting_phrase"}),
                    b("🎟 Стикер: " + r["dm_sticker_emoji"], "form", {"field": "dm_sticker_emoji"}),
                ]
            )
            buttons.append([b("🧹 Очистить историю автоответов", "confirm", {"do": "clear_reply_history"})])
        return View(
            SECTIONS[section]
            + f" · {r['name']}\nНастройки общие для всей кампании.\n"
            + (
                "По умолчанию одно объединённое сообщение. Раздельный формат требует разрешения в карточке чата."
                if section == "greetings"
                else "Первое ЛС: стикер → приветствие → текст, паузы 3–4 с. Только в ответ на входящее. Свои аккаунты и боты исключены."
            ),
            buttons + self.nav(user, section),
        )

    def template_list(self, user, campaign_id, kind, page=0):
        with self.e.db.read() as c:
            version = self.e.campaign(c, campaign_id)["version"]
            templates = self.e.templates(campaign_id, kind, version, c)
        buttons = []
        text = "📝 Шаблоны\n"
        for index, t in enumerate(templates[page * 6 : page * 6 + 6], page * 6):
            text += f"\n{index + 1}. {t['text'][:180]}" + (" [медиа]" if t["media_path"] else "")
            buttons.append(
                [
                    self.button(user, f"👁 №{index + 1}", "preview", {"id": campaign_id, "kind": kind, "index": index}),
                    self.button(
                        user,
                        f"✏️ №{index + 1}",
                        "template_edit",
                        {"id": campaign_id, "kind": kind, "index": index, "v": version},
                    ),
                    self.button(
                        user,
                        f"🗑 №{index + 1}",
                        "confirm",
                        {
                            "id": campaign_id,
                            "kind": kind,
                            "index": index,
                            "do": "template_remove",
                            "v": version,
                        },
                    ),
                ]
            )
        buttons.append(
            [
                self.button(
                    user,
                    "➕ Текст / фото / видео / файл",
                    "template_edit",
                    {"id": campaign_id, "kind": kind, "v": version},
                )
            ]
        )
        self.pagination(
            buttons, user, "templates", {"id": campaign_id, "kind": kind}, page, len(templates) > page * 6 + 6
        )
        return View(text if templates else text + "Добавьте первый шаблон", buttons + self.nav(user, "campaigns"))

    def wizard(self, user, data):
        data.setdefault("_draft_id", secrets.token_hex(16))
        self.set_state(user, "campaign_wizard", data)
        step = data["step"]
        buttons = []
        prompts = {
            1: "Название рассылки",
            2: "Добавьте один или несколько текстов. Можно прикреплять фото, видео и файлы с подписью.",
            3: "Медиа необязательно: отправьте вложение для последнего текста или продолжите.",
            4: "Интервал в один чат и пауза между чатами: например 300 / 3 секунд. Минимум 30 / 3.",
            5: "Выберите свободные аккаунты. Пары сформируются автоматически.",
            6: "N — общее число чатов на одну пару",
            7: "Включите нужные функции. Тексты приветствий и ответов доступны в карточке рассылки.",
            8: "Предпросмотр",
            9: "Сохранить рассылку? Она будет создана черновиком.",
        }
        text = f"📣 Создание · Шаг {step} из 9\n{prompts[step]}"
        if step in {2, 3}:
            text += f"\nТекстов: {len(data.get('texts', []))}"
            buttons.append([self.button(user, "Далее ▶️", "wizard_next", once=True)])
        if step == 5:
            available = self.e.db.all(
                "SELECT a.id,a.name FROM accounts a LEFT JOIN campaign_members m ON m.account_id=a.id WHERE m.account_id IS NULL AND a.account_health<>'REMOVED' ORDER BY a.id"
            )
            page = data.get("page", 0)
            for account in available[page * 6 : page * 6 + 6]:
                selected = account["id"] in data.get("accounts", [])
                buttons.append(
                    [
                        self.button(
                            user,
                            ("✅ " if selected else "⭕ ") + account["name"],
                            "wizard_pick",
                            {"id": account["id"]},
                            True,
                        )
                    ]
                )
            buttons = grid([b for group in buttons for b in group])
            if not available:
                text += "\nСвободных аккаунтов нет. Их можно добавить после создания рассылки."
            self.pagination(buttons, user, "wizard_page", {}, page, len(available) > page * 6 + 6)
            buttons.append([self.button(user, "Далее ▶️", "wizard_next", once=True)])
        if step == 7:
            for flag, title in (
                ("greeting_enabled", "Приветствия"),
                ("dm_first_enabled", "Первый ответ в ЛС"),
                ("dm_always_enabled", "Каждый ответ в ЛС"),
            ):
                buttons.append(
                    [
                        self.button(
                            user, ("✅ " if data.get(flag) else "⭕ ") + title, "wizard_flag", {"flag": flag}, True
                        )
                    ]
                )
            buttons = grid([b for group in buttons for b in group])
            buttons.append([self.button(user, "Далее ▶️", "wizard_next", once=True)])
        if step in {8, 9}:
            text += f"\n\n{data['name']}\nИнтервал: {data['interval']} с · N={data['limit']}\nАккаунтов: {len(data.get('accounts', []))}\n"
            text += "\n\n".join(
                t["text"][:500] + (" [медиа]" if t.get("media_path") else "") for t in data["texts"][:5]
            )
            buttons.append(
                [
                    self.button(
                        user,
                        "Сохранить ✅" if step == 9 else "Далее ▶️",
                        "wizard_save" if step == 9 else "wizard_next",
                        once=True,
                    )
                ]
            )
        footer = self.cancel(user)[0]
        if step > 1:
            footer.insert(0, self.button(user, "◀️ Назад", "wizard_back", once=True))
        return View(text[:3900], buttons + [footer])

    def _expect_version(self, data):
        if "v" in data:
            current = self.e.db.one("SELECT * FROM campaigns WHERE id=?", (data["id"],))
            if (
                not current
                or current["version"] != data["v"]
                or ("expected" in data and current["status"] != data["expected"])
            ):
                raise DomainError("Кнопка устарела. Откройте карточку заново")

    def _consume_action(self, user, callback_data):
        if not callback_data.startswith("u:"):
            raise DomainError("Кнопка устарела")
        with self.e.db.tx() as c:
            token = row(
                c,
                "SELECT * FROM ui_actions WHERE token=? AND admin_id=? AND expires_at>?",
                (callback_data[2:], user, self.e.clock()),
            )
            if not token or token["consumed_at"] is not None:
                raise DomainError("Кнопка устарела или уже использована")
            data = json.loads(token["data"])
            if "_form_revision" in data:
                state = row(c, "SELECT * FROM ui_states WHERE admin_id=? AND expires_at>?", (user, self.e.clock()))
                if not state or json.loads(state["data"]).get("_revision") != data["_form_revision"]:
                    raise DomainError("Кнопка предыдущего шага устарела")
            if token["once_only"]:
                c.execute("UPDATE ui_actions SET consumed_at=? WHERE token=?", (self.e.clock(), token["token"]))
        return token

    @serialized
    async def callback(self, user, callback_data):
        token = await self.e.db.run(self._consume_action, user, callback_data)
        action, data = token["action"], json.loads(token["data"])
        await self.e.db.run(self._expect_version, data)
        section, item_id = data.get("section"), data.get("id")
        if action == "account_import":
            await self.e.db.run(self.set_state, user, "account_import", {"name": "Импортированный"})
            return View(
                "Отправьте собственный .session (Telethon/Pyrogram) или ZIP с tdata. Файл будет удалён после обработки.",
                await self.e.db.run(self.cancel, user),
            )
        if action == "warmup_pool":
            return await self.e.db.run(self.warmup_pool_view, user, data.get("page", 0))
        if action == "warmup_partner":
            await self.e.db.run(self.e.set_warmup_partner, item_id, data["enabled"], data["expected"])
            return await self.e.db.run(self.warmup_pool_view, user, data.get("page", 0))
        if action == "warmup_channels":
            return await self.e.db.run(self.warmup_channels_view, user, data.get("page", 0))
        if action == "warmup_channels_add":
            await self.e.db.run(self.set_state, user, "warmup_channels")
            return View(
                "Отправьте ссылки на каналы прогрева или @username, по одной на строку.",
                await self.e.db.run(self.cancel, user),
            )
        if action == "warmup_channel_remove":
            await self.e.db.run(self.e.remove_warmup_channel, item_id)
            return await self.e.db.run(self.warmup_channels_view, user)
        if action == "legacy_unknown":
            return await self.e.db.run(self.legacy_unknown_view, user)
        if action == "legacy_resolve":
            await self.e.db.run(self.e.resolve_legacy_unknown, item_id, data["performed"])
            return await self.e.db.run(self.legacy_unknown_view, user)
        if action == "rotation":
            return await self.e.db.run(self.rotation_view, user, item_id, data.get("page", 0))
        if action == "rotation_detail":
            return await self.e.db.run(self.rotation_detail, user, item_id, data.get("page", 0))
        if action == "rotation_toggle":
            campaign = await self.e.db.run(self.e.db.one, "SELECT * FROM campaigns WHERE id=?", (item_id,))
            await self.e.db.run(
                self.e.set_rotation,
                item_id,
                data["enabled"],
                campaign["rotation_lead_days"],
                campaign["rotation_messages"],
                expected_enabled=data["expected"],
            )
            return await self.e.db.run(self.rotation_view, user, item_id)
        if action == "rotation_retry":
            await self.e.db.run(self.e.retry_rotation, item_id)
            return await self.e.db.run(self.rotation_detail, user, item_id)
        if action == "membership_retry":
            await self.e.db.run(self.e.retry_memberships, item_id)
            return await self.e.db.run(self.rotation_view, user, item_id)
        if action == "stats_details":
            return await self.e.db.run(self.stats, user, data.get("period", "today"), data.get("details", True))
        if action == "queue_settings":
            return await self.e.db.run(self.queue_settings, user)
        if action == "pair_tools":
            return View(
                "⚙️ Настройка пар\n\nПары собираются сами внутри рассылки. Ручное связывание — только при необходимости.",
                [[await self.e.db.run(self.button, user, "🔗 Связать вручную", "pair_choose")]]
                + await self.e.db.run(self.nav, user, "pairs"),
            )
        if action == "diagnostics":
            return await self.e.db.run(self.diagnostics, user)
        if action in {"main", "cancel"}:
            await self.e.db.run(self.clear, user)
            if self.auth:
                await self.auth.cancel(user)
            return await self.e.db.run(self.main, user, "Отменено" if action == "cancel" else "")
        if action == "list":
            return await self.e.db.run(
                self.listing,
                user,
                section,
                data.get("page", 0),
                data.get("search", ""),
                data.get("status", ""),
                data.get("period", "today"),
            )
        if action == "card":
            return await self.e.db.run(self.card, user, section, item_id, data.get("period", "today"))
        if action == "filters":
            tables = {
                "campaigns": ("campaigns", "status"),
                "accounts": ("accounts", "account_health"),
                "chats": ("chat_pool", "status"),
                "pairs": ("pairs", "status"),
            }
            table, col = tables[section]
            values = await self.e.db.run(self.e.db.all, f"SELECT DISTINCT {col} AS status FROM {table}")
            return View(
                "🔽 Фильтр статуса",
                grid(
                    [
                        await self.e.db.run(
                            self.button, user, label(r["status"]), "list", {"section": section, "status": r["status"]}
                        )
                        for r in values
                    ]
                )
                + [[await self.e.db.run(self.button, user, "Все", "list", {"section": section})]]
                + await self.e.db.run(self.nav, user, section),
            )
        if action in {"search", "form"}:
            await self.e.db.run(self.set_state, user, action, data)
            prompts = {
                "timezone": "Часовой пояс IANA, например Europe/Kyiv",
                "join_pause": "Пауза 50–100 секунд",
                "campaign_name": "Название рассылки",
                "account_name": "Название аккаунта",
                "interval": "Интервал повторной отправки в один чат, минимум 30 секунд",
                "chat_delay": "Пауза между разными чатами, минимум 3 секунды",
                "chat_interval": "Интервал этого чата: минимум 30 секунд; 0 — общий интервал рассылки",
                "limit": "N общих чатов на пару",
                "reply_cooldown": "Cooldown ответов в секундах",
                "dm_greeting_phrase": "Короткое приветствие для ЛС (до 200 символов)",
                "dm_sticker_emoji": "Эмодзи для выбора стикера Telegram, например 👋",
                "rotation_lead": "За сколько дней готовиться к обмену: 1–6 (обычно 3)",
                "rotation_messages": "Сколько сообщений того же текста отправить заранее: 0–2 на аккаунт в чат",
            }
            return View(
                "Шаг 1 из 1\n"
                + (
                    "Введите строку поиска"
                    if action == "search"
                    else prompts.get(data["field"], RUNTIME_TITLES.get(data["field"], "Введите значение"))
                ),
                await self.e.db.run(self.cancel, user),
            )
        if action == "confirm":
            return View(
                "Подтвердите действие: "
                + {
                    "campaign_status": "изменить состояние рассылки",
                    "account_remove": "удалить аккаунт и его сессию",
                    "account_health_ok": "ограничение аккаунта действительно снято; разрешить работу",
                    "clear_reply_history": "сбросить историю срабатываний автоответов; журнал отправок сохранится",
                    "resume_chat": "права публикации проверены, ручная проверка пройдена; возобновить чат",
                    "archive_chat": "удалить чат из активного пула",
                    "template_remove": "удалить шаблон",
                    "unknown_sent": "отправка действительно подтверждена вручную",
                    "unknown_not_sent": "проверено вручную: сообщение не отправлено",
                }[data["do"]]
                + (
                    "\nБудут разрешены действия всех готовых пар кампании в своих сменах."
                    if data.get("status") == "RUNNING"
                    else ""
                ),
                [[await self.e.db.run(self.button, user, "✅ Подтвердить", data["do"], data, True)]]
                + await self.e.db.run(self.nav, user, section),
            )
        if action == "campaign_status":
            await self.e.db.run(
                self.e.campaign_status,
                item_id,
                data["status"],
                confirmed=True,
                expected_status=data.get("expected"),
                expected_version=data.get("v"),
            )
            return (
                await self.e.db.run(self.main, user, "Рассылка удалена")
                if data["status"] == "REMOVED"
                else await self.e.db.run(self.card, user, "campaigns", item_id)
            )
        if action == "account_action":
            await self.e.db.run(self.e.update_account, item_id, data["do"])
            return await self.e.db.run(self.card, user, "accounts", item_id)
        if action == "account_remove":
            await self.e.db.run(self.e.update_account, item_id, "remove")
            return await self.e.db.run(self.listing, user, "accounts")
        if action == "account_health_ok":
            await self.e.db.run(self.e.acknowledge_restriction_resolved, item_id)
            return await self.e.db.run(self.card, user, "accounts", item_id)
        if action == "clear_reply_history":
            await self.e.db.run(self.e.clear_reply_history, item_id)
            return await self.e.db.run(self.templates_hub, user, item_id, "auto")
        if action == "option":
            await self.e.db.run(
                self.e.edit_campaign, item_id, expected_version=data.get("v"), **{data["field"]: data["value"]}
            )
            if data.get("return") == "rotation":
                return await self.e.db.run(self.rotation_view, user, item_id)
            return (
                await self.e.db.run(self.templates_hub, user, item_id, data["return"])
                if "return" in data
                else await self.e.db.run(self.card, user, "campaigns", item_id)
            )
        if action == "templates_hub":
            return await self.e.db.run(self.templates_hub, user, item_id, section)
        if action == "templates":
            return await self.e.db.run(self.template_list, user, item_id, data["kind"], data.get("page", 0))
        if action == "template_edit":
            if "index" in data and "v" not in data:
                raise DomainError("Кнопка устарела. Откройте список шаблонов заново")
            if "v" not in data:
                data["v"] = await self.e.db.run(
                    self.e.db.scalar, "SELECT version FROM campaigns WHERE id=?", (item_id,)
                )
            await self.e.db.run(self.set_state, user, "template_edit", data)
            return View(
                "Шаг 1 из 1\nОтправьте текст, фото, видео или файл с подписью.", await self.e.db.run(self.cancel, user)
            )
        if action == "template_remove":
            templates = await self.e.db.run(self.e.templates, item_id, data["kind"])
            if data["kind"] == "mailing" and len(templates) == 1:
                raise DomainError("У рассылки должен остаться хотя бы один текст")
            templates.pop(data["index"])
            await self.e.db.run(self.e.edit_campaign, item_id, data["kind"], templates, expected_version=data.get("v"))
            return await self.e.db.run(self.template_list, user, item_id, data["kind"])
        if action == "preview":
            templates = await self.e.db.run(self.e.templates, item_id, data["kind"])
            if not templates:
                raise DomainError("Сначала добавьте текст")
            index = data.get("index", 0) % len(templates)
            template = templates[index]
            buttons = (
                [[await self.e.db.run(self.button, user, "Следующий ▶️", "preview", {**data, "index": index + 1})]]
                if len(templates) > 1
                else []
            )
            return View(
                f"Предпросмотр {index + 1}/{len(templates)}\n\n{template['text']}",
                buttons + await self.e.db.run(self.nav, user, "campaigns"),
                template["media_path"],
                template["media_kind"],
            )
        if action == "add":
            if section == "campaigns":
                return await self.e.db.run(self.wizard, user, {"step": 1, "texts": [], "accounts": []})
            await self.e.db.run(self.set_state, user, "add_account_name" if section == "accounts" else "import_chats")
            return View(
                "Шаг 1 из 4\nНазвание аккаунта"
                if section == "accounts"
                else "Шаг 1 из 2\nОтправьте ссылки / ID по одной на строку или TXT-файл UTF-8.\nПрава публикации подтверждаются на следующем шаге.",
                await self.e.db.run(self.cancel, user),
            )
        if action.startswith("wizard_"):
            state = await self.e.db.run(self.state, user)
            if not state or state["state"] != "campaign_wizard":
                raise DomainError("Мастер закрыт. Начните заново")
            form = state["data"]
            if action == "wizard_next":
                if form["step"] == 2 and not form["texts"]:
                    raise DomainError("Добавьте хотя бы один текст")
                form["step"] = min(9, form["step"] + 1)
            elif action == "wizard_back":
                form["step"] = max(1, form["step"] - 1)
            elif action == "wizard_page":
                form["page"] = data["page"]
            elif action == "wizard_pick":
                selected = form.setdefault("accounts", [])
                selected.remove(item_id) if item_id in selected else selected.append(item_id)
            elif action == "wizard_flag":
                form[data["flag"]] = not form.get(data["flag"], False)
            elif action == "wizard_save":
                if form["step"] != 9:
                    raise DomainError("Сначала завершите мастер")
                campaign_id = await self.e.db.run(
                    self.e.create_campaign,
                    form["name"],
                    form["texts"],
                    form["interval"],
                    form["limit"],
                    account_ids=form["accounts"],
                    creation_key=form["_draft_id"],
                    chat_delay_seconds=form.get("chat_delay", 3),
                    **{k: form.get(k, False) for k in ("greeting_enabled", "dm_first_enabled", "dm_always_enabled")},
                )
                await self.e.db.run(self.clear, user)
                return await self.e.db.run(self.card, user, "campaigns", campaign_id)
            return await self.e.db.run(self.wizard, user, form)
        if action in {"account_campaign", "choose_campaign_accounts"}:
            page = max(0, int(data.get("page", 0)))
            if action == "account_campaign":
                records = await self.e.db.run(
                    self.e.db.all,
                    "SELECT id,name FROM campaigns WHERE status<>'REMOVED' ORDER BY id LIMIT 9 OFFSET ?",
                    (page * 8,),
                )
                buttons = [
                    [
                        await self.e.db.run(
                            self.button,
                            user,
                            r["name"],
                            "assign_account",
                            {"id": item_id, "campaign_id": r["id"]},
                            True,
                        )
                    ]
                    for r in records[:8]
                ]
            else:
                records = await self.e.db.run(
                    self.e.db.all,
                    "SELECT a.id,a.name FROM accounts a LEFT JOIN campaign_members m ON m.account_id=a.id "
                    "WHERE a.account_health<>'REMOVED' AND m.account_id IS NULL ORDER BY a.id LIMIT 9 OFFSET ?",
                    (page * 8,),
                )
                buttons = [
                    [
                        await self.e.db.run(
                            self.button,
                            user,
                            r["name"],
                            "assign_account",
                            {"id": r["id"], "campaign_id": item_id},
                            True,
                        )
                    ]
                    for r in records[:8]
                ]
            buttons = grid([b for group in buttons for b in group])
            self.pagination(buttons, user, action, data, page, len(records) > 8)
            text = (
                "📣 Выберите рассылку"
                if action == "account_campaign"
                else "👥 Свободные аккаунты\nПары собираются автоматически."
            )
            if not records:
                text += "\n\n" + (
                    "На этой странице ничего нет."
                    if page
                    else "Список пуст — сначала добавьте "
                    + ("рассылку." if action == "account_campaign" else "аккаунт.")
                )
                buttons.append(
                    [
                        await self.e.db.run(
                            self.button,
                            user,
                            "➕ Добавить",
                            "add",
                            {"section": "campaigns" if action == "account_campaign" else "accounts"},
                            True,
                        )
                    ]
                )
            return View(
                text,
                buttons + await self.e.db.run(self.nav, user, "accounts"),
            )
        if action == "assign_account":
            await self.e.db.run(self.e.assign_account, item_id, data["campaign_id"])
            return await self.e.db.run(self.card, user, "accounts", item_id)
        if action == "pair_choose":
            records = await self.e.db.run(
                self.e.db.all,
                "SELECT a.* FROM accounts a JOIN campaign_members m ON m.account_id=a.id "
                "WHERE a.account_health<>'REMOVED' AND a.id<>? "
                "AND NOT EXISTS (SELECT 1 FROM pairs p WHERE p.account_b_id IS NOT NULL "
                "AND (p.account_a_id=a.id OR p.account_b_id=a.id)) "
                "AND (? IS NULL OR m.campaign_id=(SELECT campaign_id FROM campaign_members WHERE account_id=?)) "
                "ORDER BY a.id",
                (data.get("a", -1), data.get("a"), data.get("a")),
            )
            page = data.get("page", 0)
            buttons = [
                [
                    await self.e.db.run(
                        self.button,
                        user,
                        r["name"],
                        "pair_choose" if not data.get("a") else "pair_link",
                        {"a": data.get("a") or r["id"], "b": r["id"]} if data.get("a") else {"a": r["id"]},
                        True,
                    )
                ]
                for r in records[page * 8 : page * 8 + 8]
            ]
            self.pagination(buttons, user, "pair_choose", data, page, len(records) > page * 8 + 8)
            return View(
                ("Выберите B" if data.get("a") else "Выберите A")
                if records
                else "Нет подходящих аккаунтов для ручного связывания. Автоматические пары уже учтены.",
                grid([b for group in buttons for b in group]) + await self.e.db.run(self.nav, user, "pairs"),
            )
        if action == "pair_link":
            pair_id = await self.e.db.run(self.e.link_pair, data["a"], data["b"])
            return await self.e.db.run(self.card, user, "pairs", pair_id)
        if action == "pair_chats":
            chats = (await self.e.db.run(self.e.pair_card, item_id))["chats"]
            page = data.get("page", 0)
            buttons = [
                [
                    await self.e.db.run(
                        self.button,
                        user,
                        f"№{r['chat_pool_id']} {r['title'][:40]} · {label(r['status'])}",
                        "card",
                        {"id": r["chat_pool_id"], "section": "chats"},
                    )
                ]
                for r in chats[page * 8 : page * 8 + 8]
            ]
            self.pagination(buttons, user, "pair_chats", data, page, len(chats) > page * 8 + 8)
            return View(
                f"Общая очередь пары №{item_id}\nВсего чатов: {len(chats)}",
                grid([b for group in buttons for b in group]) + await self.e.db.run(self.nav, user, "pairs"),
            )
        if action == "reconnect":
            await self.e.db.run(self.set_state, user, "account_phone", {"account_id": item_id})
            return View(
                "Подключение · Шаг 1 из 3\nВведите номер собственного аккаунта в международном формате.",
                await self.e.db.run(self.cancel, user),
            )
        if action == "check_account":
            await self.check_account(item_id)
            return await self.e.db.run(self.card, user, "accounts", item_id)
        if action == "issue":
            replacement = await self.e.db.run(self.e.decide_issue, item_id, data["do"], user)
            notice = (
                "Чат оставлен на паузе"
                if data["do"] == "KEEP"
                else (
                    f"Чат убран. Назначена замена №{replacement}"
                    if replacement
                    else "Чат убран. Ожидаем свободную замену автоматически"
                )
            )
            view = await self.e.db.run(self.issues, user)
            view.text = notice + "\n\n" + view.text
            return view
        if action == "issues":
            return await self.e.db.run(self.issues, user, data.get("page", 0))
        if action in {"archive_chat", "resume_chat"}:
            await self.e.db.run(self.e.archive_chat, item_id) if action == "archive_chat" else await self.e.db.run(
                self.e.resume_chat, item_id
            )
            return await self.e.db.run(self.card, user, "chats", item_id)
        if action == "chat_permissions":
            r = await self.e.db.run(self.e.db.one, "SELECT * FROM chat_pool WHERE id=?", (item_id,))
            buttons = [
                [
                    await self.e.db.run(
                        self.button,
                        user,
                        ("✅ " if r[field] else "⭕ ") + title,
                        "chat_permission_toggle",
                        {"id": item_id, "field": field},
                        True,
                    )
                ]
                for field, title in (
                    ("approved", "Право на автоматизированную публикацию"),
                    ("join_allowed", "Разрешено вступление"),
                    ("greeting_allowed", "Разрешены приветствия"),
                    ("split_allowed", "Разрешены три отдельных сообщения"),
                )
            ]
            return View(
                "Права подтверждает администратор. Фактическое право записи дополнительно проверяется через Telegram.",
                buttons + await self.e.db.run(self.nav, user, "chats"),
            )
        if action == "chat_permission_toggle":
            r = await self.e.db.run(self.e.db.one, "SELECT * FROM chat_pool WHERE id=?", (item_id,))
            options = {k: r[k] for k in ("approved", "join_allowed", "greeting_allowed", "split_allowed")}
            options[data["field"]] = not options[data["field"]]
            await self.e.db.run(self.e.set_chat_permissions, item_id, **options)
            return await self.e.db.run(self.card, user, "chats", item_id)
        if action == "chat_history":
            records = await self.e.db.run(
                self.e.db.all,
                "SELECT i.*,d.action,d.decided_at,d.replacement_chat_id FROM chat_issues i LEFT JOIN chat_issue_decisions d ON d.issue_id=i.id WHERE i.chat_pool_id=? ORDER BY i.id DESC LIMIT 20",
                (item_id,),
            )
            return View(
                "📜 История решений\n"
                + "\n".join(
                    f"№{r['id']} · {label(r['action'] or r['status'])} · {self.time(r['decided_at'] or r['last_at'])} · Замена {r['replacement_chat_id'] or '—'}"
                    for r in records
                ),
                await self.e.db.run(self.nav, user, "chats"),
            )
        if action == "pool":
            pool = greeting_pool()
            page = data.get("page", 0)
            buttons = []
            self.pagination(buttons, user, "pool", {}, page, len(pool["phrases"]) > page * 20 + 20)
            return View(
                "👋 30 эмодзи\n"
                + " ".join(pool["emoji"])
                + "\n\n100 коротких фраз\n"
                + "\n".join(
                    f"{i + 1}. {phrase}"
                    for i, phrase in enumerate(pool["phrases"][page * 20 : page * 20 + 20], page * 20)
                ),
                buttons + await self.e.db.run(self.nav, user, "greetings"),
            )
        if action == "periods":
            buttons = []
            for key, title in PERIODS.items():
                target = (
                    "card"
                    if data.get("return") == "card"
                    else ("entity_errors" if data.get("section") and data.get("return") == "errors" else "list")
                )
                dest = {**data, "period": key}
                if target == "list":
                    dest["section"] = "errors" if data.get("return") == "errors" else "stats"
                buttons.append([await self.e.db.run(self.button, user, title, target, dest)])
            return View(
                "📅 Выберите период",
                grid([b for group in buttons for b in group]) + await self.e.db.run(self.nav, user),
            )
        if action == "entity_errors":
            return await self.e.db.run(
                self.errors, user, data.get("period", "all"), data.get("page", 0), section, item_id
            )
        if action == "unknown":
            records = await self.e.db.run(
                self.e.db.all, "SELECT * FROM dispatch_jobs WHERE status='UNKNOWN' ORDER BY id"
            )
            page = data.get("page", 0)
            buttons, text = (
                [],
                "❓ Нужна ручная сверка\nПроверьте историю Telegram перед решением. Неизвестный результат блокирует новые действия этого аккаунта.\n",
            )
            for r in records[page * 6 : page * 6 + 6]:
                text += f"\nЗадание №{r['id']} · Акк. №{r['account_id']} · {label(r['kind'])} · {self.time(r['started_at'])}"
                buttons.append(
                    [
                        await self.e.db.run(
                            self.button,
                            user,
                            f"№{r['id']} Отправлено",
                            "confirm",
                            {"id": r["id"], "do": "unknown_sent"},
                        ),
                        await self.e.db.run(
                            self.button,
                            user,
                            f"№{r['id']} Не отправлено",
                            "confirm",
                            {"id": r["id"], "do": "unknown_not_sent"},
                        ),
                    ]
                )
            self.pagination(buttons, user, "unknown", {}, page, len(records) > page * 6 + 6)
            return View(text, buttons + await self.e.db.run(self.nav, user, "errors"))
        if action in {"unknown_sent", "unknown_not_sent"}:
            await self.e.db.run(self.e.resolve_unknown, item_id, action == "unknown_sent")
            return await self.e.db.run(self.main, user, "Результат задания учтён")
        if action == "outbox":
            records = await self.e.db.run(
                self.e.db.all,
                "SELECT * FROM notifications_outbox WHERE status IN ('PENDING','SENDING') ORDER BY priority DESC,id LIMIT 20",
            )
            return View(
                "📬 Доставка уведомлений\n"
                + (
                    "\n".join(
                        f"№{r['id']} · Попыток: {r['attempts']} · Следующая: {self.time(r['next_attempt_at'])}"
                        for r in records
                    )
                    or "Всё доставлено"
                ),
                await self.e.db.run(self.nav, user, "errors"),
            )
        if action == "import_confirm":
            state = await self.e.db.run(self.state, user)
            if not state or state["state"] != "import_confirm":
                raise DomainError("Импорт закрыт")
            form = state["data"]
            options = {k: bool(form.get(k)) for k in ("approved", "join_allowed", "greeting_allowed", "split_allowed")}
            if not options["approved"]:
                raise DomainError("Подтвердите право публикации")
            count, failures = 0, []
            for value in form["lines"]:
                if not await self._import_active(user, form):
                    return await self.e.db.run(self.main, user, f"Импорт отменён. Уже обработано: {count}")
                try:
                    chat_id, topic, link, title = await self.transport.resolve_target(value)
                    if not await self._import_active(user, form):
                        return await self.e.db.run(self.main, user, f"Импорт отменён. Уже обработано: {count}")
                    await self.e.db.run(
                        self._import_chat, user, form["_revision"], chat_id, title, topic, link, options
                    )
                    count += 1
                except Exception as exc:
                    failures.append(
                        value[:60] + ": " + (str(exc) if isinstance(exc, ValueError) else "Не удалось определить чат")
                    )
            if not await self.e.db.run(self.clear, user, form["_revision"]):
                return await self.e.db.run(self.main, user, f"Импорт отменён. Уже обработано: {count}")
            view = await self.e.db.run(self.listing, user, "chats")
            view.text = (
                f"Обработано: {count}. Дубликаты объединены. Ошибок: {len(failures)}\n"
                + "\n".join(failures[:5])
                + "\n\n"
                + view.text
            )
            return view
        if action == "import_flag":
            state = await self.e.db.run(self.state, user)
            if not state or state["state"] != "import_confirm":
                raise DomainError("Импорт закрыт")
            form = state["data"]
            form[data["field"]] = not form.get(data["field"], False)
            return await self.e.db.run(self.import_confirmation, user, form)
        if action == "dm_reply":
            await self.e.db.run(self.set_state, user, "manual_dm", data)
            return View(
                "Шаг 1 из 1\nНапишите ответ или приложите медиа. Окно ответа — 10 минут с входящего сообщения.",
                await self.e.db.run(self.cancel, user),
            )
        raise DomainError("Кнопка больше не поддерживается")

    @serialized
    async def import_account(self, user, filename, content):
        state = await self.e.db.run(self.state, user)
        if not state or state["state"] != "account_import":
            raise DomainError("Сначала откройте импорт аккаунта")
        result = await self.auth.import_file(user, filename, content)
        if result is None:
            await self.e.db.run(self.set_state, user, "account_import_password", state["data"])
            return View(
                "Введите пароль 2FA импортируемого аккаунта. Он не сохраняется.", await self.e.db.run(self.cancel, user)
            )
        return await self._save_authorized_account(user, result, state["data"])

    async def _save_authorized_account(self, user, result, form):
        phone, session, telegram_id = result
        if form.get("account_id"):
            await self.e.db.run(self.e.save_session, form["account_id"], session, telegram_id)
            account_id, choices = form["account_id"], []
        else:
            account_id, choices = await self.e.db.run(self.e.add_account, form["name"], phone, session, telegram_id)
        await self.e.db.run(self.clear, user)
        if choices:
            return View(
                "Выберите рассылку для аккаунта. Подготовка уже началась автоматически.",
                grid(
                    [
                        await self.e.db.run(
                            self.button,
                            user,
                            r["name"],
                            "assign_account",
                            {"id": account_id, "campaign_id": r["id"]},
                            True,
                        )
                        for r in choices
                    ]
                )
                + await self.e.db.run(self.nav, user, "accounts"),
            )
        view = await self.e.db.run(self.card, user, "accounts", account_id)
        view.text = "✅ Аккаунт сохранён · Подготовка началась\n\n" + view.text
        return view

    async def _import_active(self, user, form):
        state = await self.e.db.run(self.state, user)
        return bool(
            state and state["state"] == "import_confirm" and state["data"].get("_revision") == form.get("_revision")
        )

    def _import_chat(self, user, revision, chat_id, title, topic, link, options):
        with self.e.db.tx() as c:
            state = row(c, "SELECT * FROM ui_states WHERE admin_id=? AND expires_at>?", (user, self.e.clock()))
            if (
                not state
                or state["state"] != "import_confirm"
                or json.loads(state["data"]).get("_revision") != revision
            ):
                raise DomainError("Импорт отменён")
            return self.e._add_chat(c, chat_id, title, topic, link, **options)

    def import_confirmation(self, user, form):
        self.set_state(user, "import_confirm", form)
        flags = {
            "approved": "Есть право автоматизированной публикации",
            "join_allowed": "Разрешено вступление в эти чаты",
            "greeting_allowed": "Разрешены приветствия",
            "split_allowed": "Разрешены три отдельных сообщения",
        }
        buttons = [
            [self.button(user, ("✅ " if form.get(key) else "⭕ ") + title, "import_flag", {"field": key}, True)]
            for key, title in flags.items()
        ]
        buttons.append([self.button(user, "✅ Импортировать", "import_confirm", once=True)])
        return View(
            f"Импорт · Шаг 2 из 2\nСтрок: {len(form['lines'])}\nПодтвердите права и правила сообществ. Одно подтверждение применяется к этому списку.",
            buttons + self.cancel(user),
        )

    @serialized
    async def message(self, user, text="", template=None, txt_content=None):
        self.e.authorize(user)
        state = await self.e.db.run(self.state, user)
        if not state:
            return await self.e.db.run(self.main, user, "Выберите действие кнопкой")
        name, form = state["state"], state["data"]
        if name == "account_import_password":
            result = await self.auth.finish_import(user, text)
            if result is None:
                return View("Пароль не подошёл. Введите пароль 2FA заново.", await self.e.db.run(self.cancel, user))
            return await self._save_authorized_account(user, result, form)
        if name == "warmup_channels":
            total = await self.e.db.run(self.e.add_warmup_channels, text)
            await self.e.db.run(self.clear, user, form["_revision"])
            view = await self.e.db.run(self.warmup_channels_view, user)
            view.text = f"✅ Добавлено: {total}\n\n" + view.text
            return view
        if name == "search":
            await self.e.db.run(self.clear, user)
            return await self.e.db.run(self.listing, user, form["section"], search=text.strip())
        if name == "form":
            field, item_id = form["field"], form.get("id")
            if field == "chat_interval":
                await self.e.db.run(self.e.set_chat_interval, item_id, int(text))
                result = await self.e.db.run(self.card, user, "chats", item_id)
            elif field in {"rotation_lead", "rotation_messages"}:
                campaign = await self.e.db.run(self.e.db.one, "SELECT * FROM campaigns WHERE id=?", (item_id,))
                await self.e.db.run(
                    self.e.set_rotation,
                    item_id,
                    campaign["rotation_enabled"],
                    int(text) if field == "rotation_lead" else campaign["rotation_lead_days"],
                    int(text) if field == "rotation_messages" else campaign["rotation_messages"],
                )
                result = await self.e.db.run(self.rotation_view, user, item_id)
            elif field in {"timezone", "join_pause"} or field in RUNTIME_LIMITS:
                await self.e.db.run(self.e.set_setting, field, text.strip())
                result = (
                    await self.e.db.run(self.queue_settings, user)
                    if field in RUNTIME_LIMITS
                    else await self.e.db.run(self.listing, user, "settings")
                )
            elif field in {
                "campaign_name",
                "interval",
                "chat_delay",
                "limit",
                "reply_cooldown",
                "dm_greeting_phrase",
                "dm_sticker_emoji",
            }:
                key = {
                    "campaign_name": "name",
                    "interval": "interval_seconds",
                    "chat_delay": "chat_delay_seconds",
                    "limit": "chat_limit",
                    "reply_cooldown": "reply_cooldown",
                    "dm_greeting_phrase": "dm_greeting_phrase",
                    "dm_sticker_emoji": "dm_sticker_emoji",
                }[field]
                if field == "interval" and int(text) < 30:
                    raise DomainError("Интервал в чат — минимум 30 секунд")
                await self.e.db.run(
                    self.e.edit_campaign,
                    item_id,
                    **{
                        key: text.strip()
                        if field in {"campaign_name", "dm_greeting_phrase", "dm_sticker_emoji"}
                        else int(text)
                    },
                )
                result = (
                    await self.e.db.run(self.templates_hub, user, item_id, "auto")
                    if field in {"reply_cooldown", "dm_greeting_phrase", "dm_sticker_emoji"}
                    else await self.e.db.run(self.card, user, "campaigns", item_id)
                )
            else:
                await self.e.db.run(self.e.update_account, item_id, "rename", text.strip())
                result = await self.e.db.run(self.card, user, "accounts", item_id)
            await self.e.db.run(self.clear, user)
            return result
        if name == "campaign_wizard":
            step = form["step"]
            if step == 1:
                if not text.strip():
                    raise DomainError("Введите название")
                form["name"], form["step"] = text.strip()[:100], 2
            elif step == 2:
                form["texts"].append(template or {"text": text})
            elif step == 3:
                if not template or not template.get("media_path"):
                    raise DomainError("Пришлите медиа или нажмите «Далее»")
                if not form["texts"]:
                    raise DomainError("Сначала добавьте текст")
                old_text = form["texts"][-1]["text"]
                form["texts"][-1] = {**template, "text": template.get("text") or old_text}
            elif step in {4, 6}:
                parts = text.replace("/", " ").split()
                value = int(parts[0]) if step == 4 else int(text)
                if value < 1:
                    raise DomainError("Введите положительное число")
                if step == 4:
                    if value < 30:
                        raise DomainError("Интервал в чат — минимум 30 секунд")
                    form["chat_delay"] = int(parts[1]) if len(parts) > 1 else 3
                    if form["chat_delay"] < 3 or len(parts) > 2:
                        raise DomainError("Введите интервал и паузу, например 300 / 3")
                form["interval" if step == 4 else "limit"] = value
                form["step"] += 1
            else:
                raise DomainError("На этом шаге используйте кнопки")
            return await self.e.db.run(self.wizard, user, form)
        if name == "template_edit":
            await self.e.db.run(self._expect_version, form)
            templates = await self.e.db.run(self.e.templates, form["id"], form["kind"])
            if "index" in form:
                templates[form["index"]] = template or {"text": text}
            else:
                templates.append(template or {"text": text})
            await self.e.db.run(
                self.e.edit_campaign, form["id"], form["kind"], templates, expected_version=form.get("v")
            )
            await self.e.db.run(self.clear, user)
            return await self.e.db.run(self.template_list, user, form["id"], form["kind"])
        if name == "add_account_name":
            if not text.strip():
                raise DomainError("Введите название")
            await self.e.db.run(self.set_state, user, "account_phone", {"name": text.strip()[:100]})
            return View(
                "Добавление · Шаг 2 из 4\nВведите номер собственного аккаунта в международном формате.",
                await self.e.db.run(self.cancel, user),
            )
        if name == "account_phone":
            import re

            if not re.fullmatch(r"\+\d{8,15}", text.strip()):
                raise DomainError("Номер должен иметь вид +380123456789")
            await self.auth.begin(user, text.strip())
            # Phone is retained only inside the authorization object until it can be encrypted.
            await self.e.db.run(self.set_state, user, "account_code", form)
            return View(
                "Подключение · Шаг 3 из 4\nВведите код подтверждения Telegram."
                + (
                    "\nТестовый код: 00000"
                    if self.e.config.mode == "dry-run"
                    else "\nСообщение с кодом будет удалено ботом."
                ),
                await self.e.db.run(self.cancel, user),
            )
        if name in {"account_code", "account_password"}:
            result = await self.auth.finish(
                user,
                code=text.replace(" ", "") if name == "account_code" else None,
                password=text if name == "account_password" else None,
            )
            if result is None:
                await self.e.db.run(self.set_state, user, "account_password", form)
                return View(
                    "Подключение · Шаг 3 из 4\nВведите пароль двухэтапной аутентификации. Он не сохраняется.",
                    await self.e.db.run(self.cancel, user),
                )
            return await self._save_authorized_account(user, result, form)
        if name == "import_chats":
            content = txt_content if txt_content is not None else text
            lines = list(dict.fromkeys(line.strip() for line in content.splitlines() if line.strip()))
            if not lines or len(lines) > 1000:
                raise DomainError("Импортируйте от 1 до 1000 строк за один раз")
            from app.services.chat_pool import parse_target

            for line in lines:
                parse_target(line)
            return await self.e.db.run(self.import_confirmation, user, {"lines": lines})
        if name == "manual_dm":
            await self.e.db.run(
                self.e.manual_dm_reply,
                form["account_id"],
                form["peer_id"],
                form["message_id"],
                template or {"text": text},
            )
            await self.e.db.run(self.clear, user)
            return await self.e.db.run(self.main, user, "Ответ поставлен в очередь с учётом смены и состояния кампании")
        raise DomainError("Сейчас используйте кнопки или отмените действие")

    async def check_account(self, account_id):
        account = await self.e.db.run(self.e.db.one, "SELECT * FROM accounts WHERE id=?", (account_id,))
        if not account:
            raise DomainError("Аккаунт не найден")
        if self.e.clock() - account["onboarding_started_at"] < 86400:
            raise DomainError("Идёт тихий период: фоновая проверка запрещена первые 24 часа")
        if account["flood_until"] > self.e.clock():
            raise DomainError("Дождитесь окончания FloodWait")
        if self.e.config.mode == "dry-run":
            return
        try:
            client = await self.transport.client(account_id)
            me = await asyncio.wait_for(client.get_me(), 30)
        except Exception as exc:
            from app.transport.base import OperationError
            from app.transport.telegram import classify_error

            error = exc if isinstance(exc, OperationError) else classify_error(exc)
            if error.category == "critical":
                await self.e.db.run(self.e.critical, account_id, error.code)
            raise DomainError(
                "Проверка не пройдена. Переподключите аккаунт или устраните ограничение вручную"
            ) from None
        if not me:
            await self.e.db.run(self.e.critical, account_id, "SESSION_EXPIRED")
        # Successful authorization cannot prove that a moderation/spam restriction has disappeared.
        elif account["account_health"] in {"DISCONNECTED", "SESSION_EXPIRED"}:
            await self.e.db.run(self.e.connection_verified, account_id)
