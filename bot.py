from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import aiosqlite
import pytz
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums.parse_mode import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message, User
from aiogram.types import Update
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from dotenv import load_dotenv

# ========= Config =========
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is not set. Put it into .env")

DEFAULT_TZ = os.getenv("DEFAULT_TZ", "Europe/Moscow")
TZ = pytz.timezone(DEFAULT_TZ)
DB_PATH = os.getenv("DB_PATH", "giveaway.db")

# Mini App short names (настраиваются в BotFather)
MINI_APP_SHORT = os.getenv("MINI_APP_SHORT", "Myssilki")          # t.me/<bot>/Myssilki
MINI_APP_JOIN_SHORT = os.getenv("MINI_APP_JOIN_SHORT", "myapp")   # t.me/<bot>/myapp

# Render/хостинг: порт должен браться из окружения (Render сам выдаёт PORT).
HTTP_HOST = os.getenv("HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("PORT", "10000"))
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "*")  # CORS

# Публичный URL сервиса (для вебхука)
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://bot-randomus-1.onrender.com")
# Уникальный путь вебхука (с токеном), чтобы не принимали чужие POST
WEBHOOK_PATH = f"/tg-webhook/{BOT_TOKEN}"

# Идентификатор инстанса (для блокировки розыгрыша при гонке нескольких инстансов)
INSTANCE_ID = os.getenv("RENDER_INSTANCE_ID") or os.getenv("DYNO") or f"pid-{os.getpid()}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("giveaway-bot")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# Планировщик строго в UTC
scheduler = AsyncIOScheduler(
    timezone=pytz.utc,
    job_defaults={"misfire_grace_time": 3600, "coalesce": True},
)

BOT_USERNAME: Optional[str] = None

# ========= DB =========
INIT_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS giveaways (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    title              TEXT NOT NULL,
    description        TEXT,
    winners_count      INTEGER NOT NULL,
    type               TEXT NOT NULL DEFAULT 'button', -- button|referrals|comments|boosts
    start_at_utc       TEXT NOT NULL,
    end_at_utc         TEXT NOT NULL,
    required_channels  TEXT,
    target_chat        TEXT NOT NULL,
    post_chat_id       INTEGER,
    post_message_id    INTEGER,
    discussion_chat_id INTEGER,
    thread_message_id  INTEGER,
    status             TEXT NOT NULL DEFAULT 'scheduled', -- scheduled|drawing|finished|canceled
    created_by         INTEGER NOT NULL,
    created_at_utc     TEXT NOT NULL,
    photo_file_id      TEXT,
    announced          INTEGER NOT NULL DEFAULT 0, -- 0/1: итоги опубликованы
    draw_lock          TEXT                       -- id инстанса, «захватившего» розыгрыш
);

CREATE TABLE IF NOT EXISTS entries (
    giveaway_id   INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
    user_id       INTEGER NOT NULL,
    username      TEXT,
    first_name    TEXT,
    joined_at_utc TEXT NOT NULL,
    PRIMARY KEY (giveaway_id, user_id)
);

CREATE TABLE IF NOT EXISTS referrals (
    giveaway_id   INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
    referrer_id   INTEGER NOT NULL,
    referred_id   INTEGER NOT NULL,
    joined_at_utc TEXT NOT NULL,
    PRIMARY KEY (giveaway_id, referred_id)
);

CREATE TABLE IF NOT EXISTS winners (
    giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
    user_id     INTEGER NOT NULL,
    place       INTEGER NOT NULL,
    PRIMARY KEY (giveaway_id, user_id)
);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(INIT_SQL)
        # мягкие миграции
        for ddl in [
            "ALTER TABLE giveaways ADD COLUMN type TEXT NOT NULL DEFAULT 'button'",
            "ALTER TABLE giveaways ADD COLUMN discussion_chat_id INTEGER",
            "ALTER TABLE giveaways ADD COLUMN thread_message_id INTEGER",
            "ALTER TABLE giveaways ADD COLUMN photo_file_id TEXT",
            "ALTER TABLE giveaways ADD COLUMN announced INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE giveaways ADD COLUMN draw_lock TEXT",
        ]:
            try:
                await db.execute(ddl)
            except Exception:
                pass
        await db.commit()

# ========= FSM =========
class NewGiveawayType(StatesGroup):
    choose = State()

class NewGiveaway(StatesGroup):
    gtype = State()
    title = State()
    description = State()
    winners = State()
    end_at = State()
    channels = State()
    target_chat = State()
    photo = State()
    confirm = State()

# ========= Model & utils =========
@dataclass
class Giveaway:
    id: int
    title: str
    description: str
    winners_count: int
    type: str
    start_at_utc: str
    end_at_utc: str
    required_channels: str
    target_chat: str
    post_chat_id: Optional[int]
    post_message_id: Optional[int]
    discussion_chat_id: Optional[int]
    thread_message_id: Optional[int]
    status: str
    created_by: int
    created_at_utc: str
    photo_file_id: Optional[str]
    announced: int = 0
    draw_lock: Optional[str] = None

    @property
    def start_dt(self) -> datetime:
        return datetime.fromisoformat(self.start_at_utc)

    @property
    def end_dt(self) -> datetime:
        return datetime.fromisoformat(self.end_at_utc)

    @property
    def required_list(self) -> List[str]:
        if not self.required_channels:
            return []
        return [c.strip() for c in self.required_channels.split(',') if c.strip()]

# ---- DB helpers
async def fetch_giveaway(gid: int) -> Optional[Giveaway]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM giveaways WHERE id=?", (gid,))
        row = await cur.fetchone()
        return Giveaway(**dict(row)) if row else None

async def list_active_giveaways() -> List[Giveaway]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM giveaways WHERE status IN ('scheduled','drawing') ORDER BY end_at_utc ASC")
        rows = await cur.fetchall()
        return [Giveaway(**dict(r)) for r in rows]

async def add_entry(gid: int, user: User):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO entries (giveaway_id, user_id, username, first_name, joined_at_utc) VALUES (?,?,?,?,?)",
            (gid, user.id, user.username or '', user.first_name or '', datetime.now(timezone.utc).isoformat()),
        )
        await db.commit()

async def has_entry(gid: int, uid: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM entries WHERE giveaway_id=? AND user_id=?", (gid, uid))
        return await cur.fetchone() is not None

async def count_entries(gid: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM entries WHERE giveaway_id=?", (gid,))
        row = await cur.fetchone()
        return int(row[0] if row and row[0] is not None else 0)

async def get_entries(gid: int) -> List[Tuple[int, str, str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, username, first_name FROM entries WHERE giveaway_id=?", (gid,))
        rows = await cur.fetchall()
        return [(r[0], r[1], r[2]) for r in rows]

async def save_winners(gid: int, winners: List[int]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM winners WHERE giveaway_id=?", (gid,))
        for idx, uid in enumerate(winners, start=1):
            await db.execute("INSERT INTO winners (giveaway_id, user_id, place) VALUES (?,?,?)", (gid, uid, idx))
        await db.commit()

async def get_winners(gid: int) -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM winners WHERE giveaway_id=? ORDER BY place ASC", (gid,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

# referrals
async def add_referral(gid: int, referrer_id: int, referred_id: int):
    if referrer_id == referred_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO referrals (giveaway_id, referrer_id, referred_id, joined_at_utc) VALUES (?,?,?,?)",
                (gid, referrer_id, referred_id, datetime.now(timezone.utc).isoformat()),
            )
            await db.commit()
        except Exception:
            pass

async def referral_count(gid: int, uid: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM referrals WHERE giveaway_id=? AND referrer_id=?", (gid, uid))
        row = await cur.fetchone()
        return int(row[0] if row else 0)

async def referral_top(gid: int) -> List[Tuple[int, int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT referrer_id, COUNT(*) c FROM referrals WHERE giveaway_id=? GROUP BY referrer_id ORDER BY c DESC",
            (gid,),
        )
        rows = await cur.fetchall()
        return [(r[0], r[1]) for r in rows]

# ========= Keyboards =========
async def main_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🆕 Новый розыгрыш", callback_data="menu:new")
    kb.button(text="📋 Активные", callback_data="menu:list")
    kb.button(text="ℹ️ Помощь", callback_data="menu:help")
    kb.adjust(1, 2)
    return kb.as_markup()

async def type_selector_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="1) Кнопка участия", callback_data="type:button")
    kb.button(text="2) Позови друзей", callback_data="type:referrals")
    kb.button(text="3) По комментариям", callback_data="type:comments")
    kb.button(text="4) По голосам Premium", callback_data="type:boosts")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()

async def giveaway_kb(g: Giveaway) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username

    if g.type == 'button':
        total = await count_entries(g.id)
        startapp_payload = f"gid-{g.id}"
        kb.button(
            text="🎉 Участвовать",
            url=f"https://t.me/{BOT_USERNAME}/{MINI_APP_JOIN_SHORT}?startapp={startapp_payload}",
        )
        kb.button(text=f"👥 Участники: {total}", callback_data=f"count:{g.id}")

    elif g.type == 'referrals':
        startapp_payload = f"gid-{g.id}"
        kb.button(
            text="🔗 Моя ссылка",
            url=f"https://t.me/{BOT_USERNAME}/{MINI_APP_SHORT}?startapp={startapp_payload}",
        )
        kb.button(text="📈 Мои приглашения", callback_data=f"refcount:{g.id}")

    elif g.type == 'boosts':
        kb.button(text="⚡ Проверить голос", callback_data=f"boost:{g.id}")
        total = await count_entries(g.id)
        kb.button(text=f"👥 Участники: {total}", callback_data=f"count:{g.id}")

    kb.button(text="📜 Правила", callback_data=f"rules:{g.id}")
    kb.adjust(1, 1)
    return kb.as_markup()

# ========= Checks =========
async def check_requirements(user_id: int, required: List[str]) -> Tuple[bool, List[str]]:
    if not required:
        return True, []
    failed = []
    for ch in required:
        try:
            chat_id = ch
            if isinstance(chat_id, str) and chat_id.startswith('@'):
                chat_id = chat_id  # username указывать можно
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status not in ("member", "administrator", "creator"):
                failed.append(ch)
        except Exception as e:
            logger.warning(f"Requirement check failed for {ch}: {e}")
            failed.append(ch)
    return (len(failed) == 0, failed)

async def user_has_valid_boost(chat_id: int, user_id: int, since_utc: datetime) -> bool:
    try:
        boosts = await bot.get_user_chat_boosts(chat_id=chat_id, user_id=user_id)
        for b in getattr(boosts, 'boosts', []) or []:
            ad = getattr(b, 'add_date', None)
            if ad and (ad if ad.tzinfo else ad.replace(tzinfo=timezone.utc)) >= since_utc:
                return True
    except Exception as e:
        logger.warning(f"get_user_chat_boosts failed: {e}")
    return False

# ========= Handlers =========
@dp.message(Command("start"))
async def cmd_start(m: Message, command: CommandObject):
    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username

    # deep links for referrals
    if command.args:
        args = command.args
        mobj = re.match(r"ref-(\d+)-(\d+)", args)
        if mobj:
            gid = int(mobj.group(1))
            ref = int(mobj.group(2))
            g = await fetch_giveaway(gid)
            if g and g.type == 'referrals' and g.status in ('scheduled','drawing'):
                try:
                    await add_referral(gid, ref, m.from_user.id)
                except Exception:
                    pass
        mobj2 = re.match(r"getreflink-(\d+)", args)
        if mobj2:
            gid = int(mobj2.group(1))
            link = f"https://t.me/{BOT_USERNAME}?start=ref-{gid}-{m.from_user.id}"
            await m.answer(f"Ваша реферальная ссылка:\n{link}")

    text = (
        "Привет! Я бот для розыгрышей 🎁\n\n"
        "Нажмите «🆕 Новый розыгрыш», выберите формат и следуйте мастеру.\n"
        "Команды: <code>/list_giveaways</code>, <code>/draw ID</code>, <code>/cancel ID</code>."
    )
    await m.answer(text, reply_markup=await main_menu_kb())

@dp.callback_query(F.data == "menu:help")
async def menu_help(c: CallbackQuery):
    await c.message.edit_text(
        "<b>Форматы</b>:\n"
        "1) Кнопка участия — Mini App Join + проверка условий.\n"
        "2) Позови друзей — реферальные ссылки.\n"
        "3) По комментариям — 1 комментарий на пользователя.\n"
        "4) По голосам Premium (Boosts).\n\n"
        "Время вводите в <b>МСК</b> (YYYY-MM-DD HH:MM) или ISO (UTC).",
        reply_markup=await main_menu_kb(),
    )

@dp.callback_query(F.data == "menu:list")
async def menu_list(c: CallbackQuery):
    gs = await list_active_giveaways()
    if not gs:
        return await c.answer("Активных розыгрышей нет.", show_alert=True)
    lines = []
    for g in gs:
        end_local = datetime.fromisoformat(g.end_at_utc).astimezone(TZ)
        lines.append(
            f"#{g.id} • {g.title} • тип: {g.type} • до {end_local:%Y-%m-%d %H:%M} {DEFAULT_TZ} "
            f"(UTC: {g.end_dt:%Y-%m-%d %H:%M}Z) • status={g.status} • announced={g.announced}"
        )
    await c.message.edit_text("\n".join(lines), reply_markup=await main_menu_kb())

@dp.callback_query(F.data == "menu:new")
async def menu_new(c: CallbackQuery, state: FSMContext):
    await state.set_state(NewGiveawayType.choose)
    await c.message.edit_text("Выберите формат розыгрыша:", reply_markup=await type_selector_kb())

@dp.callback_query(F.data.startswith("type:"))
async def choose_type(c: CallbackQuery, state: FSMContext):
    gtype = c.data.split(":")[1]
    await state.update_data(gtype=gtype)
    await state.set_state(NewGiveaway.title)
    await c.message.edit_text("🎯 Введите заголовок розыгрыша:")

# ---- Wizard ----
from datetime import datetime as dt

@dp.message(NewGiveaway.title)
async def g_title(m: Message, state: FSMContext):
    await state.update_data(title=m.text.strip())
    await state.set_state(NewGiveaway.description)
    await m.answer("📝 Введите описание:")

@dp.message(NewGiveaway.description)
async def g_desc(m: Message, state: FSMContext):
    await state.update_data(description=m.html_text[:4000])
    await state.set_state(NewGiveaway.winners)
    await m.answer("🏆 Сколько победителей? (целое число)")

@dp.message(NewGiveaway.winners)
async def g_winners(m: Message, state: FSMContext):
    try:
        winners = int(re.findall(r"\d+", m.text)[0])
        if winners < 1:
            raise ValueError
    except Exception:
        return await m.answer("Введите целое число ≥ 1")
    await state.update_data(winners=winners)
    await state.set_state(NewGiveaway.end_at)
    await m.answer(
        f"⏰ Введите дату/время окончания <code>YYYY-MM-DD HH:MM</code> — это {DEFAULT_TZ}.\n"
        "Или ISO с офсетом (UTC), напр.: <code>2025-08-10T19:00:00Z</code>"
    )

@dp.message(NewGiveaway.end_at)
async def g_end(m: Message, state: FSMContext):
    s = m.text.strip()
    end_at_utc: Optional[datetime] = None
    try:
        if "T" in s:
            iso = s.replace("Z", "+00:00")
            dtv = datetime.fromisoformat(iso)
            if dtv.tzinfo is None:
                dtv = TZ.localize(dtv)
            end_at_utc = dtv.astimezone(pytz.utc)
        else:
            dt_local = TZ.localize(dt.strptime(s, "%Y-%m-%d %H:%M"))
            end_at_utc = dt_local.astimezone(pytz.utc)
        if end_at_utc <= datetime.now(timezone.utc):
            return await m.answer("Время уже прошло. Введите будущую дату.")
    except Exception:
        return await m.answer("Неверный формат. Используйте YYYY-MM-DD HH:MM (локально) или ISO (UTC).")

    await state.update_data(end_at_utc=end_at_utc.isoformat(), start_at_utc=datetime.now(timezone.utc).isoformat())
    await state.set_state(NewGiveaway.channels)
    await m.answer("📌 Обязательные каналы через запятую (@chan1,@chan2) или '-' если нет:")

@dp.message(NewGiveaway.channels)
async def g_channels(m: Message, state: FSMContext):
    txt = m.text.strip()
    required = '' if txt == '-' else txt
    await state.update_data(required_channels=required)
    await state.set_state(NewGiveaway.target_chat)
    await m.answer("📣 Куда публиковать пост? @username канала/чата или числовой chat_id:")

@dp.message(NewGiveaway.target_chat)
async def g_target(m: Message, state: FSMContext):
    await state.update_data(target_chat=m.text.strip())
    data = await state.get_data()
    gtype = data.get('gtype')

    if gtype == 'comments':
        end_local = datetime.fromisoformat(data['end_at_utc']).astimezone(TZ)
        text = (
            "Проверьте данные (Комментарии):\n\n"
            f"<b>{data['title']}</b>\n{data['description']}\n\n"
            f"Победителей: <b>{data['winners']}</b>\n"
            f"Окончание: <b>{end_local:%Y-%m-%d %H:%M} {DEFAULT_TZ}</b> (UTC: {data['end_at_utc']})\n"
            f"Публикация в: <code>{data['target_chat']}</code>\n"
            "Участие: отправьте один ответ-комментарий на пост. Повторные будут удаляться.\n\n"
            "Напишите 'да' для подтверждения или 'нет' для отмены."
        )
        await state.set_state(NewGiveaway.confirm)
        return await m.answer(text)

    await state.set_state(NewGiveaway.photo)
    await m.answer("🖼 Прикрепите фото одним сообщением (или '-' без фото).")

@dp.message(NewGiveaway.photo)
async def g_photo(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "-":
        await state.update_data(photo_file_id=None)
    elif m.photo:
        await state.update_data(photo_file_id=m.photo[-1].file_id)
    else:
        return await m.answer("Отправьте фото или '-' чтобы пропустить.")

    data = await state.get_data()
    end_local = datetime.fromisoformat(data['end_at_utc']).astimezone(TZ)
    text = (
        "Проверьте данные:\n\n"
        f"<b>{data['title']}</b>\n{data['description']}\n\n"
        f"Тип: <b>{data['gtype']}</b>\n"
        f"Победителей: <b>{data['winners']}</b>\n"
        f"Окончание: <b>{end_local:%Y-%m-%d %H:%M} {DEFAULT_TZ}</b> (UTC: {data['end_at_utc']})\n"
        f"Обязат. каналы: <code>{data['required_channels'] or 'нет'}</code>\n"
        f"Публикация в: <code>{data['target_chat']}</code>\n"
        f"Фото: {'есть' if data.get('photo_file_id') else 'нет'}\n\n"
        "Напишите 'да' для подтверждения или 'нет' для отмены."
    )
    await state.set_state(NewGiveaway.confirm)
    await m.answer(text)

@dp.message(NewGiveaway.confirm)
async def g_confirm(m: Message, state: FSMContext):
    if m.text.strip().lower() not in ("да", "yes", "y"):
        await state.clear()
        return await m.answer("Отменено.")
    data = await state.get_data()
    await state.clear()

    gtype = data['gtype']

    # Save to DB
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO giveaways (title, description, winners_count, type, start_at_utc, end_at_utc, required_channels, target_chat, status, created_by, created_at_utc, photo_file_id, announced) "
            "VALUES (?,?,?,?,?,?,?,?, 'scheduled', ?, ?, ?, 0)",
            (
                data['title'], data['description'], data['winners'], gtype,
                data['start_at_utc'], data['end_at_utc'], data['required_channels'], data['target_chat'],
                m.from_user.id, datetime.now(timezone.utc).isoformat(), data.get('photo_file_id'),
            ),
        )
        gid = cur.lastrowid
        await db.commit()

    # Build post text
    gid_text = f"ID: <code>{gid}</code>"
    end_local = datetime.fromisoformat(data['end_at_utc']).astimezone(TZ)
    post_text = (
        f"<b>{data['title']}</b>\n\n{data['description']}\n\n"
        f"Формат: <b>{gtype}</b>\n"
        f"⏳ До: <b>{end_local:%Y-%m-%d %H:%M} {DEFAULT_TZ}</b> (UTC: {data['end_at_utc']})\n"
        f"Победителей: <b>{data['winners']}</b>\n{gid_text}"
    )

    # keyboard preview via a fake Giveaway instance
    g_preview = Giveaway(
        id=gid, title=data['title'], description=data['description'], winners_count=data['winners'],
        type=gtype, start_at_utc=data['start_at_utc'], end_at_utc=data['end_at_utc'],
        required_channels=data['required_channels'], target_chat=data['target_chat'],
        post_chat_id=None, post_message_id=None, discussion_chat_id=None, thread_message_id=None,
        status='scheduled', created_by=m.from_user.id, created_at_utc=datetime.now(timezone.utc).isoformat(),
        photo_file_id=data.get('photo_file_id'), announced=0, draw_lock=None
    )

    sent = None
    try:
        if data.get('photo_file_id') and gtype != 'comments':
            sent = await bot.send_photo(
                chat_id=data['target_chat'],
                photo=data['photo_file_id'],
                caption=post_text,
                reply_markup=await giveaway_kb(g_preview),
            )
        else:
            extra = "\n\nНапишите <b>ОДИН</b> ответ на это сообщение — это и есть участие." if gtype == 'comments' else ''
            sent = await bot.send_message(
                chat_id=data['target_chat'],
                text=post_text + extra,
                reply_markup=None if gtype == 'comments' else await giveaway_kb(g_preview),
                disable_web_page_preview=True,
            )
        async with aiosqlite.connect(DB_PATH) as db:
            if gtype == 'comments':
                await db.execute(
                    "UPDATE giveaways SET post_chat_id=?, post_message_id=?, discussion_chat_id=?, thread_message_id=? WHERE id=?",
                    (sent.chat.id, sent.message_id, sent.chat.id, sent.message_id, gid),
                )
            else:
                await db.execute(
                    "UPDATE giveaways SET post_chat_id=?, post_message_id=? WHERE id=?",
                    (sent.chat.id, sent.message_id, gid),
                )
            await db.commit()
    except Exception as e:
        logger.exception("Failed to publish post")
        return await m.answer(f"Не удалось опубликовать пост: {e}")

    await schedule_draw_job(gid)
    await m.answer(f"Готово! Розыгрыш #{gid} опубликован в {data['target_chat']}")

# ------ Mini App Join → sendData (fallback) ------
@dp.message(F.web_app_data)
async def on_webapp_data(m: Message):
    try:
        data = json.loads(m.web_app_data.data or '{}')
    except Exception:
        data = {}
    action = data.get('action')
    gid_raw = str(data.get('gid') or '')
    gid = int(gid_raw) if gid_raw.isdigit() else 0
    if action != 'join' or not gid:
        return await m.answer("Некорректные данные Mini App.")

    g = await fetch_giveaway(gid)
    if not g or g.status not in ('scheduled','drawing') or g.type != 'button':
        return await m.answer("Этот розыгрыш недоступен для участия.")

    if await has_entry(gid, m.from_user.id):
        return await m.answer("Вы уже участвуете в этом розыгрыше 🎉")

    ok, failed = await check_requirements(m.from_user.id, g.required_list)
    if not ok:
        human = "\n".join(f"• {x}" for x in failed) or "-"
        return await m.answer("Нужно подписаться:\n" + human)

    await add_entry(gid, m.from_user)

    try:
        if g.post_chat_id and g.post_message_id:
            await bot.edit_message_reply_markup(
                chat_id=g.post_chat_id,
                message_id=g.post_message_id,
                reply_markup=await giveaway_kb(g)
            )
    except Exception as e:
        logger.exception("edit_message_reply_markup failed: %s", e)

    total = await count_entries(gid)
    await m.answer(f"Готово! Вы участвуете. Сейчас участников: {total}")

# ------ Misc callbacks ------
@dp.callback_query(F.data.startswith("count:"))
async def cb_count(c: CallbackQuery):
    gid = int(c.data.split(":")[1])
    total = await count_entries(gid)
    await c.answer(f"Сейчас участвует: {total}")

@dp.callback_query(F.data.startswith("rules:"))
async def cb_rules(c: CallbackQuery):
    gid = int(c.data.split(":")[1])
    g = await fetch_giveaway(gid)
    if not g:
        return await c.answer("Не найдено")
    req = g.required_list
    text = (
        "Правила участия:\n"
        "• Откройте Mini App и дождитесь проверки.\n"
        + ("• Подписка на: " + ", ".join(req) if req else "• Нет обязательных подписок")
    )
    await c.answer(text, show_alert=True)

@dp.callback_query(F.data.startswith("refcount:"))
async def cb_refcount(c: CallbackQuery):
    gid = int(c.data.split(":")[1])
    cnt = await referral_count(gid, c.from_user.id)
    await c.answer(f"Приглашённых: {cnt}")

@dp.callback_query(F.data.startswith("boost:"))
async def cb_boost(c: CallbackQuery):
    gid = int(c.data.split(":")[1])
    g = await fetch_giveaway(gid)
    if not g or g.status not in ('scheduled','drawing'):
        return await c.answer("Розыгрыш недоступен", show_alert=True)
    if g.type != 'boosts':
        return await c.answer("Этот розыгрыш не по голосам Premium", show_alert=True)

    ok, failed = await check_requirements(c.from_user.id, g.required_list)
    if not ok:
        human = "\n".join(f"• {x}" for x in failed) or "-"
        return await c.answer(f"Подпишитесь на обязательные каналы:\n{human}", show_alert=True)

    since = datetime.fromisoformat(g.start_at_utc).astimezone(timezone.utc)
    valid = await user_has_valid_boost(g.post_chat_id or g.target_chat, c.from_user.id, since)
    if not valid:
        return await c.answer("Не найдено активных голосов Premium за этот чат с начала конкурса.", show_alert=True)

    await add_entry(gid, c.from_user)
    try:
        if g.post_chat_id and g.post_message_id:
            await bot.edit_message_reply_markup(
                chat_id=g.post_chat_id,
                message_id=g.post_message_id,
                reply_markup=await giveaway_kb(g)
            )
    except Exception as e:
        logger.exception("edit_message_reply_markup failed: %s", e)
    total = await count_entries(gid)
    return await c.answer(f"Готово! Вы участвуете. Всего: {total}")

# ========= Комментарии (1 на пользователя) =========
@dp.message()
async def catch_comments(m: Message):
    if not m.reply_to_message:
        return
    chat_id = m.chat.id
    replied_id = m.reply_to_message.message_id

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM giveaways WHERE status IN ('scheduled','drawing') AND type='comments' AND discussion_chat_id=? AND thread_message_id=?",
            (chat_id, replied_id),
        )
        row = await cur.fetchone()
        if not row:
            return
        g = Giveaway(**dict(row))

    if await has_entry(g.id, m.from_user.id):
        try:
            await bot.delete_message(chat_id=chat_id, message_id=m.message_id)
        except Exception:
            pass
        return

    ok, failed = await check_requirements(m.from_user.id, g.required_list)
    if not ok:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=m.message_id)
        except Exception:
            pass
        return

    await add_entry(g.id, m.from_user)

# ========= Draw logic =========
async def schedule_draw_job(gid: int):
    g = await fetch_giveaway(gid)
    if not g or g.status not in ('scheduled','drawing'):
        return
    end_dt_utc = datetime.fromisoformat(g.end_at_utc)  # aware UTC
    scheduler.add_job(
        run_draw,
        DateTrigger(run_date=end_dt_utc, timezone=pytz.utc),
        args=[gid],
        id=f"draw-{gid}",
        replace_existing=True,
        misfire_grace_time=3600,
        coalesce=True,
    )
    logger.info(
        f"Scheduled draw for #{gid} at {end_dt_utc.isoformat()} (UTC) | "
        f"local={end_dt_utc.astimezone(TZ).strftime('%Y-%m-%d %H:%M')} {DEFAULT_TZ}"
    )

async def try_claim_draw(gid: int) -> bool:
    """
    Пытаемся «захватить» розыгрыш, чтобы только один инстанс сделал draw.
    Меняем status на 'drawing' и проставляем draw_lock=INSTANCE_ID.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        now_iso = datetime.now(timezone.utc).isoformat()
        cur = await db.execute(
            """
            UPDATE giveaways
               SET status='drawing', draw_lock=?
             WHERE id=? AND status='scheduled'
            """,
            (INSTANCE_ID, gid),
        )
        await db.commit()
        return cur.rowcount == 1

async def mark_announced_once(gid: int) -> bool:
    """
    Ставим announced=1 атомарно. Если уже 1 — вернёт False.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "UPDATE giveaways SET announced=1 WHERE id=? AND announced=0",
            (gid,),
        )
        await db.commit()
        return cur.rowcount == 1

async def finish_draw(gid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE giveaways SET status='finished' WHERE id=?",
            (gid,),
        )
        await db.commit()

async def run_draw(gid: int, manual: bool = False):
    g = await fetch_giveaway(gid)
    if not g or g.status not in ('scheduled','drawing'):
        return

    # Захватить розыгрыш, если он ещё scheduled
    if g.status == 'scheduled':
        claimed = await try_claim_draw(gid)
        if not claimed:
            # Кто-то другой уже начал или завершил
            logger.info(f"draw #{gid}: not claimed by {INSTANCE_ID}, skipping")
            return
        else:
            logger.info(f"draw #{gid}: claimed by {INSTANCE_ID}")

    # Перечитываем состояние после claim
    g = await fetch_giveaway(gid)
    if not g or g.status not in ('scheduled','drawing'):
        return

    winners: List[int] = []

    if g.type in ('button', 'comments'):
        entries_list = await get_entries(gid)
        pool = [e[0] for e in entries_list]
        if not pool:
            await save_winners(gid, [])
            # Публикуем «нет участников» один раз
            published = await mark_announced_once(gid)
            if published:
                await announce_results(g, [])
            await finish_draw(gid)
            return
        k = min(g.winners_count, len(pool))
        winners = random.sample(pool, k)

    elif g.type == 'referrals':
        top = await referral_top(gid)
        if not top:
            await save_winners(gid, [])
            published = await mark_announced_once(gid)
            if published:
                await announce_results(g, [])
            await finish_draw(gid)
            return
        grouped = {}
        for uid, cnt in top:
            grouped.setdefault(cnt, []).append(uid)
        counts_sorted = sorted(grouped.keys(), reverse=True)
        selected: List[int] = []
        for cnt in counts_sorted:
            bucket = grouped[cnt]
            random.shuffle(bucket)
            need = g.winners_count - len(selected)
            if need <= 0:
                break
            selected.extend(bucket[:need] if len(bucket) > need else bucket)
        winners = selected[: g.winners_count]

    elif g.type == 'boosts':
        ents = await get_entries(gid)
        pool = []
        since = datetime.fromisoformat(g.start_at_utc).astimezone(timezone.utc)
        for uid, _, _ in ents:
            if await user_has_valid_boost(g.post_chat_id or g.target_chat, uid, since):
                pool.append(uid)
        if not pool:
            await save_winners(gid, [])
            published = await mark_announced_once(gid)
            if published:
                await announce_results(g, [])
            await finish_draw(gid)
            return
        k = min(g.winners_count, 200, len(pool))
        winners = random.sample(pool, k)

    await save_winners(gid, winners)

    # Публикация итогов только один раз
    published = await mark_announced_once(gid)
    if published:
        await announce_results(g, winners)
    else:
        logger.info(f"announce #{gid}: already announced — skipping")

    await finish_draw(gid)

async def announce_results(g: Giveaway, winners: List[int]):
    if winners:
        winners_lines = [f"<a href=\"tg://user?id={uid}\">Победитель #{i}</a>" for i, uid in enumerate(winners, start=1)]
        winners_text = "\n".join(winners_lines)
    else:
        winners_text = "Не было участников. Победителей нет."

    end_local = datetime.fromisoformat(g.end_at_utc).astimezone(TZ)
    txt = (
        f"<b>ИТОГИ: {g.title}</b>\n"
        f"(дедлайн был: {end_local:%Y-%m-%d %H:%M} {DEFAULT_TZ}, UTC: {g.end_dt:%Y-%m-%d %H:%M}Z)\n\n"
        + winners_text
    )

    try:
        if g.post_chat_id and g.post_message_id:
            await bot.send_message(g.post_chat_id, txt, reply_to_message_id=g.post_message_id)
        else:
            await bot.send_message(g.target_chat, txt)
    except Exception as e:
        logger.warning(f"Failed to announce in chat: {e}")

    for uid in winners:
        try:
            await bot.send_message(uid, f"🎉 Поздравляем! Вы победили в розыгрыше «{g.title}». Админы свяжутся с вами.")
        except Exception:
            pass

# ========= HTTP API =========

def validate_webapp_init(init_data: str, bot_token: str) -> Optional[int]:
    """
    Валидация Telegram WebApp.initData.
    Сбор data_check_string — из URL-РАСКОДИРОВАННЫХ значений (parse_qsl),
    как в официальных примерах Telegram для Login Widget/WebApp.
    """
    if not init_data or "hash=" not in init_data:
        return None

    # Разбираем строку в пары (значения уже URL-decoded)
    pairs = urllib.parse.parse_qsl(init_data, keep_blank_values=True)
    got_hash = None
    filtered = []
    for k, v in pairs:
        if k == "hash":
            got_hash = v
        else:
            filtered.append((k, v))
    if not got_hash:
        return None

    # Сортируем по ключу и собираем "k=v" построчно
    filtered.sort(key=lambda x: x[0])
    data_check_string = "\n".join(f"{k}={v}" for k, v in filtered)

    # Секрет и подпись
    secret_key = hashlib.sha256(bot_token.encode()).digest()
    calc = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, got_hash):
        return None

    # Свежесть 10 минут
    try:
        # здесь используем уже распарсенные пары
        params = dict(filtered)
        auth_date = int(params.get("auth_date", "0"))
        from time import time as _now
        if _now() - auth_date > 600:
            return None
    except Exception:
        return None

    # user.id из JSON
    user_json = dict(filtered).get("user") or ""
    try:
        user_obj = json.loads(user_json)
        return int(user_obj.get("id"))
    except Exception:
        return None

    # Отсортировать по ключу и собрать data_check_string из raw-значений
    pairs_raw.sort(key=lambda x: x[0])
    data_check_string = "\n".join(f"{k}={v}" for k, v in pairs_raw)

    # Посчитать HMAC
    secret_key = hashlib.sha256(bot_token.encode()).digest()
    calc = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, got_hash):
        return None

    # Проверим свежесть (10 минут)
    try:
        params = urllib.parse.parse_qs(init_data, keep_blank_values=True)
        auth_date = int(params.get("auth_date", ["0"])[0])
        from time import time as _now
        if _now() - auth_date > 600:
            return None
    except Exception:
        return None

    # Достаём user.id
    user_json = (params.get("user", [None])[0]) or ""
    try:
        user_obj = json.loads(urllib.parse.unquote(user_json))
        return int(user_obj.get("id"))
    except Exception:
        return None

@web.middleware
async def cors_mw(request, handler):
    if request.method == 'OPTIONS':
        return web.Response(headers={
            'Access-Control-Allow-Origin': ALLOWED_ORIGIN,
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        })
    resp = await handler(request)
    resp.headers['Access-Control-Allow-Origin'] = ALLOWED_ORIGIN
    return resp

# healthcheck
async def root(request: web.Request):
    return web.Response(text="ok")

async def api_join(request: web.Request):
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "reason": "bad json"}, status=400)

    gid = int(payload.get("gid") or 0)
    init = payload.get("init") or ""
    if not gid or not init:
        return web.json_response({"ok": False, "reason": "bad params"}, status=400)

    uid = validate_webapp_init(init, BOT_TOKEN)
    if not uid:
        logger.warning("api_join 401: bad initData; len=%s; gid=%s", len(init or ""), gid)
        return web.json_response({"ok": False, "reason": "auth failed"}, status=401)

    g = await fetch_giveaway(gid)
    if not g or g.status not in ("scheduled","drawing") or g.type != "button":
        return web.json_response({"ok": False, "reason": "unavailable"}, status=404)

    if await has_entry(gid, uid):
        total = await count_entries(gid)
        return web.json_response({"ok": True, "total": total})

    ok, failed = await check_requirements(uid, g.required_list)
    if not ok:
        return web.json_response({"ok": False, "need": failed, "reason": "conditions"})

    fake_user = User(id=uid, is_bot=False, first_name="", username=None)
    await add_entry(gid, fake_user)

    try:
        if g.post_chat_id and g.post_message_id:
            await bot.edit_message_reply_markup(
                chat_id=g.post_chat_id,
                message_id=g.post_message_id,
                reply_markup=await giveaway_kb(g)
            )
    except Exception as e:
        logger.exception("edit_message_reply_markup failed: %s", e)

    total = await count_entries(gid)
    return web.json_response({"ok": True, "total": total})

# Webhook endpoint
async def tg_webhook(request: web.Request):
    try:
        data = await request.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
        return web.Response(text="ok")
    except Exception as e:
        logger.exception("webhook handle failed: %s", e)
        return web.Response(status=500, text="error")

async def start_http():
    app = web.Application(middlewares=[cors_mw])
    # healthcheck
    app.router.add_get('/', root)
    # API
    app.router.add_route('OPTIONS', '/api/join', lambda r: web.Response())
    app.router.add_post('/api/join', api_join)
    # webhook receiver
    app.router.add_post(WEBHOOK_PATH, tg_webhook)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=HTTP_HOST, port=HTTP_PORT)
    await site.start()
    logger.info(f"HTTP API started on {HTTP_HOST}:{HTTP_PORT}")

# ========= Startup =========
async def restore_schedules():
    gs = await list_active_giveaways()
    now = datetime.now(timezone.utc)
    for g in gs:
        end_dt_utc = datetime.fromisoformat(g.end_at_utc)
        if end_dt_utc <= now:
            logger.warning(f"Giveaway #{g.id} deadline passed — running draw now")
            # draw сам защитится через try_claim_draw() и announced флаг
            await run_draw(g.id, manual=True)
        else:
            await schedule_draw_job(g.id)

async def main():
    await init_db()
    scheduler.start()

    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username
        logger.info("Bot online: @%s (id=%s) | instance=%s", me.username, me.id, INSTANCE_ID)

    await start_http()
    await restore_schedules()

    # Чистим возможный старый вебхук и ставим новый (работаем по вебхуку, без polling)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        webhook_url = f"{PUBLIC_URL}{WEBHOOK_PATH}"
        await bot.set_webhook(
            url=webhook_url,
            allowed_updates=["message", "callback_query"]
        )
        logger.info(f"Webhook set to {webhook_url}")
    except Exception as e:
        logger.exception("set_webhook failed: %s", e)
        raise

    # Держим процесс живым
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")

