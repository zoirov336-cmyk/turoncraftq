"""
TuronCraft Telegram do'kon boti
- O'yinchi mahsulot tanlaydi, nik yozadi, kartaga pul o'tkazadi va chek yuboradi
- Admin "Pul tushdi" tugmasini bosadi -> bot RCON orqali serverga buyruq yuboradi
"""
import asyncio
import html
import logging
import os
import re
import sqlite3
import struct
import time

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

# ───────────────────────── SOZLAMALAR (.env) ─────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
RCON_HOST = os.getenv("RCON_HOST", "127.0.0.1")
RCON_PORT = int(os.getenv("RCON_PORT", "25575"))
RCON_PASSWORD = os.environ["RCON_PASSWORD"]
CARD_NUMBER = os.getenv("CARD_NUMBER", "0000 0000 0000 0000")
CARD_OWNER = os.getenv("CARD_OWNER", "Ism Familiya")

# ───────────────────────── MAHSULOTLAR ─────────────────────────
# {player} o'rniga o'yinchi niki qo'yiladi.
# Narxlar va rank nomlarini o'zingizga moslab o'zgartiring.
PRODUCTS = {
    # --- Case kalitlari (ArtiCases) ---
    "key_token_1": {
        "group": "keys", "title": "Token Case kaliti x1", "price": 5000,
        "commands": ["articases givekey {player} token 1"],
    },
    "key_token_5": {
        "group": "keys", "title": "Token Case kaliti x5", "price": 22000,
        "commands": ["articases givekey {player} token 5"],
    },
    "key_resource_1": {
        "group": "keys", "title": "Resource Case kaliti x1", "price": 5000,
        "commands": ["articases givekey {player} resource 1"],
    },
    "key_resource_5": {
        "group": "keys", "title": "Resource Case kaliti x5", "price": 22000,
        "commands": ["articases givekey {player} resource 5"],
    },
    # --- Ranklar (LuckPerms) ---
    "rank_vip": {
        "group": "ranks", "title": "VIP rank", "price": 30000,
        "commands": ["lp user {player} parent add vip"],
    },
    "rank_premium": {
        "group": "ranks", "title": "PREMIUM rank", "price": 60000,
        "commands": ["lp user {player} parent add premium"],
    },
}
GROUP_TITLES = {"keys": "🗝 Case kalitlari", "ranks": "👑 Ranklar"}

# ───────────────────────── RCON ─────────────────────────
async def rcon_command(cmd: str) -> str:
    """Minecraft RCON orqali bitta buyruq yuboradi va javobini qaytaradi."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(RCON_HOST, RCON_PORT), timeout=10
    )

    async def send(pid: int, ptype: int, body: str) -> None:
        data = struct.pack("<ii", pid, ptype) + body.encode("utf-8") + b"\x00\x00"
        writer.write(struct.pack("<i", len(data)) + data)
        await writer.drain()

    async def recv():
        (length,) = struct.unpack("<i", await reader.readexactly(4))
        data = await reader.readexactly(length)
        pid, ptype = struct.unpack("<ii", data[:8])
        return pid, ptype, data[8:-2].decode("utf-8", "replace")

    try:
        await send(1, 3, RCON_PASSWORD)
        pid, _, _ = await asyncio.wait_for(recv(), timeout=10)
        if pid == -1:
            raise RuntimeError("RCON paroli noto'g'ri")
        await send(2, 2, cmd)
        _, _, body = await asyncio.wait_for(recv(), timeout=10)
        return body
    finally:
        writer.close()


COLOR_RE = re.compile(r"§[0-9a-fk-orx]|&#[0-9a-fA-F]{6}|§x(§[0-9a-fA-F]){6}")
FAIL_WORDS = (
    "topilmadi", "noto‘g‘ri", "could not be found", "unknown", "incorrect",
    "error", "usage", "no player", "invalid", "not found", "does not exist",
)


def looks_failed(text: str) -> bool:
    t = text.lower()
    return any(w in t for w in FAIL_WORDS)


delivery_lock = asyncio.Lock()


async def deliver(order: sqlite3.Row) -> tuple[bool, str]:
    prod = PRODUCTS[order["product"]]
    log = []
    async with delivery_lock:
        for tmpl in prod["commands"]:
            cmd = tmpl.format(player=order["nick"])
            try:
                resp = await rcon_command(cmd)
            except Exception as e:  # noqa: BLE001
                log.append(f"/{cmd} → RCON xatosi: {e}")
                return False, "\n".join(log)
            clean = COLOR_RE.sub("", resp).strip()
            log.append(f"/{cmd} → {clean or 'OK'}")
            if looks_failed(clean):
                return False, "\n".join(log)
    return True, "\n".join(log)


# ───────────────────────── BAZA (SQLite) ─────────────────────────
db = sqlite3.connect("orders.db")
db.row_factory = sqlite3.Row
db.execute(
    """CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, username TEXT, nick TEXT, product TEXT,
        price INTEGER, status TEXT, created INTEGER, log TEXT
    )"""
)
db.commit()
# status: new -> waiting -> delivering -> done | failed | rejected | cancelled


def get_order(oid: int):
    return db.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()


def claim(oid: int, from_statuses: tuple, to_status: str) -> bool:
    """Atomik holat o'zgartirish (ikki marta berib yuborishdan himoya)."""
    q = ",".join("?" * len(from_statuses))
    cur = db.execute(
        f"UPDATE orders SET status=? WHERE id=? AND status IN ({q})",
        (to_status, oid, *from_statuses),
    )
    db.commit()
    return cur.rowcount == 1


def set_status(oid: int, status: str, log: str = ""):
    db.execute("UPDATE orders SET status=?, log=? WHERE id=?", (status, log, oid))
    db.commit()


# ───────────────────────── BOT ─────────────────────────
router = Router()
NICK_RE = re.compile(r"^[A-Za-z0-9_]{3,16}$")


class Buy(StatesGroup):
    nick = State()
    receipt = State()


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ") + " so‘m"


def main_menu():
    kb = InlineKeyboardBuilder()
    for g, title in GROUP_TITLES.items():
        kb.button(text=title, callback_data=f"grp:{g}")
    kb.adjust(1)
    return kb.as_markup()


def admin_kb(oid: int, retry: bool = False):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔁 Qayta urinish" if retry else "💰 Pul tushdi", callback_data=f"ok:{oid}")
    kb.button(text="🚫 Rad etish", callback_data=f"no:{oid}")
    kb.adjust(2)
    return kb.as_markup()


def order_text(o) -> str:
    p = PRODUCTS[o["product"]]
    return (
        f"🧾 Buyurtma #{o['id']}\n"
        f"Mahsulot: {p['title']}\n"
        f"Narx: {fmt(o['price'])}\n"
        f"Minecraft nik: {o['nick']}\n"
        f"Telegram: @{o['username'] or '—'} (ID {o['user_id']})"
    )


@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(
        "👋 <b>TuronCraft do‘koniga xush kelibsiz!</b>\nNima sotib olmoqchisiz?",
        reply_markup=main_menu(),
    )


@router.message(Command("id"))
async def my_id(m: Message):
    await m.answer(f"Sizning Telegram ID: <code>{m.from_user.id}</code>")


@router.callback_query(F.data == "menu")
async def back_menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Nima sotib olmoqchisiz?", reply_markup=main_menu())
    await c.answer()


@router.callback_query(F.data.startswith("grp:"))
async def group(c: CallbackQuery):
    g = c.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for pid, p in PRODUCTS.items():
        if p["group"] == g:
            kb.button(text=f"{p['title']} — {fmt(p['price'])}", callback_data=f"buy:{pid}")
    kb.button(text="⬅️ Orqaga", callback_data="menu")
    kb.adjust(1)
    await c.message.edit_text(GROUP_TITLES[g], reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery, state: FSMContext):
    pid = c.data.split(":")[1]
    if pid not in PRODUCTS:
        return await c.answer("Mahsulot topilmadi", show_alert=True)
    await state.set_state(Buy.nick)
    await state.update_data(pid=pid)
    await c.message.answer(
        f"✅ Tanlandi: <b>{html.escape(PRODUCTS[pid]['title'])}</b>\n\n"
        "Serverdagi <b>Minecraft nikingizni</b> aniq yozing (katta-kichik harfga e’tibor bering):"
    )
    await c.answer()


@router.message(Buy.nick, F.text)
async def got_nick(m: Message, state: FSMContext):
    nick = m.text.strip()
    if not NICK_RE.match(nick):
        return await m.answer("❌ Nik noto‘g‘ri. Faqat lotin harf, raqam va _ (3–16 belgi). Qayta yozing:")
    data = await state.get_data()
    p = PRODUCTS[data["pid"]]
    cur = db.execute(
        "INSERT INTO orders(user_id, username, nick, product, price, status, created) VALUES(?,?,?,?,?,?,?)",
        (m.from_user.id, m.from_user.username, nick, data["pid"], p["price"], "new", int(time.time())),
    )
    db.commit()
    oid = cur.lastrowid
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ To‘lov qildim", callback_data=f"paid:{oid}")
    kb.button(text="❌ Bekor qilish", callback_data=f"cancel:{oid}")
    kb.adjust(1)
    await m.answer(
        f"🧾 <b>Buyurtma #{oid}</b>\n"
        f"Mahsulot: {html.escape(p['title'])}\n"
        f"Nik: <code>{nick}</code>\n"
        f"To‘lov summasi: <b>{fmt(p['price'])}</b>\n\n"
        f"💳 Karta: <code>{CARD_NUMBER}</code>\n"
        f"👤 {html.escape(CARD_OWNER)}\n\n"
        "Pulni o‘tkazing, so‘ng <b>«To‘lov qildim»</b> tugmasini bosing va chek rasmini yuboring.",
        reply_markup=kb.as_markup(),
    )


@router.callback_query(F.data.startswith("cancel:"))
async def cancel(c: CallbackQuery):
    oid = int(c.data.split(":")[1])
    o = get_order(oid)
    if o and o["user_id"] == c.from_user.id and claim(oid, ("new",), "cancelled"):
        await c.message.edit_text(f"❌ Buyurtma #{oid} bekor qilindi.")
    await c.answer()


@router.callback_query(F.data.startswith("paid:"))
async def paid(c: CallbackQuery, state: FSMContext):
    oid = int(c.data.split(":")[1])
    o = get_order(oid)
    if not o or o["user_id"] != c.from_user.id or o["status"] != "new":
        return await c.answer("Bu buyurtma endi faol emas", show_alert=True)
    await state.set_state(Buy.receipt)
    await state.update_data(oid=oid)
    await c.message.answer("📸 To‘lov chekining <b>rasmini (skrinshot)</b> yuboring:")
    await c.answer()


@router.message(Buy.receipt, F.photo)
async def got_receipt(m: Message, state: FSMContext):
    oid = (await state.get_data())["oid"]
    if not claim(oid, ("new",), "waiting"):
        await state.clear()
        return await m.answer("Bu buyurtma allaqachon yuborilgan.")
    await state.clear()
    o = get_order(oid)
    sent = 0
    for admin in ADMIN_IDS:
        try:
            await m.bot.send_photo(
                admin, m.photo[-1].file_id,
                caption=order_text(o) + "\n\n⏳ To‘lov tekshirilishi kutilmoqda",
                reply_markup=admin_kb(oid), parse_mode=None,
            )
            sent += 1
        except Exception as e:  # noqa: BLE001
            logging.warning("Adminga yuborilmadi %s: %s", admin, e)
    if sent == 0:
        set_status(oid, "new")
        return await m.answer("⚠️ Adminga yuborib bo‘lmadi. Birozdan so‘ng qayta urinib ko‘ring.")
    await m.answer(f"✅ Chek yuborildi. Admin tasdiqlagach buyurtma #{oid} avtomatik beriladi.")


@router.message(Buy.receipt)
async def receipt_wrong(m: Message):
    await m.answer("Iltimos, chek <b>rasmini</b> yuboring (matn emas).")


async def edit_admin_msg(c: CallbackQuery, text: str, kb=None):
    try:
        await c.message.edit_caption(caption=text, reply_markup=kb, parse_mode=None)
    except Exception:  # noqa: BLE001
        pass


@router.callback_query(F.data.startswith("ok:"))
async def admin_ok(c: CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        return await c.answer("Ruxsat yo‘q", show_alert=True)
    oid = int(c.data.split(":")[1])
    if not claim(oid, ("waiting", "failed"), "delivering"):
        return await c.answer("Bu buyurtma allaqachon ishlangan", show_alert=True)
    await c.answer("Serverga yuborilmoqda...")
    o = get_order(oid)
    ok, log = await deliver(o)
    if ok:
        set_status(oid, "done", log)
        await edit_admin_msg(c, order_text(o) + f"\n\n✅ BERILDI\n{log}")
        await c.bot.send_message(
            o["user_id"],
            f"🎉 Buyurtma #{oid} tasdiqlandi! <b>{html.escape(PRODUCTS[o['product']]['title'])}</b> "
            f"<code>{o['nick']}</code> hisobiga berildi. Serverda o‘ynab ko‘ring!",
        )
    else:
        set_status(oid, "failed", log)
        await edit_admin_msg(
            c, order_text(o) + f"\n\n⚠️ SERVERGA BERIB BO‘LMADI\n{log}", admin_kb(oid, retry=True)
        )


@router.callback_query(F.data.startswith("no:"))
async def admin_no(c: CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        return await c.answer("Ruxsat yo‘q", show_alert=True)
    oid = int(c.data.split(":")[1])
    if not claim(oid, ("waiting", "failed"), "rejected"):
        return await c.answer("Bu buyurtma allaqachon ishlangan", show_alert=True)
    o = get_order(oid)
    await edit_admin_msg(c, order_text(o) + "\n\n🚫 RAD ETILDI")
    await c.bot.send_message(o["user_id"], f"🚫 Buyurtma #{oid} rad etildi. Savol bo‘lsa admin bilan bog‘laning.")
    await c.answer()


async def main():
    if not ADMIN_IDS:
        raise SystemExit("ADMIN_IDS bo'sh! .env faylga admin Telegram ID kiriting.")
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
