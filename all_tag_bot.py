import asyncio
import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import asyncpg
from telegram import ChatMember, Update
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
DEFAULT_MEMBERS_FILE = "default_members.json"

# Глобальный пул соединений с PostgreSQL
db_pool: asyncpg.Pool | None = None


# ---------- Работа с базой данных (PostgreSQL) ----------
async def init_db():
    """Создаёт таблицу, если её нет."""
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS members (
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                PRIMARY KEY (chat_id, user_id)
            )
        """)
    logger.info("Таблица members готова")


async def upsert_member(chat_id: int, user):
    """Добавляет или обновляет участника."""
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO members (chat_id, user_id, username, first_name, last_name)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (chat_id, user_id) DO UPDATE SET
                username = COALESCE(EXCLUDED.username, members.username),
                first_name = COALESCE(EXCLUDED.first_name, members.first_name),
                last_name = COALESCE(EXCLUDED.last_name, members.last_name)
        """, chat_id, user.id, user.username, user.first_name, user.last_name)


async def remove_member(chat_id: int, user_id: int):
    """Удаляет участника (при выходе)."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM members WHERE chat_id = $1 AND user_id = $2",
            chat_id, user_id
        )


async def get_members(chat_id: int):
    """Возвращает список (user_id, username, first_name)."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, username, first_name FROM members WHERE chat_id = $1",
            chat_id
        )
    return [(r["user_id"], r["username"], r["first_name"]) for r in rows]


# ---------- Дефолтный список (без изменений) ----------
def load_default_members() -> dict:
    if not os.path.exists(DEFAULT_MEMBERS_FILE):
        logger.warning(f"Файл {DEFAULT_MEMBERS_FILE} не найден — сидирование пропущено")
        return {}
    try:
        with open(DEFAULT_MEMBERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        total = sum(len(v) for v in data.values())
        logger.info(f"Загружен дефолтный список: {total} участников в {len(data)} чатах")
        return data
    except Exception as e:
        logger.error(f"Ошибка чтения {DEFAULT_MEMBERS_FILE}: {e}")
        return {}


async def seed_default_members() -> int:
    """Подмешивает дефолтных участников в PostgreSQL, не удаляя существующих."""
    defaults = load_default_members()
    if not defaults:
        return 0

    processed = 0
    async with db_pool.acquire() as conn:
        for chat_id_str, members in defaults.items():
            try:
                chat_id = int(chat_id_str)
            except ValueError:
                logger.error(f"Некорректный chat_id в JSON: {chat_id_str}")
                continue
            for m in members:
                uid = m.get("user_id")
                if not uid:
                    logger.warning(f"Пропущена запись без user_id: {m}")
                    continue
                await conn.execute("""
                    INSERT INTO members (chat_id, user_id, username, first_name, last_name)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (chat_id, user_id) DO UPDATE SET
                        username = COALESCE(EXCLUDED.username, members.username),
                        first_name = COALESCE(EXCLUDED.first_name, members.first_name),
                        last_name = COALESCE(EXCLUDED.last_name, members.last_name)
                """, chat_id, uid, m.get("username"), m.get("first_name"), m.get("last_name"))
                processed += 1
    logger.info(f"Сидирование: обработано {processed} записей")
    return processed


# ---------- Отслеживание изменений ----------
async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_member = update.chat_member
    if chat_member is None:
        return

    old, new = chat_member.old_chat_member, chat_member.new_chat_member
    was_member = old.status in (ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER) \
        or (old.status == ChatMember.RESTRICTED and getattr(old, "is_member", False))
    is_member = new.status in (ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER) \
        or (new.status == ChatMember.RESTRICTED and getattr(new, "is_member", False))

    chat_id = update.effective_chat.id
    user = new.user

    if not was_member and is_member:
        await upsert_member(chat_id, user)
        logger.info(f"+ участник {user.id} в чате {chat_id}")
    elif was_member and not is_member:
        await remove_member(chat_id, user.id)
        logger.info(f"- участник {user.id} из чата {chat_id}")


async def track_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ("group", "supergroup"):
        return
    if update.effective_user is None:
        return
    await upsert_member(update.effective_chat.id, update.effective_user)


# ---------- Команды ----------
async def handle_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    members = await get_members(chat_id)

    if not members:
        await update.message.reply_text(
            "Список пуст. Заполните default_members.json и выполните /refresh, "
            "либо попросите участников написать что-нибудь в чат."
        )
        return

    tags = []
    for user_id, username, first_name in members:
        if username:
            tags.append(f"@{username}")
        else:
            tags.append(f'<a href="tg://user?id={user_id}">{first_name or "User"}</a>')

    text = "📢 Всем участникам:\n" + " ".join(tags)
    if len(text) > 4000:
        chunk_size = 100
        for i in range(0, len(tags), chunk_size):
            chunk = " ".join(tags[i:i + chunk_size])
            if i == 0:
                chunk = "📢 Всем участникам:\n" + chunk
            await context.bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML")
    else:
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")


async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перечитывает default_members.json + подтягивает администраторов."""
    chat_id = update.effective_chat.id

    processed = await seed_default_members()

    admin_count = 0
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        for admin in admins:
            if admin.user and not admin.user.is_bot:
                await upsert_member(chat_id, admin.user)
        admin_count = len(admins)
    except Exception as e:
        logger.warning(f"get_chat_administrators не сработал: {e}")

    members = await get_members(chat_id)
    await update.message.reply_text(
        f"✅ Обновлено.\n"
        f"Из default_members.json обработано: {processed}.\n"
        f"Администраторов найдено: {admin_count}.\n"
        f"Всего в базе для этого чата: {len(members)}."
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        msg = (
            "Бот для тегирования участников.\n\n"
            "1. Добавьте меня в группу, дайте права администратора.\n"
            "2. Заполните default_members.json исходным списком.\n"
            "3. Выполните /refresh — база пополнится из JSON и админами.\n"
            "4. /all — тегнуть всех из базы.\n\n"
            "База также обновляется сама: по вступлениям/выходам и сообщениям."
        )
    else:
        msg = (
            "Бот добавлен! 🎉\n"
            "/refresh — подтянуть список из default_members.json и админов.\n"
            "/all — тегнуть всех известных участников."
        )
    await update.message.reply_text(msg)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/all — тегнуть всех из базы\n"
        "/refresh — перечитать default_members.json + подтянуть админов\n"
        "/start — информация\n"
        "/help — эта справка"
    )


# ---------- Health-сервер для Render ----------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        # Отключаем спам в логах от health-чеков
        pass


def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"Health-сервер запущен на порту {port}")
    server.serve_forever()


# ---------- Инициализация при старте ----------
async def post_init(application: Application):
    await init_db()
    await seed_default_members()
    logger.info("База данных инициализирована, дефолтный список загружен")


async def post_shutdown(application: Application):
    if db_pool:
        await db_pool.close()
        logger.info("Пул соединений с БД закрыт")


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не задана переменная окружения BOT_TOKEN")
    if not DATABASE_URL:
        raise RuntimeError("Не задана переменная окружения DATABASE_URL")

    # Health-сервер в отдельном потоке
    threading.Thread(target=run_health_server, daemon=True).start()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, track_message))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("all", handle_all))
    app.add_handler(CommandHandler("refresh", refresh))

    logger.info("Бот запущен")
    app.run_polling()


if __name__ == "__main__":
    main()