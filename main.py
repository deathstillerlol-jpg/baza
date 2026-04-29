import asyncio
import logging
import os
import sqlite3
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

bot = Bot(token=TOKEN)
dp = Dispatcher()

DB_FILE = "users.db"


# ── Инициализация и миграция базы данных ─────────────────────────────────
def init_db():
    """Создаёт таблицу и добавляет недостающие колонки (миграция)"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Создаём таблицу, если её нет
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Миграция: добавляем колонки, если их ещё нет (для старых баз)
    try:
        cur.execute("ALTER TABLE users ADD COLUMN username TEXT")
        logging.info("Добавлена колонка 'username'")
    except sqlite3.OperationalError:
        pass  # колонка уже существует

    try:
        cur.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
        logging.info("Добавлена колонка 'first_name'")
    except sqlite3.OperationalError:
        pass  # колонка уже существует

    conn.commit()
    conn.close()


def save_user(user_id: int, username: str = None, first_name: str = None):
    """Сохраняет или обновляет пользователя"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO users (user_id, username, first_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name
        """, (user_id, username, first_name))

        conn.commit()
        logging.debug(f"Пользователь {user_id} (@{username}) сохранён/обновлён")
    except Exception as e:
        logging.error(f"Ошибка при сохранении пользователя {user_id}: {e}")
    finally:
        conn.close()


def get_all_users() -> list[int]:
    """Возвращает список всех user_id"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users")
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"Ошибка чтения пользователей: {e}")
        return []
    finally:
        conn.close()


# ── Инициализация БД ─────────────────────────────────────────────────────
init_db()


# ── Хендлеры ─────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message):
    user = message.from_user
    username = user.username
    first_name = user.first_name

    save_user(user.id, username, first_name)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="НАПИСАТЬ МЕНЕДЖЕРУ",
                    url="https://t.me/sasha_teatr"
                )
            ]
        ]
    )

    await message.answer(
        "Здравствуйте! Приветствуем вас в сервисе по покупке пушкинских карт! 👋\n"
        "Нажми кнопку ниже, чтобы перейти в диалог к менеджеру:",
        reply_markup=keyboard
    )


@dp.message()
async def echo(message: Message):
    await message.answer(f"Ты написал: {message.text}")


# ── Рассылка каждые 3 часа ──────────────────────────────────────────────
async def broadcaster():
    await asyncio.sleep(30)  # даём боту запуститься
    text = (
        "Напоминание! 🔥\n"
        "БЫСТРЕЕ ПИШЕМ!\n"
        "Пиши менеджеру прямо сейчас 👇"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="НАПИСАТЬ МЕНЕДЖЕРУ →",
                    url="https://t.me/sasha_teatr"
                )
            ]
        ]
    )

    while True:
        users = get_all_users()
        logging.info(f"Рассылка → найдено {len(users)} пользователей")

        sent_count = 0
        blocked_count = 0

        for user_id in users:
            try:
                await bot.send_message(
                    user_id,
                    text,
                    reply_markup=keyboard,
                    disable_notification=True
                )
                sent_count += 1
                await asyncio.sleep(0.07)  # ~14 msg/sec — безопасный лимит
            except Exception as e:
                err_str = str(e).lower()
                if "blocked" in err_str or "forbidden" in err_str or "chat not found" in err_str:
                    blocked_count += 1
                logging.warning(f"Не удалось отправить {user_id}: {e}")

        logging.info(f"Рассылка завершена: отправлено {sent_count}, заблокировали/ошибок {blocked_count}")
        await asyncio.sleep(10800)  # 3 часа = 10800 секунд


# ── Запуск ───────────────────────────────────────────────────────────────
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(broadcaster())  # фоновая рассылка

    logging.info("Бот запущен • polling + рассылка каждые 3 часа")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
