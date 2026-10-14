import os
import asyncio
import logging
from collections import defaultdict, deque
from typing import Optional, Union
import time

from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message,
    ContentType,
    InputMediaPhoto,
    InputMediaVideo,
)

try:
    # Алиас есть в новых версиях aiogram
    from aiogram.types import MediaUnion
except ImportError:
    # В aiogram 3.17 и старше его ещё нет — собираем сами
    MediaUnion = Union[InputMediaPhoto, InputMediaVideo]
    
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_safe.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher()
router = Router()
dp.include_router(router)

user_media = defaultdict(deque)
user_last_activity = defaultdict(float)
processing_active = defaultdict(bool)
user_last_sent = defaultdict(float)

BATCH_SIZE = 10
MIN_SEND_INTERVAL = 3.0
MAX_QUEUE_SIZE = 1000
FLOOD_RETRY_DELAY = 15
IDLE_TIMEOUT = 10
MAX_ATTEMPTS = 5

processing_tasks = {}
background_tasks = set()
flushing = defaultdict(bool)


def uid(message: Message) -> Optional[int]:
    """from_user в aiogram 3 — Optional (нет у апдейтов из каналов)."""
    return message.from_user.id if message.from_user else None


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🛡️ Безопасный бот для группировки медиафайлов\n\n"
        "📸 Отправляйте фото и видео - я соберу их в альбомы по 10 штук\n"
        "⚡ Автоматически соблюдаю ограничения Telegram\n\n"
        "Команды:\n"
        "/flush - принудительно отправить все файлы\n"
        "/clear - очистить очередь\n"
        "/status - показать статус\n"
        "/pause - приостановить обработку"
    )


@router.message(Command("status"))
async def cmd_status(message: Message):
    user_id = uid(message)
    if user_id is None:
        return
    queue_size = len(user_media[user_id])
    status = "активна" if processing_active[user_id] else "неактивна"
    time_since_last = time.time() - user_last_sent.get(user_id, 0)
    await message.answer(
        f"📊 Статус:\n"
        f"Файлов в очереди: {queue_size}\n"
        f"Обработка: {status}\n"
        f"Прошло с последней отправки: {time_since_last:.1f} сек\n"
        f"Рекомендуемая задержка: {MIN_SEND_INTERVAL} сек"
    )


@router.message(Command("pause"))
async def cmd_pause(message: Message):
    user_id = uid(message)
    if user_id is None:
        return
    if processing_active[user_id]:
        processing_active[user_id] = False
        await message.answer("⏸️ Обработка приостановлена. Используйте /flush для возобновления.")
    else:
        await message.answer("⏸️ Обработка уже приостановлена")


@router.message(F.content_type.in_({ContentType.PHOTO, ContentType.VIDEO}))
async def handle_media(message: Message):
    user_id = uid(message)
    if user_id is None:
        return

    user_last_activity[user_id] = time.time()

    if len(user_media[user_id]) >= MAX_QUEUE_SIZE:
        await message.answer("❌ Очередь переполнена! Используйте /clear чтобы очистить")
        return

    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        media_type = "video"
    else:
        return

    user_media[user_id].append({
        "type": media_type,
        "file_id": file_id,
        "message_id": message.message_id,
        "timestamp": time.time(),
    })

    try:
        await bot.delete_message(user_id, message.message_id)
    except Exception as e:
        logger.warning(f"Не удалось удалить сообщение: {e}")

    # Запуск автообработки — один блок, строго ПОСЛЕ добавления файла
    # (у вас этот блок был продублирован в начале и в конце хендлера)
    if not processing_active[user_id] and len(user_media[user_id]) >= BATCH_SIZE:
        processing_active[user_id] = True
        task = asyncio.create_task(safe_process_user_media(user_id))
        processing_tasks[user_id] = task
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)


async def safe_process_user_media(user_id: int):
    """Только авторежим. Хвост — после IDLE_TIMEOUT секунд тишины."""
    logger.info(f"▶️ Автообработка для {user_id}")
    failures = 0
    try:
        while user_media[user_id] and processing_active[user_id]:
            if len(user_media[user_id]) >= BATCH_SIZE:
                elapsed = time.time() - user_last_sent.get(user_id, 0)
                if elapsed < MIN_SEND_INTERVAL:
                    await asyncio.sleep(MIN_SEND_INTERVAL - elapsed)

                if await safe_send_batch(user_id, BATCH_SIZE):
                    failures = 0
                else:
                    failures += 1
                    if failures >= MAX_ATTEMPTS:
                        logger.error(f"🛑 {MAX_ATTEMPTS} неудач подряд для {user_id}, стоп")
                        break
                    await asyncio.sleep(FLOOD_RETRY_DELAY)
                continue

            idle = time.time() - user_last_activity[user_id]
            if idle > IDLE_TIMEOUT:
                await safe_send_batch(user_id, len(user_media[user_id]))
            break
    except Exception:
        logger.exception(f"❌ Ошибка автообработки {user_id}")
    finally:
        processing_active[user_id] = False
        logger.info(f"⏹ Автообработка {user_id} остановлена")


async def safe_send_batch(user_id: int, batch_size: int) -> bool:
    n = min(batch_size, len(user_media[user_id]))
    if n <= 0:
        return True

    batch = [user_media[user_id].popleft() for _ in range(n)]

    try:
        if n == 1:
            # send_media_group не принимает 1 элемент
            item = batch[0]
            if item["type"] == "photo":
                await bot.send_photo(user_id, item["file_id"])
            else:
                await bot.send_video(user_id, item["file_id"])
        else:
            media_list: list[MediaUnion] = [
                InputMediaPhoto(media=i["file_id"]) if i["type"] == "photo"
                else InputMediaVideo(media=i["file_id"])
                for i in batch
            ]
            await bot.send_media_group(user_id, media_list)

        user_last_sent[user_id] = time.time()
        logger.info(f"✅ Отправлено {n} файлов для {user_id}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка отправки для {user_id}: {e}")
        for item in reversed(batch):
            user_media[user_id].appendleft(item)
        return False


async def flush_queue(user_id: int, status_msg: Message):
    """Фоновая выгрузка всей очереди: без окна неактивности, 1 файл тоже уходит."""
    flushing[user_id] = True
    try:
        task = processing_tasks.get(user_id)
        if task and not task.done():
            try:
                await task
            except Exception:
                pass

        processing_active[user_id] = True
        total = len(user_media[user_id])
        last_reported = 0
        failures = 0

        try:
            while user_media[user_id] and failures < MAX_ATTEMPTS:
                elapsed = time.time() - user_last_sent.get(user_id, 0)
                if elapsed < MIN_SEND_INTERVAL:
                    await asyncio.sleep(MIN_SEND_INTERVAL - elapsed)

                n = min(BATCH_SIZE, len(user_media[user_id]))
                if await safe_send_batch(user_id, n):
                    failures = 0
                else:
                    failures += 1
                    await asyncio.sleep(FLOOD_RETRY_DELAY)
                    continue

                sent = total - len(user_media[user_id])
                if sent - last_reported >= 20:
                    last_reported = sent
                    try:
                        # ВАЖНО: text передаём только keyword-ом
                        await bot.edit_message_text(
                            chat_id=user_id,
                            message_id=status_msg.message_id,
                            text=f"🚀 Отправлено {sent}/{total} файлов...",
                        )
                    except TelegramBadRequest:
                        pass
        finally:
            processing_active[user_id] = False

        sent = total - len(user_media[user_id])
        remaining = len(user_media[user_id])
        text = f"✅ Отправлено {sent} файлов!"
        if remaining:
            text += f"\n⚠️ {remaining} файлов не отправлено — повторите /flush"
        try:
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=status_msg.message_id,
                text=text,
            )
        except TelegramBadRequest:
            pass
    finally:
        flushing[user_id] = False


@router.message(Command("flush"))
async def cmd_flush(message: Message):
    user_id = uid(message)
    if user_id is None:
        return

    if not user_media[user_id]:
        await message.answer("📭 Очередь пуста")
        return
    if flushing[user_id]:
        await message.answer("⏳ /flush уже выполняется")
        return

    status_msg = await message.answer(
        f"🚀 Принудительная отправка {len(user_media[user_id])} файлов..."
    )
    task = asyncio.create_task(flush_queue(user_id, status_msg))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


@router.message(Command("clear"))
async def cmd_clear(message: Message):
    user_id = uid(message)
    if user_id is None:
        return
    if not user_media[user_id]:
        await message.answer("📭 Очередь уже пуста")
        return
    count = len(user_media[user_id])
    user_media[user_id].clear()
    processing_active[user_id] = False
    await message.answer(f"🗑️ Очередь очищена ({count} файлов)")


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🤖 Безопасный бот для группировки медиа\n\n"
        "/start - начать работу\n"
        "/flush - отправить все файлы сейчас\n"
        "/clear - очистить очередь\n"
        "/status - статус обработки\n"
        "/pause - приостановить обработку\n"
        "/help - эта справка"
    )


async def on_startup(bot: Bot):
    host = os.environ["RENDER_EXTERNAL_HOSTNAME"]
    url = f"https://{host}/webhook"
    await bot.set_webhook(url, drop_pending_updates=True)
    logger.info(f"✅ Webhook установлен: {url}")


async def health(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def main():
    logger.info("🛡️ Запуск бота...")

    if os.getenv("RENDER_EXTERNAL_HOSTNAME"):
        # --- Режим вебхука (деплой на Render) ---
        dp.startup.register(on_startup)  # раньше on_startup нигде не регистрировался!

        app = web.Application()
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
        setup_application(app, dp, bot=bot)
        app.router.add_get("/", health)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
        await site.start()
        logger.info("🌐 Webhook-режим запущен")
        try:
            await asyncio.Event().wait()
        finally:
            await runner.cleanup()
    else:
        # --- Локальный режим / служба Windows: long polling ---
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
