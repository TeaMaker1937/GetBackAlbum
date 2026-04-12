import os
import asyncio
import logging
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ContentType, InputMediaPhoto, InputMediaVideo, Update
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from collections import defaultdict, deque
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_safe.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Получаем токен из переменных окружения
# Платформа FPS.ms автоматически подхватит переменную BOT_TOKEN, если вы добавите её через панель управления
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения. Пожалуйста, добавьте его в настройках сервера на FPS.ms")

# Прокси больше не нужен, создаём обычную сессию
session = AiohttpSession(timeout=60)
bot = Bot(token=BOT_TOKEN, session=session, default=DefaultBotProperties(parse_mode='HTML'))

dp = Dispatcher()
router = Router()
dp.include_router(router)

# Хранилище для медиафайлов пользователей
user_media = defaultdict(deque)
user_last_activity = defaultdict(float)
processing_active = defaultdict(bool)
user_last_sent = defaultdict(float)  # Время последней отправки

# Глобальные настройки (соблюдаем лимиты Telegram)
BATCH_SIZE = 10
MIN_SEND_INTERVAL = 3.0  # Минимальный интервал между отправками (Telegram limit)
MAX_QUEUE_SIZE = 1000
FLOOD_RETRY_DELAY = 15  # Задержка при flood control

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
    user_id = message.from_user.id
    queue_size = len(user_media[user_id])
    status = "активна" if processing_active[user_id] else "неактивна"
    time_since_last = time.time() - user_last_sent.get(user_id, 0)
    
    status_text = (
        f"📊 Статус:\n"
        f"Файлов в очереди: {queue_size}\n"
        f"Обработка: {status}\n"
        f"Прошло с последней отправки: {time_since_last:.1f} сек\n"
        f"Рекомендуемая задержка: {MIN_SEND_INTERVAL} сек"
    )
    await message.answer(status_text)

@router.message(Command("pause"))
async def cmd_pause(message: Message):
    """Приостановка обработки"""
    user_id = message.from_user.id
    if processing_active[user_id]:
        processing_active[user_id] = False
        await message.answer("⏸️ Обработка приостановлена. Используйте /flush для возобновления.")
    else:
        await message.answer("⏸️ Обработка уже приостановлена")

@router.message(F.content_type.in_({ContentType.PHOTO, ContentType.VIDEO}))
async def handle_media(message: Message):
    user_id = message.from_user.id
    user_last_activity[user_id] = time.time()
    
    # Проверяем размер очереди
    if len(user_media[user_id]) >= MAX_QUEUE_SIZE:
        await message.answer("❌ Очередь переполнена! Используйте /clear чтобы очистить")
        return
    
    # Получаем file_id
    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        media_type = "video"
    else:
        return
    
    # Добавляем в очередь
    file_data = {
        "type": media_type,
        "file_id": file_id,
        "message_id": message.message_id,
        "timestamp": time.time()
    }
    
    user_media[user_id].append(file_data)
    
    # Удаляем оригинальное сообщение (только если очередь не переполнена)
    try:
        await bot.delete_message(user_id, message.message_id)
    except Exception as e:
        logger.warning(f"Не удалось удалить сообщение: {e}")
    
    # Запускаем обработку если она не активна и есть достаточное количество файлов
    if not processing_active[user_id] and len(user_media[user_id]) >= BATCH_SIZE:
        processing_active[user_id] = True
        asyncio.create_task(safe_process_user_media(user_id))

async def safe_process_user_media(user_id: int):
    """Безопасная обработка с соблюдением лимитов Telegram"""
    logger.info(f"🛡️ Запуск безопасной обработки для пользователя {user_id}")
    
    try:
        while user_media[user_id] and processing_active[user_id]:
            current_time = time.time()
            
            # Проверяем, прошло ли достаточно времени с последней отправки
            time_since_last_send = current_time - user_last_sent.get(user_id, 0)
            if time_since_last_send < MIN_SEND_INTERVAL:
                wait_time = MIN_SEND_INTERVAL - time_since_last_send
                logger.info(f"⏳ Ожидание {wait_time:.1f} сек перед следующей отправкой")
                await asyncio.sleep(wait_time)
            
            # Проверяем, есть ли файлы для отправки
            if len(user_media[user_id]) < BATCH_SIZE:
                # Если файлов меньше 10, но прошло много времени - отправляем что есть
                time_since_activity = current_time - user_last_activity[user_id]
                if len(user_media[user_id]) > 0 and time_since_activity > 10:
                    await safe_send_batch(user_id, len(user_media[user_id]))
                break
            
            # Отправляем пачку из 10 файлов
            success = await safe_send_batch(user_id, BATCH_SIZE)
            
            if not success:
                # При ошибке flood control ждем дольше
                logger.warning(f"🛑 Обнаружена ошибка, увеличиваю задержку для {user_id}")
                await asyncio.sleep(FLOOD_RETRY_DELAY)
                continue
                
    except Exception as e:
        logger.error(f"❌ Ошибка в процессе обработки для {user_id}: {e}")
    finally:
        processing_active[user_id] = False
        logger.info(f"⏹️ Остановка обработки для пользователя {user_id}")

async def safe_send_batch(user_id: int, batch_size: int) -> bool:
    """Безопасная отправка пачки файлов с обработкой ошибок"""
    if not user_media[user_id] or batch_size <= 0:
        return True
    
    try:
        # Собираем медиа для альбома
        media_list = []
        for _ in range(min(batch_size, len(user_media[user_id]))):
            media_item = user_media[user_id].popleft()
            
            if media_item["type"] == "photo":
                media_list.append(InputMediaPhoto(media=media_item["file_id"]))
            else:
                media_list.append(InputMediaVideo(media=media_item["file_id"]))
        
        if not media_list:
            return True
        
        # Отправляем альбом
        await bot.send_media_group(user_id, media_list)
        user_last_sent[user_id] = time.time()
        
        logger.info(f"✅ Безопасно отправлен альбом из {len(media_list)} файлов для {user_id}")
        return True
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Ошибка отправки альбома для {user_id}: {error_msg}")
        
        # Возвращаем файлы обратно в очередь при ошибке
        for media_item in media_list:
            # Восстанавливаем оригинальные данные (упрощенно)
            original_type = "photo" if isinstance(media_list[0], InputMediaPhoto) else "video"
            user_media[user_id].appendleft({
                "type": original_type,
                "file_id": media_item.media,
                "message_id": None,  # Не можем восстановить
                "timestamp": time.time()
            })
        
        # Проверяем, это ли flood control
        if "Flood control" in error_msg or "Too Many Requests" in error_msg:
            logger.warning(f"🚫 Flood control detected for {user_id}, waiting...")
            return False
        else:
            # Для других ошибок ждем немного и продолжаем
            await asyncio.sleep(5)
            return True

@router.message(Command("flush"))
async def cmd_flush(message: Message):
    """Принудительная отправка всех файлов с соблюдением лимитов"""
    user_id = message.from_user.id
    
    if not user_media[user_id]:
        await message.answer("📭 Очередь пуста")
        return
    
    count = len(user_media[user_id])
    status_msg = await message.answer(f"🚀 Безопасная отправка {count} файлов...")
    
    # Активируем обработку если она не активна
    if not processing_active[user_id]:
        processing_active[user_id] = True
        asyncio.create_task(safe_process_user_media(user_id))
    
    # Мониторим прогресс
    initial_count = count
    while user_media[user_id] and processing_active[user_id]:
        remaining = len(user_media[user_id])
        sent = initial_count - remaining
        
        if sent % 20 == 0:  # Обновляем статус каждые 20 файлов
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=status_msg.message_id,
                text=f"🚀 Отправлено {sent}/{initial_count} файлов..."
            )
        
        await asyncio.sleep(2)
    
    final_sent = initial_count - len(user_media[user_id])
    await bot.edit_message_text(
        chat_id=user_id,
        message_id=status_msg.message_id,
        text=f"✅ Безопасно отправлено {final_sent} файлов!"
    )
    
    # Удаляем статус через 5 секунд
    await asyncio.sleep(5)
    try:
        await bot.delete_message(user_id, status_msg.message_id)
    except:
        pass

@router.message(Command("clear"))
async def cmd_clear(message: Message):
    """Очистка очереди"""
    user_id = message.from_user.id
    
    if not user_media[user_id]:
        await message.answer("📭 Очередь уже пуста")
        return
    
    count = len(user_media[user_id])
    user_media[user_id].clear()
    processing_active[user_id] = False
    
    await message.answer(f"🗑️ Очередь очищена ({count} файлов)")

@router.message(Command("help"))
async def cmd_help(message: Message):
    """Помощь"""
    help_text = """
🤖 **Безопасный бот для группировки медиа**

📸 **Как использовать:**
1. Отправляйте фото или видео в бот
2. Бот автоматически соберет их в альбомы по 10 штук
3. Соблюдает ограничения Telegram для надежности

⚡ **Команды:**
/start - начать работу
/flush - отправить все файлы сейчас
/clear - очистить очередь
/status - статус обработки
/pause - приостановить обработку
/help - эта справка

🛡️ **Особенности:**
- Автоматическая защита от ограничений Telegram
- Сохранение файлов при ошибках
- Безопасные интервалы между отправками
"""
    await message.answer(help_text)

async def on_startup(bot: Bot):
    # Устанавливаем вебхук на URL, который Render даст вашему приложению
    webhook_url = f"https://{os.environ['RENDER_EXTERNAL_HOSTNAME']}/webhook"
    await bot.set_webhook(webhook_url)
    logger.info(f"Webhook set to {webhook_url}")
    
async def health(request):
    return web.Response(text="OK")
        
def main():
    # Получаем порт от Render (обязательно!)
    port = int(os.environ.get('PORT', 8080))
    
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path="/webhook")
    setup_application(app, dp, bot=bot)

     app.router.add_get('/', health)
    # Запускаем aiohttp сервер
    web.run_app(app, host='0.0.0.0', port=port)

if __name__ == '__main__':
    main()
