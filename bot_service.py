import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import os
import sys
import asyncio
from typing import Optional

sys.path.append(r'D:\Users\andre\Desktop\МАКСИМ\NOT GAMES\getBackAlbum')


class TelegramBotService(win32serviceutil.ServiceFramework):
    _svc_name_ = "TelegramGetBackAlbumBot"
    _svc_display_name_ = "Telegram GetBackAlbum Bot"
    _svc_description_ = "Служба для Telegram бота группировки медиафайлов"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.bot_loop: Optional[asyncio.AbstractEventLoop] = None
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        loop = self.bot_loop
        if loop and loop.is_running():
            try:
                # Мягкая остановка: aiogram корректно завершит start_polling
                from bot import dp
                asyncio.run_coroutine_threadsafe(dp.stop_polling(), loop)
            except Exception:
                # Fallback: грубая остановка loop (SvcStop работает из другого потока,
                # поэтому только call_soon_threadsafe)
                loop.call_soon_threadsafe(loop.stop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    async def run_bot(self):
        from bot import main
        await main()

    def main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.bot_loop = loop
        try:
            loop.run_until_complete(self.run_bot())
        except (RuntimeError, asyncio.CancelledError):
            pass  # штатная остановка службы
        except Exception as e:
            servicemanager.LogErrorMsg(f"Ошибка в службе: {e}")
        finally:
            try:
                loop.close()
            except Exception:
                pass


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TelegramBotService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TelegramBotService)