import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import os
import sys
import asyncio

# Добавляем путь к проекту в sys.path
sys.path.append(r'D:\Users\andre\Desktop\МАКСИМ\NOT GAMES\getBackAlbum')

class TelegramBotService(win32serviceutil.ServiceFramework):
    _svc_name_ = "TelegramGetBackAlbumBot"
    _svc_display_name_ = "Telegram GetBackAlbum Bot"
    _svc_description_ = "Служба для Telegram бота группировки медиафайлов"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        if hasattr(self, 'bot_task'):
            self.bot_task.cancel()

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    async def run_bot(self):
        from bot import main
        await main()

    def main(self):
        try:
            # Запускаем бота в event loop
            self.bot_task = asyncio.new_event_loop()
            asyncio.set_event_loop(self.bot_task)
            self.bot_task.run_until_complete(self.run_bot())
        except Exception as e:
            servicemanager.LogErrorMsg(f"Ошибка в службе: {str(e)}")

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TelegramBotService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TelegramBotService)