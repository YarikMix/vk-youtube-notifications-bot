"""
Показывает в статусе группы и в статусе создателя группы
количество бесед, в которых присутствует бот.
"""
import time

from main import Utils
from functions import console_log


utils = Utils()
utils.auth()

while True:
    """Меняем статус каждый день"""
    utils.set_status()
    console_log("Статус обновлён")
    time.sleep(86400)
