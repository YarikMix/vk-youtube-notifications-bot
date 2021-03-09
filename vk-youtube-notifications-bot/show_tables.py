"""
Выводит таблицу с беседами и таблицу с ютуб каналами
"""
from main import Utils, reconnect


utils = Utils()

reconnect()  # Подключаемся к базе данных

print("\n")

utils.show_chats()  # Выводим таблицу Chats

print("\n\n")

utils.show_channels()  # Выводим таблицу Channels