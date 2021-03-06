# -*- coding: utf-8 -*-
import json
import time
from datetime import datetime
from threading import Thread
from pathlib import Path

import requests
import pymysql
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.utils import get_random_id
from fuzzywuzzy import fuzz
import yaml

from functions import console_log, get_next

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR.joinpath("config.yaml")

with open(CONFIG_PATH) as ymlFile:
    config = yaml.load(ymlFile.read(), Loader=yaml.Loader)


def reconnect():
    """Подключаемся к базе данных"""
    global db, mycursor

    try:
        db.close()
        db = pymysql.connect(
            host="",
            user="",
            passwd="",
            db=""
        )
        mycursor = db.cursor()

        console_log("Переподключаемся к базе данных")

    except:
        """Если подключение первое"""
        db = pymysql.connect(
            host="",
            user="",
            passwd="",
            db=""
        )
        mycursor = db.cursor()

        console_log("Подключаемся к базе данных")

def connection():
    """Переподключаемся к базе данных каждые 5 минут"""
    while True:
        reconnect()
        time.sleep(300)

def create_tables():
    """Создаём таблицы Chats и Channels, если их нет"""
    # Создаём таблицу с чатами, если её нет
    mycursor.execute("""
    	CREATE TABLE IF NOT EXISTS Chats(
    	id int PRIMARY KEY,
    	added datetime NOT NULL
    	)
    """)

    # Создаём таблицу с ютуб каналами, если её нет
    mycursor.execute("""
    	CREATE TABLE IF NOT EXISTS Channels(
    	chat_id int,
    	channel_id VARCHAR(50),
    	channel_title VARCHAR(50),
    	last_video_id VARCHAR(50),
    	last_video_title VARCHAR(50)
    	)
    """)


class YouTubeParser(object):
    def __init__(self, api_keys):
        self.api_keys = api_keys.split(",")
        self.api_key = self.api_keys[0]

    def get_channel_info(self, title: str):
        """Возвращает id ютуб канала"""
        url = "https://www.googleapis.com/youtube/v3/search?"
        params = {
            "part": "snippet",
            "key": self.api_key,
            "q": title.lower().replace(" ", "+")
        }
        response = requests.get(url=url, params=params)
        data = json.loads(response.text)

        if response.status_code == 200:
            # Запрос прошёл успешно
            items = data["items"]
            for item in items:
                kind = item["id"]["kind"]
                if kind == "youtube#channel":
                    channel_id = item["id"]["channelId"]
                    channel_title = item["snippet"]["title"]
                    return {
                        "id": channel_id,
                        "title": channel_title
                    }
            return 404
        elif response.status_code == 403:
            self.quota_exceeded()
            return 403

    def get_last_video(self, channel_id: str):
        """Вовращает id последнего видео ютуб канала"""
        url = "https://www.googleapis.com/youtube/v3/search?"
        params = {
            "key": self.api_key,
            "channelId": channel_id,
            "id": "",
            "order": "date"
        }
        # url = f"{self.domain}/search?key={self.api_key}&channelId={channel_id}&id&order=date"
        response = requests.get(url=url, params=params)

        if response.status_code == 200:
            # Запрос прошёл успешно
            data = json.loads(response.text)
            last_video_id = data["items"][0]["id"]["videoId"]
            return last_video_id
        elif response.status_code == 403:
            self.quota_exceeded()
            return 403

    def get_video_title(self, video_id: str):
        """Вовращает название видео по его id"""
        url = "https://www.googleapis.com/youtube/v3/videos?"
        params = {
            "part": "snippet",
            "id": video_id,
            "key": self.api_key
        }
        response = requests.get(url=url, params=params)

        if response.status_code == 200:
            # Запрос прошёл успешно
            data = json.loads(response.text)
            video_title = data["items"][0]["snippet"]["title"]
            return video_title
        elif response.status_code == 403:
            self.quota_exceeded()
            return 403

    def quota_exceeded(self):
        """
        Квота превышена :(
        Меняем ключ для работы с api
        """
        self.api_key = get_next(self.api_keys, self.api_key)
        console_log("Квота превышена :(")
        console_log("Меняем ключ для доступа к YouTube api")


class Bot(object):
    def auth(self):
        # Авторизируем бота
        authorize = vk_api.VkApi(token=config["group"]["group_token"])
        self.longpoll = VkBotLongPoll(
            vk=authorize,
            group_id=config["group"]["group_id"]
        )
        self.bot = authorize.get_api()

        # Авторизируем пользователя
        vk_session = vk_api.VkApi(token=config["user"]["user_token"])
        self.upload = vk_api.VkUpload(vk_session)

    def upload_video(self, video_url: str, video_title: str):
        response = self.upload.video(
            link=video_url,
            group_id=config["group"]["group_id"],
            name=video_title
        )
        attachment = "video{}_{}".format(response["owner_id"], response["video_id"])
        return attachment

    def add_chat(self, chat_id: int):
        """Записываем в базу данных id новой беседы"""
        try:
            console_log(f"Бот добавлен в беседу {chat_id}")
            mycursor.execute("INSERT INTO Chats (id, added) VALUES (%s, %s)", (chat_id, datetime.now()))
            db.commit()
        except Exception as e:
            console_log("Что-то пошло не так")
            print(e)

    def add_channel(self, chat_id: int, channel_title: str):
        """Добавляет ютуб канал в подписки беседы"""
        channel_info = youtube.get_channel_info(channel_title)

        if channel_info == 404:
            message = "Канал с таким названием не найден"
        elif channel_info == 403:
            message = "Что-то пошло не так. Без паники! Всем оставаться на своих местах!"
        else:
            # Получаем id ютуб канала
            channel_id = channel_info["id"]

            # Выбираем из базы данных все ютуб каналы, на которые подписана беседа
            mycursor.execute("SELECT * FROM Channels where chat_id = %s", (chat_id))

            # Составляем список из id ютуб каналов, на которые подписана беседа
            channel_ids = [channel[1] for channel in mycursor.fetchall()]

            # Если беседа уже подписана на этот ютуб канал
            if channel_id in channel_ids:
                message = f"Вы уже подписаны на этот канал"
            else:
                # Максимальное число подписок - 3
                if len(channel_ids) == 3:
                    message = f"Вы подписались на максимальное количество каналов. " \
                              f"Отпишитесь от другого канала, чтобы подписаться на этот канал."

                else:
                    # Получаем информацию о ютуб канале
                    channel_title = channel_info["title"]
                    last_video_id = youtube.get_last_video(channel_id)
                    last_video_title = youtube.get_video_title(last_video_id)

                    # Добавляем ютуб канал в подписки беседы
                    mycursor.execute("""
                    INSERT INTO Channels (chat_id, channel_id, channel_title, last_video_id, last_video_title) 
                    VALUES (%s, %s, %s, %s, %s)
                    """, (chat_id, channel_id, channel_title, last_video_id, last_video_title))

                    db.commit()

                    message = f"✅Вы подписались на канал {channel_title}"
                    console_log(f"Беседа {chat_id} подписалась на канал {channel_title}")

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def remove_channel(self, chat_id: int, channel_title: str):
        channel_info = youtube.get_channel_info(channel_title)
        if channel_info == 404:
            message = "Канал с таким названием не найден"
        elif channel_info == 403:
            message = "Что-то пошло не так. Без паники! Всем оставаться на своих местах!"
        else:
            # Получаем id ютуб канала
            channel_id = channel_info["id"]

            # Получаем id всех ютуб каналов, на которые подписана беседа
            mycursor.execute("SELECT * FROM Channels where chat_id = %s", (chat_id))
            channel_ids = [channel[1] for channel in mycursor.fetchall()]

            # Если беседа не подписана на ютуб канал
            if channel_id not in channel_ids:
                message = "Как ты собрался отписаться от канала, на который не подписан? Петух"
            else:
                # Удаляем ютуб канал
                mycursor.execute("DELETE FROM Channels WHERE channel_id = %s", channel_id)

                db.commit()

                message = f"❌Вы отписались от канала {channel_title}"
                console_log(f"Беседа {chat_id} отписалась от канала {channel_title}")

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def remove_all_channels(self, chat_id: int):
        """Удаляет все ютуб каналы, на которые подписана беседа"""
        # Получаем id всех ютуб каналов, на которые подписана беседа
        mycursor.execute("SELECT channel_id FROM Channels WHERE chat_id = %s", chat_id)
        channel_ids = [channel[0] for channel in mycursor.fetchall()]

        if len(channel_ids) == 0:
            message = "От чего ты отписываться собрался? Петух"
        else:

            # Удаляем все ютуб каналы, на которые подписана беседа
            mycursor.execute("DELETE FROM Channels WHERE chat_id = %s", chat_id)

            db.commit()

            message = "Вы отписались от всех каналов"
            console_log(f"Беседа {chat_id} отписалась от всех каналов")

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def show_subscriptions(self, chat_id: int):
        """Отправляет в беседу список ютуб каналов, на которые подписана беседа"""

        # Составляем список из всех ютуб каналов, на которые подписана беседа
        mycursor.execute("SELECT * FROM Channels where chat_id = %s", (chat_id))
        channels = [channel for channel in mycursor.fetchall()]

        if len(channels) == 0:
            message = "У этой беседы нет подписок"
        else:
            message = f"{len(channels)}/3 подписок"
            for i, channel in enumerate(channels, start=1):
                message += f"\n{i}. {channel[2]}"

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def show_video(self, chat_id: int, video_title: str):
        """Отправляет в беседу найденное видео"""
        # ToDo сделать
        pass
        # with open("chats.json", encoding="utf-8") as f:
        #     data = json.load(f)
        # first_channel = data["chats"]["1"]["channels"][0]
        # self._notification(chat_id, first_channel)

    def get_help(self, chat_id: int):
        message = """
        Список команд:
        !подписаться <название канала>
        !отписаться <название канала>
        !отписаться - отписаться от всех каналов
        !видео <название видео>
        !подписки
        !помощь  
        """

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def notification(self, chat_id: int, channel: dict):
        """Отправляет в беседу уведомление о выходе нового видео"""
        channel_id = channel["channel_id"]
        channel_title = channel["channel_title"]
        video_id = channel["last_video_id"]
        video_title = channel["last_video_title"]
        video_url = f"https://www.youtube.com/watch?v={video_id}&ab_channel={channel_id}"
        message = f"На канале {channel_title} вышло новое видео!"

        console_log(message)
        console_log(video_title)

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

        self.bot.messages.send(
            chat_id=chat_id,
            message="",
            attachment=self.upload_video(video_url, video_title),
            random_id=get_random_id()
        )

    def check_chats(self):
        while True:
            console_log("Проверка всех чатов")

            # Получаем id всех чатов, в которых состоит бот
            mycursor.execute("SELECT id FROM Chats")
            chat_ids = [chat[0] for chat in mycursor.fetchall()]

            for chat_id in chat_ids:
                # Составляем список из всех ютуб каналов, на которые подписана беседа
                mycursor.execute("SELECT * FROM Channels WHERE chat_id = %s", chat_id)
                channels = [channel for channel in mycursor.fetchall()]

                for channel in channels:

                    channel_id = channel[1]
                    channel_title = channel[2]
                    last_video_id = youtube.get_last_video(channel_id)
                    # Если на канале вышло новое видео
                    if last_video_id != 403 and last_video_id != channel[3]:
                        print(last_video_id)
                        print(youtube.get_last_video(channel_id))

                        # Обновляем id последнего видео у ютуб канала
                        last_video_id = youtube.get_last_video(channel_id)
                        last_video_title = youtube.get_video_title(last_video_id)
                        mycursor.execute("""
                        UPDATE Channels 
                        SET last_video_id = %s, last_video_title = %s 
                        WHERE chat_id = %s AND channel_id = %s
                        """, (last_video_id, last_video_title, chat_id, channel_id))

                        db.commit()

                        channel = {
                            "channel_id": channel_id,
                            "channel_title": channel_title,
                            "last_video_id": last_video_id,
                            "last_video_title": last_video_title
                        }

                        # Отправляем уведомление в беседу
                        self.notification(chat_id, channel)

            time.sleep(3600)  # Следующая проверка через час

    def listen(self):
        create_tables()  # Создаём таблицы в базе данных
        console_log("Бот запущен")
        while True:
            try:
                # Отслеживаем каждое событие в беседе
                for event in self.longpoll.listen():
                    if event.type == VkBotEventType.MESSAGE_NEW and event.from_chat:
                        received_message = event.message["text"].lower()
                        chat_id = event.chat_id

                        # Если сообщение появилось из беседы и этой беседы нет в базе данных, то довабляем её id
                        mycursor.execute("SELECT id FROM Chats")
                        result = mycursor.fetchall()
                        chat_ids = [chat_id[0] for chat_id in result]
                        if chat_id not in chat_ids:
                            self.add_chat(chat_id)

                        # Если бота пригласили в новую беседу
                        if "action" in event.message:
                            if event.message["action"]["type"] == "chat_invite_user":
                                if event.message["action"]["member_id"] == -int(config["group"]["group_id"]):
                                    self.add_chat(chat_id)

                        # Пропускаем невалидные команды
                        if received_message in ["!подписаться", "!видео"]:
                            continue

                        if received_message[:12] == "!подписаться":
                            channel_title = received_message.split(" ")[1]
                            self.add_channel(chat_id, channel_title)
                        elif received_message == "!отписаться":
                            self.remove_all_channels(chat_id)
                        elif fuzz.ratio(received_message[:11], "!отписаться") > 85:
                            channel_title = received_message.split(" ")[1]
                            self.remove_channel(chat_id, channel_title)
                        elif fuzz.ratio(received_message, "!подписки") > 75:
                            self.show_subscriptions(chat_id)
                        elif fuzz.ratio(received_message, "!видео") > 75:
                            video_title = received_message.split(" ")[1]
                            self.show_video(chat_id, video_title)
                        elif fuzz.ratio(received_message, "!помощь") > 75:
                            self.get_help(chat_id)
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                # Перезагрузка серверов ВКонтакте
                print(e)
                console_log("Перезапуск бота")


if __name__ == "__main__":
    # Авторизируемся для работы с YouTube API
    youtube = YouTubeParser(
        api_keys=config["youtube"]["api_keys"]
    )

    vkbot = Bot()
    vkbot.auth()

    p1 = Thread(target=connection)
    p1.start()  # Подключаемся к базе данных
    time.sleep(0.1)
    p2 = Thread(target=vkbot.listen)
    p2.start()  # Запускаем мониторинг бесед
    time.sleep(0.1)
    p1 = Thread(target=vkbot.check_chats)
    p1.start()  # Запускаем мониторинг ютуб каналов