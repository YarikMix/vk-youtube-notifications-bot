# -*- coding: utf-8 -*-
import json
import time
from threading import Thread
from pathlib import Path

import requests
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api import VkUpload
from vk_api.utils import get_random_id
from fuzzywuzzy import fuzz
import yaml

from functions import write_json, console_log


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR.joinpath("config.yaml")


with open(CONFIG_PATH) as ymlFile:
    config = yaml.load(ymlFile.read(), Loader=yaml.Loader)


class YouTubeParser(object):
    def __init__(self, domain, api_key):
        self.domain = domain
        self.api_key = api_key

    def _get_channel_info(self, title: str):
        """Возвращает id ютуб канала"""
        query = title.lower().replace(" ", "+")
        url = f"{self.domain}/search?part=snippet&key={self.api_key}&q={query}"
        response = requests.get(url)
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
            return "Канал не найден"
        elif response.status_code == 403:
            # Квота превышена :(
            console_log("Квота превышена :(")
            return "Квота превышена"

    def _get_last_video(self, channel_id: str) -> str:
        """Вовращает id последнего видео ютуб канала"""
        url = f"{self.domain}/search?key={self.api_key}&channelId={channel_id}&id&order=date"
        response = requests.get(url)

        if response.status_code == 200:
            # Запрос прошёл успешно
            data = json.loads(response.text)
            last_video_id = data["items"][0]["id"]["videoId"]
            return last_video_id
        elif response.status_code == 403:
            # Квота превышена :(
            console_log("Квота превышена :(")
            return "Квота превышена"

    def _get_video_title(self, video_id: str) -> str:
        """Вовращает название видео по его id"""
        url = f"{self.domain}/videos?part=snippet&id={video_id}&key={self.api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            # Запрос прошёл успешно
            data = json.loads(response.text)
            video_title = data["items"][0]["snippet"]["title"]
            return video_title
        elif response.status_code == 403:
            # Квота превышена :(
            console_log("Квота превышена :(")
            return "Квота превышена"


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

    def _upload_video(self, video_url: str, video_title: str):
        response = self.upload.video(
            link=video_url,
            group_id=config["group"]["group_id"],
            name=video_title
        )
        attachment = "video{}_{}".format(response["owner_id"], response["video_id"])
        return attachment

    def _add_chat(self, chat_id: int):
        with open("chats.json", encoding="utf-8") as file:
            data = json.load(file)

        data["chats"].update({
            chat_id: {
                "channels": []
            }
        })

        with open("chats.json", "w", encoding="utf-8") as data_file:
            json.dump(data, data_file, indent=2)

    def _add_channel(self, chat_id: int, channel_title: str):
        """Добавляет ютуб канал в подписки беседы"""
        channel_info = youtube._get_channel_info(channel_title)

        if channel_info == "Канал не найден":
            message = "Канал с таким названием не найден"
        elif channel_info == "Квота превышена":
            message = "Что-то пошло не так. Без паники! Всем оставаться на своих местах!"
        else:
            with open("chats.json", encoding="utf-8") as file:
                data = json.load(file)

            # Получаем информацию о ютуб канале и о подписках беседы
            channel_id = channel_info["id"]
            channel_title = channel_info["title"]
            subscriptions = data["chats"][str(chat_id)]["channels"]

            # Если беседа подписана на ютуб канал
            if channel_id in [channel["id"] for channel in subscriptions]:
                message = f"Вы уже подписаны на канал {channel_title}"
            else:
                # Максимальное число подписок - 3
                if len(subscriptions) == 3:
                    message = f"Вы подписались на максимальное количество каналов. " \
                              f"Отпишитесь от другого канала, чтобы подписаться на канал {channel_title}"

                else:
                    last_video_id = youtube._get_last_video(channel_id)
                    last_video_title = youtube._get_video_title(last_video_id)

                    data["chats"][str(chat_id)]["channels"].append({
                        "id": channel_id,
                        "title": channel_title,
                        "last_video_id": last_video_id,
                        "last_video_title": last_video_title
                    })

                    message = f"✅Вы подписались на канал {channel_title}"
                    console_log(f"Беседа {chat_id} подписалась на канал {channel_title}")

                    with open("chats.json", "w", encoding="utf-8") as data_file:
                        json.dump(data, data_file, indent=2)

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def _remove_channel(self, chat_id: int, channel_title: str):
        with open("chats.json", encoding="utf-8") as file:
            data = json.load(file)

        channel_info = youtube._get_channel_info(channel_title)
        if channel_info == "Канал не найден":
            message = "Канал с таким названием не найден"
        elif channel_info == "Квота превышена":
            message = "Что-то пошло не так. Без паники! Всем оставаться на своих местах!"
        else:
            # Получаем информацию о ютуб канале и о подписках беседы
            channel_id = channel_info["id"]
            channel_title = channel_info["title"]
            subscriptions = data["chats"][str(chat_id)]["channels"]

            # Если беседа не подписана на ютуб канал
            if channel_id not in [channel["id"] for channel in subscriptions]:
                message = "Вы не можете отписаться от канала, на который не подписаны"
            else:
                # Ищем ютуб канал среди подписок беседы и удаляем его
                for channel in subscriptions:
                    if channel["id"] == channel_id:
                        data["chats"][str(chat_id)]["channels"].remove(channel)
                        break

                message = f"❌Вы отписались от канала {channel_title}"
                console_log(f"Беседа {chat_id} отписалась от канала {channel_title}")

                with open("chats.json", "w", encoding="utf-8") as file:
                    json.dump(data, file, indent=2)

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def _remove_all_channels(self, chat_id: str):
        # ToDo: сделать
        pass

    def _show_subscriptions(self, chat_id: str):
        with open("chats.json", encoding="utf-8") as file:
            data = json.load(file)

        subscriptions = data["chats"][str(chat_id)]["channels"]

        if len(subscriptions) == 0:
            message = "У этой беседы нет подписок"
        else:
            message = f"{len(subscriptions)}/3 подписок"
            for i, channel in enumerate(subscriptions, start=1):
                message += f"\n{i}. {channel['title']}"

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def _get_help(self, chat_id: int):
        message = "Список команд:\n" \
                  "!подписаться <название канала>\n" \
                  "!отписаться <название канала>\n" \
                  "!подписки\n" \
                  "!помощь"

        self.bot.messages.send(
            chat_id=chat_id,
            message=message,
            random_id=get_random_id()
        )

    def _notification(self, chat_id: int, channel: dict):
        channel_id = channel["id"]
        channel_title = channel["title"]
        video_id = channel["last_video_id"]
        video_title = youtube._get_video_title(video_id)
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
            attachment=self._upload_video(video_url, video_title),
            random_id=get_random_id()
        )

    def check_chats(self):
        while True:
            console_log("Проверка всех чатов")

            with open("chats.json", encoding="utf-8") as file:
                data = json.load(file)

            for chat_id, channels in data["chats"].items():
                channels = channels["channels"]
                for i, channel in enumerate(channels):
                    channel_id = channel["id"]
                    archive_last_video_id = channel["last_video_id"]
                    last_video_id = youtube._get_last_video(channel_id)
                    if last_video_id == "Квота превышена":
                        console_log("Квота превышена :(")
                    else:
                        # Если на канале вышло новое видео
                        if archive_last_video_id != last_video_id:
                            # Обновляем id последнего видео у канала
                            data["chats"][str(chat_id)]["channels"][i]["last_video_id"] = last_video_id

                            # Отправляем уведомление в беседу
                            self._notification(chat_id, channel)

                with open("chats.json", "w", encoding="utf-8") as data_file:
                    json.dump(data, data_file, indent=2)

            time.sleep(1800)  # Следующая проверка через 30 минут

    def listen(self):
        console_log("Бот запущен")
        while True:
            try:
                # Отслеживаем каждое событие в беседе
                for event in self.longpoll.listen():
                    if event.type == VkBotEventType.MESSAGE_NEW and event.from_chat:
                        received_message = event.message["text"].lower()
                        chat_id = event.chat_id

                        with open("chats.json", encoding="utf-8") as f:
                            data = json.load(f)

                        chat_ids = [int(chat_id) for chat_id in data["chats"].keys()]
                        if chat_id not in chat_ids:
                            console_log(f"Бот добавлен в беседу {chat_id}")
                            self._add_chat(chat_id)

                        # Если бота пригласили в новую беседу
                        if "action" in event.message:
                            if event.message["action"]["type"] == "chat_invite_user":
                                if event.message["action"]["member_id"] == -int(config["group"]["group_id"]):
                                    console_log(f"Бот добавлен в беседу {chat_id}")
                                    self._add_chat(chat_id)

                        if fuzz.ratio(received_message[:12], "!подписаться") > 75:
                            channel_title = received_message.split(" ")[1]
                            self._add_channel(chat_id, channel_title)
                        elif fuzz.ratio(received_message[:11], "!отписаться") > 75:
                            channel_title = received_message.split(" ")[1]
                            self._remove_channel(chat_id, channel_title)
                        elif fuzz.ratio(received_message, "!подписки") > 75:
                            self._show_subscriptions(chat_id)
                        elif fuzz.ratio(received_message, "!видео") > 75:
                            with open("chats.json", encoding="utf-8") as f:
                                data = json.load(f)

                            first_channel = data["chats"]["1"]["channels"][0]
                            self._notification(chat_id, first_channel)
                        elif fuzz.ratio(received_message, "!помощь") > 75:
                            self._get_help(chat_id)
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                print(e)
                console_log("Перезапуск бота")


if __name__ == "__main__":
    # Авторизируемся для работы с YouTube API
    youtube = YouTubeParser(
        domain=config["youtube"]["domain"],
        api_key=config["youtube"]["api_key"]
    )

    vkbot = Bot()
    vkbot.auth()

    p1 = Thread(target = vkbot.check_chats)
    p1.start()  # Запускаем мониторинг ютуб каналов
    time.sleep(0.1)
    p2 = Thread(target = vkbot.listen)
    p2.start()  # Запускаем мониторинг бесед