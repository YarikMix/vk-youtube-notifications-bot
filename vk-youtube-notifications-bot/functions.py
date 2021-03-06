import json
import pytz
import datetime


def write_json(data):
    """Печает в файл data.json переданные данные"""
    with open("data.json", "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def get_time():
    """
    Возвращает время формата ДД.ММ.ГГ ЧЧ:ММ:СС (по МСК)
    Например, 01.01.01 13:37:00
    """
    return datetime.datetime.strftime(datetime.datetime.now(pytz.timezone("Europe/Moscow")), "%d.%m.%Y %H:%M:%S")


def console_log(text):
    """Bывод текста в консоль со временем"""
    print("[{}] {}".format(get_time(), text))


def get_next(arr, current):
    """Вовращает следующий элемент списка"""
    i = arr.index(current)
    return arr[(i + 1) % len(arr)]
