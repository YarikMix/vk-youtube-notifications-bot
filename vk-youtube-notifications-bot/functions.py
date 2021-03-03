import pytz
import datetime


def get_time():
    """
    Возвращает время формата ДД.ММ.ГГ ЧЧ:ММ:СС (по МСК)
    Например, 01.01.01 13:37:00
    """
    return datetime.datetime.strftime(datetime.datetime.now(pytz.timezone("Europe/Moscow")), "%d.%m.%Y %H:%M:%S")

def console_log(text):
    """Bывод текста в консоль со временем"""
    print("[{}] {}".format(get_time(), text))