import pymysql

from main import config
from functions import console_log


def reconnect():
    """Подключаемся к базе данных"""
    global db, mycursor

    try:
        db.close()
        db = pymysql.connect(
            host=config["database"]["host"],
            user=config["database"]["user"],
            passwd=config["database"]["passwd"],
            db=config["database"]["db"]
        )
        mycursor = db.cursor()

        console_log("Переподключаемся к базе данных")

    except:
        """Если подключение первое"""
        db = pymysql.connect(
            host=config["database"]["host"],
            user=config["database"]["user"],
            passwd=config["database"]["passwd"],
            db=config["database"]["db"]
        )
        mycursor = db.cursor()

        console_log("Подключаемся к базе данных")

# Подключаемся к базе данных
reconnect()
# Выполняем запрос
mycursor.execute("SELECT * FROM Channels")
result = mycursor.fetchall()

print(result)
print("Запрос сделан успешно")