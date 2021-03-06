import pymysql
import pandas as pd

db = pymysql.connect(host="", user="", passwd="", db="")

frame = pd.read_sql("SELECT * FROM Chats", db)

pd.set_option('display.expand_frame_repr', False)

frame = frame.to_string(index=False)

print(frame)

db.close()