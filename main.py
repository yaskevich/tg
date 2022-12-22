import os
import traceback
import json
from dotenv import load_dotenv # python-dotenv
import psycopg2 # psycopg2-binary
from telethon.sync import TelegramClient
import sqlite3
from datetime import datetime, timedelta
# from telethon.sync import TelegramClient, events
# from telethon.tl.types import PeerUser, PeerChat, PeerChannel
# select chat_id, data->'$.message' from messages order by created DESC limit 100;
load_dotenv(verbose=True)
api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
ids = os.getenv("ID").split()
counter = 0
# f = open("debug.txt", "w", encoding='utf-8')
# print(api_id, api_hash)
date_mark = datetime.today() - timedelta(days=5)

database_dict = {
    "messages":
        """CREATE TABLE IF NOT EXISTS messages (
                id INTEGER NOT NULL PRIMARY KEY UNIQUE,	
                chat_id INTEGER,
                data json,	
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                imagepath text,
                tg_id integer unique
        )""",
    "users":
        """CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,	
                username text,	
                firstname text,
                lastname text,
                tg_id integer unique
        )""",
    "channels":
        """CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,	
                name text,        
                tg_id integer unique
        )""",
}


# CREATE TABLE groups (
#                 id INTEGER NOT NULL PRIMARY KEY UNIQUE,	
#                 chat_id INTEGER,
#                 data json,	
#                 created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 imagepath text,
#                 tg_id integer unique
#         );


try:
    sqlite_connection = sqlite3.connect('tg.db')
    cursor = sqlite_connection.cursor()
    print("Connected")
    cursor.execute(database_dict["messages"])
    sqlite_select_query = "select sqlite_version()"
    cursor.execute(sqlite_select_query)
    record = cursor.fetchall()
    print("Version", record[0][0])

    # conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host='localhost')
    # cursor = conn.cursor()
    # group_id = 0
    # cursor.execute("SELECT table_name FROM information_schema.columns WHERE table_schema = 'public' GROUP BY table_name")
    # res = cursor.fetchall()
    # if res:
        # tables = [x[0] for x in res]
        # diff = set(database_dict.keys()).difference(set(tables))
        # if(len(diff)):
            # for table in diff:
                # print("init table:", table)
                # # cursor.execute(database_dict[table])
                # # cursor.execute("ALTER TABLE %s OWNER TO %s", (table, db_user))
                # # conn.commit()
                
    # exit()

    with TelegramClient('tg', api_id, api_hash) as client:
        # for dialog in client.iter_dialogs():
        #     # print(dialog.title, dialog.id)
        #     if dialog.id < 0:
        #         print(dialog.title, dialog.id)
        # exit()
            # if dialog.title == group_name:
                # print(dialog, file=f)
                # break
        # c = client.get_entity(PeerChat(int(id)))
        # , min_id = 100000, limit=2

        for person in client.get_participants(int(ids[1])):
            who = person.to_dict()
            # print(who["username"], who["first_name"], who["last_name"], who["id"])
            cursor.execute("INSERT INTO users(username, firstname, lastname, tg_id) VALUES (?, ?, ?, ?) ON CONFLICT (tg_id) DO NOTHING",
            (who["username"], who["first_name"], who["last_name"], who["id"]))
        
        # conn.commit()

        for id in ids:
            print("fetching", str(id))
            for message in client.iter_messages(int(id), reverse=True, offset_date = date_mark):
                # print(message.id)
                # print (message.to_dict())
                message_json = json.dumps(message.to_dict(), ensure_ascii=False, default=str)
                counter +=1
                jpg_name = ""

                try:
                    if hasattr(message, "media") and hasattr(message.media, "photo"):
                        # print('File Name :' + str(dir(message.media.photo)))
                        # print('File Name :' + message.file.ext, message.media.photo.dc_id)
                        jpg_name = str(message.media.photo.id) + message.file.ext
                        jpg_path = os.path.join("media", jpg_name)
                        if not os.path.exists(jpg_path):
                            saved_path = message.download_media(jpg_path)
                    cursor.execute("INSERT INTO messages (tg_id, data, imagepath, chat_id) VALUES (?, ?, ?, ?) ON CONFLICT (tg_id) DO NOTHING", (message.id, message_json, jpg_name, id))

                except:
                    print("=====================================")
                    print(traceback.format_exc())
                    print(message_json)
                    print("=====================================")
                    exit()
            print("got", counter)
            counter = 0
        # conn.commit()
        # cursor.execute("SELECT COUNT(*) from messages")
        # # select count(*) from messages where length(imagepath) > 0;
        # res = cursor.fetchall()
        # print(f"DB count: {res[0][0]}")
        # cursor.close()
        # conn.close()
    sqlite_connection.commit()
    cursor.close()

except sqlite3.Error as error:
    print("SQLite error!", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()
        print("Closed")
