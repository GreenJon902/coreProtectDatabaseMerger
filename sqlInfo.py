import abc
import os
import sqlite3

import mysql.connector

import pickle

CACHE_MYSQL = os.getenv("CACHE_MYSQL", "0") == "1"


class SqlInfo:
    users: dict[int, str]  # Maps from database uid to minecraft uuid
    worlds: dict[int, str]  # Maps from database wid to minecraft world name
    blockdata_map: dict[int, str]  # Maps from database wid to minecraft world name

    def __init__(self, details):
        """
        Creates a new SqlInfo object, this exists to help read and store data from a database.
        :param details: A dictionary holding required information for a specific type of SqlInfo.
        """

        cnx, cursor = self.getConnectionAndCursor(details)
        self.load(cursor, details)
        cursor.close()
        cnx.close()

    def getConnectionAndCursor(self, details):
        raise NotImplemented()

    def load(self, cursor, details):
        print(f"Loading:")
        self.users = SqlInfo.load_dict(cursor, "rowid", "uuid",
                                       f"{details['prefix']}user{details['postfix']}", False)
        print(f"\tUsers: {self.users}")
        self.worlds = SqlInfo.load_dict(cursor, "id", "world",
                                        f"{details['prefix']}world{details['postfix']}", False)
        print(f"\tWorlds: {self.worlds}")
        self.blockdata_map = SqlInfo.load_dict(cursor, "id", "data",
                                               f"{details['prefix']}blockdata_map{details['postfix']}", False)
        print(f"\tBlockdata_Map: {self.blockdata_map}")

    @classmethod
    def load_dict(cls, cursor, keyName, valueName, tableName, allowNone) -> dict[int, str]:
        users = {}

        cursor.execute(f"SELECT {keyName}, {valueName} FROM {tableName}")
        for uid, uuid in cursor:
            if not allowNone and uuid is None:
                print(f"\t\tIgnoring {keyName}: {valueName} as is None")
                continue
            users[uid] = uuid

        return users


class MySqlInfo(SqlInfo):
    def __init__(self, details):
        if CACHE_MYSQL and os.path.exists("mySqlCache"):
            obj = pickle.load(open("mySqlCache", "rb"))
            self.users = obj.users
            self.worlds = obj.worlds
            self.blockdata_map = obj.blockdata_map
            print(f"Got from cache:\n\tUsers: {self.users}\n\tWorld: {self.worlds}\n\tBlockdata_Map: {self.blockdata_map}")
            return

        super().__init__(details)

        if CACHE_MYSQL:
            pickle.dump(self, open("mySqlCache", "wb"))

    def getConnectionAndCursor(self, details):
        cnx = mysql.connector.connect(**details)
        cursor = cnx.cursor()
        return cnx, cursor


class SqlLiteInfo(SqlInfo):
    rows_raw: list

    def getConnectionAndCursor(self, details):
        cnx = sqlite3.connect(details["path"])
        cursor = cnx.cursor()
        return cnx, cursor

    def load(self, cursor, details):
        super().load(cursor, details)

        cursor.execute(f"SELECT count() FROM {details['prefix']}block{details['postfix']}")
        count = cursor.fetchone()[0]

        self.rows_raw = [None] * count  # Optimize memory allocation
        cursor.execute(f"SELECT time, user, wid, x, y, z, type, data, meta, blockdata, action, rolled_back "
                       f"FROM {details['prefix']}block{details['postfix']} LIMIT 1000")  # TODO: Remove limit
        i = 0
        for values in cursor:
            self.rows_raw.append(zip(["time", "user", "wid", "x", "y", "z", "type", "data", "meta", "blockdata",
                                      "action", "rolled_back"], values))
            if i % 500000 == 0:
                print(f"\t\t{i}/{count}")
            i += 1
        print(f"\tRows Raw: ...")
