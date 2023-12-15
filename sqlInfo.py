import abc
import os
import sqlite3
from typing import Union

import mysql.connector

from bidict import bidict

import pickle

from sqlLiteRow import SqlLiteRow, MissingDataException

CACHE_MYSQL = os.getenv("CACHE_MYSQL", "0") == "1"


class SqlInfo:
    users: dict[str, bidict[int, str]]  # Inner maps from database uid to time or user or uuid, Outer selects which
    worlds: bidict[int, str]  # Maps from database wid to minecraft world name
    blockdata_map: bidict[int, str]  # Maps from database wid to minecraft world name
    material_map: bidict[int, str]  # Maps from database block type to minecraft block name

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

        # For user, prefer to use uuid when can (aka when player) otherwise default to user (when natural)
        self.users = SqlInfo.load_dict(cursor, f"{details['prefix']}user{details['postfix']}", "rowid",
                                       {"time": dict, "user": bidict, "uuid": bidict}, type_=dict)
        print(f"\tUsers: {self.users}")
        self.worlds = SqlInfo.load_dict(cursor, f"{details['prefix']}world{details['postfix']}", "id", "world")
        print(f"\tWorlds: {self.worlds}")
        self.blockdata_map = SqlInfo.load_dict(cursor, f"{details['prefix']}blockdata_map{details['postfix']}", "id",
                                               "data")
        print(f"\tBlockdata_Map: {self.blockdata_map}")
        self.material_map = SqlInfo.load_dict(cursor, f"{details['prefix']}material_map{details['postfix']}", "id",
                                               "material")
        print(f"\tMaterial_Map: {self.blockdata_map}")

    @classmethod
    def load_dict(cls, cursor, tableName, keyName, valueName: Union[str, dict[str, type]], type_: type = bidict):
        if isinstance(valueName, str):
            result = type_()

            cursor.execute(f"SELECT {keyName}, {valueName} FROM {tableName}")
            for k, v in cursor:
                if v is not None:
                    result[k] = v
        else:
            result = type_()
            for vName, subtype_ in valueName.items():
                print(f"\t\tLoading for {vName}")
                result[vName] = SqlInfo.load_dict(cursor, tableName, keyName, vName, type_=subtype_)

        return result


class MySqlInfo(SqlInfo):
    def __init__(self, details):
        if CACHE_MYSQL and os.path.exists("mySqlCache"):
            obj = pickle.load(open("mySqlCache", "rb"))
            self.users = obj.users
            self.worlds = obj.worlds
            self.blockdata_map = obj.blockdata_map
            self.material_map = obj.material_map
            print(f"Got from cache:\n\tUsers: {self.users}\n\tWorld: {self.worlds}\n\tBlockdata_Map: {self.blockdata_map}Material_Map: {self.material_map}")
            return

        super().__init__(details)

        if CACHE_MYSQL:
            pickle.dump(self, open("mySqlCache", "wb"))

    def getConnectionAndCursor(self, details):
        cnx = mysql.connector.connect(user=details["user"], host=details["host"], password=details["password"],
                                      database=details["database"])
        cursor = cnx.cursor()
        return cnx, cursor


class SqlLiteInfo(SqlInfo):
    rows: list

    def getConnectionAndCursor(self, details):
        cnx = sqlite3.connect(details["path"])
        cursor = cnx.cursor()
        return cnx, cursor

    def load(self, cursor, details):
        super().load(cursor, details)

        cursor.execute(f"SELECT count() FROM {details['prefix']}block{details['postfix']}")
        count = cursor.fetchone()[0]

        self.rows = [None] * count  # Optimize memory allocation
        cursor.execute(f"SELECT time, user, wid, x, y, z, type, data, meta, blockdata, action, rolled_back "
                       f"FROM {details['prefix']}block{details['postfix']} LIMIT 1010")  ##FIXME:  REMOVE LIMIT
        i = 0
        missingData = False  # So we can get all missing data and then crash after wards when we know it all
        for values in cursor:
            try:
                self.rows[i] = SqlLiteRow(*values, self, details["mySqlInfo"])
            except MissingDataException:
                missingData = True
            if i % 500000 == 0:
                print(f"\t\t{i}/{count} - {i/count * 100}%")
            i += 1
        print(f"\tRows Raw: ...")
        if missingData:
            raise MissingDataException(details)
