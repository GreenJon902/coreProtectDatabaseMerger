import os

import mysql.connector

CACHE_MYSQL = os.getenv("CACHE_MYSQL", "0") == "1"


class MySqlInfo:
    users: dict[int, str]  # Maps from database uid to minecraft uuid

    def __init__(self, details):
        cnx = mysql.connector.connect(**details)
        print(cnx)
        cnx.close()
