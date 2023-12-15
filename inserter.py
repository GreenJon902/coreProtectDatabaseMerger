import mysql


def insert(details, sqlLiteInfo):
    cnx = mysql.connector.connect(user=details["user"], host=details["host"], password=details["password"],
                                  database=details["database"])
    cursor = cnx.cursor()
    print(sqlLiteInfo.rows[0])
    print(sqlLiteInfo.rows[1001])

    cursor.close()
    cnx.close()