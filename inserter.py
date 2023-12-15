import mysql.connector

from sqlInfo import SqlLiteInfo


def insert(details, sqlLiteInfo: SqlLiteInfo):
    cnx = mysql.connector.connect(user=details["user"], host=details["host"], password=details["password"],
                                  database=details["database"])
    cursor = cnx.cursor()

    query = (f"INSERT INTO {details['prefix']}block{details['postfix']} (time, user, wid, x, y, z, type, data, "
             f"meta, blockdata, action, rolled_back, imported) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
             f"1)")
    print(query)
    data = [
        (row.time, row.mySqlUuid, row.mySqlWid, row.x, row.y, row.z, row.mySqlType_, row.data, row.meta,
         row.mySqlBlockdata.encode() if row.mySqlBlockdata is not None else row.mySqlBlockdata,
         row.action, row.rolled_back) for row in sqlLiteInfo.rows
    ]

    print("Rows to be inserted: " + str(len(data)) + ". Examples:")
    print(sqlLiteInfo.rows[0:10])
    print(data[0:10])
    if input("All data is prepared, type 'y' to continue") == 'y':
        print("Starting...")
        cursor.executemany(query, data)
        print("Done")

    cursor.close()
    cnx.commit()
    cnx.close()

