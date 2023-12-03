import os.path
import sys
import sqlInfo
import sqlLiteRow

if __name__ == '__main__':
    if len(sys.argv) != 8 or "-h" in sys.argv or "--help" in sys.argv:
        print("Usage:")
        print(f"{sys.argv[0]} path_of_sql_lite host database user prefix mySqlPostfix sqlLitePostfix")
        print("Password is to be entered in password.txt")
        sys.exit(0)

    if not os.path.exists("password.txt"):
        open("password.txt", "w").close()

    mySqlDetails = {"host": sys.argv[2], "database": sys.argv[3], "user": sys.argv[4],
                    "password": open("password.txt", "r").read(), "prefix": sys.argv[5], "postfix": sys.argv[6]}
    mySqlInfo = sqlInfo.MySqlInfo(mySqlDetails)

    sqlLiteDetails = {"path": sys.argv[1], "prefix": sys.argv[5], "postfix": sys.argv[7], "mySqlInfo": mySqlInfo,
                      "mySqlPostfix": sys.argv[6]}
    sqlLiteInfo = sqlInfo.SqlLiteInfo(sqlLiteDetails)
