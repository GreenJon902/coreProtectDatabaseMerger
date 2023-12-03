import os.path
import sys
import mySqlInfo

if __name__ == '__main__':
    if len(sys.argv) != 5 or "-h" in sys.argv or "--help" in sys.argv:
        print("Usage:")
        print(f"{sys.argv[0]} path_of_sql_lite host database user")
        print("Password is to be entered in password.txt")
        sys.exit(0)

    if not os.path.exists("password.txt"):
        open("password.txt", "w").close()

    sqlLitePath = sys.argv[1]
    mySqlDetails = {"host": sys.argv[2], "database": sys.argv[3], "user": sys.argv[4], "password": open("password.txt", "r").read()}

    mySqlInfo = mySqlInfo.MySqlInfo(mySqlDetails)
