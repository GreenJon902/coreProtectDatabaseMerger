import typing

if typing.TYPE_CHECKING:
    from sqlInfo import SqlInfo


class MissingDataException(Exception):
    def __str__(self):
        return "Missing data, see above"


alreadyDeclaredMissing = set()  # To ensure missing data is not repeated


class SqlLiteRow:
    def __init__(self, time, sqlLiteUser, sqlLiteWid, x, y, z, type_, data, meta, sqlLiteBlockdata, action, rolled_back,
                 sqlLiteInfo: "SqlInfo", mySqlInfo: "SqlInfo"):  # Hint as SqlInfo rows aren't loaded yet.
        missingData = False  # So we can get all missing data and then crash afterwards when we know it all

        self.time = time

        self.sqlLiteUser = sqlLiteUser
        name = sqlLiteInfo.users[sqlLiteUser]
        try:
            self.mySqlUser = mySqlInfo.users.inverse[name]
        except KeyError as e:
            message = "No user: " + str(e)
            if message not in alreadyDeclaredMissing:
                print(message)
                alreadyDeclaredMissing.add(message)
            missingData = True

        self.sqlLiteWid = sqlLiteWid
        name = sqlLiteInfo.worlds[sqlLiteWid]
        try:
            self.mySqlWid = mySqlInfo.worlds.inverse[name]
        except KeyError as e:
            message = "No world: " + str(e)
            if message not in alreadyDeclaredMissing:
                print(message)
                alreadyDeclaredMissing.add(message)
            missingData = True

        self.x = x
        self.y = y
        self.z = z
        self.type_ = type_
        self.data = data
        self.meta = meta

        if sqlLiteBlockdata is None:
            self.sqlLiteBlockdata = None
            self.mySqlBlockdata = None
        else:
            self.sqlLiteBlockdata = sqlLiteBlockdata.decode()
            names = [sqlLiteInfo.blockdata_map[int(n)] for n in sqlLiteBlockdata.decode().split(",")]
            newIds = ["None"] * len(names)
            for i, name in enumerate(names):
                try:
                    newIds[i] = str(mySqlInfo.blockdata_map.inverse[name])
                except KeyError as e:
                    message = "No blockdata: " + str(e)
                    if message not in alreadyDeclaredMissing:
                        print(message)
                        alreadyDeclaredMissing.add(message)
                    missingData = True
            self.mySqlBlockdata = ",".join(newIds)

        self.action = action
        self.rolled_back = rolled_back

        if missingData:
            raise MissingDataException
