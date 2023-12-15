import sqlite3
import typing

if typing.TYPE_CHECKING:
    from sqlInfo import SqlInfo


class MissingDataException(Exception):
    def __init__(self, details=None):

        if details is not None:
            cnx = sqlite3.connect(details["path"])
            cursor = cnx.cursor()


            usersString = (f"INSERT INTO {details['prefix']}user{details['mySqlPostfix']}"
                           f"(time, user, uuid) VALUES ")
            worldsString = (f"INSERT INTO {details['prefix']}world{details['mySqlPostfix']} "
                            f"(world) VALUES ")
            blockdataString = f"INSERT INTO {details['prefix']}blockdata_map{details['mySqlPostfix']} VALUES "
            materialString = f"INSERT INTO {details['prefix']}material_map{details['mySqlPostfix']} VALUES "


            if len(missingUsers) == 0:
                usersString = ""
            else:
                for missingUser in missingUsers:
                    uuid = missingUser[0]
                    if uuid is None:
                        uuid = "NULL"
                    usersString += f"({missingUser[2]}, \"{missingUser[1]}\", \"{uuid}\"),"
                usersString = usersString.rstrip(",")
                usersString += ";"


            if len(missingWorlds) == 0:
                worldsString = ""
            else:
                for missingWorld in missingWorlds:
                    worldsString += f"(\"{missingWorld}\"),"
                worldsString = worldsString.rstrip(",")
                worldsString += ";"

            if len(missingBlockdata) == 0:
                blockdataString = ""
            else:
                for item in missingBlockdata:
                    blockdataString += f"(\"{item}\"),"
                blockdataString = blockdataString.rstrip(",")
                blockdataString += ";"

            if len(missingMaterials) == 0:
                materialString = ""
            else:
                for item in missingMaterials:
                    materialString += f"(\"{item}\"),"
                materialString = materialString.rstrip(",")
                materialString += ";"

            self.string = ("There is missing data, please use the following sql statements and then re run the script:\n"
                           "\n") + usersString + "\n\nThe next three will need modification and then and the row id set\n\n" + worldsString + "\n\n" + blockdataString + "\n\n" + materialString + "\n\n"

            cursor.close()
            cnx.close()

        else:
            self.string = "There is missing data, see above"



    def __str__(self):
        return self.string


alreadyDeclaredMissing: set[str] = set()  # To ensure missing data is not repeated
missingUsers = set()
missingWorlds = set()
missingBlockdata = set()
missingMaterials = set()


class SqlLiteRow:
    def __init__(self, time, sqlLiteUser, sqlLiteWid, x, y, z, sqlLiteType_, data, meta, sqlLiteBlockdata, action, rolled_back,
                 sqlLiteInfo: "SqlInfo", mySqlInfo: "SqlInfo"):  # Hint as SqlInfo rows aren't loaded yet.
        missingData = False  # So we can get all missing data and then crash afterwards when we know it all

        self.time = time

        self.sqlLiteUser = sqlLiteUser
        uuid = sqlLiteInfo.users["uuid"][sqlLiteUser] if sqlLiteUser in sqlLiteInfo.users["uuid"] else None
        user = sqlLiteInfo.users["user"][sqlLiteUser]
        try:
            if uuid is not None:
                self.mySqlUuid = mySqlInfo.users["uuid"].inverse[uuid]
            else:
                self.mySqlUuid = mySqlInfo.users["user"].inverse[user]
        except KeyError as e:

            message = f"No user: {user}, {uuid}"
            if message not in alreadyDeclaredMissing:
                print(message)
                alreadyDeclaredMissing.add(message)
                missingUsers.add((sqlLiteInfo.users["uuid"][sqlLiteUser], sqlLiteInfo.users["user"][sqlLiteUser],
                                  sqlLiteInfo.users["time"][sqlLiteUser]))
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
                missingWorlds.add(name)
            missingData = True

        self.x = x
        self.y = y
        self.z = z

        self.sqlLiteType_ = sqlLiteType_
        material = sqlLiteInfo.material_map[self.sqlLiteType_]
        try:
            self.mySqlType_ = mySqlInfo.material_map.inverse[material]
        except KeyError as e:
            message = f"No material: {sqlLiteType_}, {material}"
            if message not in alreadyDeclaredMissing:
                print(message)
                alreadyDeclaredMissing.add(message)
                missingMaterials.add(material)
            missingData = True


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
                        missingBlockdata.add(name)
                    missingData = True
            self.mySqlBlockdata = ",".join(newIds)

        self.action = action
        self.rolled_back = rolled_back

        if missingData:
            raise MissingDataException

    def __str__(self):
        return (f"Row({self.time}, ({self.sqlLiteUser}, {self.mySqlUuid}), ({self.sqlLiteWid}, {self.mySqlWid}), "
                f"{self.x}, {self.y}, {self.z}, ({self.sqlLiteType_}, {self.mySqlType_}), {self.data}, {self.meta}, "
                f"({self.sqlLiteBlockdata}, {self.mySqlBlockdata}), {self.action}, {self.rolled_back})")



