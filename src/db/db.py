import jaydebeapi
import string
import os

class Connection:
    def __init__(self, dbpath):
        self.__dbpath = dbpath
        self.__libpath = os.path.join( os.path.dirname(os.path.realpath(__file__)), "ucanaccess/" )
        ucanaccess_jars = [
            os.path.join(self.__libpath,"ucanaccess-5.0.0.jar"),
            os.path.join(self.__libpath,"lib/commons-lang3-3.8.1.jar"),
            os.path.join(self.__libpath,"lib/commons-logging-1.2.jar"),
            os.path.join(self.__libpath,"lib/hsqldb-2.5.0.jar"),
            os.path.join(self.__libpath,"lib/jackcess-3.0.1.jar") ]

        classpath = ":".join(ucanaccess_jars)
        self.connection = jaydebeapi.connect(
            "net.ucanaccess.jdbc.UcanaccessDriver",
            f"jdbc:ucanaccess://{self.__dbpath};newDatabaseVersion=V2010",
            ["", ""], classpath)

class Datahandling(object):

    def __init__(self, connection):
        self.__connection = connection

    def __toObjList(self, cursor, className):
        """ Given a DB API 2.0 cursor object that has been executed, returns
        a list of objects that maps each field name to a class member. """
        lst = []
        for row in cursor.fetchall():
            # first convert row to a dictionary
            rowdict={}
            for idx, col in enumerate(cursor.description):
                rowdict[col[0]] = row[idx]        
            # create class of type and set properties to dictionary
            instance = type(className, (), rowdict)
            lst.append(instance)
        return lst

    def query(self, tableName, whereFilter):
        sql = f"SELECT * from {tableName} "
        if(whereFilter is not None):
            sql = sql + f" where {whereFilter}"
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        lstObj = self.__toObjList(cursor, tableName)
        cursor.close()
        return lstObj

if __name__ == '__main__':
    conn = Connection("/home/zombu/accessdir/pruefungsausschuss.accdb")
    dh = Datahandling(conn.connection)
    res=dh.query('Stammdaten', None)
    res=dh.query('Faelle', '[Fall abgeschlossen] is not null')
    res=dh.query('[Art des Falles]', None)
    print(len(res))
    for d in res:
        attrs = vars(d)
        print(', '.join("%s: %s" % item for item in attrs.items()))
        print('\n')
