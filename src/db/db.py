import jaydebeapi
import string
import os
from datetime import datetime, date

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

class DBHandling(object):

    def __init__(self, connection):
        self.__connection = connection

    def executeQuery(self, sqlString):
        cursor = self.__connection.cursor()
        cursor.execute(sqlString)
        lstOfDict = self.__toListOfDict(cursor)
        cursor.close()
        return lstOfDict

    def getDateTimeString(self, dt=None):
        '''get DateTime-String formatted for Access-DB (#%Y-%m-%d %H:%M:%S#)
            If Date(Time) not proved use datetime.now() '''
        if dt is None:
            dt = datetime.now()
        return f'#{dt.strftime("%Y-%m-%d %H:%M:%S")}#'

    # update('Faelle', '[Fall abgeschlossen]=#2021-01-06 00:00:00#', 'ID=3321')
    def update(self, tableName, valueString, whereFilter):
        sql = f"UPDATE {tableName} SET {valueString} WHERE {whereFilter}"
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        cursor.close()

    def executeNoQuery(self, sqlString):
        cursor = self.__connection.cursor()
        cursor.execute(sqlString)
        cursor.close()


    def __toObjList(self, cursor, className):
        """ Given a DB API 2.0 cursor object that has been executed, returns
        a list of objects that maps each field name to a class member. """
        lst = []
        for row in cursor.fetchall():
            # first convert row to a dictionary
            rowdict={}
            for idx, col in enumerate(cursor.description):
                memberName = col[0].strip().replace(' ', '__')
                rowdict[memberName] = row[idx]
            # create class of type and set properties to dictionary
            instance = type(className, (), rowdict)
            lst.append(instance)
        return lst


    def __query2ObjList(self, tableName, whereFilter):
        ''' Query ausfuehren und Ergebnis als Klassen-Instanzen-Liste abliefern.
        Schwierigkeiten: Spaltennamen mit Leerzeichen und die Ergebniswerte sind Java-Datentype (bei JayDeBeApi),
        deshalb eher nicht verwenden (ist jetzt mal private) '''
        sql = f"SELECT * from {tableName} "
        if(whereFilter is not None):
            sql = sql + f" where {whereFilter}"
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        lstObj = self.__toObjList(cursor, tableName)
        cursor.close()
        return lstObj

    def __toListOfDict(self, cursor):
        """ Given a DB API 2.0 cursor object that has been executed, returns
        a list of dictionaries. """
        lst = []
        for row in cursor.fetchall():
            # first convert row to a dictionary
            rowdict={}
            for idx, col in enumerate(cursor.description):
                rowdict[col[0]] = row[idx]
            lst.append(rowdict)
        return lst

if __name__ == '__main__':
    conn = Connection("/home/zombu/accessdir/pruefungsausschuss.accdb")
    dh = DBHandling(conn.connection)
    lst = dh.executeQuery("select * from Faelle where [Fall abgeschlossen] is null")
    for dictObj in lst:
        print(', '.join("%s=%s" % item for item in dictObj.items()))
        print('\n')
    d=lst[0]
    #dh.update('Faelle', f'[Fall abgeschlossen]={dh.getDateTimeString(date.today())}', f'ID={d["ID"]}')
    # oder:
    dh.executeNoQuery(f"update Faelle SET [Fall abgeschlossen]={dh.getDateTimeString()} WHERE ID={d['ID']}")
