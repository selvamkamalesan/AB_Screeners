from modules.libraries import *

class AB_Screener_DB:
    def __init__(self):
        self.__host = Props.DB_HOST
        self.__port = Props.DB_PORT
        self.__user = Props.DB_USER
        self.__pass = Props.DB_PASS
        self.__schema = Props.DB_SCHEMA

        self.__mydb = None
        self.__mycursor = None

    def connection_int(self):
        self.__mydb = mysql.connector.connect(
            host=self.__host,
            user=self.__user,
            password=self.__pass,
            database=self.__schema
        )
        self.__mycursor = self.__mydb.cursor()

    def connection_close(self):
        self.__mycursor.close()
        self.__mydb.close()

        self.__mycursor = None
        self.__mydb = None

    def insert_query(self,df,table_name):

        created_by = "python"
        self.connection_int()

        df = df.reset_index(drop=True)

        df["created_by"] = created_by
        df["active_status"] = '1'
        df = df[["prevltp","ltp","faodiff","faochange","instname","symbol","expdate","strikeprice","opttype","updtime","qty","openinterest","chgopenint","script_identifier","created_on","created_by","active_status"]]
        df["expdate"] = df["expdate"].dt.strftime('%Y-%m-%d %H:%M:%S')
        df["updtime"] = df["updtime"].dt.strftime('%Y-%m-%d %H:%M:%S')
        df["created_on"] = df["created_on"].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        value = list(df.itertuples(index=False, name=None))

        query = "INSERT INTO " + table_name + "(PrevLtp,LTP,FaOdiff,FaOchange,InstName,Symbol,ExpDate,StrikePrice,OptType,UpdTime,Qty,OpenInterest,chgOpenInt,scrip_identifier,created_on,created_by,active_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        self.__mycursor.executemany(query, value)

        self.__mydb.commit()
        self.connection_close()

    def select_query(self,table_name):
        self.connection_int()
        query = "select * from %s where created_on = (SELECT Max(created_on) FROM %s);"%(table_name,table_name)
        print(query)
        self.__mycursor.execute(query)
        column_name = [i[0] for i in self.__mycursor.description]
        data = self.__mycursor.fetchall()
        self.connection_close()
        return {
            "stat":"ok",
            "data":data,
            "column":column_name
        }

class fii_dii_DB:
    def __init__(self):
        self.__host = Props.DB_HOST_FII_DII
        self.__port = Props.DB_PORT_FII_DII
        self.__user = Props.DB_USER_FII_DII
        self.__pass = Props.DB_PASS_FII_DII
        self.__schema = Props.DB_SCHEMA_FII_DII

        self.__mydb = None
        self.__mycursor = None

    def connection_int(self):
        self.__mydb = mysql.connector.connect(
            host=self.__host,
            user=self.__user,
            password=self.__pass,
            database=self.__schema
        )
        self.__mycursor = self.__mydb.cursor()

    def connection_close(self):
        self.__mycursor.close()
        self.__mydb.close()

        self.__mycursor = None
        self.__mydb = None

    def select_query(self,from_Date):
        self.connection_int()
        if from_Date == 0:
            query = "select * from future_month_data.tbl_fii_dii_data where created_on =(SELECT created_on FROM future_month_data.tbl_fii_dii_data order by created_on desc limit 1);"
        else:
            query = "select * from future_month_data.tbl_fii_dii_data where created_on = '%s';"%(from_Date.strftime("%Y-%m-%d"))
        self.__mycursor.execute(query)
        column_name = [i[0] for i in self.__mycursor.description]
        data = self.__mycursor.fetchall()
        self.connection_close()
        return {
            "stat":"ok",
            "data":data,
            "column":column_name
        }