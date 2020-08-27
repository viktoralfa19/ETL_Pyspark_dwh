## Configuracion de Contextos
from pyspark.sql import SparkSession
from ExceptionManager import ExceptionManager
import psycopg2

class DBContextDw: 
    """Clase que permite establecer la configuración con la bodega de datos o DataWarehouse."""  
    def __init__ (self,Database,
                  driverClass='spark.driver.extraClassPath',
                  urlDriver='/home/jovyan/postgresql-42.2.12.jar',
                  HostDb='10.30.80.3',
                  Port='5432'):
        self.spark = SparkSession.builder.config(driverClass, urlDriver).getOrCreate()
        self.HostDb = HostDb
        self.Port = Port
        self.UserName = "user_sirio"
        self.Password ="Cen.2020.sirio"
        self.DataBase = Database      
        self.conn = psycopg2.connect(host=self.HostDb, port = self.Port, database=self.DataBase, user=self.UserName, password=self.Password)
        
    def GetDataDW(self, query):
        """Método para retornar datos de una data warehouse en dataframes, de una consulta SQL especificada."""        
        url = 'jdbc:postgresql://{0}/{1}'.format(self.HostDb,self.DataBase)
        dataFrameDw = self.spark.read.format("jdbc")\
                               .option("url", url)\
                               .option("query", query) \
                               .option("user", self.UserName) \
                               .option("password", self.Password).load()
        return dataFrameDw
    
    def GetAllDataTableDW (self, table):
        """Método para retornar datos de una tabla del data warehouse en dataframes""" 
        url = 'jdbc:postgresql://{0}/{1}'.format(self.HostDb,self.DataBase)
        properties = {'user': self.UserName, 'password': self.Password}
        dataFrameDw = self.spark.read.jdbc(url=url, table=table, properties=properties)
        
        return dataFrameDw
    
    
    def InsertDataDW (self, dfTable, tableName, modo = 'append'):
        """Método para insertar datos de un dataframe a la bodega de datos DW.""" 
        
        result = False
        try: 
            if dfTable is not None:                
                url = 'jdbc:postgresql://{0}/{1}'.format(self.HostDb,self.DataBase)
                dfTable.write.format("jdbc")\
                             .options(url=url, dbtable=tableName, user=self.UserName,password=self.Password)\
                             .mode(modo).save()
                result = True
            return result 
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise 
            
    def UpsertDataDW (self, query):
        """Método para insertar o actualizar datos de una dimensión a la bodega de datos DW.""" 
        
        try: 
            cur = self.conn.cursor()
            cur.execute(query)
            Upsert_rows = cur.rowcount
            self.conn.commit()
            cur.close()
            self.conn.close()   
            return True 
        
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise 
 
        
    def DeletePathHdfs(self, path, HostHdfs='10.30.80.3', PortHdfs=9000):
        try:
            sc = self.spark.sparkContext
            URI = sc._gateway.jvm.java.net.URI
            Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
            FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
            fs = FileSystem.get(URI("hdfs://{0}:{1}".format(HostHdfs,str(PortHdfs))), sc._jsc.hadoopConfiguration())
            fs.delete(Path(path))
            #print(path)
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise 
        finally: 
            return result