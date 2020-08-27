## Acceso a datos desde HDFS
from pyspark.sql import SparkSession
class GenericDataFrame():
    """Clase para generar DataFrames Spark y trabajar en la lógica del ETL"""
    def __init__ (self, hdfsContext):
        self.hdfsContext = hdfsContext
        self.spark = SparkSession.builder.appName("Sirio").getOrCreate()
    
    def GetDataHdfs(self, tableName,fileName):
        """Método para retornar el DataFrame desde hadoop"""
        path = self.hdfsContext.HdfsPath(tableName,fileName)
        dataFrame = self.spark.read.json(path,multiLine=True)            
        return dataFrame