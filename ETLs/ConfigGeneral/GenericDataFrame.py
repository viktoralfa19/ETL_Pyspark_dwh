## Acceso a datos desde HDFS
from pyspark.sql import SparkSession
class GenericDataFrame:
    """Clase para generar DataFrames Spark y trabajar en la lógica del ETL"""
    def __init__ (self, hdfsContext, driverClass='spark.driver.extraClassPath', urlDriver='/home/jovyan/postgresql-42.2.12.jar'):
        self.hdfsContext = hdfsContext
        self.spark = SparkSession.builder.config(driverClass, urlDriver).appName("Sirio").getOrCreate()
    
    def GetDataHdfs(self, tableName,fileName,multiLine=True):
        """Método para retornar el DataFrame desde hadoop"""
        path = self.hdfsContext.HdfsPath(tableName,fileName)
        dataFrame = self.spark.read.json(path,multiLine=multiLine)            
        return dataFrame
    
    def GetDataCsvHdfs(self, tableName,fileName,header='true',delimiter='\t'):
        """Método para retornar el DataFrame desde hadoop"""
        path = self.hdfsContext.HdfsPath(tableName,fileName)
        dataFrame = self.spark.read.option('header',header).option('delimiter',delimiter).csv(path)            
        return dataFrame