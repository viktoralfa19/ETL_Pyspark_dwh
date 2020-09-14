from HDFSContext import HDFSContext
from ExceptionManager import ExceptionManager
from GenericDataFrame import GenericDataFrame

from pyspark.sql.functions import to_timestamp, col, regexp_replace

class GetPosiciones:
    """Clase para obtener las posiciones para realizar el proceso de limpieza de la potencia aparente."""
    def __init__ (self,TableName,
                  Path='PI_INTEGRATOR',
                  DataBase='TRANSMISION',
                  Schema='',
                  Header='true',
                  Delimiter='\t'):
        self.path = Path
        self.dataBase = DataBase
        self.schema = Schema
        self.header = Header
        self.delimiter = Delimiter
        self.tableName = TableName
        self.genericDataFrame = GenericDataFrame(HDFSContext(Path=self.path,
                                                             DataBase=self.dataBase,
                                                             Schema=self.schema))
        
       
       
    def GetData(self, fecha_inicio,fecha_fin,fileName,posicion=None):
        try:
            data = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                        fileName=fileName,
                                                        header=self.header,
                                                        delimiter=self.delimiter)
            if(posicion is None):
                posiciones = data\
                .select('Id',col('Codigo').alias('Posicion'),
                        regexp_replace(col('Potencia Aparente'),',','.').cast('double').alias('Potencia'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Potencia').alias('TagPotencia'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'),
                        regexp_replace(col('Proteccion'),',','.').cast('float').alias('Proteccion'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                posiciones = data\
                .select('Id',col('Codigo').alias('Posicion'),
                        regexp_replace(col('Potencia Aparente'),',','.').cast('double').alias('Potencia'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Potencia').alias('TagPotencia'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'),
                        regexp_replace(col('Proteccion'),',','.').cast('float').alias('Proteccion'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Posicion') == posicion))

            return posiciones
        except Exception as error:
            ExceptionManager.Treatment(error)
            

    def GetDataQuality(self, fecha_inicio,fecha_fin,fileName,posicion=None):
        try:
            data = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                        fileName=fileName,
                                                        header=self.header,
                                                        delimiter=self.delimiter)
            if(posicion is None):
                posiciones = data\
                .select('Id',col('Codigo').alias('Posicion'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Potencia').alias('TagPotencia'),
                        regexp_replace(col('Proteccion'),',','.').cast('float').alias('Proteccion'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                posiciones = data\
                .select('Id',col('Codigo').alias('Posicion'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Potencia').alias('TagPotencia'),
                        regexp_replace(col('Proteccion'),',','.').cast('float').alias('Proteccion'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Posicion') == posicion))

            return posiciones
        except Exception as error:
            ExceptionManager.Treatment(error)
            