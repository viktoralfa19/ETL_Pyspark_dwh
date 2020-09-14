from HDFSContext import HDFSContext
from ExceptionManager import ExceptionManager
from GenericDataFrame import GenericDataFrame

from pyspark.sql.functions import to_timestamp, col, regexp_replace

class GetBarras:
    """Clase para obtener las barras para realizar el proceso de limpieza del voltaje."""
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
        
       
       
    def GetData(self, fecha_inicio,fecha_fin,fileName,barra=None):
        try:
            data = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                        fileName=fileName,
                                                        header=self.header,
                                                        delimiter=self.delimiter)
            if(barra is None):
                barras = data\
                .select('Id',col('Codigo').alias('Barra'),
                        regexp_replace(col('Voltaje'),',','.').cast('double').alias('Voltaje'),
                        col('Calidad Voltaje').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Voltaje').alias('TagVoltaje'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                barras = data\
                .select('Id',col('Codigo').alias('Barra'),
                        regexp_replace(col('Voltaje'),',','.').cast('double').alias('Voltaje'),
                        col('Calidad Voltaje').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Voltaje').alias('TagVoltaje'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Barra') == barra))

            return barras
        except Exception as error:
            ExceptionManager.Treatment(error)
            

    def GetDataQuality(self, fecha_inicio,fecha_fin,fileName,barra=None):
        try:
            data = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                        fileName=fileName,
                                                        header=self.header,
                                                        delimiter=self.delimiter)
            if(barra is None):
                barras = data\
                .select('Id',col('Codigo').alias('Barra'),
                        col('Calidad Voltaje').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Voltaje').alias('TagVoltaje'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                barras = data\
                .select('Id',col('Codigo').alias('Barra'),
                        col('Calidad Voltaje').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Voltaje').alias('TagVoltaje'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Barra') == barra))

            return barras
        except Exception as error:
            ExceptionManager.Treatment(error)
            