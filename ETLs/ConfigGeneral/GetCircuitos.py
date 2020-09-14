from HDFSContext import HDFSContext
from ExceptionManager import ExceptionManager
from GenericDataFrame import GenericDataFrame

from pyspark.sql.functions import to_timestamp, col, regexp_replace

class GetCircuitos:
    """Clase para obtener los circuitos para realizar el proceso de limpieza de la potencia aparente."""
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
        
       
       
    def GetDataOrigen(self, fecha_inicio,fecha_fin,fileName,circuito=None):
        try:
            dataOrigen = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                        fileName=fileName,
                                                        header=self.header,
                                                        delimiter=self.delimiter)
            if(circuito is None):
                circuitos = dataOrigen\
                .select('Id',col('Codigo').alias('Circuito'),
                        regexp_replace(col('Potencia Aparente'),',','.').cast('double').alias('Potencia'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidadOrigen'),
                        col('Tag Potencia').alias('TagPotenciaOrigen'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        col('NumCircuitos').cast('int').alias('NumCircuitos'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                circuitos = dataOrigen\
                .select('Id',col('Codigo').alias('circuito'),
                        regexp_replace(col('Potencia Aparente'),',','.').cast('double').alias('Potencia'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidadOrigen'),
                        col('Tag Potencia').alias('TagPotenciaOrigen'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        col('NumCircuitos').cast('int').alias('NumCircuitos'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Circuito') == circuito))

            return circuitos
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def GetDataDestino(self, fecha_inicio,fecha_fin,fileName,circuito=None):
        try:
            dataDestino = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                               fileName=fileName,
                                                               header=self.header,
                                                               delimiter=self.delimiter)
            if(circuito is None):
                circuitos = dataDestino\
                .select('Id',col('Codigo').alias('Circuito'),
                        regexp_replace(col('Potencia Aparente Destino'),',','.').cast('double').alias('Potencia'),
                        col('Calidad Potencia Destino').alias('Calidad'),
                        col('Tag Calidad Destino').alias('TagCalidadDestino'),
                        col('Tag Potencia Destino').alias('TagPotenciaDestino'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        col('NumCircuitos').cast('int').alias('NumCircuitos'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                circuitos = dataDestino\
                .select('Id',col('Codigo').alias('circuito'),
                        regexp_replace(col('Potencia Aparente Destino'),',','.').cast('double').alias('Potencia'),
                        col('Calidad Potencia Destino').alias('Calidad'),
                        col('Tag Calidad Destino').alias('TagCalidadDestino'),
                        col('Tag Potencia Destino').alias('TagPotenciaDestino'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        col('NumCircuitos').cast('int').alias('NumCircuitos'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Circuito') == circuito))

            return circuitos
        except Exception as error:
            ExceptionManager.Treatment(error)
            

    def GetDataQualityOrigen(self, fecha_inicio,fecha_fin,fileName,circuito=None):
        try:
            dataOrigen = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                        fileName=fileName,
                                                        header=self.header,
                                                        delimiter=self.delimiter)
            if(circuito is None):
                circuitos = dataOrigen\
                .select('Id',col('Codigo').alias('Circuito'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Potencia').alias('TagPotencia'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                circuitos = dataOrigen\
                .select('Id',col('Codigo').alias('Circuito'),
                        col('Calidad Potencia').alias('Calidad'),
                        col('Tag Calidad').alias('TagCalidad'),
                        col('Tag Potencia').alias('TagPotencia'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Circuito') == circuito))

            return circuitos
        except Exception as error:
            ExceptionManager.Treatment(error)
            
            
            
    def GetDataQualityDestino(self, fecha_inicio,fecha_fin,fileName,circuito=None):
        try:
            dataDestino = self.genericDataFrame.GetDataCsvHdfs(tableName=self.tableName,
                                                               fileName=fileName,
                                                               header=self.header,
                                                               delimiter=self.delimiter)
            if(circuito is None):
                circuitos = dataDestino\
                .select('Id',col('Codigo').alias('Circuito'),
                        col('Calidad Potencia Destino').alias('Calidad'),
                        col('Tag Calidad Destino').alias('TagCalidad'),
                        col('Tag Potencia Destino').alias('TagPotencia'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin))
            else:
                circuitos = dataDestino\
                .select('Id',col('Codigo').alias('Circuito'),
                        col('Calidad Potencia Destino').alias('Calidad'),
                        col('Tag Calidad Destino').alias('TagCalidad'),
                        col('Tag Potencia Destino').alias('TagPotencia'),
                        regexp_replace(col('Lim_MaxOperacion'),',','.').cast('float').alias('LimMaxOperacion'),
                        regexp_replace(col('Lim_OperacionContinuo'),',','.').cast('float').alias('LimOperacionContinuo'),
                        regexp_replace(col('Lim_Termico'),',','.').cast('float').alias('LimTermico'),
                        to_timestamp(col('Estampa de Tiempo'),'dd/MM/yy HH:mm:ss').alias('Fecha'))\
                .filter((col('Fecha')>=fecha_inicio) & (col('Fecha')<=fecha_fin) &\
                        (col('Circuito') == circuito))

            return circuitos
        except Exception as error:
            ExceptionManager.Treatment(error)