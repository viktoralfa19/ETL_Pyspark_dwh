from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries

from pyspark.sql import functions as Funct
from pyspark.sql.functions import regexp_replace,row_number, col,upper,udf, when
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime


class ETLParametrosBL():
    """Lógica de negocio para el ingreso de datos en la dimensión de los parámetros hidrólógicos."""
    def __init__ (self,database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar'):
        dbContext = DBContextDw(Database=database,urlDriver=urlDriver)
        self._accesoDatos = EtlAL(dbContext)    
        self.catalogoDW = None
        
    def Elt_main(self):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Dimensiones de Parámetros Hidrológicos ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: dim_param_hidrologicos \n')

            print('1. Extracción de datos')
            extract_data = self.Extract_data_agentes()
            
            print('2. Transformación de datos')
            transform_data = self.Transform_data(extract_data)
            
            print('3. Cargar  datos\n')
            self.Load_data(transform_data,'cen_dws.dim_param_hidrologicos')
        except Exception as error:
            ExceptionManager.Treatment(error)
    
    def Extract_data_agentes (self):
        """Método que realiza la extracción de datos de Componente de la demanda desde el HDFS"""
        
        datos = [(1, 'Altura Neta', 'AlturaNeta'),
                (2, 'Caudal Desagüe Fondo', 'CaudalDesFondo'),
                (3, 'Caudal Entrada Hora', 'CaudalEntradaHora'),
                (4, 'Caudal Entrada Promedio', 'CaudalEntradaProm'),
                (5, 'Caudal Lateral Promedio', 'CaudalLateralProm'),
                (6, 'Caudal Turbinado', 'CaudalTurbinado'),
                (7, 'Caudal Vertido', 'CaudalVertido'),
                (8, 'Caudal Promedio', 'Caudal_Prom'),
                (9, 'Cota de Descarga', 'CotaDescarga'),
                (10, 'Embalse Destino', 'Embalse_Destino'),
                (11, 'Energía Emergencia Embalse Destino', 'Energia_Emergencia_Emb_Destino'),
                (12, 'Energía Remanente Almacenada Embalse Destino', 'Energia_Rem_Alm_Emb_Destino'),
                (13, 'Energía Remanente Almacenada', 'Energia_Rem_Almacenada'),
                (14, 'Energía Remanente Total Almacenada', 'Energia_Rem_Total_Almacenada'),
                (15, 'Horas Apertura Desfondo Diario', 'Horas_Aper_Desfondo_dia'),
                (16, 'Nivel', 'Nivel'),
                (17, 'Nivel Promedio', 'Nivel_Prom'),
                (18, 'Reserva Energética', 'Res_Energetica'),
                (19, 'Volumen Almacenado', 'Vol_Almacenado'),
                (20, 'Volumen Desagüe Fondo', 'Vol_Des_Fondo'),
                (21, 'Volumen Turbinado', 'Vol_Turbinado'),
                (22, 'Volumen Vertido', 'Vol_Vertido')]
        
        parametros = self._accesoDatos._dBContextDw.spark.createDataFrame(datos,['id','paramhid_nombre','paramhid_id_bk'])\
        .select('paramhid_id_bk','paramhid_nombre')
        
        return parametros
    
    def Transform_data (self, extract_data):
        """Método que transforma los datos obtenidos desde el HDFS, identifica si es ingreso nuevo o actualización"""

        maxPk = self._accesoDatos.GetMaxPkTable('SELECT COALESCE(MAX(paramhid_id_pk),0) pk FROM cen_dws.dim_param_hidrologicos')   
        self.catalogoDW = self._accesoDatos.GetAllData('cen_dws.dim_param_hidrologicos')
        fecha_carga = datetime.datetime.today()

        extract_data = extract_data.distinct()
        
        if self.catalogoDW.count()==0: 
            catalogos = extract_data.select((maxPk+row_number().over(Window.partitionBy()\
                            .orderBy(extract_data.paramhid_id_bk))).alias('paramhid_id_pk'),'*', 
                            Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))
        else:
            # Con el query se pretende verificar que registros son nuevos y que registros son los que han sufrido modificaciones 
            data_to_compare = extract_data.join(self.catalogoDW,
                                                (extract_data.paramhid_id_bk==self.catalogoDW.paramhid_id_bk), how='left')\
                                        .select(Funct.when(self.catalogoDW.paramhid_id_pk.isNull(),0)\
                                                .otherwise(self.catalogoDW.paramhid_id_pk).alias("paramhid_id_pk"), 
                                                extract_data.paramhid_id_bk,extract_data.paramhid_nombre)                

            # Identificación de registros nuevos que deben ser insertados en el DWH
            dataNw = data_to_compare.filter(data_to_compare.paramhid_id_pk==0).drop('paramhid_id_pk')

            catalogos = dataNw.select((maxPk+row_number().over(Window.partitionBy()\
                                                               .orderBy(dataNw.paramhid_id_bk))).alias('paramhid_id_pk'),
                                      '*',Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))

            # Identificación de registros que han sufrido cambios en la base transaccional y deben ser modificados en el DW
            dataMdf = data_to_compare.filter(data_to_compare.paramhid_id_pk!=0)

            data_modificada = dataMdf.exceptAll(self.catalogoDW.drop('fecha_carga'))\
            .select('*',Funct.concat(Funct.lit(fecha_carga)).cast("timestamp").alias("fecha_carga"))

            catalogos = catalogos.union(data_modificada)

        if catalogos.count()==0: 
            catalogos = None
        return catalogos

    def Load_data(self,transform_data, table):
        """Método que realiza la carga de datos en la bodega de datos DW."""
        if transform_data is not None: 
            if self.catalogoDW.count() == 0:
                result = self._accesoDatos.Insert(transform_data, table)
            else :
                result = self.Load_Upsert_data(transform_data)
                result.show()
                result = True
       
            if result == True: 
                mensaje = " **** EXITOSO: Datos insertados correctamente en la dimensión de {0}.**** ".format(table)
            else: 
                mensaje = " **** ERROR: Error al insertar datos en la dimensión de {0}.****".format(table)
        else :
            mensaje = " **** WARNING: No existen datos para insertar en la dimensión {0}.****".format(table)
    
        print(mensaje) 
        
    def Load_Upsert_data(self,transform_data):
        """Método que realiza la lógica de creación de queries para el método upsert. """
        result_transact = []
        transform_data_map = transform_data.rdd.map(lambda x: (x.paramhid_id_pk, [x.paramhid_id_bk,x.paramhid_nombre,x.fecha_carga]))
        
        querys_data_insert = transform_data_map.map(lambda x: Queries.Upsert_Query_Dim_Parametros_Hidro(x))

        for index in querys_data_insert.collect():
            res = []
            pk = index[0]
            query = index[1]

            result = self._accesoDatos.UpsertDimension(query)

            res.extend([pk,result])
            result_transact.append(res)            
            
        schema = StructType([
                StructField("pk", IntegerType(),False),
                StructField("Result", BooleanType(),False)
            ])
        
        result_transact =  self._accesoDatos._dBContextDw.spark.createDataFrame(result_transact,schema=schema)
        
        return result_transact