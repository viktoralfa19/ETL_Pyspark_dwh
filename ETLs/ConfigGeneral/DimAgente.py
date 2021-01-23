from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries

from pyspark.sql import functions as Funct
from pyspark.sql.functions import regexp_replace,row_number, col,upper,udf
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime


class ETLAgenteBL():
    """Lógica de negocio para el ingreso de datos en la dimensión de AGENTES"""
    def __init__ (self,database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar'):
        dbContext = DBContextDw(Database=database,urlDriver=urlDriver)
        self._accesoDatos = EtlAL(dbContext)    
        self.catalogoDW = None
        
    def Elt_main(self):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Dimensiones de AGENTE ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: dim_agente \n')

            print('1. Extracción de datos')
            extract_data = self.Extract_data_agentes()
            
            print('2. Transformación de datos')
            transform_data = self.Transform_data(extract_data)
            
            print('3. Cargar  datos\n')
            self.Load_data(transform_data,'cen_dws.dim_agente')
        except Exception as error:
            ExceptionManager.Treatment(error)
    
    def Extract_data_agentes (self):
        """Método que realiza la extracción de datos de Agentes desde el HDFS"""
        self._genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        agentes = self._genericDataFrame.GetDataHdfs('AGENTES','file_AGENTE_*')
        agentes = agentes.select(col('EMPRESA_CODIGO').alias('agt_empresa_id_bk'),upper(col('EMPRESA_NOMBRE')).alias('agt_empresa'),
                                 col('REGION_CODIGO').alias('agt_region_id_bk'),upper(col('REGION_NOMBRE')).alias('agt_region'),
                                 col('UNEGOCIO_CODIGO').alias('agt_und_negocio_id_bk'),
                                 upper(col('UNEGOCIO_NOMBRE')).alias('agt_und_negocio'),
                                 col('CLASE_UNEGOCIO_CODIGO').alias('agt_clase_unegocio_id_bk'),
                                 upper(col('CLASE_UNEGOCIO_NOMBRE')).alias('agt_clase_unegocio'),
                                 col('ESTACION_CODIGO').alias('agt_estacion_id_bk'),
                                 upper(col('ESTACION_NOMBRE')).alias('agt_estacion'),
                                 col('TIPO_ESTACION_CODIGO').alias('agt_tipo_estacion_id_bk'),
                                 upper(col('TIPO_ESTACION_NOMBRE')).alias('agt_tipo_estacion'),
                                 col('GGENERACION_CODIGO').alias('agt_grupo_gen_id_bk'),
                                 upper(col('GGENERACION_NOMBRE')).alias('agt_grupo_gen'),
                                 col('VOLTAJE_CODIGO').alias('agt_voltaje_id_bk'),col('VOLTAJE_NOMBRE').alias('agt_voltaje'),
                                 col('TIPO_ID').alias('agt_tipo_elemento_id_bk'),upper(col('TIPO')).alias('agt_tipo_elemento'),
                                 col('ELEMENTO_CODIGO').alias('agt_elemento_id_bk'),upper(col('ELEMENTO_NOMBRE')).alias('agt_elemento'),
                                 col('OPERACION_COMERCIAL').alias('agt_operacion_comercial').cast('timestamp'))
        
        return agentes
    
    def Transform_data (self, extract_data):
        """Método que transforma los datos obtenidos desde el HDFS, identifica si es ingreso nuevo o actualización"""
        paramReplace = ['NO VIGENTE','NO USAR','NO _USAR','\)','\(','\.','CENTRAL','CELEC EP -','CENACE -','CNEL EP','EE ','XM SA ESP']        
        valueReplace=  ['','','','','','','','','','','EE. ','XM S.A. E.S.P.']

        maxPk = self._accesoDatos.GetMaxPkTable('SELECT COALESCE(MAX(agt_id_pk),0) pk FROM cen_dws.dim_agente')   
        self.catalogoDW = self._accesoDatos.GetAllData('cen_dws.dim_agente')
        fecha_carga = datetime.datetime.today()

        for toReplace, replacement in zip(paramReplace, valueReplace):
            extract_data = extract_data\
            .select(extract_data.agt_empresa_id_bk,
                    Funct.trim(regexp_replace(extract_data.agt_empresa, toReplace, replacement)).alias('agt_empresa'),
                    extract_data.agt_region_id_bk, 
                    extract_data.agt_region,
                    extract_data.agt_und_negocio_id_bk, 
                    Funct.trim(regexp_replace(extract_data.agt_und_negocio, toReplace, replacement)).alias('agt_und_negocio'),
                    extract_data.agt_clase_unegocio_id_bk, 
                    extract_data.agt_clase_unegocio,
                    extract_data.agt_estacion_id_bk, 
                    Funct.trim(regexp_replace(extract_data.agt_estacion, toReplace, replacement)).alias('agt_estacion'),
                    extract_data.agt_tipo_estacion_id_bk, 
                    extract_data.agt_tipo_estacion,
                    extract_data.agt_grupo_gen_id_bk, 
                    Funct.trim(regexp_replace(extract_data.agt_grupo_gen, toReplace, replacement)).alias('agt_grupo_gen'),
                    extract_data.agt_voltaje_id_bk, 
                    extract_data.agt_voltaje,
                    extract_data.agt_tipo_elemento_id_bk, 
                    extract_data.agt_tipo_elemento,
                    extract_data.agt_elemento_id_bk, 
                    Funct.trim(regexp_replace(extract_data.agt_elemento, toReplace, replacement)).alias('agt_elemento'),
                    extract_data.agt_operacion_comercial)

        extract_data = extract_data\
        .groupby('agt_empresa_id_bk','agt_empresa','agt_region_id_bk','agt_region','agt_und_negocio_id_bk','agt_und_negocio',
                 'agt_clase_unegocio_id_bk','agt_clase_unegocio','agt_estacion_id_bk','agt_estacion','agt_tipo_estacion_id_bk',
                 'agt_tipo_estacion','agt_grupo_gen_id_bk','agt_grupo_gen','agt_voltaje_id_bk','agt_voltaje','agt_tipo_elemento_id_bk',
                 'agt_tipo_elemento','agt_elemento_id_bk','agt_elemento')\
        .agg(Funct.min('agt_operacion_comercial').alias('agt_operacion_comercial')).distinct()
        
        
        schema = StructType([StructField('agt_empresa_id_bk', StringType(), False),
                             StructField('agt_empresa', StringType(), False),
                             StructField('agt_region_id_bk', StringType(), False),
                             StructField('agt_region', StringType(), False),
                             StructField('agt_und_negocio_id_bk', StringType(), False),
                             StructField('agt_und_negocio', StringType(), False),
                             StructField('agt_clase_unegocio_id_bk', StringType(), False),
                             StructField('agt_clase_unegocio', StringType(), False),
                             StructField('agt_estacion_id_bk', StringType(), False),
                             StructField('agt_estacion', StringType(), False),
                             StructField('agt_tipo_estacion_id_bk', ShortType(), False),
                             StructField('agt_tipo_estacion', StringType(), False),
                             StructField('agt_grupo_gen_id_bk', StringType(), False),
                             StructField('agt_grupo_gen', StringType(), False),
                             StructField('agt_voltaje_id_bk', StringType(), False),
                             StructField('agt_voltaje', StringType(), False),
                             StructField('agt_tipo_elemento_id_bk', ShortType(), False),
                             StructField('agt_tipo_elemento', StringType(), False),
                             StructField('agt_elemento_id_bk', StringType(), False),
                             StructField('agt_elemento', StringType(), False),
                             StructField('agt_operacion_comercial', TimestampType(), False)])
        
        agente_adicional = self._accesoDatos._dBContextDw.spark.createDataFrame([('NA','No Aplica','NA','No Aplica',
                                                                                  'NA','No Aplica','NA','No Aplica',
                                                                                  'NA','No Aplica',-1,'No Aplica',
                                                                                  'NA','No Aplica','NA','No Aplica',
                                                                                  -1,'No Aplica','NA','No Aplica',
                                                                                  datetime.datetime(1900,1,1,0,0))],schema)
        extract_data = extract_data.union(agente_adicional)
        
        
        if self.catalogoDW.count()==0: 
            catalogos = extract_data.select((maxPk+row_number().over(Window.partitionBy()\
                            .orderBy(extract_data.agt_empresa_id_bk,
                                     extract_data.agt_und_negocio_id_bk,
                                     extract_data.agt_clase_unegocio_id_bk,
                                     extract_data.agt_estacion_id_bk,
                                     extract_data.agt_elemento_id_bk))).alias('agt_id_pk'),'*', 
                            Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))
        else:
            # Con el query se pretende verificar que registros son nuevos y que registros son los que han sufrido modificaciones                
            data_to_compare = extract_data.join(self.catalogoDW,
                                                (extract_data.agt_empresa_id_bk==self.catalogoDW.agt_empresa_id_bk) &\
                                                (extract_data.agt_und_negocio_id_bk==self.catalogoDW.agt_und_negocio_id_bk) &\
                                                (extract_data.agt_clase_unegocio_id_bk==self.catalogoDW.agt_clase_unegocio_id_bk) &\
                                                (extract_data.agt_estacion_id_bk==self.catalogoDW.agt_estacion_id_bk) &\
                                                (extract_data.agt_elemento_id_bk==self.catalogoDW.agt_elemento_id_bk), how='left')\
                                        .select(Funct.when(self.catalogoDW.agt_id_pk.isNull(),0)\
                                                .otherwise(self.catalogoDW.agt_id_pk).alias("agt_id_pk"), 
                                                extract_data.agt_empresa_id_bk, extract_data.agt_empresa, 
                                                extract_data.agt_region_id_bk, extract_data.agt_region, 
                                                extract_data.agt_und_negocio_id_bk, extract_data.agt_und_negocio,
                                                extract_data.agt_clase_unegocio_id_bk, extract_data.agt_clase_unegocio, 
                                                extract_data.agt_estacion_id_bk, extract_data.agt_estacion, 
                                                extract_data.agt_tipo_estacion_id_bk, extract_data.agt_tipo_estacion,
                                                extract_data.agt_grupo_gen_id_bk, extract_data.agt_grupo_gen,
                                                extract_data.agt_voltaje_id_bk, extract_data.agt_voltaje,
                                                extract_data.agt_tipo_elemento_id_bk, extract_data.agt_tipo_elemento,
                                                extract_data.agt_elemento_id_bk, extract_data.agt_elemento,
                                                extract_data.agt_operacion_comercial)                

            # Identificación de registros nuevos que deben ser insertados en el DWH
            dataNw = data_to_compare.filter(data_to_compare.agt_id_pk==0).drop('agt_id_pk')

            catalogos = dataNw.select((maxPk+row_number().over(Window.partitionBy()\
                                                               .orderBy(dataNw.agt_empresa_id_bk,
                                                                        dataNw.agt_und_negocio_id_bk,
                                                                        dataNw.agt_clase_unegocio_id_bk,
                                                                        dataNw.agt_estacion_id_bk,
                                                                        dataNw.agt_elemento_id_bk))).alias('agt_id_pk'),
                                      '*',Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))

            # Identificación de registros que han sufrido cambios en la base transaccional y deben ser modificados en el DW
            dataMdf = data_to_compare.filter(data_to_compare.agt_id_pk!=0)

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
        transform_data_map = transform_data.rdd.map(lambda x: (x.agt_id_pk, [x.agt_empresa_id_bk, x.agt_empresa, 
                                                                             x.agt_region_id_bk, x.agt_region,
                                                                             x.agt_und_negocio_id_bk, x.agt_und_negocio,
                                                                             x.agt_clase_unegocio_id_bk, x.agt_clase_unegocio,
                                                                             x.agt_estacion_id_bk, x.agt_estacion,
                                                                             x.agt_tipo_estacion_id_bk, x.agt_tipo_estacion,
                                                                             x.agt_grupo_gen_id_bk, x.agt_grupo_gen,
                                                                             x.agt_voltaje_id_bk, x.agt_voltaje,
                                                                             x.agt_tipo_elemento_id_bk, x.agt_tipo_elemento,
                                                                             x.agt_elemento_id_bk, x.agt_elemento,
                                                                             x.agt_operacion_comercial,x.fecha_carga]))
        
        querys_data_insert = transform_data_map.map(lambda x: Queries.Upsert_Query_Dim_Agente(x))

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
        
        result_transact =  self._genericDataFrame.spark.createDataFrame(result_transact,schema=schema)
        
        return result_transact