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


class ETLAgenteOrigenBL():
    """Lógica de negocio para el ingreso de datos en la dimensión de AGENTES de ORIGEN"""
    def __init__ (self,database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar'):
        dbContext = DBContextDw(Database=database,urlDriver=urlDriver)
        self._accesoDatos = EtlAL(dbContext)    
        self.catalogoDW = None
        
    def Elt_main(self):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Dimensiones de AGENTEs de ORIGEN ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: dim_agt_origen \n')

            print('1. Extracción de datos')
            extract_data = self.Extract_data_agentes()
            
            print('2. Transformación de datos')
            transform_data = self.Transform_data(extract_data)
            
            print('3. Cargar  datos\n')
            self.Load_data(transform_data,'cen_dws.dim_agt_origen')
        except Exception as error:
            ExceptionManager.Treatment(error)
    
    def Extract_data_agentes (self):
        """Método que realiza la extracción de datos de Agentes desde el HDFS"""
        self._genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        agentes = self._genericDataFrame.GetDataHdfs('AGENTES','file_AGENTE_*')
        agentes = agentes.select(col('EMPRESA_CODIGO').alias('agtorg_empresa_id_bk'),upper(col('EMPRESA_NOMBRE')).alias('agtorg_empresa'),
                                 col('REGION_CODIGO').alias('agtorg_region_id_bk'),upper(col('REGION_NOMBRE')).alias('agtorg_region'),
                                 col('UNEGOCIO_CODIGO').alias('agtorg_und_negocio_id_bk'),
                                 upper(col('UNEGOCIO_NOMBRE')).alias('agtorg_und_negocio'),
                                 col('CLASE_UNEGOCIO_CODIGO').alias('agtorg_clase_unegocio_id_bk'),
                                 upper(col('CLASE_UNEGOCIO_NOMBRE')).alias('agtorg_clase_unegocio'))
        
        return agentes
    
    def Transform_data (self, extract_data):
        """Método que transforma los datos obtenidos desde el HDFS, identifica si es ingreso nuevo o actualización"""
        paramReplace = ['NO VIGENTE','NO USAR','NO _USAR','\)','\(','\.','CENTRAL','CELEC EP -','CENACE -','CNEL EP','EE ','XM SA ESP']        
        valueReplace=  ['','','','','','','','','','','EE. ','XM S.A. E.S.P.']

        maxPk = self._accesoDatos.GetMaxPkTable('SELECT COALESCE(MAX(agtorg_id_pk),0) pk FROM cen_dws.dim_agt_origen')   
        self.catalogoDW = self._accesoDatos.GetAllData('cen_dws.dim_agt_origen')
        fecha_carga = datetime.datetime.today()

        for toReplace, replacement in zip(paramReplace, valueReplace):
            extract_data = extract_data\
            .select(extract_data.agtorg_empresa_id_bk,
                    Funct.trim(regexp_replace(extract_data.agtorg_empresa, toReplace, replacement)).alias('agtorg_empresa'),
                    extract_data.agtorg_region_id_bk, 
                    extract_data.agtorg_region,
                    extract_data.agtorg_und_negocio_id_bk, 
                    Funct.trim(regexp_replace(extract_data.agtorg_und_negocio, toReplace, replacement)).alias('agtorg_und_negocio'),
                    extract_data.agtorg_clase_unegocio_id_bk, 
                    extract_data.agtorg_clase_unegocio)

        extract_data = extract_data.distinct()
        
        if self.catalogoDW.count()==0: 
            catalogos = extract_data.select((maxPk+row_number().over(Window.partitionBy()\
                            .orderBy(extract_data.agtorg_empresa_id_bk,
                                     extract_data.agtorg_und_negocio_id_bk,
                                     extract_data.agtorg_clase_unegocio_id_bk))).alias('agtorg_id_pk'),'*', 
                            Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))
        else:
            # Con el query se pretende verificar que registros son nuevos y que registros son los que han sufrido modificaciones                
            data_to_compare = extract_data.join(self.catalogoDW,
                                                (extract_data.agtorg_empresa_id_bk==self.catalogoDW.agtorg_empresa_id_bk) &\
                                                (extract_data.agtorg_und_negocio_id_bk==self.catalogoDW.agtorg_und_negocio_id_bk) &\
                                                (extract_data.agtorg_clase_unegocio_id_bk==self.catalogoDW.agtorg_clase_unegocio_id_bk),
                                                how='left')\
                                        .select(Funct.when(self.catalogoDW.agtorg_id_pk.isNull(),0)\
                                                .otherwise(self.catalogoDW.agtorg_id_pk).alias("agtorg_id_pk"), 
                                                extract_data.agtorg_empresa_id_bk, extract_data.agtorg_empresa, 
                                                extract_data.agtorg_region_id_bk, extract_data.agtorg_region, 
                                                extract_data.agtorg_und_negocio_id_bk, extract_data.agtorg_und_negocio,
                                                extract_data.agtorg_clase_unegocio_id_bk, extract_data.agtorg_clase_unegocio)                

            # Identificación de registros nuevos que deben ser insertados en el DWH
            dataNw = data_to_compare.filter(data_to_compare.agtorg_id_pk==0).drop('agtorg_id_pk')

            catalogos = dataNw.select((maxPk+row_number().over(Window.partitionBy()\
                                                               .orderBy(dataNw.agtorg_empresa_id_bk,
                                                                        dataNw.agtorg_und_negocio_id_bk,
                                                                        dataNw.agtorg_clase_unegocio_id_bk))).alias('agtorg_id_pk'),
                                      '*',Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))

            # Identificación de registros que han sufrido cambios en la base transaccional y deben ser modificados en el DW
            dataMdf = data_to_compare.filter(data_to_compare.agtorg_id_pk!=0)

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
        transform_data_map = transform_data.rdd.map(lambda x: (x.agtorg_id_pk, [x.agtorg_empresa_id_bk, x.agtorg_empresa, 
                                                                                x.agtorg_region_id_bk, x.agtorg_region,
                                                                                x.agtorg_und_negocio_id_bk, x.agtorg_und_negocio,
                                                                                x.agtorg_clase_unegocio_id_bk, x.agtorg_clase_unegocio,
                                                                                x.fecha_carga]))
        
        querys_data_insert = transform_data_map.map(lambda x: Queries.Upsert_Query_Dim_Origen(x))

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