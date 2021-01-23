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

class ETLModoAgcBL():
    """Lógica de negocio para el ingreso de datos en la dimensión de Modo AGC"""
    def __init__ (self,database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar'):
        dbContext = DBContextDw(Database=database,urlDriver=urlDriver)
        self._accesoDatos = EtlAL(dbContext)    
        self.catalogoDW = None
        
    def Elt_main(self):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Dimensiones del Modo AGC ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: dim_modo_agc \n')

            print('1. Extracción de datos')
            extract_data = self.Extract_data_agentes()
            
            print('2. Transformación de datos')
            transform_data = self.Transform_data(extract_data)
            
            print('3. Cargar  datos\n')
            self.Load_data(transform_data,'cen_dws.dim_modo_agc')
        except Exception as error:
            ExceptionManager.Treatment(error)
    
    def Extract_data_agentes (self):
        """Método que realiza la extracción de datos del modo AGC desde el HDFS"""
        
        datos = [(1,'TLB'),(2,'FF'),(3,'TL')]
        componentes = self._accesoDatos._dBContextDw.spark.createDataFrame(datos,['id','agcmodo_nombre'])\
        .select('agcmodo_nombre')
        
        return componentes
    
    def Transform_data (self, extract_data):
        """Método que transforma los datos obtenidos desde el HDFS, identifica si es ingreso nuevo o actualización"""

        maxPk = self._accesoDatos.GetMaxPkTable('SELECT COALESCE(MAX(agcmodo_id_pk),0) pk FROM cen_dws.dim_modo_agc')   
        self.catalogoDW = self._accesoDatos.GetAllData('cen_dws.dim_modo_agc')
        fecha_carga = datetime.datetime.today()

        extract_data = extract_data.distinct()
        
        if self.catalogoDW.count()==0: 
            catalogos = extract_data.select((maxPk+row_number().over(Window.partitionBy()\
                            .orderBy(extract_data.agcmodo_nombre))).alias('agcmodo_id_pk'),'*', 
                            Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))
        else:
            # Con el query se pretende verificar que registros son nuevos y que registros son los que han sufrido modificaciones 
            data_to_compare = extract_data.join(self.catalogoDW,
                                                (extract_data.agcmodo_nombre==self.catalogoDW.agcmodo_nombre), how='left')\
                                        .select(Funct.when(self.catalogoDW.agcmodo_id_pk.isNull(),0)\
                                                .otherwise(self.catalogoDW.agcmodo_id_pk).alias("agcmodo_id_pk"), 
                                                extract_data.agcmodo_nombre)                

            # Identificación de registros nuevos que deben ser insertados en el DWH
            dataNw = data_to_compare.filter(data_to_compare.agcmodo_id_pk==0).drop('agcmodo_id_pk')

            catalogos = dataNw.select((maxPk+row_number().over(Window.partitionBy()\
                                                               .orderBy(dataNw.agcmodo_nombre))).alias('agcmodo_id_pk'),
                                      '*',Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))

            # Identificación de registros que han sufrido cambios en la base transaccional y deben ser modificados en el DW
            dataMdf = data_to_compare.filter(data_to_compare.agcmodo_id_pk!=0)

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
        transform_data_map = transform_data.rdd.map(lambda x: (x.agcmodo_id_pk, [x.agcmodo_nombre,x.fecha_carga]))
        
        querys_data_insert = transform_data_map.map(lambda x: Queries.Upsert_Query_Dim_Modo_Agc(x))

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