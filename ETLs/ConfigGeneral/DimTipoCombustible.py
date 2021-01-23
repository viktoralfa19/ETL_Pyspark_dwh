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


class ETLTipoCombustibleBL():
    """Lógica de negocio para el ingreso de datos en la dimensión de tipos de combustible"""
    def __init__ (self,database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar'):
        dbContext = DBContextDw(Database=database,urlDriver=urlDriver)
        self._accesoDatos = EtlAL(dbContext)    
        self.catalogoDW = None
        
    def Elt_main(self):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Dimensiones de Tipos de Combustible ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: dim_tipo_combustible \n')

            print('1. Extracción de datos')
            extract_data = self.Extract_data_agentes()
            
            print('2. Transformación de datos')
            transform_data = self.Transform_data(extract_data)
            
            print('3. Cargar  datos\n')
            self.Load_data(transform_data,'cen_dws.dim_tipo_combustible')
        except Exception as error:
            ExceptionManager.Treatment(error)
    
    def Extract_data_agentes (self):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""
        self._genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        gop = self._genericDataFrame.GetDataHdfs('CFG_TipoCombustible','file_CFG_TipoCombustible')
        
        self._genericDataFrame=GenericDataFrame(HDFSContext(DataBase='CATALOGOS'))
        gpl = self._genericDataFrame.GetDataHdfs('CAT_Combustibles_Homologados','file_CAT_Combustibles_Homologados')
        gtc = self._genericDataFrame.GetDataHdfs('CAT_Combustibles_Gtc','file_CAT_Combustibles_Gtc')
        gpl_gop = self._genericDataFrame.GetDataHdfs('CAT_Combustibles_Gpl_Gop','file_CAT_Combustibles_Gpl_Gop')
        gpl_gtc = self._genericDataFrame.GetDataHdfs('CAT_Combustibles_Gpl_Gtc','file_CAT_Combustibles_Gpl_Gtc')
        
        combustibles = gpl\
        .join(gpl_gop, gpl.ID_COMBUSTIBLE == gpl_gop.ID_COMBUSTIBLE_GPL)\
        .join(gop, gpl_gop.ID_COMBUSTIBLE_GOP == gop.IdTipoCombustible)\
        .join(gpl_gtc, gpl.ID_COMBUSTIBLE == gpl_gtc.ID_COMBUSTIBLE_GPL)\
        .join(gtc, gpl_gtc.ID_COMBUSTIBLE_GTC == gtc.ID_COMBUSTIBLE)\
        .select(gpl.CODIGO.alias('tipcomb_gpl_id_bk'),
                upper(gpl.NOMBRE).alias('tipcomb_gpl_combustible'),
                gop.Codigo.alias('tipcomb_gop_id_bk'),
                upper(gop.Nombre).alias('tipcomb_gop_combustible'),
                gtc.CODIGO.alias('tipcomb_gtc_id_bk'),
                upper(gtc.NOMBRE).alias('tipcomb_gtc_combustible'))
        
        return combustibles
    
    def Transform_data (self, extract_data):
        """Método que transforma los datos obtenidos desde el HDFS, identifica si es ingreso nuevo o actualización"""

        maxPk = self._accesoDatos.GetMaxPkTable('SELECT COALESCE(MAX(tipcomb_id_pk),0) pk FROM cen_dws.dim_tipo_combustible')   
        self.catalogoDW = self._accesoDatos.GetAllData('cen_dws.dim_tipo_combustible')
        fecha_carga = datetime.datetime.today()

        extract_data = extract_data.distinct()
        
        if self.catalogoDW.count()==0: 
            catalogos = extract_data.select((maxPk+row_number().over(Window.partitionBy()\
                            .orderBy(extract_data.tipcomb_gpl_id_bk,
                                     extract_data.tipcomb_gop_id_bk,
                                     extract_data.tipcomb_gtc_id_bk))).alias('tipcomb_id_pk'),'*', 
                            Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))
        else:
            # Con el query se pretende verificar que registros son nuevos y que registros son los que han sufrido modificaciones 
            data_to_compare = extract_data.join(self.catalogoDW,
                                                (extract_data.tipcomb_gpl_id_bk==self.catalogoDW.tipcomb_gpl_id_bk) &\
                                                (extract_data.tipcomb_gop_id_bk==self.catalogoDW.tipcomb_gop_id_bk) &\
                                                (extract_data.tipcomb_gtc_id_bk==self.catalogoDW.tipcomb_gtc_id_bk), how='left')\
                                        .select(Funct.when(self.catalogoDW.tipcomb_id_pk.isNull(),0)\
                                                .otherwise(self.catalogoDW.tipcomb_id_pk).alias("tipcomb_id_pk"), 
                                                extract_data.tipcomb_gpl_id_bk,extract_data.tipcomb_gpl_combustible,
                                                extract_data.tipcomb_gop_id_bk,extract_data.tipcomb_gop_combustible,
                                                extract_data.tipcomb_gtc_id_bk,extract_data.tipcomb_gtc_combustible)                

            # Identificación de registros nuevos que deben ser insertados en el DWH
            dataNw = data_to_compare.filter(data_to_compare.tipcomb_id_pk==0).drop('tipcomb_id_pk')

            catalogos = dataNw.select((maxPk+row_number().over(Window.partitionBy()\
                                                               .orderBy(dataNw.tipcomb_gpl_id_bk,
                                                                        dataNw.tipcomb_gop_id_bk,
                                                                        dataNw.tipcomb_gtc_id_bk,))).alias('tipcomb_id_pk'),
                                      '*',Funct.concat(Funct.lit(fecha_carga)).cast('timestamp').alias('fecha_carga'))

            # Identificación de registros que han sufrido cambios en la base transaccional y deben ser modificados en el DW
            dataMdf = data_to_compare.filter(data_to_compare.tipcomb_id_pk!=0)

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
        transform_data_map = transform_data.rdd.map(lambda x: (x.tipcomb_id_pk, [x.tipcomb_gpl_id_bk, x.tipcomb_gpl_combustible,
                                                                                 x.tipcomb_gop_id_bk,x.tipcomb_gop_combustible,
                                                                                 x.tipcomb_gtc_id_bk,x.tipcomb_gtc_combustible,
                                                                                 x.fecha_carga]))
        
        querys_data_insert = transform_data_map.map(lambda x: Queries.Upsert_Query_Dim_Tipo_Combustible(x))

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