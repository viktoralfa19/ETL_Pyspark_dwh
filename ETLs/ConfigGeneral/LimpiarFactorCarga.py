from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, split
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarFactorCarga():
    """Clase que permite realizar la extracción, limpieza y validación de datos del factor de carga de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Factor de Carga ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_factor_carga \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarFactorCarga.Extract_Transform_data_agentes(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarFactorCarga.Load_data(transform_data,'cen_dws.fact_factor_carga',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data_agentes (fecha_procesar,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""  
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        
        potencia = genericDataFrame.GetDataHdfs('TMP_System_Perdidas_Potencia',
                                                'file_TMP_System_Perdidas_Potencia_'+str(fecha_procesar.year)+'*')
        if(potencia is None):
            return None
        energia = genericDataFrame.GetDataHdfs('TMP_System_Perdidas_Energia',
                                               'file_TMP_System_Perdidas_Energia_'+str(fecha_procesar.year)+'*')
        if(energia is None):
            return None
        fecha_fin = potencia.groupby().agg(func.max('Fecha').cast('timestamp').alias('Fecha')).first()[0]
        print(fecha_fin)
        dias_anio = int(fecha_fin.strftime("%j"))
        print(dias_anio)
        return LimpiarFactorCarga.CargarDatosFactorCarga(potencia,energia,fecha_procesar.year,dias_anio,fecha_procesar)
      
    
    def CargarDatosFactorCarga(dfhd_perdidas_potencia,dfhd_perdidas_energia, anio, dias_anio,fecha_inicio):
        """Método que limpiar los datos de factor de carga y las fechas de los mismos."""

        try:  
            fecha_actual = datetime.datetime.now()
            df_potencia = dfhd_perdidas_potencia\
            .select('Fecha',split(col('Fecha'),'-').getItem(0).alias('Anio'),'DeePerdidas')\
            .groupby('Anio').agg(func.max('DeePerdidas').alias('Potencia_Maxima'))

            df_energia = dfhd_perdidas_energia\
            .select('Fecha',split(col('Fecha'),'-').getItem(0).alias('Anio'),'MWH_Generacion','MWH_Exportacion')\
            .withColumn('Demanda_Acumulada',col('MWH_Generacion')-col('MWH_Exportacion'))\
            .groupby('Anio').agg(func.sum('Demanda_Acumulada').alias('Demanda_Acumulada'))

            df_factor_carga = df_potencia.join(df_energia, df_potencia.Anio == df_energia.Anio)\
            .select(regexp_replace(regexp_replace(lit(fecha_inicio).cast('date').cast('string')
                                                  ,'-',''),':','').cast('integer').alias('tmpo_id_fk'),
                    ((col('Demanda_Acumulada')/(col('Potencia_Maxima')*24*dias_anio))*100.0).alias('factcrg_fact_carga'))\
            .withColumn('fecha_carga',lit(fecha_actual))

            return df_factor_carga
        except Exception as error:
            ExceptionManager.Treatment(error)
            
            
    
    def Load_data(transform_data, table, accesoDatos):
        if transform_data is False:
            mensaje = " **** ERROR: Las dimensiones tienen ausencias de datos. {0}.****".format(table)
        elif transform_data is not None: 
            result = accesoDatos.Insert(transform_data, table)
            if result == True: 
                mensaje = " **** EXITOSO: Datos insertados correctamente en la dimensión de {0}.**** ".format(table)
            else: 
                mensaje = " **** ERROR: Error al insertar datos en la dimensión de {0}.****".format(table)
        else :
            mensaje = " **** WARNING: No existen datos para insertar en la dimensión {0}.****".format(table)
    
        print(mensaje) 