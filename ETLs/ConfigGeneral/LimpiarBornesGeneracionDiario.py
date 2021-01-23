from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarBornesGeneracionDiario():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Bornes de Generacion Diario de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Bornes de Generacion Diario ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_bgen_diario \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarBornesGeneracionDiario.Extract_Transform_data(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarBornesGeneracionDiario.Load_data(transform_data,'cen_dws.fact_bgen_diario',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data (fecha_inicio,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        fecha = fecha_inicio.strftime('%Y_%m_%d')
        demanda = genericDataFrame.GetDataHdfs('TMP_System_Dem','file_TMP_System_Dem_'+fecha)
        gen_tipos = genericDataFrame.GetDataHdfs('TMP_System_Gen_Tipos','file_TMP_System_Gen_Tipos_'+fecha)
        
        if(demanda is None):
            return None
        
        if(gen_tipos is None):
            return None
        
        bornes = LimpiarBornesGeneracionDiario.Procesamiento(demanda,gen_tipos)
                
        fecha_actual = datetime.datetime.now()
        
        bornes = bornes\
        .select(regexp_replace(substring(bornes.Fecha.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                bornes.Demanda.alias('energ_demanda'),
                bornes.Produccion.alias('energ_produccion'),
                bornes.Exportacion.alias('energ_consumo'))\
        .withColumn('fecha_carga',lit(fecha_actual))

        return bornes        
    
    def Procesamiento(dfhd_demanda,dfhd_generacion):
        """Método que realiza el procesamiento de los DataFrames necesarios"""    
        #Obtenemos datos DataFrame a Procesar
        #print('Producción: ' + str(datetime.datetime.now()))
        df_datos_produccion = dfhd_generacion.groupby('Fecha').agg(func.sum('Energia').alias('Energia'))
        df_datos_demanda = dfhd_demanda.filter(col('Tipo')=='DEMANDAS').groupby('Fecha').agg(func.sum('Energia')\
                                                                                             .alias('Energia'))
        #print('Exportacion: ' + str(datetime.datetime.now()))
        df_datos_exportacion = dfhd_demanda.filter(col('Tipo')=='EXPORTACIÓN').groupby('Fecha')\
        .agg(func.sum('Energia').alias('Energia'))

        #print('Datos Totales: ' + str(datetime.datetime.now()))
        df_datos = df_datos_produccion\
        .join(df_datos_exportacion, df_datos_produccion.Fecha == df_datos_exportacion.Fecha)\
        .select(df_datos_produccion.Fecha,
                df_datos_produccion.Energia.alias('Produccion'),
                (df_datos_produccion.Energia-df_datos_exportacion.Energia).alias('Demanda'),
                df_datos_exportacion.Energia.alias('Exportacion'),
                regexp_replace(df_datos_produccion.Fecha, '-', '').alias('tmpprod_id_pk'))
        return df_datos        
    
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
    