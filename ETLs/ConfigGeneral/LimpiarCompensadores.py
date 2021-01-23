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

class LimpiarCompensadores():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Compensadores de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Compensadores ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_compensadores \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarCompensadores.Extract_Transform_data(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarCompensadores.Load_data(transform_data,'cen_dws.fact_compensadores',accesoDatos)
            
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
        dvcompensador = genericDataFrame.GetDataHdfs('DV_Compensador','file_DV_Compensador_'+fecha)
        
        if(dvcompensador is None):
            return None
        
        agentes = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'TRA') &\
                                                                      (col('agt_tipo_elemento_id_bk') == 5))
        
        dvcompensador = dvcompensador.withColumn('FechaHora',(func.concat(col('Fecha'),lit(' '),col('Hora'))).cast('timestamp'))
        
        fecha_actual = datetime.datetime.now()
        
        dvcompensador = dvcompensador\
        .join(agentes, 
              (dvcompensador.Empresa==agentes.agt_empresa_id_bk) &\
              (dvcompensador.UNegocio==agentes.agt_und_negocio_id_bk) &\
              (dvcompensador.Subestacion==agentes.agt_estacion_id_bk) &\
              (dvcompensador.Compensador==agentes.agt_elemento_id_bk), how='left')\
        .select(agentes.agt_id_pk.alias('agt_id_fk'),
                trim(regexp_replace(substring(dvcompensador.FechaHora.cast('string'),11,6),':',''))\
                .cast(IntegerType()).alias('hora_id_fk'),
                regexp_replace(substring(dvcompensador.FechaHora.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                dvcompensador.ValorValidado.cast('float').alias('comp_potencia')).withColumn('fecha_carga',lit(fecha_actual))
        
        faltantes = dvcompensador.filter((dvcompensador.agt_id_fk.isNull()) | (dvcompensador.tmpo_id_fk.isNull()) | \
                                         (dvcompensador.hora_id_fk.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False        
        
        return dvcompensador
    
    
    
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
    