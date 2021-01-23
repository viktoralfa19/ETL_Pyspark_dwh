from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from RefactorizarEnsEs import Refactorizar

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarEnsEs():
    """Clase que permite realizar la extracción, limpieza y validación de datos de ENS y ES de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(deteleFactQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de ENS y ES ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_ens_es \n')
            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
            
            if(deteleFactQuery is not None):
                query = deteleFactQuery
                accesoDatos.Delete(query)
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarEnsEs.Extract_Transform_data(accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarEnsEs.Load_data(transform_data,'cen_dws.fact_ens_es',accesoDatos)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data (accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""                
        genericDataFrame = GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        empresa = genericDataFrame.GetDataHdfs('CFG_Empresa','file_CFG_Empresa')
        unegocio = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio','file_CFG_UnidadNegocio')
        unegocio_tipo = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio_TipoUnidad','file_CFG_UnidadNegocio_TipoUnidad')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='BDTREV2')) 
        bempresa = genericDataFrame.GetDataHdfs('EMPRESA','file_EMPRESA')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SAMWEB')) 
        carga = genericDataFrame.GetDataHdfs('EJE_CARGA','file_EJE_CARGA_*')

        if(len(carga.head(1))==0):
            return None
        
        ################################### CATÁLOGO DE UNIDADES DE NEGOCIO
        unidad_negocio = Refactorizar.DafaFrameUnidadNegocio(bempresa,empresa,unegocio)
        
        ################################### OBTENER ENERGÍA SUMINISTRADA DIARIA
        df_fechas = carga\
        .select(col('EJECAR_FECHA_INICIAL').cast(TimestampType()).alias('FechaInicio'),
                col('EJECAR_FECHA_FINAL').cast(TimestampType()).alias('FechaFin')).distinct()\
        .groupby().agg(func.min('FechaInicio').alias('FechaInicio'),func.max('FechaFin').alias('FechaFin'))\
        .select(func.year('FechaInicio').alias('AnioInicio'),func.year('FechaFin').alias('AnioFin'))
        
        inicio = int(str(df_fechas.first()[0])+'0101')
        fin = int(str(df_fechas.first()[1])+'1231')
        
        df_es = accesoDatos\
        .GetData('select d.tmpo_id_fk "FechaId", a.agt_und_negocio_id_bk "UNegocioCodigo",\
        a.agt_und_negocio "UNegocio", sum(d.demdr_potencia) "Energia"\
        from cen_dws.fact_demanda_diaria d \
        inner join cen_dws.dim_agente a on d.agt_id_fk=a.agt_id_pk\
        where d.tmpo_id_fk between {0} and {1} group by d.tmpo_id_fk, a.agt_und_negocio_id_bk, a.agt_und_negocio'.format(inicio,fin))
        
        ################################### ASIGANAR EMPRESA A ENERGÍA SUMINISTRADA DIARIA
        df_es = Refactorizar.AsignarEmpresa(df_es,unidad_negocio)

        ################################### OBTENER ENERGÍA NO SUMINISTRADA MANTENIMIENTOS
        df_ens_mantenimiento = Refactorizar.ObtenerEnsMantenimiento(carga,unidad_negocio,unegocio)

        ################################### OBTENER DATOS TOTALES
        df_datos_totales = Refactorizar.ObtenerTotales(df_es,df_ens_mantenimiento)
        
        
        agente = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'DIS') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 13) | \
                                                                     ((col('agt_empresa_id_bk')=='NA') &\
                                                                      (col('agt_und_negocio_id_bk')=='NA') &\
                                                                      (col('agt_estacion_id_bk')=='NA') &\
                                                                      (col('agt_elemento_id_bk')=='NA')))
        
        agente = accesoDatos.GetAllData('cen_dws.dim_agente')
        agt_origen = accesoDatos.GetAllData('cen_dws.dim_agt_origen')
        origen = accesoDatos.GetAllData('cen_dws.dim_asignacion_origen')
        
        fecha_actual = datetime.datetime.now()
        
        ################################### FALLAS
        ens_es = Refactorizar.LimpiarEnsEs(df_datos_totales,agente,agt_origen,origen)\
        .withColumn('fecha_carga',lit(fecha_actual))

                
        return ens_es

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
    