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

class LimpiarProduccionMediaHoraria():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Producción Media Horaria de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Producción Media Horaria ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_produccion_mediohoraria \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarProduccionMediaHoraria.Extract_Transform_data(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarProduccionMediaHoraria.Load_data(transform_data,'cen_dws.fact_produccion_mediohoraria',accesoDatos)
            
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
        dvgeneracion = genericDataFrame.GetDataHdfs('DV_Generacion','file_DV_Generacion_'+fecha)
        
        if(dvgeneracion is None):
            return None
        
        agente_unidad = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'GEN') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 1))
        
        unidad = genericDataFrame.GetDataHdfs('CFG_Unidad','file_CFG_Unidad')
        central = genericDataFrame.GetDataHdfs('CFG_Central','file_CFG_Central')
        tipo_tec = genericDataFrame.GetDataHdfs('CFG_TipoTecnologia','file_CFG_TipoTecnologia')
        tipo_comb = genericDataFrame.GetDataHdfs('CFG_TipoCombustible','file_CFG_TipoCombustible')
        tipo_gen = genericDataFrame.GetDataHdfs('CFG_TipoGeneracion','file_CFG_TipoGeneracion')
        
        combustible = accesoDatos.GetAllData('cen_dws.dim_tipo_combustible')
        generacion = accesoDatos.GetAllData('cen_dws.dim_tipo_generacion')
        tecnologia = accesoDatos.GetAllData('cen_dws.dim_tipo_tecnologia')
        
        
        dvgeneracion = dvgeneracion\
        .join(unidad, dvgeneracion.Unidad == unidad.Codigo)\
        .join(central, dvgeneracion.Central == central.Codigo)\
        .join(tipo_gen, central.IdTipoGeneracion == tipo_gen.IdTipoGeneracion)\
        .join(tipo_comb, unidad.IdTipoCombustible == tipo_comb.IdTipoCombustible)\
        .join(tipo_tec, unidad.IdTipoTecnologia == tipo_tec.IdTipoTecnologia)\
        .select(dvgeneracion.Empresa,
                dvgeneracion.UNegocio,
                dvgeneracion.Central,
                dvgeneracion.Unidad,
                dvgeneracion.Fecha,
                dvgeneracion.Hora,
                dvgeneracion.MV_Validado,
                dvgeneracion.MVAR_Validado,
                tipo_gen.Codigo.alias('TipoGeneracion'),
                tipo_comb.Codigo.alias('TipoCombustible'),
                tipo_tec.Codigo.alias('TipoTecnologia'))\
        .withColumn('FechaHora',(func.concat(col('Fecha'),lit(' '),col('Hora'))).cast('timestamp'))
        
        fecha_actual = datetime.datetime.now()
        
        dvgeneracion = dvgeneracion\
        .join(agente_unidad, 
              (dvgeneracion.Empresa==agente_unidad.agt_empresa_id_bk) &\
              (dvgeneracion.UNegocio==agente_unidad.agt_und_negocio_id_bk) &\
              (dvgeneracion.Central==agente_unidad.agt_estacion_id_bk) &\
              (dvgeneracion.Unidad==agente_unidad.agt_elemento_id_bk), how='left')\
        .join(combustible, dvgeneracion.TipoCombustible==combustible.tipcomb_gop_id_bk, how='left')\
        .join(generacion, dvgeneracion.TipoGeneracion==generacion.tipgen_id_bk, how='left')\
        .join(tecnologia, dvgeneracion.TipoTecnologia==tecnologia.tiptec_id_bk, how='left')\
        .select(regexp_replace(substring(dvgeneracion.FechaHora.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                agente_unidad.agt_id_pk.alias('agt_id_fk'),
                trim(regexp_replace(substring(dvgeneracion.FechaHora.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_id_fk'),
                tecnologia.tiptec_id_pk.alias('tiptec_id_fk'),
                generacion.tipgen_id_pk.alias('tipgen_id_fk'),
                combustible.tipcomb_id_pk.alias('tipcomb_id_fk'),
                dvgeneracion.MV_Validado.cast('float').alias('prodmh_potencia_activa'),
                dvgeneracion.MVAR_Validado.cast('float').alias('prodmh_potencia_reactiva')).withColumn('fecha_carga',lit(fecha_actual))
        
        faltantes = dvgeneracion.filter((dvgeneracion.agt_id_fk.isNull()) | (dvgeneracion.tmpo_id_fk.isNull()) |\
                                    (dvgeneracion.hora_id_fk.isNull()) | (dvgeneracion.tipcomb_id_fk.isNull()) |\
                                    (dvgeneracion.tipgen_id_fk.isNull()) | (dvgeneracion.tiptec_id_fk.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False        
        
        return dvgeneracion
    
    
    
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
    