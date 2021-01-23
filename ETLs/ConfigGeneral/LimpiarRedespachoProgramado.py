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

class LimpiarRedespachoProgramado():
    """Clase que permite realizar la extracción, limpieza y validación de despacho y redespacho programado de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Despacho y Redespacho ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_desp_redesp_horario \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarRedespachoProgramado.Extract_Transform_data(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarRedespachoProgramado.Load_data(transform_data,'cen_dws.fact_desp_redesp_horario',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data(fecha_inicio,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        fecha = fecha_inicio.strftime('%Y_%m_%d')
        fecha_anterior = (fecha_inicio-timedelta(days=1)).strftime('%Y_%m_%d')
        despacho = genericDataFrame.GetDataHdfs('DPL_Despacho_Programado',
                                                'file_DPL_Despacho_Programado_'+fecha).fillna({'MV':0})
        despacho_anterior = genericDataFrame.GetDataHdfs('DPL_Despacho_Programado',
                                                         'file_DPL_Despacho_Programado_'+fecha_anterior).fillna({'MV':0})
        if(despacho is None):
            return None
        if(despacho_anterior is None):
            return None
        unidad = genericDataFrame.GetDataHdfs('CFG_Unidad','file_CFG_Unidad')
        central = genericDataFrame.GetDataHdfs('CFG_Central','file_CFG_Central')
        tipo_tec = genericDataFrame.GetDataHdfs('CFG_TipoTecnologia','file_CFG_TipoTecnologia')
        tipo_comb = genericDataFrame.GetDataHdfs('CFG_TipoCombustible','file_CFG_TipoCombustible')
        tipo_gen = genericDataFrame.GetDataHdfs('CFG_TipoGeneracion','file_CFG_TipoGeneracion')
        
        combustible = accesoDatos.GetAllData('cen_dws.dim_tipo_combustible')
        generacion = accesoDatos.GetAllData('cen_dws.dim_tipo_generacion')
        tecnologia = accesoDatos.GetAllData('cen_dws.dim_tipo_tecnologia')
        numero = accesoDatos.GetAllData('cen_dws.dim_nro_redespacho')
        
        agente = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'GEN') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 1))
                
        
        despacho_anterior = despacho_anterior\
        .withColumn('FechaHora',(func.concat(col('Fecha'),lit(' '),col('Hora'))).cast('timestamp')+func.expr('INTERVAL 1 HOURS'))
        
        despacho = despacho\
        .withColumn('FechaHora',(func.concat(col('Fecha'),lit(' '),col('Hora'))).cast('timestamp')+func.expr('INTERVAL 1 HOURS'))
        
        despacho = despacho.union(despacho_anterior).filter(to_date(col('FechaHora'),'%Y-%m-%d %H:%M')==fecha_inicio.date())\
        .orderBy('Unidad','FechaHora')
        
        despacho = despacho\
        .join(unidad, despacho.Unidad == unidad.Codigo)\
        .join(central, despacho.Central == central.Codigo)\
        .join(tipo_gen, central.IdTipoGeneracion == tipo_gen.IdTipoGeneracion)\
        .join(tipo_comb, unidad.IdTipoCombustible == tipo_comb.IdTipoCombustible)\
        .join(tipo_tec, unidad.IdTipoTecnologia == tipo_tec.IdTipoTecnologia)\
        .select(despacho.Empresa,
                despacho.UNegocio,
                despacho.Central,
                despacho.Unidad,
                despacho.NumRedespacho,
                despacho.FechaHora,
                despacho.MV,
                despacho.Precio,
                tipo_gen.Codigo.alias('TipoGeneracion'),
                tipo_comb.Codigo.alias('TipoCombustible'),
                tipo_tec.Codigo.alias('TipoTecnologia'))
        
        fecha_actual = datetime.datetime.now()
        
        despacho = despacho\
        .join(agente, 
              (despacho.Empresa==agente.agt_empresa_id_bk) &\
              (despacho.UNegocio==agente.agt_und_negocio_id_bk) &\
              (despacho.Central==agente.agt_estacion_id_bk) &\
              (despacho.Unidad==agente.agt_elemento_id_bk), how='left')\
        .join(combustible, despacho.TipoCombustible==combustible.tipcomb_gop_id_bk, how='left')\
        .join(generacion, despacho.TipoGeneracion==generacion.tipgen_id_bk, how='left')\
        .join(tecnologia, despacho.TipoTecnologia==tecnologia.tiptec_id_bk, how='left')\
        .join(numero, despacho.NumRedespacho==numero.nro_valor, how='left')\
        .select(agente.agt_id_pk.alias('agt_id_fk'),
                regexp_replace(substring(despacho.FechaHora.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                trim(regexp_replace(substring(despacho.FechaHora.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_id_fk'),
                combustible.tipcomb_id_pk.alias('tipcomb_id_fk'),
                generacion.tipgen_id_pk.alias('tipgen_id_fk'),
                tecnologia.tiptec_id_pk.alias('tiptec_id_fk'),
                numero.nro_id_pk.alias('nro_id_fk'),
                despacho.MV.cast('float').alias('redesp_mv'),
                despacho.Precio.cast('float').alias('redesp_precio')).withColumn('fecha_carga',lit(fecha_actual))
        
        faltantes = despacho.filter((despacho.agt_id_fk.isNull()) | (despacho.tmpo_id_fk.isNull()) |\
                                    (despacho.hora_id_fk.isNull()) | (despacho.tipcomb_id_fk.isNull()) |\
                                    (despacho.tipgen_id_fk.isNull()) | (despacho.tiptec_id_fk.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False

        return despacho
    
    
    
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
    