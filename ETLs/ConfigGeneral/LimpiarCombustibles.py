from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarCombustibles():
    """Clase que permite realizar la extracción, limpieza y validación de datos de combustibles de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Combustibles ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_stock_combustible \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarCombustibles.Extract_Transform_data_agentes(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarCombustibles.Load_data(transform_data,'cen_dws.fact_stock_combustible',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data_agentes (fecha_inicio,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        fecha = fecha_inicio.strftime('%Y_%m_%d')
        combustibles = genericDataFrame.GetDataHdfs('AGT_Combustibles','file_AGT_Combustibles_'+fecha)
        
        if(combustibles is None):
            return None
        
        agente = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'GEN') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 10))
        
        agente_unidad = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'GEN') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 1))
        
        grupo_combustible = accesoDatos.GetAllData('cen_dws.dim_grupo_combustible')
        tipo_combustible = accesoDatos.GetAllData('cen_dws.dim_tipo_combustible')
        fecha = accesoDatos.GetAllData('cen_dws.dim_tiempo')
        
        central = genericDataFrame.GetDataHdfs('CFG_Central','file_CFG_Central')
        unidad = genericDataFrame.GetDataHdfs('CFG_Unidad','file_CFG_Unidad')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='CATALOGOS')) 
        central_combustible = genericDataFrame.GetDataHdfs('CAT_Centrales_Combustible','file_CAT_Centrales_Combustible')
        comb_homologado = genericDataFrame.GetDataHdfs('CAT_Combustibles_Homologados','file_CAT_Combustibles_Homologados')
        
        
        central = central\
        .join(unidad, unidad.IdCentral == central.IdCentral)\
        .select(central.IdCentral,central.Codigo,central.Nombre,
                unidad.IdUnidad,unidad.Nombre.alias('Unidad'),
                to_date(unidad.FechaAlta,'yyyy-MM-dd').alias('FechaAlta'),
                when(unidad.FechaBaja.isNull(),fecha_inicio)\
                .otherwise(to_date(unidad.FechaBaja,'yyyy-MM-dd')).alias('FechaBaja'))\
        .filter(~col('Unidad').like('%Central%') & \
                ((col('FechaAlta')<=fecha_inicio) & (fecha_inicio<=col('FechaBaja'))))\
        .groupby('IdCentral','Codigo','Nombre')\
        .agg(func.count(col('IdUnidad')).alias('NroUnidades'))
        
        unidad = unidad\
        .select('IdUnidad','Codigo','Nombre','IdCentral',
                to_date(col('FechaAlta'),'yyyy-MM-dd').alias('FechaAlta'),
                when(col('FechaBaja').isNull(),fecha_inicio).otherwise(to_date(col('FechaBaja'),'yyyy-MM-dd'))\
                .alias('FechaBaja'))\
        .filter((col('FechaAlta')<=fecha_inicio) & (fecha_inicio<=col('FechaBaja')))
        
        combustibles = combustibles\
        .join(agente,
              (combustibles.Empresa==agente.agt_empresa_id_bk)&\
              (combustibles.UNegocio==agente.agt_und_negocio_id_bk)&\
              (combustibles.Central==agente.agt_estacion_id_bk),how='left')\
        .join(grupo_combustible, combustibles.GrupoCombustible==grupo_combustible.grcomb_id_bk,how='left')\
        .join(tipo_combustible, combustibles.TipoCombustible==tipo_combustible.tipcomb_gpl_id_bk,how='left')\
        .join(fecha, combustibles.Fecha.cast('timestamp')==fecha.tmpo_fecha,how='left')\
        .select(combustibles.Empresa,
                combustibles.UNegocio,
                combustibles.Central,
                agente.agt_id_pk.alias('agente'),
                agente.agt_elemento_id_bk.alias('elemento'),
                fecha.tmpo_id_pk.alias('fecha'),
                tipo_combustible.tipcomb_id_pk.alias('tipo'),
                grupo_combustible.grcomb_id_pk.alias('grupo'),
                combustibles.StockCombArranque,
                combustibles.StockCombOperacion,
                (combustibles.HorasDispPotEfectiva/24.0).alias('HorasDispPotEfectiva'),
                lit('O').alias('ClaseCombustible'))
        
        
        faltantes = combustibles.filter((combustibles.agente.isNull()) | (combustibles.fecha.isNull()) |\
                                        (combustibles.tipo.isNull()) | (combustibles.grupo.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False
        
        combustibles_arranque = combustibles\
        .join(central, combustibles.elemento == central.Codigo)\
        .join(central_combustible, central.IdCentral == central_combustible.ID_CENTRAL)\
        .join(unidad, central.IdCentral == unidad.IdCentral)\
        .join(comb_homologado, central_combustible.ID_COMBUSTIBLE_ARRANQUE == comb_homologado.ID_COMBUSTIBLE)\
        .join(tipo_combustible, comb_homologado.CODIGO == tipo_combustible.tipcomb_gpl_id_bk)\
        .join(agente_unidad,
              (combustibles.Empresa==agente_unidad.agt_empresa_id_bk)&\
              (combustibles.UNegocio==agente_unidad.agt_und_negocio_id_bk)&\
              (combustibles.Central==agente_unidad.agt_estacion_id_bk) &\
              (unidad.Codigo==agente_unidad.agt_elemento_id_bk))\
        .filter(central_combustible.ID_COMBUSTIBLE_ARRANQUE.isNotNull() & \
                ~unidad.Nombre.like('%Central%'))\
        .select(agente_unidad.agt_id_pk,
                combustibles.elemento,
                combustibles.fecha,
                tipo_combustible.tipcomb_id_pk,
                combustibles.grupo,
                unidad.Codigo.alias('Unidad'),
                combustibles.StockCombArranque,
                combustibles.StockCombOperacion,
                combustibles.HorasDispPotEfectiva,
                (combustibles.StockCombArranque/central.NroUnidades).alias('StockCombustible'),
                lit('A').alias('ClaseCombustible')).distinct()
        
        fecha_actual = datetime.datetime.now()
        
        combustibles = combustibles\
        .select(col('grupo').alias('grcomb_id_fk'),
                col('agente').alias('agt_id_fk'),
                col('fecha').alias('tmpo_id_fk'),
                col('tipo').alias('tipcomb_id_fk'),
                col('ClaseCombustible').alias('comb_clase'),
                col('StockCombOperacion').cast('float').alias('comb_stock_comb'),
                col('HorasDispPotEfectiva').alias('comb_dias_operacion'))
        
        combustibles_arranque = combustibles_arranque\
        .select(col('grupo').alias('grcomb_id_fk'),
                col('agt_id_pk').alias('agt_id_fk'),
                col('fecha').alias('tmpo_id_fk'),
                col('tipcomb_id_pk').alias('tipcomb_id_fk'),
                col('ClaseCombustible').alias('comb_clase'),
                col('StockCombustible').cast('float').alias('comb_stock_comb'),
                col('HorasDispPotEfectiva').alias('comb_dias_operacion'))
        
        combustibles = combustibles.union(combustibles_arranque).withColumn('fecha_carga',lit(fecha_actual)).orderBy('grcomb_id_fk')
        
        return combustibles
    
    
    
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
    