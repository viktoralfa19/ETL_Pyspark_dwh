from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from RefactorizarAgc import Refactorizar

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarVariablesOpAgc():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Variables OP AGC de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery,deteleFactQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Variables Op. AGC ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_agc \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
            
            if(deteleFactQuery is not None):
                query = deteleFactQuery.format(int(datetime.datetime.strftime(fecha_inicio,'%Y%m%d')),
                                               int(datetime.datetime.strftime(fecha_fin,'%Y%m%d')))
                accesoDatos.Delete(query)
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarVariablesOpAgc.Extract_Transform_data(fecha_inicio,accesoDatos)
            #transform_data.filter((col('tmpo_id_fk')==20070817) & (col('hora_id_fk')==1418) & (col('agt_cent_id_fk')==2008)).show()
            print('2. Cargar  datos\n')
            LimpiarVariablesOpAgc.Load_data(transform_data,'cen_dws.fact_agc',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data (fecha_inicio,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        anio = fecha_inicio.strftime('%Y')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        empresa = genericDataFrame.GetDataHdfs('CFG_Empresa','file_CFG_Empresa')
        unegocio = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio','file_CFG_UnidadNegocio')
        subestacion = genericDataFrame.GetDataHdfs('CFG_SubEstacion','file_CFG_SubEstacion')
        linea = genericDataFrame.GetDataHdfs('CFG_Linea','file_CFG_Linea')
        central = genericDataFrame.GetDataHdfs('CFG_Central','file_CFG_Central')
        unidad = genericDataFrame.GetDataHdfs('CFG_Unidad','file_CFG_Unidad')
        circuito = genericDataFrame.GetDataHdfs('CFG_Circuito','file_CFG_Circuito')
        posicion = genericDataFrame.GetDataHdfs('CFG_Posicion','file_CFG_Posicion')
        transformador = genericDataFrame.GetDataHdfs('CFG_Transformador','file_CFG_Transformador')
        barra = genericDataFrame.GetDataHdfs('CFG_Barra','file_CFG_Barra')
        compensador = genericDataFrame.GetDataHdfs('CFG_Compensador','file_CFG_Compensador')
        unegocio_tipo = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio_TipoUnidad','file_CFG_UnidadNegocio_TipoUnidad')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='BDTREV2')) 
        bempresa = genericDataFrame.GetDataHdfs('EMPRESA','file_EMPRESA')
        bcentral = genericDataFrame.GetDataHdfs('CENTRAL','file_CENTRAL')
        blinea = genericDataFrame.GetDataHdfs('LINEA','file_LINEA')
        bsubestacion = genericDataFrame.GetDataHdfs('SUBESTACION','file_SUBESTACION')
        bbarra = genericDataFrame.GetDataHdfs('BARRA','file_BARRA')
        bcircuito = genericDataFrame.GetDataHdfs('CIRCUITO','file_CIRCUITO')
        bcompensador = genericDataFrame.GetDataHdfs('COMPENSADOR','file_COMPENSADOR')
        bposicion = genericDataFrame.GetDataHdfs('POSICION','file_POSICION')
        btrafo = genericDataFrame.GetDataHdfs('TRAFO','file_TRAFO')
        bunidad = genericDataFrame.GetDataHdfs('UNIDAD','file_UNIDAD')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='BOSNI')) 
        origen = genericDataFrame.GetDataHdfs('EAC_PASODTL','file_EAC_PASODTL')
        
        falla = genericDataFrame.GetDataHdfs('FALLA','file_FALLA_*').filter(col('ANIO')==anio)
        falla_dtl = genericDataFrame.GetDataHdfs('FALLA_DTL','file_FALLA_DTL_*').filter(col('ANIO')==anio)
        agc = genericDataFrame.GetDataHdfs('TBLF_AGC','file_TBLF_AGC_*').filter(col('ANIO')==anio)
        rpf = genericDataFrame.GetDataHdfs('TBLF_RPF','file_TBLF_RPF_*').filter(col('ANIO')==anio)
        evento = genericDataFrame.GetDataHdfs('EVENTO','file_EVENTO_*').filter(col('ANIO')==anio)
        evento_dtl = genericDataFrame.GetDataHdfs('EVENTO_DTL','file_EVENTO_DTL_*').filter(col('ANIO')==anio)
        
        if(agc is None):
            return None
        
        ############################## PROCESAR AGENTES
        datos_totales = LimpiarVariablesOpAgc.AgregarAgentes(bempresa,bcentral,blinea,bsubestacion,
                                                                bbarra,bcircuito,bcompensador,bposicion,
                                                                btrafo,bunidad,empresa,unegocio,subestacion,linea,
                                                                central,unidad,circuito,posicion,transformador,
                                                                barra,compensador,falla,falla_dtl,agc,evento,evento_dtl,
                                                                genericDataFrame.spark)
        ############################## PROCESAR RPF
        datos_totales = Refactorizar.AgregarRpf(datos_totales,rpf)
        
        agentes = accesoDatos.GetAllData('cen_dws.dim_agente')
        agt_gen = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'GEN') &\
                                                                      (col('agt_tipo_elemento_id_bk') == 1))
        pasos = accesoDatos.GetAllData('cen_dws.dim_pasos')
        modos = accesoDatos.GetAllData('cen_dws.dim_modo_agc')
        
        datos_totales = Refactorizar.LimpiarAgc(datos_totales,agentes,agt_gen,pasos,modos)
        
        idNAagente = agentes.filter((col('agt_empresa_id_bk')=='NA') &\
                                    (col('agt_und_negocio_id_bk')=='NA') &\
                                    (col('agt_estacion_id_bk')=='NA') &\
                                    (col('agt_elemento_id_bk')=='NA')).select('agt_id_pk').first()[0]
        
        fecha_actual = datetime.datetime.now()
        
        datos_totales = datos_totales.withColumn('fecha_carga',lit(fecha_actual))\
        .fillna({'agt_cent_id_fk':idNAagente,'agt_id_fk':idNAagente})
        
        return datos_totales
    
    def AgregarAgentes(df_empresa,df_central,df_linea,df_subestacion,df_barra,df_circuito,df_compensador,df_posicion,
                       df_transformador,df_unidad,df_cat_empresa,df_cat_unidad_negocio,df_cat_subestacion,df_cat_linea,df_cat_central,
                       df_cat_unidad,df_cat_circuito,df_cat_posicion,df_cat_transformador,df_cat_barra,df_cat_compensador,
                       df_fallas,df_fallas_dtl,df_tblf_agc,df_evento,df_evento_dtl,spark):
        ##### EMPRESA            
        unidad_negocio = Refactorizar.DafaFrameUnidadNegocio(df_empresa,df_cat_empresa,df_cat_unidad_negocio)

        ##### ESTACION         
        estacion = Refactorizar.DafaFrameEstacion(df_central,df_cat_central,df_linea,df_cat_linea,df_subestacion,
                                                  df_cat_subestacion)

        ##### ELEMENTO
        elemento = Refactorizar.DafaFrameElemento(df_barra,df_circuito,df_compensador,df_posicion,
                                                  df_transformador,df_unidad,df_cat_barra,df_cat_circuito,
                                                  df_cat_compensador,df_cat_posicion,df_cat_transformador,
                                                  df_cat_unidad)

        ##### DETALLE AGC
        df_datos = Refactorizar.AgregarDetalles(df_fallas,df_fallas_dtl,df_tblf_agc,unidad_negocio,estacion,
                                   elemento,df_evento,df_evento_dtl,spark)

        ##### AGREGAMOS EMPRESAS GENERACION AGC
        df_datos = Refactorizar.AgregarAgtAgc(df_datos,unidad_negocio,estacion,elemento)

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
    