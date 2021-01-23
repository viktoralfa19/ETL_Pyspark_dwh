from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from RefactorizarInterrupcionesSni import Refactorizar
from CreacionVistasInterrupciones import CreacionVistas

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarInterrupcionesSni():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Fallas del SNI de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery,deteleFactQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Fallas del SNI ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_fallas_sni \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
            
            if(deteleFactQuery is not None):
                query = deteleFactQuery.format(int(datetime.datetime.strftime(fecha_inicio,'%Y%m%d')),
                                               int(datetime.datetime.strftime(fecha_fin,'%Y%m%d')))
                accesoDatos.Delete(query)
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarInterrupcionesSni.Extract_Transform_data(fecha_inicio,accesoDatos)
            #transform_data.filter((col('agt_id_fk')==2938) &\
            #                           (col('tmpo_falla_id_fk')==20051024) &\
            #                           (col('hora_falla_id_fk')==1537)).show()
            print('2. Cargar  datos\n')
            LimpiarInterrupcionesSni.Load_data(transform_data,'cen_dws.fact_fallas_sni',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data (fecha_inicio,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""        
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
        origen = genericDataFrame.GetDataHdfs('TPF_ORIGEN_FALLA','file_TPF_ORIGEN_FALLA')
        primero = genericDataFrame.GetDataHdfs('TPF_PRIMER_NIVEL','file_TPF_PRIMER_NIVEL')
        segundo = genericDataFrame.GetDataHdfs('TPF_SEGUNDO_NIVEL','file_TPF_SEGUNDO_NIVEL')
        clasificacion = genericDataFrame.GetDataHdfs('TPF_CLASIF','file_TPF_CLASIF')
        grupo = genericDataFrame.GetDataHdfs('TPF_CLASIF_GRP','file_TPF_CLASIF_GRP')
        
        falla = genericDataFrame.GetDataHdfs('FALLA','file_FALLA_*').filter(col('ANIO')==anio)
        falla_dtl = genericDataFrame.GetDataHdfs('FALLA_DTL','file_FALLA_DTL_*').filter(col('ANIO')==anio)
        gen = genericDataFrame.GetDataHdfs('TBLF_GEN','file_TBLF_GEN_*').filter(col('ANIO')==anio)
        snt = genericDataFrame.GetDataHdfs('TBLF_SNT','file_TBLF_SNT_*').filter(col('ANIO')==anio)
        eac = genericDataFrame.GetDataHdfs('TBLF_EAC','file_TBLF_EAC_*').filter(col('ANIO')==anio)
        crg = genericDataFrame.GetDataHdfs('TBLF_CRG','file_TBLF_CRG_*').filter(col('ANIO')==anio)
        crg_parcial = genericDataFrame.GetDataHdfs('TBLF_CRG_PARCIALES','file_TBLF_CRG_PARCIALES_*').filter(col('ANIO')==anio)
        evento = genericDataFrame.GetDataHdfs('EVENTO','file_EVENTO_*').filter(col('ANIO')==anio)
        
        if(len(falla.head(1))==0):
            return None
        
        ############################## PROCESAR AGENTES
        #datos_totales,detalle,det_snt,det_gen = LimpiarInterrupcionesSni.AgregarAgentes(bempresa,bcentral,blinea,bsubestacion,            
        datos_totales = LimpiarInterrupcionesSni.AgregarAgentes(bempresa,bcentral,blinea,bsubestacion,
                                                                                        bbarra,bcircuito,bcompensador,bposicion,
                                                                                        btrafo,bunidad,empresa,unegocio,subestacion,linea,
                                                                                        central,unidad,circuito,posicion,transformador,
                                                                                        barra,compensador,falla,falla_dtl,gen,snt,evento)
        
        ############################## PROCESAR AGENTE ORIGEN Y ORIGEN
        datos_totales = LimpiarInterrupcionesSni.AgregarOrigen(datos_totales,bempresa,empresa,unegocio,origen)
        
        ############################## PROCESAR CLASIFICACION
        datos_totales = Refactorizar.AgregarClasificacion(datos_totales,clasificacion,grupo,segundo,primero)
        
        ############################## PROCESAR VISTAS
        ############################## VISTA DE CARGA DESCONECTADA
        vRepSafCRGEns = CreacionVistas.CrearvRepSafCRGEns(genericDataFrame.spark,crg,falla,falla_dtl,unegocio,unegocio_tipo,
                                                          crg_parcial)

        ##############################  VISTA DE CARGA EAC
        vRepSafEACEns = CreacionVistas.CrearvRepSafEACEns(genericDataFrame.spark,eac,falla,falla_dtl,unegocio,unegocio_tipo,
                                                          crg_parcial)
        
        ##############################  PROCESAR FALLAS DE DISTRIBUCIÓN SIN CRG NI PARCIALES            
        crg = vRepSafCRGEns.groupby(col('FALLA_ID').alias('FallaIdCrg'))\
        .agg(func.sum('TIEMPO').alias('TIEMPOCrg'),
             func.sum('CARGA').alias('CARGACrg'),
             func.sum('ENS').alias('ENSCrg'),
             func.max('HORA_CONECTA').alias('HoraMax'))

        eac = vRepSafEACEns.groupby(col('FALLA_ID').alias('FallaIdEac'))\
        .agg(func.sum('TIEMPO').alias('TIEMPOEac'),
             func.sum('CARGA').alias('CARGAEac'),
             func.sum('ENS').alias('ENSEac'),
             func.max('HORA_CONECTA').alias('HoraMax'))
         
        ##############################  AGREGAR HECHOS EN LOS DATOS TOTALES.
        datos_totales = Refactorizar.AgregarHechos(datos_totales,crg,eac)

        agentes = accesoDatos.GetAllData('cen_dws.dim_agente')
        equivalentes = accesoDatos.GetAllData('cen_dws.dim_agente')
        agt_origen = accesoDatos.GetAllData('cen_dws.dim_agt_origen')
        origen = accesoDatos.GetAllData('cen_dws.dim_asignacion_origen')
        clasificacion = accesoDatos.GetAllData('cen_dws.dim_clasificacion_fallas')
        
        idNAagente = agentes.filter((col('agt_empresa_id_bk')=='NA') &\
                                    (col('agt_und_negocio_id_bk')=='NA') &\
                                    (col('agt_estacion_id_bk')=='NA') &\
                                    (col('agt_elemento_id_bk')=='NA')).select('agt_id_pk').first()[0]
        
        idNAagtorigen = agt_origen.filter((col('agtorg_empresa_id_bk')=='NA') &\
                                    (col('agtorg_und_negocio_id_bk')=='NA')).select('agtorg_id_pk').first()[0]
        
        idNAorigen = origen.filter((col('asigorg_origen_id_bk')=='NA')).select('asigorg_id_pk').first()[0]
        
        datos_totales = Refactorizar.LimpiarFallasSni(agentes,equivalentes,agt_origen,origen,clasificacion,datos_totales)
        
        fecha_actual = datetime.datetime.now()
        
        datos_totales = datos_totales\
        .withColumn('fecha_carga',lit(fecha_actual))\
        .fillna({'agt_id_fk':idNAagente,'agteqv_id_fk':idNAagente,'agtorg_id_fk':idNAagtorigen,'asigorg_id_fk':idNAorigen})
        
        datos_totales = datos_totales\
        .select('agt_id_fk',
                'tmpo_falla_id_fk',
                'hora_falla_id_fk',
                'tmpo_dispon_id_fk',
                'tmpo_cierre_id_fk',
                'tmpo_max_norm_id_fk',
                'tmpo_fin_id_fk',
                'hora_dispon_id_fk',
                'hora_fin_id_fk',
                'hora_cierre_id_fk',
                'hora_max_norm_id_fk',
                'asigorg_id_fk',
                'agtorg_id_fk',
                'agteqv_id_fk',
                'clasf_id_fk',
                'falla_numero',
                'falla_tmp_indisponibilidad',
                'falla_tmp_normalizacion_carga',
                'falla_potencia_disponible',
                'falla_potencia_disparada',
                'falla_carga_desconectada',
                'falla_carga_desconectada_eac', 
                'falla_ens_trn','falla_ens_trn_man',
                'falla_ens_sistema',
                'falla_ens_sistema_eac',
                'falla_ens_sistema_man',
                'falla_elemento_principal',
                'falla_consecuencia',
                'obsr_causa',
                'obsr_proteccion_actuada',
                'obsr_elemen_disp_distrib',
                'obsr_consecuencias',
                'obsr_obs_generales',
                'fecha_carga')

        return datos_totales
    
    
    def AgregarAgentes(df_empresa,df_central,df_linea,df_subestacion,df_barra,df_circuito,
                       df_compensador,df_posicion,df_transformador,df_unidad,df_cat_empresa,df_cat_unidad_negocio,
                       df_cat_subestacion,df_cat_linea,df_cat_central,df_cat_unidad,
                       df_cat_circuito,df_cat_posicion,df_cat_transformador,df_cat_barra,df_cat_compensador,
                       df_fallas,df_fallas_dtl,df_tblf_gen,df_tblf_snt,df_evento):
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
        ##### CREAR AGENTE EQUIVALENTE
        df_agt_equivalente = Refactorizar.AgenteEquivalente(unidad_negocio,estacion,elemento)

        ##### DETALLE
        df_datos_dtl = Refactorizar.AgregarDetalles(df_fallas,df_fallas_dtl,unidad_negocio,estacion,elemento,df_evento)
        
        ##### CREAR SNT
        df_datos_snt = Refactorizar.AgregarSnt(df_fallas,df_tblf_snt,unidad_negocio,estacion,elemento,df_evento) 

        ##### CREAR GEN
        df_datos_gen = Refactorizar.AgregarGen(df_fallas,df_tblf_gen,unidad_negocio,estacion,elemento,df_evento)
        
        df_datos = df_datos_dtl.union(df_datos_snt).union(df_datos_gen)

        df_datos_total = df_datos.join(df_agt_equivalente, df_datos.EquivalenteId == df_agt_equivalente.ElementoIdEq, how='left')

        return df_datos_total
    
    def AgregarOrigen(datos_totales,df_empresa,df_cat_empresa,df_cat_unidad_negocio,df_origen):
        ##### EMPRESA
        unidad_negocio = Refactorizar.DafaFrameUnidadNegocio(df_empresa,df_cat_empresa,df_cat_unidad_negocio)

        ##### EMPRESA - UNIDAD NEGOCIO
        datos_totales = Refactorizar.AgregarAgtOrigen(datos_totales,unidad_negocio,df_origen)
        return datos_totales
    
    
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
    