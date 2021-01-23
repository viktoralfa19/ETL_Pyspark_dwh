from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from RefactorizarDistribucionSni import Refactorizar
from CreacionVistasDistribucionesEns import CreacionVistas

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarDistribucionEns():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Distribución ENS de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery,deteleFactQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Distribución ENS ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_dist_ens \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
            
            if(deteleFactQuery is not None):
                query = deteleFactQuery.format(int(datetime.datetime.strftime(fecha_inicio,'%Y%m%d')),
                                               int(datetime.datetime.strftime(fecha_fin,'%Y%m%d')))
                accesoDatos.Delete(query)
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarDistribucionEns.Extract_Transform_data(fecha_inicio,accesoDatos)
            #transform_data.filter((col('agt_id_fk')==2310) &\
            #                           (col('tmpo_id_fk')==20170103) &\
            #                           (col('hora_id_fk')==2132)).show()
            print('2. Cargar  datos\n')
            LimpiarDistribucionEns.Load_data(transform_data,'cen_dws.fact_dist_ens',accesoDatos)
            
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
        unegocio_tipo = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio_TipoUnidad','file_CFG_UnidadNegocio_TipoUnidad')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='BDTREV2')) 
        bempresa = genericDataFrame.GetDataHdfs('EMPRESA','file_EMPRESA')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='BOSNI')) 
        origen = genericDataFrame.GetDataHdfs('TPF_ORIGEN_FALLA','file_TPF_ORIGEN_FALLA')
        
        falla = genericDataFrame.GetDataHdfs('FALLA','file_FALLA_*').filter(col('ANIO')==anio)
        falla_dtl = genericDataFrame.GetDataHdfs('FALLA_DTL','file_FALLA_DTL_*').filter(col('ANIO')==anio)
        eac = genericDataFrame.GetDataHdfs('TBLF_EAC','file_TBLF_EAC_*').filter(col('ANIO')==anio)
        crg = genericDataFrame.GetDataHdfs('TBLF_CRG','file_TBLF_CRG_*').filter(col('ANIO')==anio)
        crg_parcial = genericDataFrame.GetDataHdfs('TBLF_CRG_PARCIALES','file_TBLF_CRG_PARCIALES_*').filter(col('ANIO')==anio)
        
        if(len(falla.head(1))==0):
            return None
        
        ################################### CATÁLOGO DE UNIDADES DE NEGOCIO
        unidad_negocio = Refactorizar.DafaFrameUnidadNegocio(bempresa,empresa,unegocio)
        
        ################################### PROCESAR VISTAS
        ##### VISTA DE CARGA DESCONECTADA
        vRepSafCRGEns = CreacionVistas.CrearvRepSafCRGEns(genericDataFrame.spark,crg,falla,
                                                          falla_dtl,unegocio,
                                                          unegocio_tipo,crg_parcial)
        ##### VISTA DE CARGA EAC
        vRepSafEACEns = CreacionVistas.CrearvRepSafEACEns(genericDataFrame.spark,eac,falla,
                                                          falla_dtl,unegocio,
                                                          unegocio_tipo,crg_parcial) 
        
        ################################### PROCESAR ENERGÍA NO SUMINISTRADA INTERNA
        df_falla_interna_c = Refactorizar.ObtenerFallaDetalleInternoConCarga(falla,falla_dtl)
        df_falla_interna_s = Refactorizar.ObtenerFallaDetalleInternoSinCarga(falla,falla_dtl,crg,eac,crg_parcial)

        df_ens_interna_c = Refactorizar.ObtenerEnsInternaConCarga(unidad_negocio,df_falla_interna_c,vRepSafCRGEns)
        df_ens_interna_s = Refactorizar.ObtenerEnsInternaSinCarga(unidad_negocio,df_falla_interna_s,vRepSafCRGEns)

        df_ens_interna = df_ens_interna_c.union(df_ens_interna_s)
        
        ################################### PROCESAR ENERGÍA NO SUMINISTRADA AFECTACION SNT
        df_falla_afectacion_snt = Refactorizar.ObtenerFallaDetalleAfectacionSnt(falla,falla_dtl)
        df_ens_afectacion_snt = Refactorizar.ObtenerEnsGeneral(unidad_negocio,df_falla_afectacion_snt,vRepSafCRGEns)

        ################################### PROCESAR ENERGÍA NO SUMINISTRADA EXTERNA TRANSMISION
        df_falla_externa_trans = Refactorizar.ObtenerFallaDetalleExternaTrans(falla,falla_dtl)
        df_ens_externa_trans = Refactorizar.ObtenerEnsGeneral(unidad_negocio,df_falla_externa_trans,vRepSafCRGEns)

        ################################### PROCESAR ENERGÍA NO SUMINISTRADA EXTERNA GENERACION
        df_falla_externa_gen = Refactorizar.ObtenerFallaDetalleExternaGen(falla,falla_dtl)
        df_ens_externa_gen = Refactorizar.ObtenerEnsGeneral(unidad_negocio,df_falla_externa_gen,vRepSafCRGEns)

        ################################### PROCESAR ENERGÍA NO SUMINISTRADA SISTEMICO SPS
        df_falla_sistemico = Refactorizar.ObtenerFallaDetalleSistemicoSps(falla,falla_dtl)
        df_ens_sistemico = Refactorizar.ObtenerEnsGeneral(unidad_negocio,df_falla_sistemico,vRepSafCRGEns)

        ################################### PROCESAR ENERGÍA NO SUMINISTRADA OTROS
        df_falla_otros = Refactorizar.ObtenerFallaDetalleOtros(falla,falla_dtl)
        df_ens_otros = Refactorizar.ObtenerEnsGeneral(unidad_negocio,df_falla_otros,vRepSafCRGEns)

        ################################### PROCESAR ENERGÍA NO SUMINISTRADA EAC
        df_falla_detalle = Refactorizar.ObtenerFallaDetalleEAC(falla,falla_dtl)
        df_ens_eac = Refactorizar.ObtenerEnsEAC(df_falla_detalle,vRepSafEACEns)

        ################################### PROCESAR DATOS
        df_total = Refactorizar.JoinInternaAfectacion(df_ens_interna,df_ens_afectacion_snt)            
        df_total = Refactorizar.JoinTotalExternaTrans(df_total,df_ens_externa_trans)
        df_total = Refactorizar.JoinTotalExternaGen(df_total,df_ens_externa_gen)
        df_total = Refactorizar.JoinTotalSistemico(df_total,df_ens_sistemico)
        df_total = Refactorizar.JoinTotalOtros(df_total,df_ens_otros)
        df_total = Refactorizar.JoinTotalEac(df_total,df_ens_eac)
        
        
        ################################## NO ASIGNADO
        sin_clasificar_crg = vRepSafCRGEns\
        .join(df_total,
              (vRepSafCRGEns.FALLA_ID == df_total.FallaId) &\
              (vRepSafCRGEns.EMPRESA_ID == df_total.EmpresaId), how='left')\
        .filter(df_total.FallaId.isNull())\
        .select(vRepSafCRGEns.FALLA_ID,
                vRepSafCRGEns.EMPRESA_ID,
                vRepSafCRGEns.EMPRESA_CODIGO,
                vRepSafCRGEns.FALLA_FECHA,
                vRepSafCRGEns.HORA_CONECTA,
                vRepSafCRGEns.CARGA,
                vRepSafCRGEns.TIEMPO,
                vRepSafCRGEns.ENS)
        
        df_falla_sin_asignar = Refactorizar.ObtenerFallaDetalleSinAsignar(falla,falla_dtl)
        df_ens_sin_asignar = Refactorizar.ObtenerEnsGeneral(unidad_negocio,df_falla_sin_asignar,sin_clasificar_crg)
        df_total = Refactorizar.JoinTotalSinAsignar(df_total,df_ens_sin_asignar)
        
        #sin_clasificar_eac = vRepSafEACEns\
        #.join(df_total,
        #      (vRepSafEACEns.FALLA_ID == df_total.FallaId) &\
        #      (vRepSafEACEns.EMPRESA_ID == df_total.EmpresaId), how='left')\
        #.filter(df_total.FallaId.isNull())\
        #.select(vRepSafEACEns.FALLA_ID,
        #        vRepSafEACEns.EMPRESA_ID,
        #        vRepSafEACEns.EMPRESA_CODIGO,
        #        vRepSafEACEns.FALLA_FECHA,
        #        vRepSafEACEns.HORA_CONECTA,
        #        vRepSafEACEns.CARGA,
        #        vRepSafEACEns.TIEMPO,
        #        vRepSafEACEns.ENS)
        #sin_clasificar_eac.show(300)        

        ################################### ASIGNAR AGENTES,ORIGEN, PASOS
        datos_totales = LimpiarDistribucionEns.AgregarDatos(unidad_negocio,origen,df_total)
        
        agente = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'DIS') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 13) | \
                                                                     ((col('agt_empresa_id_bk')=='NA') &\
                                                                      (col('agt_und_negocio_id_bk')=='NA') &\
                                                                      (col('agt_estacion_id_bk')=='NA') &\
                                                                      (col('agt_elemento_id_bk')=='NA')))
        agt_origen = accesoDatos.GetAllData('cen_dws.dim_agt_origen')
        origen = accesoDatos.GetAllData('cen_dws.dim_asignacion_origen')
        pasos = accesoDatos.GetAllData('cen_dws.dim_pasos')
        
        #idNAagente = agentes.filter((col('agt_empresa_id_bk')=='NA') &\
        #                            (col('agt_und_negocio_id_bk')=='NA') &\
        #                            (col('agt_estacion_id_bk')=='NA') &\
        #                            (col('agt_elemento_id_bk')=='NA')).select('agt_id_pk').first()[0]
        
        #idNAagtorigen = agt_origen.filter((col('agtorg_empresa_id_bk')=='NA') &\
        #                            (col('agtorg_und_negocio_id_bk')=='NA')).select('agtorg_id_pk').first()[0]
        
        #idNAorigen = origen.filter((col('asigorg_origen_id_bk')=='NA')).select('asigorg_id_pk').first()[0]
                
        fecha_actual = datetime.datetime.now()
        
        
        ################################### FALLAS
        fallas_dist = Refactorizar.LimpiarFallasDist(datos_totales,agente,agt_origen,origen,pasos)\
        .withColumn('fecha_carga',lit(fecha_actual))
        
        return fallas_dist
    
    
    def AgregarDatos(unidad_negocio,df_origen,df_total):          
        df_datos_totales = Refactorizar.AgregarAgtDistribucion(df_total,unidad_negocio)
        df_datos_totales = Refactorizar.AgregarAgtOrigen(df_datos_totales,unidad_negocio,df_origen)
        return df_datos_totales
    

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
    