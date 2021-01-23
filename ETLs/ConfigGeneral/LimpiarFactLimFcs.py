from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from RefactoizarFactLimFcs import Refactorizar

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarFactLimFcs():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Límites FCS de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery,deteleFactQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Límites FCS ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_lim_fcs \n')

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
            #LimpiarDistribucionEns.Load_data(transform_data,'cen_dws.fact_dist_ens',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data (fecha_inicio,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""        
        anio = fecha_inicio.strftime('%Y')
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='BDTREV2')) 
        bempresa = genericDataFrame.GetDataHdfs('EMPRESA','file_EMPRESA')
        bcentral = genericDataFrame.GetDataHdfs('CENTRAL','file_CENTRA')
        blinea = genericDataFrame.GetDataHdfs('LINEA','file_LINEA')
        bsubestacion = genericDataFrame.GetDataHdfs('SUBESTACION','file_SUBESTACION')
        btipoelemento = genericDataFrame.GetDataHdfs('TIPOELEMENTO','file_TIPOELEMENTO')
        bbarra = genericDataFrame.GetDataHdfs('BARRA','file_BARRA')
        bcircuito = genericDataFrame.GetDataHdfs('CIRCUITO','file_CIRCUITO')
        bcompensador = genericDataFrame.GetDataHdfs('COMPENSADOR','file_COMPENSADOR')
        bposicion = genericDataFrame.GetDataHdfs('POSICION','file_POSICION')
        btrafo = genericDataFrame.GetDataHdfs('TRAFO','file_TRAFO')
        bunidad = genericDataFrame.GetDataHdfs('UNIDAD','file_UNIDAD')
        
        genericDataFrame = GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        empresa = genericDataFrame.GetDataHdfs('CFG_Empresa','file_CFG_Empresa')
        unegocio = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio','file_CFG_UnidadNegocio')
        unegocio_tipo = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio_TipoUnidad','file_CFG_UnidadNegocio_TipoUnidad')
        subestacion = genericDataFrame.GetDataHdfs('CFG_SubEstacion','file_CFG_SubEstacion')
        linea = genericDataFrame.GetDataHdfs('CFG_Linea','file_CFG_Linea')
        cenral = genericDataFrame.GetDataHdfs('CFG_Central','file_CFG_Central')
        nivelVoltaje = genericDataFrame.GetDataHdfs('CFG_NivelVoltaje','file_CFG_NivelVoltaje')
        unidad = genericDataFrame.GetDataHdfs('CFG_Unidad','file_CFG_Unidad')
        circuito = genericDataFrame.GetDataHdfs('CFG_Circuito','file_CFG_Circuito')
        posoiion = genericDataFrame.GetDataHdfs('CFG_Posicion','file_CFG_Posicion')
        transformador = genericDataFrame.GetDataHdfs('CFG_Transformador','file_CFG_Transformador')
        barra = genericDataFrame.GetDataHdfs('CFG_Barra','file_CFG_Barra')
        compensador  = genericDataFrame.GetDataHdfs('CFG_Compensador','file_CFG_Compensador')
        
        genericDataFrame=Generic+DataFrame(HDFSContext(DataBase='BOSNI')) 
        falla = genericDataFrame.GetDataHdfs('FALLA','file_FALLA_*').filter(col('ANIO')==anio)
        falla_dtl = genericDataFrame.GetDataHdfs('FALLA_DTL','file_FALLA_DTL_*').filter(col('ANIO')==anio)
        gen = genericDataFrame.GetDataHdfs('TBLF_GEN','file_TBLF_GEN_*').filter(col('ANIO')==anio)
        snt = genericDataFrame.GetDataHdfs('TBLF_SNT','file_TBLF_SNT_*').filter(col('ANIO')==anio)
        evento = genericDataFrame.GetDataHdfs('EVENTO','file_EVENTO_*').filter(col('ANIO')==anio)
        evento_dtl = genericDataFrame.GetDataHdfs('EVENTO_DTL','file_EVENTO_DTL_*').filter(col('ANIO')==anio)
        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SAMWEB')) 
        carga = genericDataFrame.GetDataHdfs('EJE_CARGA','file_EJE_CARGA_*')
        ejecucion = genericDataFrame.GetDataHdfs('EJE_EJECUCION','file_EJE_EJECUCION_*')

        if(len(falla.head(1))==0):
            return None
        if(len(ejecucion.head(1))==0):
            return None
        
        ################################### CATÁLOGO DE UNIDADES DE NEGOCIO
        unidad_negocio = Refactorizar.DafaFrameUnidadNegocio(bempresa,empresa,unegocio)
        
        ################################### PROCESAR TIEMPOS DE INDISPONIBILIDAD DE FALLAS  
        if(anio != None):
            inicio = int(str(anio)+'0101')
            fin = int(str(anio)+'1231')
        else:
            inicio = int('201201010000')
            fin = int(str(datetime.datetime.today().year)+'12312359')
        
        
        ################################### CONSULTA DE AGENTES
        df_agentes = Utilitarios\
        .ConvertPandasToSpark(self._genericDataFrame.spark,
                              self._fallasSniDA.GetAllAgenteSni(),
                              Estructuras.Schema_agente_sni())

        ################################### INDIPONIBILIDAD DE FALLAS
        df_indispon_fallas = accesoDatos.GetAllData('cen_dws.dim_agt_origen')
        Utilitarios\
        .ConvertPandasToSpark(self._genericDataFrame.spark,
                              self._fallasSniDA.GetAllFallasTransmision(inicio,fin),
                              Estructuras.SchemaDatosFallasTransmision())

        df_indisponibilidad_falla = df_indispon_fallas\
        .select(when(col('agteqv_id_fk').cast(IntegerType()).isNull(),col('agtsni_id_fk'))\
                .otherwise(col('agteqv_id_fk').cast(IntegerType())).alias('agt_id_fk'),
                col('agtsni_tipo_elemento'),
                col('tmp_id_pk'),
                col('tmp_fecha'),
                col('tmp_anio'),
                col('tmp_semestre'),
                col('tmpd_id_pk').cast(LongType()).alias('tmpd_id_pk'),
                col('tmpd_fecha'),
                col('falla_tmp_indisponibilidad'))
        
        
        
        
        
        
        
        
        
        
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
    