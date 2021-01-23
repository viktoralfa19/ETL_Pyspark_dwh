from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim, lpad, concat, unix_timestamp, round
import datetime
from datetime import timedelta

class Refactorizar:
    """Contiene metodos auxiliares del negocio"""
    @staticmethod
    def DafaFrameUnidadNegocio(df_empresa,df_cat_empresa,df_cat_unidad_negocio):
        df_unidad_negocio = df_empresa.select(col('EMPRESA_ID').alias('UNegocioId'),
                                              col('EMPRESA_CODIGO').alias('UNegocioCodigo'),
                                              col('EMPRESA_NOMBRE').alias('UNegocio'),
                                              col('EMPRESA_CODIGO').alias('EmpresaCodigo'),
                                              col('EMPRESA_NOMBRE').alias('Empresa'),
                                              col('TIPOEMPRESA_ID').alias('IdTipoUnidadNegocio'))

        df_cat_unidad_negocio = df_cat_unidad_negocio\
        .join(df_cat_empresa, df_cat_unidad_negocio.IdEmpresa == df_cat_empresa.IdEmpresa)\
        .select(df_cat_unidad_negocio.IdUNegocio.alias('UNegocioId'),
                df_cat_unidad_negocio.Codigo.alias('UNegocioCodigo'),
                df_cat_unidad_negocio.Nombre.alias('UNegocio'),
                df_cat_empresa.Codigo.alias('EmpresaCodigo'),
                df_cat_empresa.Nombre.alias('Empresa'),
                df_cat_unidad_negocio.IdTipoUnidadNegocio)

        return df_cat_unidad_negocio.union(df_unidad_negocio).distinct()
                      
    @staticmethod      
    def ObtenerEnsMantenimiento(df_cargas,unegocio,cat_unegocio):    
        formato = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        tiempoNormalizacion = \
        (unix_timestamp(to_timestamp('EJECAR_FECHA_FINAL','yyyy-MM-dd HH:mm:ss'), format=formato)
        - unix_timestamp(to_timestamp('EJECAR_FECHA_INICIAL','yyyy-MM-dd HH:mm:ss'), format=formato))/3600.0

        df_parciales = df_cargas\
        .filter((df_cargas.EJECAR_ES_PARCIAL==True) & (df_cargas.EJECAR_TIPO=='Desconexión'))\
        .select('EJECAR_ID_EJECUCION','EJECAR_ID_EMPRESA','EJECAR_ID_UNIDAD_NEGOCIO','EJECAR_FECHA_INICIAL',
                'EJECAR_FECHA_FINAL','EJECAR_POTENCIA','EJECAR_ID_UNEGOCIO_ORIGEN','EJECAR_ENS',
                tiempoNormalizacion.alias('HORAS'))\
        .withColumn('ENS',col('EJECAR_POTENCIA')*col('HORAS'))\
        .groupby('EJECAR_ID_EJECUCION','EJECAR_ID_EMPRESA','EJECAR_ID_UNIDAD_NEGOCIO')\
        .agg(func.min('EJECAR_FECHA_INICIAL').alias('HORA_INICIO'),
             func.max('EJECAR_FECHA_FINAL').alias('HORA_FIN'),
             func.sum('HORAS').alias('HORAS_TOT'),
             func.max('EJECAR_POTENCIA').alias('MAX_MW'),
             when(func.sum('ENS').isNull(),0).otherwise(func.sum('ENS')).alias('ENS_TOT'),
             when(func.sum('EJECAR_ENS').isNull(),0).otherwise(func.sum('EJECAR_ENS')).alias('ENS_MANUAL_TOT'),
             func.max('EJECAR_ID_UNEGOCIO_ORIGEN').alias('EJECAR_ID_UNEGOCIO_ORIGEN'),
             func.year(func.min('EJECAR_FECHA_INICIAL').cast(TimestampType())).alias('ANIO'))

        df_no_parciales = df_cargas\
        .filter((df_cargas.EJECAR_ES_PARCIAL==False) & (df_cargas.EJECAR_TIPO=='Desconexión'))\
        .select('EJECAR_ID_EJECUCION','EJECAR_ID_EMPRESA','EJECAR_ID_UNIDAD_NEGOCIO',
                col('EJECAR_FECHA_INICIAL').alias('HORA_INICIO'),col('EJECAR_FECHA_FINAL').alias('HORA_FIN'),
                tiempoNormalizacion.alias('HORAS_TOT'),col('EJECAR_POTENCIA').alias('MAX_MW'),
                (col('EJECAR_POTENCIA')*tiempoNormalizacion).alias('ENS_TOT'),
                when(col('EJECAR_ENS').isNull(),0).otherwise(col('EJECAR_ENS')).alias('ENS_MANUAL_TOT'),
                'EJECAR_ID_UNEGOCIO_ORIGEN',
                func.year('EJECAR_FECHA_INICIAL').alias('ANIO'))

        df_totales = df_parciales.union(df_no_parciales)

        df_totales = df_totales\
        .join(unegocio, df_totales.EJECAR_ID_UNEGOCIO_ORIGEN == unegocio.UNegocioId, how='left')\
        .select(df_totales.EJECAR_ID_EJECUCION.alias('EjecucionId'),
                df_totales.EJECAR_ID_EMPRESA.alias('EmpresaId'),
                df_totales.EJECAR_ID_UNIDAD_NEGOCIO.alias('UNegocioId'),
                func.substring(regexp_replace(to_date('HORA_INICIO','yyyy-MM-dd'), '-', ''),0,8).alias('FechaId').cast('integer'),
                df_totales.HORA_INICIO.alias('HoraInicio'),
                df_totales.HORA_FIN.alias('HoraFin'),
                df_totales.HORAS_TOT.alias('TIEMPO'),
                df_totales.MAX_MW.alias('CARGA'),
                round(df_totales.ENS_TOT,4).alias('ENS'),
                round(df_totales.ENS_MANUAL_TOT,4).alias('EnsManual'),
                df_totales.ANIO.alias('Anio'),
                unegocio.UNegocioCodigo.alias('UNegocioCodigoOrg'),
                unegocio.EmpresaCodigo.alias('EmpresaCodigoOrg'),

                when(unegocio.IdTipoUnidadNegocio==1,'GEN')\
                .otherwise(when(unegocio.IdTipoUnidadNegocio==2,'TRANS')\
                .otherwise(when(unegocio.IdTipoUnidadNegocio==3,'DISTR')\
                .otherwise(when(unegocio.IdTipoUnidadNegocio==4,'OT')\
                .otherwise(when(unegocio.IdTipoUnidadNegocio==5,'INT')\
                .otherwise(None))))).alias('Origen'))

        df_totales = df_totales\
        .join(unegocio, df_totales.UNegocioId == unegocio.UNegocioId, how='left')\
        .select(df_totales.EjecucionId,
                df_totales.EmpresaId,
                unegocio.EmpresaCodigo.alias('EmpresaCodigo'),
                df_totales.UNegocioId,
                unegocio.UNegocioCodigo.alias('UNegocioCodigo'),
                df_totales.FechaId,
                df_totales.HoraInicio,
                df_totales.HoraFin,
                df_totales.TIEMPO,
                df_totales.CARGA,
                df_totales.ENS,
                df_totales.EnsManual,
                df_totales.Anio,
                df_totales.UNegocioCodigoOrg,
                df_totales.EmpresaCodigoOrg,
                df_totales.Origen)\
        .groupby('EmpresaCodigo','UNegocioId','UNegocioCodigo','FechaId',
                 'UNegocioCodigoOrg','EmpresaCodigoOrg','Origen')\
        .agg(func.sum('TIEMPO').alias('TIEMPO'),
             func.sum('CARGA').alias('CARGA'),
            func.sum('ENS').alias('ENS'))

        return df_totales
            
    def ObtenerTotales(df_es,df_ens_mantenimiento):
        df_datos = df_es\
        .join(df_ens_mantenimiento, (df_es.FechaId == df_ens_mantenimiento.FechaId) &\
              (df_es.UNegocioCodigo == df_ens_mantenimiento.UNegocioCodigo), how='full')\
        .select(when(df_es.FechaId.isNull(),df_ens_mantenimiento.FechaId).otherwise(df_es.FechaId).alias('tmpo_id_fk'),
                when(df_es.EmpresaCodigo.isNull(),df_ens_mantenimiento.EmpresaCodigo)\
                .otherwise(df_es.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_es.UNegocioCodigo.isNull(),df_ens_mantenimiento.UNegocioCodigo)\
                .otherwise(df_es.UNegocioCodigo).alias('UNegocioCodigo'),
                col('EmpresaCodigoOrg').alias('EmpresaCodigoOrg'),
                col('UNegocioCodigoOrg').alias('UNegocioCodigoOrg'),
                col('Origen').alias('Origen'),
                when(df_es.Energia.isNull(),0).otherwise(df_es.Energia).alias('ES'),
                when(df_ens_mantenimiento.ENS.isNull(),0).otherwise(df_ens_mantenimiento.ENS).alias('ENSM'))
        
        df_datos = df_datos\
        .select('tmpo_id_fk','EmpresaCodigo','UNegocioCodigo',
                when(col('EmpresaCodigoOrg').isNull(),'NA').otherwise(col('EmpresaCodigoOrg')).alias('EmpresaCodigoOrg'),
                when(col('UNegocioCodigoOrg').isNull(),'NA').otherwise(col('UNegocioCodigoOrg')).alias('UNegocioCodigoOrg'),
                when(col('Origen').isNull(),'NA').otherwise(col('Origen')).alias('Origen'),'ES','ENSM')
        
        return df_datos
    
    @staticmethod
    def AsignarEmpresa(df_es,unidad_negocio):
        df_datos = df_es\
        .join(unidad_negocio, df_es.UNegocioCodigo == unidad_negocio.UNegocioCodigo)\
        .select(df_es.FechaId,
                unidad_negocio.EmpresaCodigo,
                unidad_negocio.Empresa,
                df_es.UNegocioCodigo,
                df_es.UNegocio,
                df_es.Energia)
        return df_datos
    
    @staticmethod
    def LimpiarEnsEs(datos,agentes_dist,agentes_origen,origen):        
        #Proceso de limpieza
        df_fallas = datos\
        .join(agentes_dist, 
              (datos.EmpresaCodigo == agentes_dist.agt_empresa_id_bk) & \
              (datos.UNegocioCodigo == agentes_dist.agt_und_negocio_id_bk))\
        .join(agentes_origen, 
              (datos.EmpresaCodigoOrg == agentes_origen.agtorg_empresa_id_bk) & \
              (datos.UNegocioCodigoOrg == agentes_origen.agtorg_und_negocio_id_bk), how='left')\
        .join(origen, 
              (datos.Origen == origen.asigorg_origen_id_bk), how='left')\
        .select(datos.tmpo_id_fk,
                agentes_dist.agt_id_pk.alias('agt_id_fk'),
                agentes_origen.agtorg_id_pk.alias('agtorg_id_fk'),
                origen.asigorg_id_pk.alias('asigorg_id_fk'),
                datos.ENSM.alias('enses_ensm').cast('float'),
                datos.ES.alias('enses_es').cast('float')).distinct()
        return df_fallas