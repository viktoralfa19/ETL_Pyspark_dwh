from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim, lpad, concat, unix_timestamp, round
import datetime
from datetime import timedelta


class CreacionVistas:
    """Otorga método rapidos para formar las vistas de datos que se necesitan para realizar cálculos"""

    @staticmethod
    def CrearvRepSafCRGEns(spark,df_crg,df_falla,df_falla_dtl,unidad_negocio,df_cat_unegocio_tipo_unidad,df_parciales):
        """Vista que presenta información de Cargas desconectadas, obtienes datos de:
        - Cargas Parciales
        - Cargas Sin Parciales
        - Cargas de Empresas del Detalle con Parciales,
        Faltando Cargas de Empresas del Detalle sin Parciales"""
        unegocio_distribucion = unidad_negocio\
        .join(df_cat_unegocio_tipo_unidad, unidad_negocio.IdUNegocio == df_cat_unegocio_tipo_unidad.IdUnegocio)\
        .filter(df_cat_unegocio_tipo_unidad.IdTipoUnidadNegocio == 3)\
        .select(unidad_negocio.IdUNegocio.alias('EMPRESA_ID'),
                unidad_negocio.Codigo.alias('EMPRESA_CODIGO'),
                unidad_negocio.Nombre.alias('EMPRESA_NOMBRE'))

        PARCIALES = df_parciales\
        .join(df_falla, df_parciales.FALLA_ID == df_falla.FALLA_ID)\
        .join(unegocio_distribucion, df_parciales.EMPRESA_ID == unegocio_distribucion.EMPRESA_ID)\
        .filter((when(df_parciales.CRG.isNull(), True).otherwise(df_parciales.CRG) == True))\
        .groupby(df_parciales.FALLA_ID,
                 df_parciales.EMPRESA_ID,
                 unegocio_distribucion.EMPRESA_CODIGO,
                 unegocio_distribucion.EMPRESA_NOMBRE)\
        .agg(func.max(df_falla.FALLA_FECHA).alias('FALLA_FECHA'),
             func.max(df_parciales.FHORA_FINAL_CONECTA).alias('HORA_CONECTA'),
             func.sum(when(df_parciales.CARGA_CONECTA.isNull(),0).otherwise(df_parciales.CARGA_CONECTA)).alias('CARGA'),
             func.sum(when(df_parciales.TIEMPO_SUBTOTAL_SEG.isNull(),0).otherwise(df_parciales.TIEMPO_SUBTOTAL_SEG)).alias('TIEMPO'),
             func.sum(when(df_parciales.ENS_SUBTOTAL.isNull(),0).otherwise(df_parciales.ENS_SUBTOTAL)).alias('ENS'))\
        .withColumn('TIPO',lit(1))

        formato = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        tiempoNormalizacion = \
        (unix_timestamp(to_timestamp('TBLF_CRG_HORA_NORMAL','yyyy-MM-dd HH:mm:ss'), format=formato)
        - unix_timestamp(to_timestamp('FALLA_FECHA','yyyy-MM-dd HH:mm:ss'), format=formato))/3600.0

        datos = df_crg\
        .join(df_falla, df_crg.FALLA_ID == df_falla.FALLA_ID)\
        .join(df_parciales,
              (df_crg.FALLA_ID == df_parciales.FALLA_ID) &\
              (df_crg.EMPRESA_ID == df_parciales.EMPRESA_ID) &\
              (when(df_parciales.CRG.isNull(), True).otherwise(df_parciales.CRG) == True), how='left')\
        .join(df_falla_dtl,
              (df_crg.FALLA_ID == df_falla_dtl.FALLA_ID) &\
              (df_crg.EMPRESA_ID == df_falla_dtl.EMPRESA_ID), how='left')\
        .filter((df_parciales.FALLA_ID.isNull()) & (df_falla_dtl.FALLA_ID.isNull()))\
        .select(df_crg.FALLA_ID,
                df_crg.EMPRESA_ID,
                df_falla.FALLA_FECHA,
                df_crg.TBLF_CRG_HORA_NORMAL,
                df_crg.TBLF_CRG_MW)\
        .withColumn('TIEMPO',round(tiempoNormalizacion,4))\

        datos_sumarizados = datos\
        .groupby(datos.FALLA_ID,
                 datos.EMPRESA_ID)\
        .agg(func.max(datos.FALLA_FECHA).alias('FALLA_FECHA'),
             func.max(datos.TBLF_CRG_HORA_NORMAL).alias('HORA_CONECTA'),
             func.sum(datos.TBLF_CRG_MW).alias('CARGA'),
             func.sum(datos.TIEMPO).alias('TIEMPO'))

        CRG = datos_sumarizados\
        .join(unegocio_distribucion, datos.EMPRESA_ID == unegocio_distribucion.EMPRESA_ID)\
        .select(datos_sumarizados.FALLA_ID,
                datos_sumarizados.EMPRESA_ID,
                unegocio_distribucion.EMPRESA_CODIGO,
                unegocio_distribucion.EMPRESA_NOMBRE,
                datos_sumarizados.FALLA_FECHA,
                datos_sumarizados.HORA_CONECTA,
                datos_sumarizados.CARGA,
                datos_sumarizados.TIEMPO,
                round(datos_sumarizados.CARGA*datos_sumarizados.TIEMPO,2).alias('ENS'))\
        .withColumn('TIPO',lit(2))

        formato = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        tiempoNormalizacion = \
        (unix_timestamp(to_timestamp('HORA_CONECTA','yyyy-MM-dd HH:mm:ss'), format=formato)
        - unix_timestamp(to_timestamp('FALLA_FECHA','yyyy-MM-dd HH:mm:ss'), format=formato))/3600.0

        datos_dtl = df_falla\
        .join(df_falla_dtl, df_falla.FALLA_ID == df_falla_dtl.FALLA_ID)\
        .join(unegocio_distribucion, df_falla_dtl.EMPRESA_ID == unegocio_distribucion.EMPRESA_ID)\
        .join(df_crg,
              (df_falla.FALLA_ID == df_crg.FALLA_ID) &\
              (df_falla_dtl.EMPRESA_ID == df_crg.EMPRESA_ID), how='left')\
        .join(df_parciales,
              (df_falla.FALLA_ID == df_parciales.FALLA_ID) &\
              (df_falla_dtl.EMPRESA_ID == df_parciales.EMPRESA_ID), how='left')\
        .filter(((df_parciales.FALLA_ID.isNull()) & (df_crg.FALLA_ID.isNull())) &\
                (df_falla_dtl.FALLA_CLASE == 4))\
        .select(df_falla.FALLA_ID,
                df_falla_dtl.EMPRESA_ID,
                unegocio_distribucion.EMPRESA_CODIGO,
                unegocio_distribucion.EMPRESA_NOMBRE,
                df_falla.FALLA_FECHA,
                df_falla_dtl.FALLA_DTL_FECHA_DISPON.alias('HORA_CONECTA'),
                when(df_falla_dtl.FALLA_DTL_POTENCIA_MW.isNull(),0).otherwise(df_falla_dtl.FALLA_DTL_POTENCIA_MW).alias('CARGA'),
               )\
        .withColumn('TIEMPO',round(tiempoNormalizacion,4))

        DTL = datos_dtl\
        .select('FALLA_ID',
                'EMPRESA_ID',
                'EMPRESA_CODIGO',
                'EMPRESA_NOMBRE',
                'FALLA_FECHA',
                'HORA_CONECTA',
                'CARGA',
                when(col('TIEMPO').isNull(),0).otherwise(col('TIEMPO')).alias('TIEMPO'),
                (col('CARGA')*when(col('TIEMPO').isNull(),0).otherwise(col('TIEMPO'))).alias('ENS'))\
        .withColumn('TIPO',lit(3))

        PARTE_ABC = PARCIALES.union(CRG).union(DTL)

        #PARTE_ABC.groupby().agg(func.sum('TIEMPO').alias('Tiempo'),
        #                        func.sum('ENS').alias('ENS')).show(PARTE_ABC.count())

        #print(PARTE_ABC.count())
        #PARTE_ABC.show(PARTE_ABC.count())

        return PARTE_ABC
            
    @staticmethod
    def CrearvRepSafEACEns(spark,df_eac,df_falla,df_falla_dtl,unidad_negocio,df_cat_unegocio_tipo_unidad,df_parciales):
        """Vista que presenEACta información de Cargas desconectadas, obtienes datos de:
        - Cargas Parciales 
        - Cargas EAC Sin Parciales EAC
        - Cargas de Empresas del Detalle con Parciales EAC"""
        unegocio_distribucion = unidad_negocio\
        .join(df_cat_unegocio_tipo_unidad, unidad_negocio.IdUNegocio == df_cat_unegocio_tipo_unidad.IdUnegocio)\
        .filter(df_cat_unegocio_tipo_unidad.IdTipoUnidadNegocio == 3)\
        .select(unidad_negocio.IdUNegocio.alias('EMPRESA_ID'),
                unidad_negocio.Codigo.alias('EMPRESA_CODIGO'),
                unidad_negocio.Nombre.alias('EMPRESA_NOMBRE'))

        PARCIALES = df_parciales\
        .join(df_falla, df_parciales.FALLA_ID == df_falla.FALLA_ID)\
        .join(unegocio_distribucion, df_parciales.EMPRESA_ID == unegocio_distribucion.EMPRESA_ID)\
        .filter((when(df_parciales.CRG.isNull(), True).otherwise(df_parciales.CRG) == False))\
        .groupby(df_parciales.FALLA_ID,
                 df_parciales.EMPRESA_ID,
                 unegocio_distribucion.EMPRESA_CODIGO,
                 unegocio_distribucion.EMPRESA_NOMBRE)\
        .agg(func.max(df_falla.FALLA_FECHA).alias('FALLA_FECHA'),
             func.max(df_parciales.FHORA_FINAL_CONECTA).alias('HORA_CONECTA'),
             func.sum(when(df_parciales.CARGA_CONECTA.isNull(),0).otherwise(df_parciales.CARGA_CONECTA)).alias('CARGA'),
             func.sum(when(df_parciales.TIEMPO_SUBTOTAL_SEG.isNull(),0).otherwise(df_parciales.TIEMPO_SUBTOTAL_SEG)).alias('TIEMPO'),
             func.sum(when(df_parciales.ENS_SUBTOTAL.isNull(),0).otherwise(df_parciales.ENS_SUBTOTAL)).alias('ENS'))

        formato = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        tiempoNormalizacion = \
        (unix_timestamp(to_timestamp('TBLF_EAC_HORA_RECONEC','yyyy-MM-dd HH:mm:ss'), format=formato)
        - unix_timestamp(to_timestamp('FALLA_FECHA','yyyy-MM-dd HH:mm:ss'), format=formato))/3600.0

        datos = df_eac\
        .join(df_falla, df_eac.FALLA_ID == df_falla.FALLA_ID)\
        .join(df_parciales,
              (df_eac.FALLA_ID == df_parciales.FALLA_ID) &\
              (df_eac.EMPRESA_ID == df_parciales.EMPRESA_ID), how='left')\
        .join(df_falla_dtl,
              (df_eac.FALLA_ID == df_falla_dtl.FALLA_ID) &\
              (df_eac.EMPRESA_ID == df_falla_dtl.EMPRESA_ID), how='left')\
        .filter((df_parciales.FALLA_ID.isNull()) & (df_falla_dtl.FALLA_ID.isNull()))\
        .select(df_eac.FALLA_ID,
                df_eac.EMPRESA_ID,
                df_falla.FALLA_FECHA,
                df_eac.TBLF_EAC_HORA_RECONEC,
                (df_eac.TBLF_EAC_MW_ANTES-df_eac.TBLF_EAC_MW_DESPUES).alias('CARGA'))\
        .withColumn('TIEMPO',round(tiempoNormalizacion,4))\

        datos_sumarizados = datos\
        .groupby(datos.FALLA_ID,
                 datos.EMPRESA_ID)\
        .agg(func.max(datos.FALLA_FECHA).alias('FALLA_FECHA'),
             func.max(datos.TBLF_EAC_HORA_RECONEC).alias('HORA_CONECTA'),
             func.sum(datos.CARGA).alias('CARGA'),
             func.sum(datos.TIEMPO).alias('TIEMPO'))

        EAC = datos_sumarizados\
        .join(unegocio_distribucion, datos.EMPRESA_ID == unegocio_distribucion.EMPRESA_ID)\
        .select(datos_sumarizados.FALLA_ID,
                datos_sumarizados.EMPRESA_ID,
                unegocio_distribucion.EMPRESA_CODIGO,
                unegocio_distribucion.EMPRESA_NOMBRE,
                datos_sumarizados.FALLA_FECHA,
                datos_sumarizados.HORA_CONECTA,
                datos_sumarizados.CARGA,
                datos_sumarizados.TIEMPO,
                round(datos_sumarizados.CARGA*datos_sumarizados.TIEMPO,2).alias('ENS'))           

        PARTE_ABC = PARCIALES.union(EAC)

        #PARTE_ABC.groupby().agg(func.sum('TIEMPO').alias('Tiempo'),
        #                        func.sum('ENS').alias('ENS')).show()

        #print(PARTE_ABC.count())
        #PARTE_ABC.show(PARTE_ABC.count())

        return PARTE_ABC
            
