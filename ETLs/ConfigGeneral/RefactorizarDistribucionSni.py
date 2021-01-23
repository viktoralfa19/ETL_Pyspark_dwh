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
                                              col('EMPRESA_NOMBRE').alias('Empresa'))

        df_cat_unidad_negocio = df_cat_unidad_negocio\
        .join(df_cat_empresa, df_cat_unidad_negocio.IdEmpresa == df_cat_empresa.IdEmpresa)\
        .select(df_cat_unidad_negocio.IdUNegocio.alias('UNegocioId'),
                df_cat_unidad_negocio.Codigo.alias('UNegocioCodigo'),
                df_cat_unidad_negocio.Nombre.alias('UNegocio'),
                df_cat_empresa.Codigo.alias('EmpresaCodigo'),
                df_cat_empresa.Nombre.alias('Empresa'))

        return df_cat_unidad_negocio.union(df_unidad_negocio).distinct()
            
    
    @staticmethod      
    def ObtenerFallaDetalleInternoConCarga(df_fallas,df_fallas_dtl):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .filter((df_fallas_dtl.FALLA_CLASE == 4) &\
                (df_fallas_dtl.FALLA_DTL_CRG_DESCON == 1))\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()
        return df_falla_detalle
    
    
    @staticmethod      
    def ObtenerFallaDetalleInternoSinCarga(df_fallas,df_fallas_dtl,crg,eac,parciales):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .join(crg, (df_fallas.FALLA_ID == crg.FALLA_ID) & (df_fallas_dtl.EMPRESA_ID == crg.EMPRESA_ID),how='left')\
        .join(eac, (df_fallas.FALLA_ID == eac.FALLA_ID) & (df_fallas_dtl.EMPRESA_ID == eac.EMPRESA_ID),how='left')\
        .join(parciales, (df_fallas.FALLA_ID == parciales.FALLA_ID) & (df_fallas_dtl.EMPRESA_ID == parciales.EMPRESA_ID),how='left')\
        .filter((df_fallas_dtl.FALLA_CLASE == 4) &\
                (crg.FALLA_ID.isNull()) &\
                (eac.FALLA_ID.isNull()) &\
                (parciales.FALLA_ID.isNull()))\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()
        return df_falla_detalle
    
    
    @staticmethod      
    def ObtenerFallaDetalleAfectacionSnt(df_fallas,df_fallas_dtl):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .filter((df_fallas_dtl.FALLA_CLASE != 4) &\
                (df_fallas_dtl.FALLA_DTL_CRG_DESCON == 1) &\
                (~df_fallas.FALLA_RPT_NRO.isNull()))\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()
        return df_falla_detalle
    
    
    @staticmethod      
    def ObtenerFallaDetalleExternaTrans(df_fallas,df_fallas_dtl):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla pero solo se toma el elemento principal asi que no se repetirá y solo reportes no registros."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .filter((df_fallas_dtl.FALLA_DTL_ID == 1) &\
                (~df_fallas.FALLA_RPT_NRO.isNull()) &\
                (df_fallas_dtl.FALLA_DTL_CRG_DESCON == 2))\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()

        return df_falla_detalle
            
            
    @staticmethod      
    def ObtenerFallaDetalleExternaGen(df_fallas,df_fallas_dtl):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla, y solo toma reportes no registros."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .filter((df_fallas_dtl.FALLA_RPT_NRO>0) &\
                (df_fallas_dtl.FALLA_DTL_CRG_DESCON == 3))\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()

        return df_falla_detalle
            
    @staticmethod      
    def ObtenerFallaDetalleSistemicoSps(df_fallas,df_fallas_dtl):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla y solo toma reportes no registros y solo toma reportes no registros."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .filter((df_fallas_dtl.FALLA_RPT_NRO>0) &\
                (df_fallas_dtl.FALLA_DTL_CRG_DESCON.isin({5,7})))\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()
        
        return df_falla_detalle
            
    @staticmethod      
    def ObtenerFallaDetalleOtros(df_fallas,df_fallas_dtl):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla, y solo toma reportes no registros."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .filter((df_fallas_dtl.FALLA_RPT_NRO>0) &\
                (df_fallas_dtl.FALLA_DTL_CRG_DESCON == 6))\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()
        
        return df_falla_detalle
            
            
    @staticmethod      
    def ObtenerFallaDetalleEAC(df_fallas,df_fallas_dtl):
        """Estoy suponiendo que la empresa origen es la misma por cada empresa del detalle de la falla, 
        pero se debe corroborar, ya que se debe tomar en cuenta esto, porque si en el detalle, existen dos 
        o más empresas de distribución, por cada una de ellas se repetiría el conjunto de empresas con ENS 
        asociadas a la falla, y solo toma reportes no registros."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                lit(8).alias('OrigenId'),
                lit(None).alias('EmpresaOrigenId')).distinct()
        return df_falla_detalle
            
    
    @staticmethod
    def ObtenerEnsInternaSinCarga(unidad_negocio,df_falla,df_crg):
        """ENERGÍA NO SUMINISTRADA DE FALLAS DE DISTRIBUCIÓN ASIGNADAS A DISTRIBUIDOR"""           
        ############################## DETALLES PRINCIPALES
        df_parciales = df_crg.filter(df_crg.TIPO == 3)

        df_datos = df_parciales\
        .join(df_falla, (df_parciales.FALLA_ID == df_falla.FallaId) &\
              (df_parciales.EMPRESA_ID == df_falla.EmpresaId))\
        .select(df_falla.FallaId,
                df_falla.FechaFalla,
                when(df_falla.Numero.isNull(),'-').otherwise(df_falla.Numero).alias('Numero'),
                when(df_falla.Paso.isNull(),0).otherwise(df_falla.Paso).alias('Paso'),
                df_falla.EnsManual,
                df_falla.OrigenId,
                df_falla.EmpresaOrigenId,
                df_parciales.EMPRESA_ID.alias('EmpresaId'),
                df_parciales.EMPRESA_CODIGO.alias('EmpresaCodigo'),
                when(df_parciales.HORA_CONECTA.isNull(),datetime.datetime(1900,1,1,0,0))\
                .otherwise(df_parciales.HORA_CONECTA).alias('FechaConecta'),
                df_parciales.CARGA.alias('CARGA'),
                df_parciales.TIEMPO.alias('TIEMPO'),
                df_parciales.ENS.alias('ENS'))\
        .groupby('FallaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId',
                 'EmpresaId','EmpresaCodigo','FechaConecta')\
        .agg(func.sum('CARGA').alias('CARGA'),
             func.sum('TIEMPO').alias('TIEMPO'),
             func.sum('ENS').alias('ENS'))

        return df_datos
      
    @staticmethod
    def ObtenerEnsInternaConCarga(unidad_negocio,df_falla,df_crg):
        """ENERGÍA NO SUMINISTRADA DE FALLAS DE DISTRIBUCIÓN ASIGNADAS A DISTRIBUIDOR"""
        ############################## PARCIALES
        df_parciales = df_crg.filter(df_crg.TIPO == 1)
        df_falla = df_falla.select('FallaId','FechaFalla','Numero','EnsManual','OrigenId','Paso','EmpresaOrigenId').distinct()

        df_datos_p = df_parciales\
        .join(df_falla, (df_parciales.FALLA_ID == df_falla.FallaId))\
        .select(df_falla.FallaId,
                df_falla.FechaFalla,
                when(df_falla.Numero.isNull(),'-').otherwise(df_falla.Numero).alias('Numero'),
                when(df_falla.Paso.isNull(),0).otherwise(df_falla.Paso).alias('Paso'),
                df_falla.EnsManual,
                df_falla.OrigenId,
                df_falla.EmpresaOrigenId,
                df_parciales.EMPRESA_ID.alias('EmpresaId'),
                df_parciales.EMPRESA_CODIGO.alias('EmpresaCodigo'),
                when(df_parciales.HORA_CONECTA.isNull(),datetime.datetime(1900,1,1,0,0))\
                .otherwise(df_parciales.HORA_CONECTA).alias('FechaConecta'),
                df_parciales.CARGA.alias('CARGA'),
                df_parciales.TIEMPO.alias('TIEMPO'),
                df_parciales.ENS.alias('ENS')).distinct()\
        .groupby('FallaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId',
                 'EmpresaId','EmpresaCodigo','FechaConecta')\
        .agg(func.sum('CARGA').alias('CARGA'),
             func.sum('TIEMPO').alias('TIEMPO'),
             func.sum('ENS').alias('ENS'))

        ############################## CARGAS PRINCIPALES
        df_parciales = df_crg.filter(df_crg.TIPO == 2)

        df_datos_c = df_parciales\
        .join(df_falla, (df_parciales.FALLA_ID == df_falla.FallaId))\
        .select(df_falla.FallaId,
                df_falla.FechaFalla,
                df_falla.Numero,
                df_falla.Paso,
                df_falla.EnsManual,
                df_falla.OrigenId,
                df_falla.EmpresaOrigenId,
                df_parciales.EMPRESA_ID.alias('EmpresaId'),
                df_parciales.EMPRESA_CODIGO.alias('EmpresaCodigo'),
                when(df_parciales.HORA_CONECTA.isNull(),datetime.datetime(1900,1,1,0,0))\
                .otherwise(df_parciales.HORA_CONECTA).alias('FechaConecta'),
                df_parciales.CARGA.alias('CARGA'),
                df_parciales.TIEMPO.alias('TIEMPO'),
                df_parciales.ENS.alias('ENS')).distinct()\
        .groupby('FallaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId',
                 'EmpresaId','EmpresaCodigo','FechaConecta')\
        .agg(func.sum('CARGA').alias('CARGA'),
             func.sum('TIEMPO').alias('TIEMPO'),
             func.sum('ENS').alias('ENS'))

        df_datos = df_datos_p.union(df_datos_c)

        return df_datos
            
            
    @staticmethod
    def ObtenerEnsGeneral(unidad_negocio,df_falla,df_crg):
        """ENERGÍA NO SUMINISTRADA DE FALLAS DE DISTRIBUCIÓN ASIGNADAS A DISTRIBUIDOR"""
        ############################## TOTALES
        df_parciales = df_crg
        df_falla = df_falla.select('FallaId','FechaFalla','Numero','EnsManual','OrigenId','Paso','EmpresaOrigenId').distinct()

        df_datos = df_parciales\
        .join(df_falla, (df_parciales.FALLA_ID == df_falla.FallaId))\
        .select(df_falla.FallaId,
                df_falla.FechaFalla,
                df_falla.Numero,
                when(df_falla.Paso.isNull(),0).otherwise(df_falla.Paso).alias('Paso'),
                df_falla.EnsManual,
                df_falla.OrigenId,
                df_falla.EmpresaOrigenId,
                df_parciales.EMPRESA_ID.alias('EmpresaId'),
                df_parciales.EMPRESA_CODIGO.alias('EmpresaCodigo'),
                when(df_parciales.HORA_CONECTA.isNull(),datetime.datetime(1900,1,1,0,0))\
                .otherwise(df_parciales.HORA_CONECTA).alias('FechaConecta'),
                df_parciales.CARGA.alias('CARGA'),
                df_parciales.TIEMPO.alias('TIEMPO'),
                df_parciales.ENS.alias('ENS'))\
        .groupby('FallaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId',
                 'EmpresaId','EmpresaCodigo','FechaConecta')\
        .agg(func.sum('CARGA').alias('CARGA'),
             func.sum('TIEMPO').alias('TIEMPO'),
             func.sum('ENS').alias('ENS'))

        return df_datos
            
    @staticmethod
    def ObtenerEnsEAC(df_falla,df_eac):
        """ENERGÍA NO SUMINISTRADA DE FALLAS DE DISTRIBUCIÓN ASIGNADAS A DISTRIBUIDOR"""
        ############################## TOTALES
        df_parciales = df_eac
        df_falla = df_falla.select('FallaId','FechaFalla','Numero','EnsManual','OrigenId','Paso','EmpresaOrigenId').distinct()
        
        df_datos = df_parciales\
        .join(df_falla, (df_parciales.FALLA_ID == df_falla.FallaId))\
        .select(df_falla.FallaId,
                df_falla.FechaFalla,
                df_falla.Numero,
                when(df_falla.Paso.isNull(),0).otherwise(df_falla.Paso).alias('Paso'),
                df_falla.EnsManual,
                df_falla.OrigenId,
                df_falla.EmpresaOrigenId,
                df_parciales.EMPRESA_ID.alias('EmpresaId'),
                df_parciales.EMPRESA_CODIGO.alias('EmpresaCodigo'),
                df_parciales.EMPRESA_NOMBRE.alias('Empresa'),
                when(df_parciales.HORA_CONECTA.isNull(),datetime.datetime(1900,1,1,0,0))\
                .otherwise(df_parciales.HORA_CONECTA).alias('FechaConecta'),
                df_parciales.CARGA.alias('CARGA'),
                df_parciales.TIEMPO.alias('TIEMPO'),
                df_parciales.ENS.alias('ENS'))\
        .groupby('FallaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId',
                 'EmpresaId','EmpresaCodigo','FechaConecta')\
        .agg(func.sum('CARGA').alias('CARGA'),
             func.sum('TIEMPO').alias('TIEMPO'),
             func.sum('ENS').alias('ENS'))
        
        return df_datos
    
    
    
    
    
    @staticmethod      
    def ObtenerFallaDetalleSinAsignar(df_fallas,df_fallas_dtl):
        """Solo fallas que noo han podido ser identificadas su origen."""
        df_falla_detalle = df_fallas\
        .join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                df_fallas_dtl.EMPRESA_ID.alias('EmpresaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsManual'),
                df_fallas_dtl.FALLA_DTL_ID,
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'))\
        .groupby('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId')\
        .agg(func.min('FALLA_DTL_ID').alias('FALLA_DTL_ID'))\
        .select('FallaId','EmpresaId','FechaFalla','Numero','Paso','EnsManual','OrigenId','EmpresaOrigenId').distinct()
        
        return df_falla_detalle
        
        
    @staticmethod
    def JoinInternaAfectacion(df_ens_interna,df_ens_afectacion_snt):
        df_total = df_ens_interna\
        .join(df_ens_afectacion_snt,
              (df_ens_interna.FallaId == df_ens_afectacion_snt.FallaId) &\
              (df_ens_interna.EmpresaId == df_ens_afectacion_snt.EmpresaId), how='full')\
        .select(when(df_ens_interna.FallaId.isNull(),df_ens_afectacion_snt.FallaId)\
                .otherwise(df_ens_interna.FallaId).alias('FallaId'),
                when(df_ens_interna.FechaFalla.isNull(),df_ens_afectacion_snt.FechaFalla)\
                .otherwise(df_ens_interna.FechaFalla).alias('FechaFalla'),
                when(df_ens_interna.Numero.isNull(),df_ens_afectacion_snt.Numero)\
                .otherwise(df_ens_interna.Numero).alias('Numero'),
                when(df_ens_interna.Paso.isNull(),df_ens_afectacion_snt.Paso)\
                .otherwise(df_ens_interna.Paso).alias('Paso'),
                when(df_ens_interna.EnsManual.isNull(),df_ens_afectacion_snt.EnsManual)\
                .otherwise(df_ens_interna.EnsManual).alias('EnsManual'),
                when(df_ens_interna.OrigenId.isNull(),df_ens_afectacion_snt.OrigenId)\
                .otherwise(df_ens_interna.OrigenId).alias('OrigenId'),
                when(df_ens_interna.EmpresaOrigenId.isNull(),df_ens_afectacion_snt.EmpresaOrigenId)\
                .otherwise(df_ens_interna.EmpresaOrigenId).alias('EmpresaOrigenId'),
                when(df_ens_interna.EmpresaId.isNull(),df_ens_afectacion_snt.EmpresaId)\
                .otherwise(df_ens_interna.EmpresaId).alias('EmpresaId'),
                when(df_ens_interna.EmpresaCodigo.isNull(),df_ens_afectacion_snt.EmpresaCodigo)\
                .otherwise(df_ens_interna.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_ens_interna.FechaConecta.isNull(),df_ens_afectacion_snt.FechaConecta)\
                .otherwise(df_ens_interna.FechaConecta).alias('FechaConecta'),
                when(df_ens_interna.CARGA.isNull(),0).otherwise(df_ens_interna.CARGA).alias('CargaInterna'),
                when(df_ens_interna.TIEMPO.isNull(),0).otherwise(df_ens_interna.TIEMPO).alias('TiempoInterna'),
                when(df_ens_interna.ENS.isNull(),0).otherwise(df_ens_interna.ENS).alias('EnsInterna'),
                when(df_ens_afectacion_snt.CARGA.isNull(),0).otherwise(df_ens_afectacion_snt.CARGA).alias('CargaAfectacionSnt'),
                when(df_ens_afectacion_snt.TIEMPO.isNull(),0).otherwise(df_ens_afectacion_snt.TIEMPO).alias('TiempoAfectacionSnt'),
                when(df_ens_afectacion_snt.ENS.isNull(),0).otherwise(df_ens_afectacion_snt.ENS).alias('EnsAfectacionSnt'))
        return df_total
            
    @staticmethod
    def JoinTotalExternaTrans(df_total,df_ens_externa_trans):
        df_total = df_total\
        .join(df_ens_externa_trans,
              (df_total.FallaId == df_ens_externa_trans.FallaId) &\
              (df_total.EmpresaId == df_ens_externa_trans.EmpresaId), how='full')\
        .select(when(df_total.FallaId.isNull(),df_ens_externa_trans.FallaId)\
                .otherwise(df_total.FallaId).alias('FallaId'),
                when(df_total.FechaFalla.isNull(),df_ens_externa_trans.FechaFalla)\
                .otherwise(df_total.FechaFalla).alias('FechaFalla'),
                when(df_total.Numero.isNull(),df_ens_externa_trans.Numero)\
                .otherwise(df_total.Numero).alias('Numero'),
                when(df_total.Paso.isNull(),df_ens_externa_trans.Paso)\
                .otherwise(df_total.Paso).alias('Paso'),
                when(df_total.EnsManual.isNull(),df_ens_externa_trans.EnsManual)\
                .otherwise(df_total.EnsManual).alias('EnsManual'),
                when(df_total.OrigenId.isNull(),df_ens_externa_trans.OrigenId)\
                .otherwise(df_total.OrigenId).alias('OrigenId'),
                when(df_total.EmpresaOrigenId.isNull(),df_ens_externa_trans.EmpresaOrigenId)\
                .otherwise(df_total.EmpresaOrigenId).alias('EmpresaOrigenId'),
                when(df_total.EmpresaId.isNull(),df_ens_externa_trans.EmpresaId)\
                .otherwise(df_total.EmpresaId).alias('EmpresaId'),
                when(df_total.EmpresaCodigo.isNull(),df_ens_externa_trans.EmpresaCodigo)\
                .otherwise(df_total.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_total.FechaConecta.isNull(),df_ens_externa_trans.FechaConecta)\
                .otherwise(df_total.FechaConecta).alias('FechaConecta'),
                when(df_total.CargaInterna.isNull(),0).otherwise(df_total.CargaInterna).alias('CargaInterna'),
                when(df_total.TiempoInterna.isNull(),0).otherwise(df_total.TiempoInterna).alias('TiempoInterna'),
                when(df_total.EnsInterna.isNull(),0).otherwise(df_total.EnsInterna).alias('EnsInterna'),
                when(df_total.CargaAfectacionSnt.isNull(),0).otherwise(df_total.CargaAfectacionSnt).alias('CargaAfectacionSnt'),
                when(df_total.TiempoAfectacionSnt.isNull(),0).otherwise(df_total.TiempoAfectacionSnt).alias('TiempoAfectacionSnt'),
                when(df_total.EnsAfectacionSnt.isNull(),0).otherwise(df_total.EnsAfectacionSnt).alias('EnsAfectacionSnt'),
                when(df_ens_externa_trans.CARGA.isNull(),0).otherwise(df_ens_externa_trans.CARGA).alias('CargaExternaTrans'),
                when(df_ens_externa_trans.TIEMPO.isNull(),0).otherwise(df_ens_externa_trans.TIEMPO).alias('TiempoExternaTrans'),
                when(df_ens_externa_trans.ENS.isNull(),0).otherwise(df_ens_externa_trans.ENS).alias('EnsExternaTrans'))
        return df_total
        
        
    @staticmethod
    def JoinTotalExternaGen(df_total,df_ens_externa_gen):
        df_total = df_total\
        .join(df_ens_externa_gen,
              (df_total.FallaId == df_ens_externa_gen.FallaId) &\
              (df_total.EmpresaId == df_ens_externa_gen.EmpresaId), how='full')\
        .select(when(df_total.FallaId.isNull(),df_ens_externa_gen.FallaId)\
                .otherwise(df_total.FallaId).alias('FallaId'),
                when(df_total.FechaFalla.isNull(),df_ens_externa_gen.FechaFalla)\
                .otherwise(df_total.FechaFalla).alias('FechaFalla'),
                when(df_total.Numero.isNull(),df_ens_externa_gen.Numero)\
                .otherwise(df_total.Numero).alias('Numero'),
                when(df_total.Paso.isNull(),df_ens_externa_gen.Paso)\
                .otherwise(df_total.Paso).alias('Paso'),
                when(df_total.EnsManual.isNull(),df_ens_externa_gen.EnsManual)\
                .otherwise(df_total.EnsManual).alias('EnsManual'),
                when(df_total.OrigenId.isNull(),df_ens_externa_gen.OrigenId)\
                .otherwise(df_total.OrigenId).alias('OrigenId'),
                when(df_total.EmpresaOrigenId.isNull(),df_ens_externa_gen.EmpresaOrigenId)\
                .otherwise(df_total.EmpresaOrigenId).alias('EmpresaOrigenId'),
                when(df_total.EmpresaId.isNull(),df_ens_externa_gen.EmpresaId)\
                .otherwise(df_total.EmpresaId).alias('EmpresaId'),
                when(df_total.EmpresaCodigo.isNull(),df_ens_externa_gen.EmpresaCodigo)\
                .otherwise(df_total.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_total.FechaConecta.isNull(),df_ens_externa_gen.FechaConecta)\
                .otherwise(df_total.FechaConecta).alias('FechaConecta'),
                when(df_total.CargaInterna.isNull(),0).otherwise(df_total.CargaInterna).alias('CargaInterna'),
                when(df_total.TiempoInterna.isNull(),0).otherwise(df_total.TiempoInterna).alias('TiempoInterna'),
                when(df_total.EnsInterna.isNull(),0).otherwise(df_total.EnsInterna).alias('EnsInterna'),
                when(df_total.CargaAfectacionSnt.isNull(),0).otherwise(df_total.CargaAfectacionSnt).alias('CargaAfectacionSnt'),
                when(df_total.TiempoAfectacionSnt.isNull(),0).otherwise(df_total.TiempoAfectacionSnt).alias('TiempoAfectacionSnt'),
                when(df_total.EnsAfectacionSnt.isNull(),0).otherwise(df_total.EnsAfectacionSnt).alias('EnsAfectacionSnt'),
                when(df_total.CargaExternaTrans.isNull(),0).otherwise(df_total.CargaExternaTrans).alias('CargaExternaTrans'),
                when(df_total.TiempoExternaTrans.isNull(),0).otherwise(df_total.TiempoExternaTrans).alias('TiempoExternaTrans'),
                when(df_total.EnsExternaTrans.isNull(),0).otherwise(df_total.EnsExternaTrans).alias('EnsExternaTrans'),
                when(df_ens_externa_gen.CARGA.isNull(),0).otherwise(df_ens_externa_gen.CARGA).alias('CargaExternaGen'),
                when(df_ens_externa_gen.TIEMPO.isNull(),0).otherwise(df_ens_externa_gen.TIEMPO).alias('TiempoExternaGen'),
                when(df_ens_externa_gen.ENS.isNull(),0).otherwise(df_ens_externa_gen.ENS).alias('EnsExternaGen'))
        return df_total
            
            
    @staticmethod
    def JoinTotalSistemico(df_total,df_ens_sistemico):
        df_total = df_total\
        .join(df_ens_sistemico,
              (df_total.FallaId == df_ens_sistemico.FallaId) &\
              (df_total.EmpresaId == df_ens_sistemico.EmpresaId), how='full')\
        .select(when(df_total.FallaId.isNull(),df_ens_sistemico.FallaId)\
                .otherwise(df_total.FallaId).alias('FallaId'),
                when(df_total.FechaFalla.isNull(),df_ens_sistemico.FechaFalla)\
                .otherwise(df_total.FechaFalla).alias('FechaFalla'),
                when(df_total.Numero.isNull(),df_ens_sistemico.Numero)\
                .otherwise(df_total.Numero).alias('Numero'),
                when(df_total.Paso.isNull(),df_ens_sistemico.Paso)\
                .otherwise(df_total.Paso).alias('Paso'),
                when(df_total.EnsManual.isNull(),df_ens_sistemico.EnsManual)\
                .otherwise(df_total.EnsManual).alias('EnsManual'),
                when(df_total.OrigenId.isNull(),df_ens_sistemico.OrigenId)\
                .otherwise(df_total.OrigenId).alias('OrigenId'),
                when(df_total.EmpresaOrigenId.isNull(),df_ens_sistemico.EmpresaOrigenId)\
                .otherwise(df_total.EmpresaOrigenId).alias('EmpresaOrigenId'),
                when(df_total.EmpresaId.isNull(),df_ens_sistemico.EmpresaId)\
                .otherwise(df_total.EmpresaId).alias('EmpresaId'),
                when(df_total.EmpresaCodigo.isNull(),df_ens_sistemico.EmpresaCodigo)\
                .otherwise(df_total.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_total.FechaConecta.isNull(),df_ens_sistemico.FechaConecta)\
                .otherwise(df_total.FechaConecta).alias('FechaConecta'),
                when(df_total.CargaInterna.isNull(),0).otherwise(df_total.CargaInterna).alias('CargaInterna'),
                when(df_total.TiempoInterna.isNull(),0).otherwise(df_total.TiempoInterna).alias('TiempoInterna'),
                when(df_total.EnsInterna.isNull(),0).otherwise(df_total.EnsInterna).alias('EnsInterna'),
                when(df_total.CargaAfectacionSnt.isNull(),0).otherwise(df_total.CargaAfectacionSnt).alias('CargaAfectacionSnt'),
                when(df_total.TiempoAfectacionSnt.isNull(),0).otherwise(df_total.TiempoAfectacionSnt).alias('TiempoAfectacionSnt'),
                when(df_total.EnsAfectacionSnt.isNull(),0).otherwise(df_total.EnsAfectacionSnt).alias('EnsAfectacionSnt'),
                when(df_total.CargaExternaTrans.isNull(),0).otherwise(df_total.CargaExternaTrans).alias('CargaExternaTrans'),
                when(df_total.TiempoExternaTrans.isNull(),0).otherwise(df_total.TiempoExternaTrans).alias('TiempoExternaTrans'),
                when(df_total.EnsExternaTrans.isNull(),0).otherwise(df_total.EnsExternaTrans).alias('EnsExternaTrans'),
                when(df_total.CargaExternaGen.isNull(),0).otherwise(df_total.CargaExternaGen).alias('CargaExternaGen'),
                when(df_total.TiempoExternaGen.isNull(),0).otherwise(df_total.TiempoExternaGen).alias('TiempoExternaGen'),
                when(df_total.EnsExternaGen.isNull(),0).otherwise(df_total.EnsExternaGen).alias('EnsExternaGen'),
                when(df_ens_sistemico.CARGA.isNull(),0).otherwise(df_ens_sistemico.CARGA).alias('CargaSistemico'),
                when(df_ens_sistemico.TIEMPO.isNull(),0).otherwise(df_ens_sistemico.TIEMPO).alias('TiempoSistemico'),
                when(df_ens_sistemico.ENS.isNull(),0).otherwise(df_ens_sistemico.ENS).alias('EnsSistemico'))
        return df_total
            
    @staticmethod
    def JoinTotalOtros(df_total,df_ens_otros):
        df_total = df_total\
        .join(df_ens_otros,
              (df_total.FallaId == df_ens_otros.FallaId) &\
              (df_total.EmpresaId == df_ens_otros.EmpresaId), how='full')\
        .select(when(df_total.FallaId.isNull(),df_ens_otros.FallaId)\
                .otherwise(df_total.FallaId).alias('FallaId'),
                when(df_total.FechaFalla.isNull(),df_ens_otros.FechaFalla)\
                .otherwise(df_total.FechaFalla).alias('FechaFalla'),
                when(df_total.Numero.isNull(),df_ens_otros.Numero)\
                .otherwise(df_total.Numero).alias('Numero'),
                when(df_total.Paso.isNull(),df_ens_otros.Paso)\
                .otherwise(df_total.Paso).alias('Paso'),
                when(df_total.EnsManual.isNull(),df_ens_otros.EnsManual)\
                .otherwise(df_total.EnsManual).alias('EnsManual'),
                when(df_total.OrigenId.isNull(),df_ens_otros.OrigenId)\
                .otherwise(df_total.OrigenId).alias('OrigenId'),
                when(df_total.EmpresaOrigenId.isNull(),df_ens_otros.EmpresaOrigenId)\
                .otherwise(df_total.EmpresaOrigenId).alias('EmpresaOrigenId'),
                when(df_total.EmpresaId.isNull(),df_ens_otros.EmpresaId)\
                .otherwise(df_total.EmpresaId).alias('EmpresaId'),
                when(df_total.EmpresaCodigo.isNull(),df_ens_otros.EmpresaCodigo)\
                .otherwise(df_total.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_total.FechaConecta.isNull(),df_ens_otros.FechaConecta)\
                .otherwise(df_total.FechaConecta).alias('FechaConecta'),
                when(df_total.CargaInterna.isNull(),0).otherwise(df_total.CargaInterna).alias('CargaInterna'),
                when(df_total.TiempoInterna.isNull(),0).otherwise(df_total.TiempoInterna).alias('TiempoInterna'),
                when(df_total.EnsInterna.isNull(),0).otherwise(df_total.EnsInterna).alias('EnsInterna'),
                when(df_total.CargaAfectacionSnt.isNull(),0).otherwise(df_total.CargaAfectacionSnt).alias('CargaAfectacionSnt'),
                when(df_total.TiempoAfectacionSnt.isNull(),0).otherwise(df_total.TiempoAfectacionSnt).alias('TiempoAfectacionSnt'),
                when(df_total.EnsAfectacionSnt.isNull(),0).otherwise(df_total.EnsAfectacionSnt).alias('EnsAfectacionSnt'),
                when(df_total.CargaExternaTrans.isNull(),0).otherwise(df_total.CargaExternaTrans).alias('CargaExternaTrans'),
                when(df_total.TiempoExternaTrans.isNull(),0).otherwise(df_total.TiempoExternaTrans).alias('TiempoExternaTrans'),
                when(df_total.EnsExternaTrans.isNull(),0).otherwise(df_total.EnsExternaTrans).alias('EnsExternaTrans'),
                when(df_total.CargaExternaGen.isNull(),0).otherwise(df_total.CargaExternaGen).alias('CargaExternaGen'),
                when(df_total.TiempoExternaGen.isNull(),0).otherwise(df_total.TiempoExternaGen).alias('TiempoExternaGen'),
                when(df_total.EnsExternaGen.isNull(),0).otherwise(df_total.EnsExternaGen).alias('EnsExternaGen'),
                when(df_total.CargaSistemico.isNull(),0).otherwise(df_total.CargaSistemico).alias('CargaSistemico'),
                when(df_total.TiempoSistemico.isNull(),0).otherwise(df_total.TiempoSistemico).alias('TiempoSistemico'),
                when(df_total.EnsSistemico.isNull(),0).otherwise(df_total.EnsSistemico).alias('EnsSistemico'),
                when(df_ens_otros.CARGA.isNull(),0).otherwise(df_ens_otros.CARGA).alias('CargaOtros'),
                when(df_ens_otros.TIEMPO.isNull(),0).otherwise(df_ens_otros.TIEMPO).alias('TiempoOtros'),
                when(df_ens_otros.ENS.isNull(),0).otherwise(df_ens_otros.ENS).alias('EnsOtros'))
        return df_total
          
    @staticmethod
    def JoinTotalEac(df_total,df_ens_eac):
        df_total = df_total\
        .join(df_ens_eac,
              (df_total.FallaId == df_ens_eac.FallaId) &\
              (df_total.EmpresaId == df_ens_eac.EmpresaId), how='full')\
        .select(when(df_total.FallaId.isNull(),df_ens_eac.FallaId)\
                .otherwise(df_total.FallaId).alias('FallaId'),
                when(df_total.FechaFalla.isNull(),df_ens_eac.FechaFalla)\
                .otherwise(df_total.FechaFalla).alias('FechaFalla'),
                when(df_total.Numero.isNull(),df_ens_eac.Numero)\
                .otherwise(df_total.Numero).alias('Numero'),
                when(df_total.Paso.isNull(),df_ens_eac.Paso)\
                .otherwise(df_total.Paso).alias('Paso'),
                when(df_total.EnsManual.isNull(),df_ens_eac.EnsManual)\
                .otherwise(df_total.EnsManual).alias('EnsManual'),
                when(df_total.OrigenId.isNull(),df_ens_eac.OrigenId)\
                .otherwise(df_total.OrigenId).alias('OrigenId'),
                when(df_total.EmpresaOrigenId.isNull(),df_ens_eac.EmpresaOrigenId)\
                .otherwise(df_total.EmpresaOrigenId).alias('EmpresaOrigenId'),
                when(df_total.EmpresaId.isNull(),df_ens_eac.EmpresaId)\
                .otherwise(df_total.EmpresaId).alias('EmpresaId'),
                when(df_total.EmpresaCodigo.isNull(),df_ens_eac.EmpresaCodigo)\
                .otherwise(df_total.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_total.FechaConecta.isNull(),df_ens_eac.FechaConecta)\
                .otherwise(df_total.FechaConecta).alias('FechaConecta'),
                when(df_total.CargaInterna.isNull(),0).otherwise(df_total.CargaInterna).alias('CargaInterna'),
                when(df_total.TiempoInterna.isNull(),0).otherwise(df_total.TiempoInterna).alias('TiempoInterna'),
                when(df_total.EnsInterna.isNull(),0).otherwise(df_total.EnsInterna).alias('EnsInterna'),
                when(df_total.CargaAfectacionSnt.isNull(),0).otherwise(df_total.CargaAfectacionSnt).alias('CargaAfectacionSnt'),
                when(df_total.TiempoAfectacionSnt.isNull(),0).otherwise(df_total.TiempoAfectacionSnt).alias('TiempoAfectacionSnt'),
                when(df_total.EnsAfectacionSnt.isNull(),0).otherwise(df_total.EnsAfectacionSnt).alias('EnsAfectacionSnt'),
                when(df_total.CargaExternaTrans.isNull(),0).otherwise(df_total.CargaExternaTrans).alias('CargaExternaTrans'),
                when(df_total.TiempoExternaTrans.isNull(),0).otherwise(df_total.TiempoExternaTrans).alias('TiempoExternaTrans'),
                when(df_total.EnsExternaTrans.isNull(),0).otherwise(df_total.EnsExternaTrans).alias('EnsExternaTrans'),
                when(df_total.CargaExternaGen.isNull(),0).otherwise(df_total.CargaExternaGen).alias('CargaExternaGen'),
                when(df_total.TiempoExternaGen.isNull(),0).otherwise(df_total.TiempoExternaGen).alias('TiempoExternaGen'),
                when(df_total.EnsExternaGen.isNull(),0).otherwise(df_total.EnsExternaGen).alias('EnsExternaGen'),
                when(df_total.CargaSistemico.isNull(),0).otherwise(df_total.CargaSistemico).alias('CargaSistemico'),
                when(df_total.TiempoSistemico.isNull(),0).otherwise(df_total.TiempoSistemico).alias('TiempoSistemico'),
                when(df_total.EnsSistemico.isNull(),0).otherwise(df_total.EnsSistemico).alias('EnsSistemico'),
                when(df_total.CargaOtros.isNull(),0).otherwise(df_total.CargaOtros).alias('CargaOtros'),
                when(df_total.TiempoOtros.isNull(),0).otherwise(df_total.TiempoOtros).alias('TiempoOtros'),
                when(df_total.EnsOtros.isNull(),0).otherwise(df_total.EnsOtros).alias('EnsOtros'),
                when(df_ens_eac.CARGA.isNull(),0).otherwise(df_ens_eac.CARGA).alias('CargaEac'),
                when(df_ens_eac.TIEMPO.isNull(),0).otherwise(df_ens_eac.TIEMPO).alias('TiempoEac'),
                when(df_ens_eac.ENS.isNull(),0).otherwise(df_ens_eac.ENS).alias('EnsEac'))
        return df_total
    
    
    @staticmethod
    def JoinTotalSinAsignar(df_total,df_ens_sin):
        df_total = df_total\
        .join(df_ens_sin,
              (df_total.FallaId == df_ens_sin.FallaId) &\
              (df_total.EmpresaId == df_ens_sin.EmpresaId), how='full')\
        .select(when(df_total.FallaId.isNull(),df_ens_sin.FallaId)\
                .otherwise(df_total.FallaId).alias('FallaId'),
                when(df_total.FechaFalla.isNull(),df_ens_sin.FechaFalla)\
                .otherwise(df_total.FechaFalla).alias('FechaFalla'),
                when(df_total.Numero.isNull(),df_ens_sin.Numero)\
                .otherwise(df_total.Numero).alias('Numero'),
                when(df_total.Paso.isNull(),df_ens_sin.Paso)\
                .otherwise(df_total.Paso).alias('Paso'),
                when(df_total.EnsManual.isNull(),df_ens_sin.EnsManual)\
                .otherwise(df_total.EnsManual).alias('EnsManual'),
                when(df_total.OrigenId.isNull(),df_ens_sin.OrigenId)\
                .otherwise(df_total.OrigenId).alias('OrigenId'),
                when(df_total.EmpresaOrigenId.isNull(),df_ens_sin.EmpresaOrigenId)\
                .otherwise(df_total.EmpresaOrigenId).alias('EmpresaOrigenId'),
                when(df_total.EmpresaId.isNull(),df_ens_sin.EmpresaId)\
                .otherwise(df_total.EmpresaId).alias('EmpresaId'),
                when(df_total.EmpresaCodigo.isNull(),df_ens_sin.EmpresaCodigo)\
                .otherwise(df_total.EmpresaCodigo).alias('EmpresaCodigo'),
                when(df_total.FechaConecta.isNull(),df_ens_sin.FechaConecta)\
                .otherwise(df_total.FechaConecta).alias('FechaConecta'),
                when(df_total.CargaInterna.isNull(),0).otherwise(df_total.CargaInterna).alias('CargaInterna'),
                when(df_total.TiempoInterna.isNull(),0).otherwise(df_total.TiempoInterna).alias('TiempoInterna'),
                when(df_total.EnsInterna.isNull(),0).otherwise(df_total.EnsInterna).alias('EnsInterna'),
                when(df_total.CargaAfectacionSnt.isNull(),0).otherwise(df_total.CargaAfectacionSnt).alias('CargaAfectacionSnt'),
                when(df_total.TiempoAfectacionSnt.isNull(),0).otherwise(df_total.TiempoAfectacionSnt).alias('TiempoAfectacionSnt'),
                when(df_total.EnsAfectacionSnt.isNull(),0).otherwise(df_total.EnsAfectacionSnt).alias('EnsAfectacionSnt'),
                when(df_total.CargaExternaTrans.isNull(),0).otherwise(df_total.CargaExternaTrans).alias('CargaExternaTrans'),
                when(df_total.TiempoExternaTrans.isNull(),0).otherwise(df_total.TiempoExternaTrans).alias('TiempoExternaTrans'),
                when(df_total.EnsExternaTrans.isNull(),0).otherwise(df_total.EnsExternaTrans).alias('EnsExternaTrans'),
                when(df_total.CargaExternaGen.isNull(),0).otherwise(df_total.CargaExternaGen).alias('CargaExternaGen'),
                when(df_total.TiempoExternaGen.isNull(),0).otherwise(df_total.TiempoExternaGen).alias('TiempoExternaGen'),
                when(df_total.EnsExternaGen.isNull(),0).otherwise(df_total.EnsExternaGen).alias('EnsExternaGen'),
                when(df_total.CargaSistemico.isNull(),0).otherwise(df_total.CargaSistemico).alias('CargaSistemico'),
                when(df_total.TiempoSistemico.isNull(),0).otherwise(df_total.TiempoSistemico).alias('TiempoSistemico'),
                when(df_total.EnsSistemico.isNull(),0).otherwise(df_total.EnsSistemico).alias('EnsSistemico'),
                when(df_total.CargaOtros.isNull(),0).otherwise(df_total.CargaOtros).alias('CargaOtros'),
                when(df_total.TiempoOtros.isNull(),0).otherwise(df_total.TiempoOtros).alias('TiempoOtros'),
                when(df_total.EnsOtros.isNull(),0).otherwise(df_total.EnsOtros).alias('EnsOtros'),
                when(df_total.CargaEac.isNull(),0).otherwise(df_total.CargaEac).alias('CargaEac'),
                when(df_total.TiempoEac.isNull(),0).otherwise(df_total.TiempoEac).alias('TiempoEac'),
                when(df_total.EnsEac.isNull(),0).otherwise(df_total.EnsEac).alias('EnsEac'),
                
                when(df_ens_sin.CARGA.isNull(),0).otherwise(df_ens_sin.CARGA).alias('CargaSin'),
                when(df_ens_sin.TIEMPO.isNull(),0).otherwise(df_ens_sin.TIEMPO).alias('TiempoSin'),
                when(df_ens_sin.ENS.isNull(),0).otherwise(df_ens_sin.ENS).alias('EnsSin'))
        return df_total
    

    @staticmethod        
    def AgregarAgtDistribucion(datos_totales,unidad_negocio):
        datos_totales = datos_totales\
        .join(unidad_negocio, datos_totales.EmpresaId == unidad_negocio.UNegocioId, how='left')\
        .select(datos_totales.FallaId,
                datos_totales.FechaFalla,
                datos_totales.Numero,
                datos_totales.Paso,
                datos_totales.EnsManual,
                datos_totales.OrigenId,
                datos_totales.EmpresaOrigenId,
                unidad_negocio.EmpresaCodigo.alias('EmpresaCodigo'),
                unidad_negocio.UNegocioCodigo.alias('UNegocioCodigo'),
                datos_totales.FechaConecta,
                datos_totales.CargaInterna,
                datos_totales.TiempoInterna,
                datos_totales.EnsInterna,
                datos_totales.CargaAfectacionSnt,
                datos_totales.TiempoAfectacionSnt,
                datos_totales.EnsAfectacionSnt,
                datos_totales.CargaExternaTrans,
                datos_totales.TiempoExternaTrans,
                datos_totales.EnsExternaTrans,
                datos_totales.CargaExternaGen,
                datos_totales.TiempoExternaGen,
                datos_totales.EnsExternaGen,
                datos_totales.CargaSistemico,
                datos_totales.TiempoSistemico,
                datos_totales.EnsSistemico,
                datos_totales.CargaOtros,
                datos_totales.TiempoOtros,
                datos_totales.EnsOtros,
                datos_totales.CargaEac,
                datos_totales.TiempoEac,
                datos_totales.EnsEac,
                datos_totales.CargaSin,
                datos_totales.TiempoSin,
                datos_totales.EnsSin)
        return datos_totales
            
    @staticmethod        
    def AgregarAgtOrigen(datos_totales,unidad_negocio,df_origen):
        datos_totales = datos_totales\
        .join(df_origen, datos_totales.OrigenId == df_origen.TPF_ORIGEN_ID, how='left')\
        .join(unidad_negocio, datos_totales.EmpresaOrigenId == unidad_negocio.UNegocioId, how='left')\
        .select(datos_totales.FallaId,
                datos_totales.FechaFalla,
                datos_totales.Numero,
                datos_totales.Paso,
                datos_totales.EnsManual,
                datos_totales.EmpresaCodigo,
                datos_totales.UNegocioCodigo,
                datos_totales.FechaConecta,
                datos_totales.CargaInterna,
                datos_totales.TiempoInterna,
                datos_totales.EnsInterna,
                datos_totales.CargaAfectacionSnt,
                datos_totales.TiempoAfectacionSnt,
                datos_totales.EnsAfectacionSnt,
                datos_totales.CargaExternaTrans,
                datos_totales.TiempoExternaTrans,
                datos_totales.EnsExternaTrans,
                datos_totales.CargaExternaGen,
                datos_totales.TiempoExternaGen,
                datos_totales.EnsExternaGen,
                datos_totales.CargaSistemico,
                datos_totales.TiempoSistemico,
                datos_totales.EnsSistemico,
                datos_totales.CargaOtros,
                datos_totales.TiempoOtros,
                datos_totales.EnsOtros,
                datos_totales.CargaEac,
                datos_totales.TiempoEac,
                datos_totales.EnsEac,
                datos_totales.CargaSin,
                datos_totales.TiempoSin,
                datos_totales.EnsSin,
                when(df_origen.TPF_ORIGEN_CODIGO.isNull(),'NA').otherwise(df_origen.TPF_ORIGEN_CODIGO).alias('OrigenCodigo'),
                when(unidad_negocio.EmpresaCodigo.isNull(),'NA').otherwise(unidad_negocio.EmpresaCodigo).alias('EmpresaCodigoOrg'),
                when(unidad_negocio.UNegocioCodigo.isNull(),'NA').otherwise(unidad_negocio.UNegocioCodigo).alias('UNegocioCodigoOrg'))
        
        return datos_totales
    
    @staticmethod
    def LimpiarFallasDist(datos,agentes_dist,agentes_origen,origen,pasos):        
        #Proceso de limpieza
        df_fallas = datos\
        .join(agentes_dist, 
              (datos.EmpresaCodigo == agentes_dist.agt_empresa_id_bk) & \
              (datos.UNegocioCodigo == agentes_dist.agt_und_negocio_id_bk))\
        .join(agentes_origen, 
              (datos.EmpresaCodigoOrg == agentes_origen.agtorg_empresa_id_bk) & \
              (datos.UNegocioCodigoOrg == agentes_origen.agtorg_und_negocio_id_bk), how='left')\
        .join(origen, 
              (datos.OrigenCodigo == origen.asigorg_origen_id_bk), how='left')\
        .join(pasos, 
              (when(datos.Paso.isNull(),0).otherwise(datos.Paso) == pasos.pasos_numero), how='left')\
        .select(agentes_dist.agt_id_pk.alias('agt_id_fk'),
                regexp_replace(substring(datos.FechaFalla.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                trim(regexp_replace(substring(datos.FechaFalla.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_id_fk'),
                agentes_origen.agtorg_id_pk.alias('agtorg_id_fk'),
                origen.asigorg_id_pk.alias('asigorg_id_fk'),
                pasos.pasos_id_pk.alias('pasos_id_fk'),
                when(datos.Numero.isNull(),'-').otherwise(datos.Numero).alias('falla_numero'),
                datos.CargaInterna.alias('ensd_interna_crg').cast('float'),
                datos.TiempoInterna.alias('ensd_interna_tmp').cast('float'),
                datos.EnsInterna.alias('ensd_interna').cast('float'),
                datos.CargaAfectacionSnt.alias('ensd_afect_snt_crg').cast('float'),
                datos.TiempoAfectacionSnt.alias('ensd_afect_snt_tmp').cast('float'),
                datos.EnsAfectacionSnt.alias('ensd_afect_snt').cast('float'),
                datos.CargaExternaTrans.alias('ensd_ext_trans_crg').cast('float'),
                datos.TiempoExternaTrans.alias('ensd_ext_trans_tmp').cast('float'),
                datos.EnsExternaTrans.alias('ensd_ext_trans').cast('float'),
                datos.CargaExternaGen.alias('ensd_ext_gen_crg').cast('float'),
                datos.TiempoExternaGen.alias('ensd_ext_gen_tmp').cast('float'),
                datos.EnsExternaGen.alias('ensd_ext_gen').cast('float'),
                datos.CargaSistemico.alias('ensd_sistemico_sps_crg').cast('float'),
                datos.TiempoSistemico.alias('ensd_sistemico_sps_tmp').cast('float'),
                datos.EnsSistemico.alias('ensd_sistemico_sps').cast('float'),
                datos.CargaOtros.alias('ensd_otros_crg').cast('float'),
                datos.TiempoOtros.alias('ensd_otros_tmp').cast('float'),
                datos.EnsOtros.alias('ensd_otros').cast('float'),
                datos.CargaEac.alias('ensd_eac_crg').cast('float'),
                datos.TiempoEac.alias('ensd_eac_tmp').cast('float'),
                datos.EnsEac.alias('ensd_eac').cast('float'),
                datos.TiempoSin.alias('ensd_no_definido_tmp').cast('float'),
                datos.CargaSin.alias('ensd_no_definido_crg').cast('float'),
                datos.EnsSin.alias('ensd_no_definido').cast('float'),
                datos.EnsManual.alias('ensd_manual').cast('float')).distinct()

        df_fallas = df_fallas\
        .groupby('agt_id_fk','tmpo_id_fk','hora_id_fk','agtorg_id_fk','asigorg_id_fk','pasos_id_fk','falla_numero')\
        .agg(func.sum('ensd_interna_crg').alias('ensd_interna_crg'),func.sum('ensd_interna_tmp').alias('ensd_interna_tmp'),
             func.sum('ensd_interna').alias('ensd_interna'),
             func.sum('ensd_afect_snt_crg').alias('ensd_afect_snt_crg'),func.sum('ensd_afect_snt_tmp').alias('ensd_afect_snt_tmp'),
             func.sum('ensd_afect_snt').alias('ensd_afect_snt'),
             func.sum('ensd_ext_trans_crg').alias('ensd_ext_trans_crg'),func.sum('ensd_ext_trans_tmp').alias('ensd_ext_trans_tmp'),
             func.sum('ensd_ext_trans').alias('ensd_ext_trans'),
             func.sum('ensd_ext_gen_crg').alias('ensd_ext_gen_crg'),func.sum('ensd_ext_gen_tmp').alias('ensd_ext_gen_tmp'),
             func.sum('ensd_ext_gen').alias('ensd_ext_gen'),
             func.sum('ensd_sistemico_sps_crg').alias('ensd_sistemico_sps_crg'),
             func.sum('ensd_sistemico_sps_tmp').alias('ensd_sistemico_sps_tmp'),
             func.sum('ensd_sistemico_sps').alias('ensd_sistemico_sps'),
             func.sum('ensd_otros_crg').alias('ensd_otros_crg'),func.sum('ensd_otros_tmp').alias('ensd_otros_tmp'),
             func.sum('ensd_otros').alias('ensd_otros'),
             func.sum('ensd_eac_crg').alias('ensd_eac_crg'),func.sum('ensd_eac_tmp').alias('ensd_eac_tmp'),
             func.sum('ensd_eac').alias('ensd_eac'),
             func.sum('ensd_no_definido_tmp').alias('ensd_no_definido_tmp'),func.sum('ensd_no_definido_crg').alias('ensd_no_definido_crg'),
             func.sum('ensd_no_definido').alias('ensd_no_definido'),func.sum('ensd_manual').alias('ensd_manual')).distinct()
        return df_fallas
    
            