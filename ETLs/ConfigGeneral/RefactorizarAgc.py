from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim, lpad, concat, unix_timestamp, round
import datetime
from datetime import timedelta
from pyspark.sql import Window

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
    def DafaFrameEstacion(df_central,df_cat_central,df_linea,df_cat_linea,df_subestacion,df_cat_subestacion):
        df_central = df_central.select(col('ESTACION_ID').alias('EstacionId'),
                                       
                                       when(col('ESTACION_CODIGO')=='BABAA','BABAH')\
                                       .otherwise(when(col('ESTACION_CODIGO')=='ASANA','ASANT')\
                                       .otherwise(when(col('ESTACION_CODIGO')=='ATINA','ATINT')\
                                       .otherwise(when(col('ESTACION_CODIGO')=='EDOSA','EDOST')\
                                       .otherwise(when(col('ESTACION_CODIGO')=='LCEMA','LCEMT')\
                                       .otherwise(when(col('ESTACION_CODIGO')=='ASAFA','ASAFT')\
                                       .otherwise(when(col('ESTACION_CODIGO')=='MANDA','MANDH')\
                                       .otherwise(col('ESTACION_CODIGO')))))))).alias('EstacionCodigo'),
                                       
                                       col('ESTACION_NOMBRE').alias('Estacion'),
                                       col('ESTACION_ID').alias('EstacionPadreId'))

        df_cat_central = df_cat_central.select(col('IdCentral').alias('EstacionId'),  
                                               
                                               when(col('Codigo')=='BABAA','BABAH')\
                                               .otherwise(when(col('Codigo')=='ASANA','ASANT')\
                                               .otherwise(when(col('Codigo')=='ATINA','ATINT')\
                                               .otherwise(when(col('Codigo')=='EDOSA','EDOST')\
                                               .otherwise(when(col('Codigo')=='LCEMA','LCEMT')\
                                               .otherwise(when(col('Codigo')=='ASAFA','ASAFT')\
                                               .otherwise(when(col('Codigo')=='MANDA','MANDH')\
                                               .otherwise(col('Codigo')))))))).alias('EstacionCodigo'),
                                               
                                               col('Nombre').alias('Estacion'),
                                               col('IdCentral').alias('EstacionPadreId'))

        df_cat_central = df_cat_central.union(df_central).distinct()

        df_linea = df_linea.select(col('ESTACION_ID').alias('EstacionId'),
                                   col('ESTACION_CODIGO').alias('EstacionCodigo'),
                                   col('ESTACION_NOMBRE').alias('Estacion'),
                                   col('ESTACION_ID').alias('EstacionPadreId'))

        df_cat_linea = df_cat_linea.select(col('IdLinea').alias('EstacionId'),
                                           col('Codigo').alias('EstacionCodigo'),
                                           col('Nombre').alias('Estacion'),
                                           col('IdLinea').alias('EstacionPadreId'))

        df_cat_linea = df_cat_linea.union(df_linea).distinct()

        df_subestacion = df_subestacion\
        .join(df_cat_subestacion, df_cat_subestacion.IdSubestacion == df_subestacion.SUBESTACION_ID_PADRE)\
        .select(col('ESTACION_ID').alias('EstacionId'),
                col('Codigo').alias('EstacionCodigo'),
                col('Nombre').alias('Estacion'),
                col('SUBESTACION_ID_PADRE').alias('EstacionPadreId'))

        df_cat_subestacion = df_cat_subestacion.select(col('IdSubestacion').alias('EstacionId'),
                                                       col('Codigo').alias('EstacionCodigo'),
                                                       col('Nombre').alias('Estacion'),
                                                       col('IdSubestacion').alias('EstacionPadreId'))

        df_cat_subestacion = df_subestacion.union(df_cat_subestacion).distinct()

        return df_cat_central.union(df_cat_linea).union(df_cat_subestacion).distinct()
            
    @staticmethod
    def DafaFrameElemento(df_barra,df_circuito,df_compensador,df_posicion,df_transformador,df_unidad,
                          df_cat_barra,df_cat_circuito,df_cat_compensador,df_cat_posicion,df_cat_transformador,
                          df_cat_unidad):
        df_barra = df_barra.select(col('ELEMENTO_ID').alias('ElementoId'),
                                   col('ELEMENTO_CODIGO').alias('ElementoCodigo'),
                                   col('ELEMENTO_NOMBRE').alias('Elemento'),
                                   col('ELEMENTO_TIPO').alias('Tipo'),
                                   col('NIVELVOLTAJE_ID').alias('VoltajeId'),
                                   col('EMPRESA_ID').alias('UNegocioId'),
                                   col('ESTACION_ID').alias('EstacionId'),
                                   lit(0).alias('PotEfectiva'))

        df_cat_barra = df_cat_barra.select(col('IdBarra').alias('ElementoId'),
                                           col('Codigo').alias('ElementoCodigo'),
                                           col('Nombre').alias('Elemento'),
                                           lit(2).alias('Tipo'),
                                           col('VoltajeID').alias('VoltajeId'),
                                           col('IdUNegocio').alias('UNegocioId'),
                                           col('IdSubestacion').alias('EstacionId'),
                                           lit(0).alias('PotEfectiva'))

        df_cat_barra = df_cat_barra.union(df_barra).distinct()

        df_circuito = df_circuito.select(col('ELEMENTO_ID').alias('ElementoId'),
                                         col('ELEMENTO_CODIGO').alias('ElementoCodigo'),
                                         col('ELEMENTO_NOMBRE').alias('Elemento'),
                                         col('ELEMENTO_TIPO').alias('Tipo'),
                                         col('NIVELVOLTAJE_ID').alias('VoltajeId'),
                                         col('EMPRESA_ID').alias('UNegocioId'),
                                         col('ESTACION_ID').alias('EstacionId'),
                                         lit(0).alias('PotEfectiva'))

        df_cat_circuito = df_cat_circuito.select(col('IdCircuito').alias('ElementoId'),
                                                 col('Codigo').alias('ElementoCodigo'),
                                                 col('Nombre').alias('Elemento'),
                                                 lit(3).alias('Tipo'),
                                                 col('IdNivelVoltaje').alias('VoltajeId'),
                                                 col('IdUNegocio').alias('UNegocioId'),
                                                 col('IdLinea').alias('EstacionId'),
                                                 lit(0).alias('PotEfectiva'))

        df_cat_circuito = df_cat_circuito.union(df_circuito).distinct()

        df_compensador = df_compensador.select(col('ELEMENTO_ID').alias('ElementoId'),
                                               col('ELEMENTO_CODIGO').alias('ElementoCodigo'),
                                               col('ELEMENTO_NOMBRE').alias('Elemento'),
                                               col('ELEMENTO_TIPO').alias('Tipo'),
                                               col('NIVELVOLTAJE_ID').alias('VoltajeId'),
                                               col('EMPRESA_ID').alias('UNegocioId'),
                                               col('ESTACION_ID').alias('EstacionId'),
                                               lit(0).alias('PotEfectiva'))

        df_cat_compensador = df_cat_compensador.select(col('IdCompensador').alias('ElementoId'),
                                                       col('Codigo').alias('ElementoCodigo'),
                                                       col('Nombre').alias('Elemento'),
                                                       lit(5).alias('Tipo'),
                                                       col('IdNivelVoltaje').alias('VoltajeId'),
                                                       col('IdUNegocio').alias('UNegocioId'),
                                                       col('IdSubestacion').alias('EstacionId'),
                                                       lit(0).alias('PotEfectiva'))

        df_cat_compensador = df_cat_compensador.union(df_compensador).distinct()

        df_posicion = df_posicion.select(col('ELEMENTO_ID').alias('ElementoId'),
                                         col('ELEMENTO_CODIGO').alias('ElementoCodigo'),
                                         col('ELEMENTO_NOMBRE').alias('Elemento'),
                                         col('ELEMENTO_TIPO').alias('Tipo'),
                                         col('NIVELVOLTAJE_ID').alias('VoltajeId'),
                                         col('EMPRESA_ID').alias('UNegocioId'),
                                         col('ESTACION_ID').alias('EstacionId'),
                                         lit(0).alias('PotEfectiva'))

        df_cat_posicion = df_cat_posicion.select(col('IdPosicion').alias('ElementoId'),
                                                 col('Codigo').alias('ElementoCodigo'),
                                                 col('Nombre').alias('Elemento'),
                                                 lit(0).alias('Tipo'),
                                                 col('IdNivelVoltaje').alias('VoltajeId'),
                                                 col('IdUNegocio').alias('UNegocioId'),
                                                 col('IdSubestacion').alias('EstacionId'),
                                                 lit(0).alias('PotEfectiva'))

        df_cat_posicion = df_cat_posicion.union(df_posicion).distinct()


        df_transformador = df_transformador.select(col('ELEMENTO_ID').alias('ElementoId'),
                                                   col('ELEMENTO_CODIGO').alias('ElementoCodigo'),
                                                   col('ELEMENTO_NOMBRE').alias('Elemento'),
                                                   col('ELEMENTO_TIPO').alias('Tipo'),
                                                   col('NIVELVOLTAJE_ID').alias('VoltajeId'),
                                                   col('EMPRESA_ID').alias('UNegocioId'),
                                                   col('ESTACION_ID').alias('EstacionId'),
                                                   lit(0).alias('PotEfectiva'))

        df_cat_transformador = df_cat_transformador.select(col('IdTransformador').alias('ElementoId'),
                                                           col('Codigo').alias('ElementoCodigo'),
                                                           col('Nombre').alias('Elemento'),
                                                           lit(4).alias('Tipo'),
                                                           col('IdNivelVoltaje').alias('VoltajeId'),
                                                           col('IdUNegocio').alias('UNegocioId'),
                                                           col('IdSubestacion').alias('EstacionId'),
                                                           lit(0).alias('PotEfectiva'))

        df_cat_transformador = df_cat_transformador.union(df_transformador).distinct()

        df_unidad = df_unidad.select(col('ELEMENTO_ID').alias('ElementoId'),
                                     
                                     when(col('ELEMENTO_CODIGO')=='ASAFTVAUA1','ASAFTVAU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ASAFTVAUA1','ASAFTVAU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ASANTTGUA1','ASANTTGU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ASANTTGUA2','ASANTTGU02')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ASANTTGUA3','ASANTTGU03')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ASANTTGUA4','ASANTTGU04')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ASANTTGUA5','ASANTTGU05')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ASANTTGUA6','ASANTTGU06')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ATINTTGUA1','ATINTTGU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='ATINTTGUA2','ATINTTGU02')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='BABAHEMUA1','BABAHEMU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='BABAHEMUA2','BABAHEMU02')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='EDOSTNCUA1','EDOSTNCU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='LCEMTMCUA1','LCEMTMCU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='MANDHPAU0A','MANDHPAU01')\
                                     .otherwise(when(col('ELEMENTO_CODIGO')=='MANDHPAUAA','MANDHPAU02')\
                                     .otherwise(col('ELEMENTO_CODIGO'))))))))))))))))).alias('ElementoCodigo'),
                                     
                                     col('ELEMENTO_NOMBRE').alias('Elemento'),
                                     col('ELEMENTO_TIPO').alias('Tipo'),
                                     lit(0).alias('VoltajeId'),
                                     col('EMPRESA_ID').alias('UNegocioId'),
                                     col('ESTACION_ID').alias('EstacionId'),
                                     col('UNIDAD_POTENCIAEFECTIVA').alias('PotEfectiva'))

        df_cat_unidad = df_cat_unidad.select(col('IdUnidad').alias('ElementoId'),
                                             
                                             when(col('Codigo')=='ASAFTVAUA1','ASAFTVAU01')\
                                             .otherwise(when(col('Codigo')=='ASAFTVAUA1','ASAFTVAU01')\
                                             .otherwise(when(col('Codigo')=='ASANTTGUA1','ASANTTGU01')\
                                             .otherwise(when(col('Codigo')=='ASANTTGUA2','ASANTTGU02')\
                                             .otherwise(when(col('Codigo')=='ASANTTGUA3','ASANTTGU03')\
                                             .otherwise(when(col('Codigo')=='ASANTTGUA4','ASANTTGU04')\
                                             .otherwise(when(col('Codigo')=='ASANTTGUA5','ASANTTGU05')\
                                             .otherwise(when(col('Codigo')=='ASANTTGUA6','ASANTTGU06')\
                                             .otherwise(when(col('Codigo')=='ATINTTGUA1','ATINTTGU01')\
                                             .otherwise(when(col('Codigo')=='ATINTTGUA2','ATINTTGU02')\
                                             .otherwise(when(col('Codigo')=='BABAHEMUA1','BABAHEMU01')\
                                             .otherwise(when(col('Codigo')=='BABAHEMUA2','BABAHEMU02')\
                                             .otherwise(when(col('Codigo')=='EDOSTNCUA1','EDOSTNCU01')\
                                             .otherwise(when(col('Codigo')=='LCEMTMCUA1','LCEMTMCU01')\
                                             .otherwise(when(col('Codigo')=='MANDHPAU0A','MANDHPAU01')\
                                             .otherwise(when(col('Codigo')=='MANDHPAUAA','MANDHPAU02')\
                                             .otherwise(col('Codigo'))))))))))))))))).alias('ElementoCodigo'),
                                             
                                             col('Nombre').alias('Elemento'),
                                             lit(1).alias('Tipo'),
                                             lit(0).alias('VoltajeId'),
                                             col('IdUNegocio').alias('UNegocioId'),
                                             col('IdCentral').alias('EstacionId'),
                                             col('Pot_Efectiva').alias('PotEfectiva'))

        df_cat_unidad = df_cat_unidad.union(df_unidad).distinct()

        dt_elemento_total = df_cat_barra.union(df_cat_circuito).union(df_cat_compensador)\
                .union(df_cat_posicion).union(df_cat_transformador).union(df_cat_unidad).distinct()

        return dt_elemento_total
            
    
    @staticmethod
    def AgregarDetalles(df_fallas,df_fallas_dtl,df_tblf_agc,unidad_negocio,estacion,elemento,df_evento,df_evento_dtl,spark):

        df_datos = df_fallas.join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .join(unidad_negocio, df_fallas_dtl.EMPRESA_ID == unidad_negocio.UNegocioId)\
        .join(estacion, df_fallas_dtl.ESTACION_ID == estacion.EstacionId, how = 'left')\
        .join(elemento, df_fallas_dtl.ELEMENTO_ID == elemento.ElementoId, how = 'left')\
        .join(df_evento, df_fallas_dtl.EVENTO_CIERRA_ID == df_evento.EVENTO_ID, how = 'left')\
        .filter(df_fallas_dtl.FALLA_DTL_ID == 1)\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                df_fallas.FALLA_FRECUENCIA.alias('Frecuencia'),
                df_fallas.FALLA_NIVEL_PASO.alias('Paso'),
                unidad_negocio.EmpresaCodigo,
                unidad_negocio.UNegocioCodigo,
                
                when(df_fallas_dtl.FALLA_CLASE==4,unidad_negocio.UNegocioCodigo)\
                .otherwise(estacion.EstacionCodigo).alias('EstacionCodigo'),
                
                when(df_fallas_dtl.FALLA_CLASE==1,'GEN')\
                .otherwise(when(df_fallas_dtl.FALLA_CLASE==2,'TRA')\
                .otherwise(when(df_fallas_dtl.FALLA_CLASE==4,'DIS')\
                .otherwise(when(df_fallas_dtl.FALLA_CLASE==8,'NA')\
                .otherwise('INT')))).alias('Clase'),
                
                when(df_fallas_dtl.FALLA_CLASE==4,unidad_negocio.UNegocioCodigo)\
                .otherwise(elemento.ElementoCodigo).alias('ElementoCodigo'),
                
                df_fallas.FALLA_PRE_POTENCIA.alias('PotenciaPre'),
                df_fallas.FALLA_PRE_POTENCIA.alias('PotenciaPost'))

        df_eventos = df_evento\
        .join(df_evento_dtl, df_evento.EVENTO_ID==df_evento_dtl.EVENTO_ID)\
        .filter((df_evento.EVENTO_CLASE==128) & (df_evento_dtl.EVENTO_DTL_PODER>=0))\
        .select(df_evento.EVENTO_ID,
                df_evento.EVENTO_FECHA.cast(TimestampType()).alias('EVENTO_FECHA'),
                df_evento.EVENTO_CLASE,
                df_evento_dtl.EVENTO_DTL_PODER)

        eventos = df_eventos.select('EVENTO_FECHA','EVENTO_DTL_PODER').distinct()\
        .withColumn('ID',func.row_number().over(Window.partitionBy()\
                                                .orderBy('EVENTO_FECHA','EVENTO_DTL_PODER')))

        eventos_par = eventos.filter(eventos.ID%2 == 0).select(col('EVENTO_FECHA').alias('FechaPar'),
                                                               col('EVENTO_DTL_PODER').alias('PotenciaPar'),
                                                               col('ID').alias('IdPar'))

        eventos_impar = eventos.filter(eventos.ID%2 != 0).select(col('EVENTO_FECHA').alias('FechaImpar'),
                                                               col('EVENTO_DTL_PODER').alias('PotenciaImpar'),
                                                               col('ID').alias('IdImpar'))

        
        if(eventos_par.select('FechaPar').groupby().agg(func.min('FechaPar')).first()[0] is None):
            anio = df_fallas.select(col('FALLA_FECHA').cast('timestamp')).groupby().agg(func.min('FALLA_FECHA')).first()[0].year
        else:
            anio = eventos_par.select('FechaPar').groupby().agg(func.min('FechaPar')).first()[0].year
            
        fechaInicio = str(anio)+'-01-01 00:00:00'
        
        eventos_ida = eventos_impar.join(eventos_par, (eventos_par.IdPar-1)==eventos_impar.IdImpar,how='left')\
        .select(eventos_impar.FechaImpar.alias('FechaInicio'),
                when(eventos_par.FechaPar.isNull(),datetime.datetime.now()).otherwise(eventos_par.FechaPar).alias('FechaFin'),
                eventos_impar.PotenciaImpar.alias('PotInicio'),
                when(eventos_par.PotenciaPar.isNull(),0).otherwise(eventos_par.PotenciaPar).alias('PotFin'))

        eventos_vuelta = eventos_impar.join(eventos_par, (eventos_par.IdPar+1)==eventos_impar.IdImpar,how='left')\
        .select(when(eventos_par.FechaPar.isNull(),fechaInicio).otherwise(eventos_par.FechaPar).alias('FechaInicio'),
                eventos_impar.FechaImpar.alias('FechaFin'),
                when(eventos_par.PotenciaPar.isNull(),0).otherwise(eventos_par.PotenciaPar).alias('PotInicio'),
                eventos_impar.PotenciaImpar.alias('PotFin'))

        eventos = eventos_ida.union(eventos_vuelta).orderBy(col('FechaInicio'))


        ################################## JOIN PARA ENCONTRAR EL VALOR
        df_datos = df_datos\
        .join(eventos, (df_datos.FechaFalla > eventos.FechaInicio) &\
              (df_datos.FechaFalla <= eventos.FechaFin),how='left')\
        .select(df_datos.FallaId,
                df_datos.FechaFalla,
                df_datos.Numero,
                df_datos.Frecuencia,
                df_datos.Paso,
                df_datos.EmpresaCodigo,
                df_datos.UNegocioCodigo,
                df_datos.EstacionCodigo,
                df_datos.Clase,
                df_datos.ElementoCodigo,
                df_datos.PotenciaPre,
                df_datos.PotenciaPost,
                when(eventos.PotFin.isNull(),0).otherwise(eventos.PotFin).alias('IntercambioProgramado'))

        ################################## ADJUNTAR INFORMACION AGC
        df_datos_agc = df_tblf_agc\
        .join(df_datos, df_datos.FallaId == df_tblf_agc.FALLA_ID)\
        .select(df_datos.FallaId,
                df_datos.FechaFalla,
                df_datos.Numero,
                df_datos.Frecuencia,
                df_datos.Paso,
                df_datos.EmpresaCodigo,
                df_datos.UNegocioCodigo,
                df_datos.EstacionCodigo,
                df_datos.Clase,
                df_datos.ElementoCodigo,
                df_tblf_agc.BIAS,
                df_tblf_agc.EMPRESA_ID,
                df_tblf_agc.ESTACION_ID,
                df_tblf_agc.ELEMENTO_ID,
                df_tblf_agc.MODO_AGC,
                df_datos.PotenciaPre.alias('IntercambioPrevio'),
                df_datos.PotenciaPost.alias('IntercambioPost'),
                df_datos.IntercambioProgramado.alias('IntercambioProgramado'))

        return df_datos_agc
            
    @staticmethod        
    def AgregarAgtAgc(df_datos,unidad_negocio,estacion,elemento):
        unidad_negocio = unidad_negocio.select(col('UNegocioId').alias('UnegId'),
                                               col('UNegocioCodigo').alias('UnegCodigo'),
                                               col('UNegocio').alias('Uneg'),
                                               col('EmpresaCodigo').alias('EmpCodigo'),
                                               col('Empresa').alias('Emp'))

        estacion = estacion.select(col('EstacionId').alias('EstId'),
                                   col('EstacionCodigo').alias('EstCodigo'),
                                   col('Estacion').alias('Est'))

        elemento = elemento.select(col('ElementoId').alias('ElemId'),
                                   col('ElementoCodigo').alias('ElemCodigo'),
                                   col('Elemento').alias('Elem'))

        df_datos = df_datos\
        .join(unidad_negocio, df_datos.EMPRESA_ID == unidad_negocio.UnegId)\
        .join(estacion, df_datos.ESTACION_ID == estacion.EstId)\
        .join(elemento, df_datos.ELEMENTO_ID == elemento.ElemId)\
        .select(df_datos.FallaId,
                df_datos.FechaFalla,
                df_datos.Numero,
                df_datos.Frecuencia,
                when(df_datos.Paso.isNull(),0).otherwise(df_datos.Paso).alias('Paso'),
                df_datos.EmpresaCodigo,
                df_datos.UNegocioCodigo,
                df_datos.EstacionCodigo,
                df_datos.Clase,
                df_datos.ElementoCodigo,
                when(df_datos.BIAS.isNull(),0).otherwise(df_datos.BIAS).alias('Bias'),
                unidad_negocio.EmpCodigo.alias('EmpresaAgcCodigo'),
                df_datos.EMPRESA_ID.alias('UNegocioAgcId'),
                unidad_negocio.UnegCodigo.alias('UNegocioAgcCodigo'),
                df_datos.ESTACION_ID.alias('EstacionAgcId'),
                estacion.EstCodigo.alias('EstacionAgcCodigo'),
                df_datos.ELEMENTO_ID.alias('ElementoAgcId'),
                elemento.ElemCodigo.alias('ElementoAgcCodigo'),
                df_datos.MODO_AGC.alias('Modo'),
                when(df_datos.IntercambioPrevio.isNull(),0).otherwise(df_datos.IntercambioPrevio).alias('IntercambioPrevio'),
                when(df_datos.IntercambioPost.isNull(),0).otherwise(df_datos.IntercambioPost).alias('IntercambioPost'),
                when(df_datos.IntercambioProgramado.isNull(),0).otherwise(df_datos.IntercambioProgramado).alias('IntercambioProgramado'))
        return df_datos
            
    def AgregarRpf(df_datos,df_tblf_rpf):
        df_tblf_rpf = df_tblf_rpf.withColumn('Numero',concat(df_tblf_rpf.ANIO,lit('-'),lpad(df_tblf_rpf.FALLA_RPT_NRO,3,'0')))
        df_datos_totales = df_datos\
        .join(df_tblf_rpf, (df_tblf_rpf.Numero==df_datos.Numero) & \
              (df_tblf_rpf.EMPRESA_ID==df_datos.UNegocioAgcId) & \
              (df_tblf_rpf.ESTACION_ID==df_datos.EstacionAgcId) & \
              (df_tblf_rpf.ELEMENTO_ID==df_datos.ElementoAgcId), how='left')\
        .select(df_datos.FallaId,
                df_datos.FechaFalla,
                when(df_datos.Numero.isNull(),'-').otherwise(df_datos.Numero).alias('Numero'),
                when(df_datos.Frecuencia.isNull(),0).otherwise(df_datos.Frecuencia).alias('Frecuencia'),
                df_datos.Paso,
                df_datos.EmpresaCodigo,
                df_datos.UNegocioCodigo,
                df_datos.EstacionCodigo,
                df_datos.Clase,
                df_datos.ElementoCodigo,
                df_datos.Bias,
                df_datos.EmpresaAgcCodigo,
                df_datos.UNegocioAgcCodigo,
                df_datos.EstacionAgcCodigo,
                df_datos.ElementoAgcCodigo,
                df_datos.Modo,
                when(df_tblf_rpf.TBLF_RPF_MW_ANTES.isNull(),0).otherwise(df_tblf_rpf.TBLF_RPF_MW_ANTES).alias('RpfAntes'),
                when(df_tblf_rpf.TBLF_RPF_MW_DESPUES.isNull(),0).otherwise(df_tblf_rpf.TBLF_RPF_MW_DESPUES).alias('RpfDespues'),
                df_datos.IntercambioPrevio,
                df_datos.IntercambioPost,
                df_datos.IntercambioProgramado)

        return df_datos_totales            

    def LimpiarAgc(datos,agentes_sni,agentes_agc,pasos,modos):
        datos_finales = datos\
        .join(agentes_sni, 
              (datos.EmpresaCodigo == agentes_sni.agt_empresa_id_bk) & \
              (datos.UNegocioCodigo == agentes_sni.agt_und_negocio_id_bk) & \
              (datos.EstacionCodigo == agentes_sni.agt_estacion_id_bk) & \
              (datos.Clase == agentes_sni.agt_clase_unegocio_id_bk) &\
              (datos.ElementoCodigo == agentes_sni.agt_elemento_id_bk), how='left')\
        .join(agentes_agc, 
              (datos.EmpresaAgcCodigo == agentes_agc.agt_empresa_id_bk) & \
              (datos.UNegocioAgcCodigo == agentes_agc.agt_und_negocio_id_bk) & \
              (datos.EstacionAgcCodigo == agentes_agc.agt_estacion_id_bk) & \
              (datos.ElementoAgcCodigo == agentes_agc.agt_elemento_id_bk), how='left')\
        .join(pasos, 
              (datos.Paso == pasos.pasos_numero), how='left')\
        .join(modos, 
              (datos.Modo == modos.agcmodo_id_pk), how='left')\
        .select(regexp_replace(substring(datos.FechaFalla.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                trim(regexp_replace(substring(datos.FechaFalla.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_id_fk'),
                agentes_agc.agt_id_pk.alias('agt_cent_id_fk'),
                agentes_sni.agt_id_pk.alias('agt_id_fk'),                
                
                pasos.pasos_id_pk.alias('pasos_id_fk'),
                modos.agcmodo_id_pk.alias('agcmodo_id_fk'),
                
                datos.Numero.alias('falla_numero'),
                
                datos.Bias.alias('agc_bias').cast('float'),
                datos.Frecuencia.alias('agc_frecuencia').cast('float'),
                datos.RpfAntes.alias('agc_rpf_antes').cast('float'),
                datos.RpfDespues.alias('agc_rpf_despues').cast('float'),
                datos.IntercambioPrevio.alias('agc_intercambio_previo').cast('float'),
                datos.IntercambioProgramado.alias('agc_intercambio_programado').cast('float'),
                datos.IntercambioPost.alias('agc_intercambio_post').cast('float'))

        return datos_finales