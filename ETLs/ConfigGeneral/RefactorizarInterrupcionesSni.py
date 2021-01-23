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
    def AgregarDetalles(df_fallas,df_fallas_dtl,unidad_negocio,estacion,elemento,df_evento):
        #dfc1 = df_fallas_dtl.groupby('FALLA_ID').agg(func.count('FALLA_ID').alias('CUENTA'))
        #estacion.filter(col('EstacionId')==476).show(estacion.count())
        df_datos = df_fallas.join(df_fallas_dtl, df_fallas.FALLA_ID == df_fallas_dtl.FALLA_ID)\
        .join(unidad_negocio, df_fallas_dtl.EMPRESA_ID == unidad_negocio.UNegocioId)\
        .join(estacion, df_fallas_dtl.ESTACION_ID == estacion.EstacionId, how = 'left')\
        .join(elemento, df_fallas_dtl.ELEMENTO_ID == elemento.ElementoId, how = 'left')\
        .join(df_evento, df_fallas_dtl.EVENTO_CIERRA_ID == df_evento.EVENTO_ID, how = 'left')\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
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
                
                when(df_fallas_dtl.FALLA_DTL_FECHA_DISPON.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_fallas_dtl.FALLA_DTL_FECHA_DISPON,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaDispon'),
                when(df_fallas_dtl.FALLA_DTL_POTENCIA_DISPON.isNull(),
                     0).otherwise(df_fallas_dtl.FALLA_DTL_POTENCIA_DISPON).alias('PotDisponible'),
                when(df_fallas_dtl.FALLA_DTL_POTENCIA_MW.isNull(),
                     0).otherwise(df_fallas_dtl.FALLA_DTL_POTENCIA_MW).alias('PotDisparada'),
                when(df_fallas.FALLA_ENS_TRN.isNull(),
                     0).otherwise(df_fallas.FALLA_ENS_TRN).alias('EnsTrans'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),
                     0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsSysManual'),
                df_fallas_dtl.TPF_CLASIF_ID.alias('ClasificacionId'),
                df_fallas_dtl.FALLA_DTL_CRG_DESCON.alias('OrigenId'),
                when(df_evento.EVENTO_FECHA.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_evento.EVENTO_FECHA,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaCierra'),
                when(df_fallas.FALLA_FECHA_FINAL.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_fallas.FALLA_FECHA_FINAL,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaFin'),
                df_fallas_dtl.ELEMENTO_EQUIVALENTE.alias('EquivalenteId'),
                df_fallas_dtl.EMPRESA_ORIGEN.alias('EmpresaOrigenId'),
                
                when(df_fallas_dtl.PRINCIPAL.cast('boolean')==False,'')\
                .otherwise(when(df_fallas.FALLA_CONSEC.isNull(),'')\
                           .otherwise(df_fallas.FALLA_CONSEC)).alias('DetalleConsecuencia'),

                when(df_fallas_dtl.PRINCIPAL.cast('boolean')==False,'')\
                .otherwise(when(df_fallas_dtl.FALLA_DTL_NOTA.isNull(),'')\
                           .otherwise(df_fallas_dtl.FALLA_DTL_NOTA)).alias('Nota'),
                
                when(df_fallas.FALLA_NOTA.isNull(),'')\
                           .otherwise(df_fallas.FALLA_NOTA).alias('NotaDetalle'),
                
                when(df_fallas_dtl.FALLA_DTL_PROTEC_ACTUADA.isNull(),'')\
                           .otherwise(df_fallas_dtl.FALLA_DTL_PROTEC_ACTUADA).alias('ProteccionActuada'),
                
                when(df_fallas_dtl.FALLA_DTL_ELEMEN_DISPARADO.isNull(),'')\
                           .otherwise(df_fallas_dtl.FALLA_DTL_ELEMEN_DISPARADO).alias('ElementoDisparado'),
                
                df_fallas_dtl.PRINCIPAL.alias('Principal').cast('boolean'),
                lit(False).alias('Consecuencia')).distinct()  

        return df_datos
            
    @staticmethod
    def AgregarSnt(df_fallas,df_tblf_snt,unidad_negocio,estacion,elemento,df_evento):
        df_datos = df_fallas.join(df_tblf_snt, df_fallas.FALLA_ID == df_tblf_snt.FALLA_ID)\
        .join(unidad_negocio, df_tblf_snt.EMPRESA_ID == unidad_negocio.UNegocioId)\
        .join(estacion, df_tblf_snt.ESTACION_ID == estacion.EstacionId, how = 'left')\
        .join(elemento, df_tblf_snt.ELEMENTO_ID == elemento.ElementoId, how = 'left')\
        .join(df_evento, df_tblf_snt.TBLF_SNT_EVENTO_CIERRA_ID == df_evento.EVENTO_ID, how = 'left')\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                unidad_negocio.EmpresaCodigo,
                unidad_negocio.UNegocioCodigo,
                estacion.EstacionCodigo,

                when(elemento.Tipo.isin({0,2,3,4,5,7}),'TRA')\
                .otherwise(when(elemento.Tipo.isin({13}),'DIS')\
                .otherwise('GEN')).alias('Clase'),
                
                elemento.ElementoCodigo,
                when(df_tblf_snt.TBLF_SNT_FECHA_DISPON.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_tblf_snt.TBLF_SNT_FECHA_DISPON,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaDispon'),
                lit(0).alias('PotDisponible'),
                when(df_tblf_snt.TBLF_SNT_MW.isNull(),
                     0).otherwise(df_tblf_snt.TBLF_SNT_MW).alias('PotDisparada'),
                when(df_fallas.FALLA_ENS_TRN.isNull(),
                     0).otherwise(df_fallas.FALLA_ENS_TRN).alias('EnsTrans'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),
                     0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsSysManual'),
                df_tblf_snt.TBLF_SNT_CAUSA_FALLA.alias('ClasificacionId'),
                df_tblf_snt.TBLF_SNT_ORIGEN.alias('OrigenId'),
                when(df_evento.EVENTO_FECHA.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_evento.EVENTO_FECHA,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaCierra'),
                when(df_fallas.FALLA_FECHA_FINAL.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_fallas.FALLA_FECHA_FINAL,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaFin'),
                df_tblf_snt.ELEMENTO_EQUIVALENTE.alias('EquivalenteId'),
                df_tblf_snt.EMPRESA_ORIGEN.alias('EmpresaOrigenId'),
                lit('').alias('DetalleConsecuencia'),
                when(df_tblf_snt.TBLF_SNT_NOTA.isNull(),'')\
                           .otherwise(df_tblf_snt.TBLF_SNT_NOTA).alias('Nota'),
                lit('').alias('NotaDetalle'),
                lit('').alias('ProteccionActuada'),
                lit('').alias('ElementoDisparado'),
                lit(False).alias('Principal'),
                lit(True).alias('Consecuencia')).distinct()
        return df_datos
        
    @staticmethod
    def AgregarGen(df_fallas,df_tblf_gen,unidad_negocio,estacion,elemento,df_evento):
        df_datos = df_fallas.join(df_tblf_gen, df_fallas.FALLA_ID == df_tblf_gen.FALLA_ID)\
        .join(unidad_negocio, df_tblf_gen.EMPRESA_ID == unidad_negocio.UNegocioId)\
        .join(estacion, df_tblf_gen.ESTACION_ID == estacion.EstacionId, how = 'left')\
        .join(elemento, df_tblf_gen.ELEMENTO_ID == elemento.ElementoId, how = 'left')\
        .select(df_fallas.FALLA_ID.alias('FallaId'),
                to_timestamp(df_fallas.FALLA_FECHA,'yyyy-MM-dd HH:mm:ss').alias('FechaFalla'),
                concat(df_fallas.ANIO,lit('-'),lpad(df_fallas.FALLA_RPT_NRO,3,'0')).alias('Numero'),
                unidad_negocio.EmpresaCodigo,
                unidad_negocio.UNegocioCodigo,
                estacion.EstacionCodigo,

                when(elemento.Tipo.isin({0,2,3,4,5,7}),'TRA')\
                .otherwise(when(elemento.Tipo.isin({13}),'DIS')\
                .otherwise('GEN')).alias('Clase'),
                
                elemento.ElementoCodigo,
                when(df_tblf_gen.TBLF_GEN_FECHA_DISPON.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_tblf_gen.TBLF_GEN_FECHA_DISPON,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaDispon'),
                when(df_tblf_gen.TBLF_GEN_POTENCIA_DISPON.isNull(),
                     0).otherwise(df_tblf_gen.TBLF_GEN_POTENCIA_DISPON).alias('PotDisponible'),
                when(df_tblf_gen.TBLF_GEN_MW.isNull(),
                     0).otherwise(df_tblf_gen.TBLF_GEN_MW).alias('PotDisparada'),
                when(df_fallas.FALLA_ENS_TRN.isNull(),
                     0).otherwise(df_fallas.FALLA_ENS_TRN).alias('EnsTrans'),
                when(df_fallas.FALLA_ENS_SYS_MANUAL.isNull(),
                     0).otherwise(df_fallas.FALLA_ENS_SYS_MANUAL).alias('EnsSysManual'),
                df_tblf_gen.TBLF_GEN_CAUSA_FALLA.alias('ClasificacionId'),
                df_tblf_gen.TBLF_GEN_ORIGEN.alias('OrigenId'),
                lit(datetime.datetime(1900,1,1,0,0)).alias('FechaCierra'),
                when(df_fallas.FALLA_FECHA_FINAL.isNull(),
                     datetime.datetime(1900,1,1,0,0)).otherwise(to_timestamp(df_fallas.FALLA_FECHA_FINAL,
                                                                             'yyyy-MM-dd HH:mm')).alias('FechaFin'),
                lit(None).alias('EquivalenteId'),
                df_tblf_gen.EMPRESA_ORIGEN.alias('EmpresaOrigenId'),
                lit('').alias('DetalleConsecuencia'),
                when(df_tblf_gen.TBLF_GEN_NOTA.isNull(),'')\
                           .otherwise(df_tblf_gen.TBLF_GEN_NOTA).alias('Nota'),
                lit('').alias('NotaDetalle'),
                lit('').alias('ProteccionActuada'),
                lit('').alias('ElementoDisparado'),
                lit(False).alias('Principal'),
                lit(True).alias('Consecuencia')
                )
        return df_datos
            
    @staticmethod      
    def AgenteEquivalente(unidad_negocio,estacion,elemento):
        df_data = elemento.join(unidad_negocio, elemento.UNegocioId == unidad_negocio.UNegocioId)\
        .join(estacion, elemento.EstacionId == estacion.EstacionId)\
        .select(unidad_negocio.EmpresaCodigo.alias('EmpresaCodigoEq'),
                unidad_negocio.UNegocioCodigo.alias('UNegocioCodigoEq'),
                estacion.EstacionCodigo.alias('EstacionCodigoEq'),
                lit('TRA').alias('ClaseEq'),
                elemento.ElementoCodigo.alias('ElementoCodigoEq'),
                elemento.ElementoId.alias('ElementoIdEq'))\
        .filter(elemento.Tipo.isin({3,4}))

        return df_data
            
    @staticmethod        
    def AgregarAgtOrigen(datos_totales,unidad_negocio,df_origen):
        datos_totales = datos_totales\
        .join(df_origen, datos_totales.OrigenId == df_origen.TPF_ORIGEN_ID, how='left')\
        .join(unidad_negocio, datos_totales.EmpresaOrigenId == unidad_negocio.UNegocioId, how='left')\
        .select(datos_totales.FallaId,
                datos_totales.FechaFalla,
                when(datos_totales.Numero.isNull(),'-').otherwise(datos_totales.Numero).alias('Numero'),
                datos_totales.EmpresaCodigo,
                datos_totales.UNegocioCodigo,
                when(datos_totales.Clase=='DIS',datos_totales.UNegocioCodigo)\
                .otherwise(datos_totales.EstacionCodigo).alias('EstacionCodigo'),
                datos_totales.Clase,
                when(datos_totales.Clase=='DIS',datos_totales.UNegocioCodigo)\
                .otherwise(datos_totales.ElementoCodigo).alias('ElementoCodigo'),
                datos_totales.FechaDispon,
                datos_totales.PotDisponible,
                datos_totales.PotDisparada,
                datos_totales.EnsTrans,
                datos_totales.EnsSysManual,
                datos_totales.ClasificacionId,
                datos_totales.FechaCierra,
                datos_totales.FechaFin,
                datos_totales.EquivalenteId,
                datos_totales.EmpresaOrigenId,
                df_origen.TPF_ORIGEN_CODIGO.alias('OrigenCodigo'),
                unidad_negocio.EmpresaCodigo.alias('EmpresaCodigoOrg'),
                unidad_negocio.UNegocioCodigo.alias('UNegocioCodigoOrg'),
                when(datos_totales.EmpresaCodigoEq.isNull(),'NA')\
                .otherwise(datos_totales.EmpresaCodigoEq).alias('EmpresaCodigoEq'),
                when(datos_totales.UNegocioCodigoEq.isNull(),'NA')\
                .otherwise(datos_totales.UNegocioCodigoEq).alias('UNegocioCodigoEq'),
                when(datos_totales.EstacionCodigoEq.isNull(),'NA')\
                .otherwise(datos_totales.EstacionCodigoEq).alias('EstacionCodigoEq'),
                when(datos_totales.ElementoCodigoEq.isNull(),'NA')\
                .otherwise(datos_totales.ElementoCodigoEq).alias('ElementoCodigoEq'),
                datos_totales.DetalleConsecuencia,
                datos_totales.Nota,
                datos_totales.NotaDetalle,
                datos_totales.ProteccionActuada,
                datos_totales.ElementoDisparado,
                datos_totales.Principal,
                datos_totales.Consecuencia)
        return datos_totales
            
    @staticmethod
    def AgregarClasificacion(datos_totales,df_clasificacion,df_grupo_clasif,df_segundo_nivel,df_primer_nivel):
        datos_totales = datos_totales\
        .join(df_clasificacion, datos_totales.ClasificacionId == df_clasificacion.TPF_CLASIF_ID)\
        .join(df_grupo_clasif, df_clasificacion.TPF_CLASIF_GRP_ID == df_grupo_clasif.TPF_CLASIF_GRP_ID)\
        .join(df_segundo_nivel, df_grupo_clasif.TPF_CLASIF_GRP_NIVEL2 == df_segundo_nivel.IdSegundoNivel)\
        .join(df_primer_nivel, df_grupo_clasif.TPF_CLASIF_GRP_NIVEL1 == df_primer_nivel.IdPrimerNivel)\
        .select(datos_totales.FallaId,
                datos_totales.FechaFalla,
                datos_totales.Numero,
                datos_totales.EmpresaCodigo,
                datos_totales.UNegocioCodigo,
                datos_totales.EstacionCodigo,
                datos_totales.Clase,
                datos_totales.ElementoCodigo,
                datos_totales.FechaDispon,
                datos_totales.PotDisponible,
                datos_totales.PotDisparada,
                datos_totales.EnsTrans,
                datos_totales.EnsSysManual,
                datos_totales.FechaCierra,
                datos_totales.FechaFin,
                datos_totales.OrigenCodigo,
                datos_totales.EmpresaCodigoOrg,
                datos_totales.UNegocioCodigoOrg,
                datos_totales.EmpresaCodigoEq,
                datos_totales.UNegocioCodigoEq,
                datos_totales.EstacionCodigoEq,
                datos_totales.ElementoCodigoEq,
                df_clasificacion.TPF_CLASIF_ID.alias('ClasificacionCodigo'),
                df_grupo_clasif.TPF_CLASIF_GRP_ID.alias('GrupoCodigo'),
                df_segundo_nivel.Codigo.alias('SegundoNivelCodigo'),
                df_primer_nivel.Codigo.alias('PrimerNivelCodigo'),
                datos_totales.DetalleConsecuencia,
                datos_totales.Nota,
                datos_totales.NotaDetalle,
                datos_totales.ProteccionActuada,
                datos_totales.ElementoDisparado,
                datos_totales.Principal,
                datos_totales.Consecuencia)
        return datos_totales
            
    @staticmethod
    def AgregarHechos(datos_totales,crg,eac):
        datos = datos_totales\
        .join(crg, (datos_totales.FallaId == crg.FallaIdCrg) &\
                   (datos_totales.Principal == True), how='left')\
        .join(eac, (datos_totales.FallaId == eac.FallaIdEac) &\
                   (datos_totales.Principal == True), how='left')\
        .select(datos_totales.FallaId,
                datos_totales.FechaFalla,
                datos_totales.FechaDispon,
                datos_totales.FechaCierra,
                datos_totales.FechaFin,
                datos_totales.OrigenCodigo,
                datos_totales.EmpresaCodigoOrg,
                datos_totales.UNegocioCodigoOrg,
                datos_totales.EmpresaCodigoEq,
                datos_totales.UNegocioCodigoEq,
                datos_totales.EstacionCodigoEq,
                datos_totales.ElementoCodigoEq,
                datos_totales.ClasificacionCodigo,
                datos_totales.GrupoCodigo,
                datos_totales.SegundoNivelCodigo,
                datos_totales.PrimerNivelCodigo,
                datos_totales.Numero,
                datos_totales.EmpresaCodigo,
                datos_totales.UNegocioCodigo,
                datos_totales.EstacionCodigo,
                datos_totales.Clase,
                datos_totales.ElementoCodigo,
                datos_totales.PotDisponible,
                datos_totales.PotDisparada,
                datos_totales.EnsTrans,
                datos_totales.EnsSysManual,
                when(crg.TIEMPOCrg.isNull(),0).otherwise(crg.TIEMPOCrg).alias('TiempoCrg'),
                when(crg.CARGACrg.isNull(),0).otherwise(crg.CARGACrg).alias('CargaCrg'),
                when(crg.ENSCrg.isNull(),0).otherwise(crg.ENSCrg).alias('EnsCrg'),
                when(crg.HoraMax.isNull(),lit(datetime.datetime(1900,1,1,0,0))).otherwise(crg.HoraMax).alias('HoraMaxCrg'),
                when(eac.TIEMPOEac.isNull(),0).otherwise(eac.TIEMPOEac).alias('TiempoEac'),
                when(eac.CARGAEac.isNull(),0).otherwise(eac.CARGAEac).alias('CargaEac'),
                when(eac.ENSEac.isNull(),0).otherwise(eac.ENSEac).alias('EnsEac'),
                datos_totales.DetalleConsecuencia,
                datos_totales.Nota,
                datos_totales.NotaDetalle,
                datos_totales.ProteccionActuada,
                datos_totales.ElementoDisparado,
                datos_totales.Principal,
                datos_totales.Consecuencia)
        return datos
    
    #datos.EmpresaCodigo,datos.UNegocioCodigo,datos.Clase,datos.EstacionCodigo,datos.ElementoCodigo,
    def LimpiarFallasSni(agentes,equivalentes,agt_origen,origen,clasificacion,datos):
        formato = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        tiempoIndiponibilidad = \
        (unix_timestamp(to_timestamp('FechaDispon','yyyy-MM-dd HH:mm:ss'), format=formato)
        - unix_timestamp(to_timestamp('FechaFalla','yyyy-MM-dd HH:mm:ss'), format=formato))/60.0

        df_fallas = datos\
        .join(clasificacion, 
              (datos.ClasificacionCodigo == clasificacion.clsf_clasif_id_bk) & \
              (datos.GrupoCodigo == clasificacion.clsf_grupo_id_bk) & \
              (datos.SegundoNivelCodigo == clasificacion.clsf_nivel_dos_id_bk) & \
              (datos.PrimerNivelCodigo == clasificacion.clsf_nivel_uno_id_bk))\
        .join(agentes, 
              (datos.EmpresaCodigo == agentes.agt_empresa_id_bk) & \
              (datos.UNegocioCodigo == agentes.agt_und_negocio_id_bk) & \
              (datos.EstacionCodigo == agentes.agt_estacion_id_bk) & \
              (datos.Clase == agentes.agt_clase_unegocio_id_bk) &\
              (datos.ElementoCodigo == agentes.agt_elemento_id_bk),how='left')\
        .join(equivalentes, 
              (datos.EmpresaCodigoEq == equivalentes.agt_empresa_id_bk) & \
              (datos.UNegocioCodigoEq == equivalentes.agt_und_negocio_id_bk) & \
              (datos.EstacionCodigoEq == equivalentes.agt_estacion_id_bk) & \
              (datos.ElementoCodigoEq == equivalentes.agt_elemento_id_bk), how='left')\
        .join(agt_origen, 
              (datos.EmpresaCodigoOrg == agt_origen.agtorg_empresa_id_bk) & \
              (datos.UNegocioCodigoOrg == agt_origen.agtorg_und_negocio_id_bk), how='left')\
        .join(origen, 
              (datos.OrigenCodigo == origen.asigorg_origen_id_bk), how='left')\
        .select(agentes.agt_id_pk.alias('agt_id_fk'),
                regexp_replace(substring(datos.FechaFalla.cast('string'),0,10),'-','').cast('int').alias('tmpo_falla_id_fk'),
                trim(regexp_replace(substring(datos.FechaFalla.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_falla_id_fk'),
                regexp_replace(substring(datos.FechaDispon.cast('string'),0,10),'-','').cast('int').alias('tmpo_dispon_id_fk'),
                trim(regexp_replace(substring(datos.FechaDispon.cast('string'),11,6),':'
                                    ,'')).cast(IntegerType()).alias('hora_dispon_id_fk'),
                regexp_replace(substring(datos.FechaCierra.cast('string'),0,10),'-','').cast('int').alias('tmpo_cierre_id_fk'),
                trim(regexp_replace(substring(datos.FechaCierra.cast('string'),11,6),':',
                                    '')).cast(IntegerType()).alias('hora_cierre_id_fk'),
                regexp_replace(substring(datos.FechaFin.cast('string'),0,10),'-','').cast('int').alias('tmpo_fin_id_fk'),
                trim(regexp_replace(substring(datos.FechaFin.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_fin_id_fk'),
                
                regexp_replace(substring(datos.HoraMaxCrg.cast('string'),0,10),'-','').cast('int').alias('tmpo_max_norm_id_fk'),
                trim(regexp_replace(substring(datos.HoraMaxCrg.cast('string'),11,6),':',
                                    '')).cast(IntegerType()).alias('hora_max_norm_id_fk'),
                
                origen.asigorg_id_pk.alias('asigorg_id_fk'),
                agt_origen.agtorg_id_pk.alias('agtorg_id_fk'),
                equivalentes.agt_id_pk.alias('agteqv_id_fk'),
                clasificacion.clasf_id_pk.alias('clasf_id_fk'),
                
                datos.Numero.alias('falla_numero'),
                
                when(tiempoIndiponibilidad<0,-1.0)\
                .otherwise(round(tiempoIndiponibilidad,4)).alias('falla_tmp_indisponibilidad').cast('float'),
                (round(datos.TiempoCrg,4)).alias('falla_tmp_normalizacion_carga').cast('float'),
                round(datos.PotDisponible,4).alias('falla_potencia_disponible').cast('float'),
                round(datos.PotDisparada,4).alias('falla_potencia_disparada').cast('float'),
                (round(datos.CargaCrg,4)).alias('falla_carga_desconectada').cast('float'),
                round(datos.CargaEac,4).alias('falla_carga_desconectada_eac').cast('float'),
                
                when(agentes.agt_clase_unegocio!='TRANSMISIÓN',0)\
                .otherwise(when(tiempoIndiponibilidad<0,-1.0)\
                .otherwise(round(tiempoIndiponibilidad,4))*round(datos.PotDisparada,4)).alias('falla_ens_trn').cast('float'),
                
                when(agentes.agt_clase_unegocio!='TRANSMISIÓN',0)\
                .otherwise(round(datos.EnsTrans,4)).alias('falla_ens_trn_man').cast('float'),
                (round(datos.EnsCrg,4)+round(datos.EnsEac,4)).alias('falla_ens_sistema').cast('float'),
                round(datos.EnsEac,4).alias('falla_ens_sistema_eac').cast('float'),
                round(datos.EnsSysManual,4).alias('falla_ens_sistema_man').cast('float'),
                
                datos.Principal.alias('falla_elemento_principal'),
                datos.Consecuencia.alias('falla_consecuencia'),
                datos.Nota.alias('obsr_causa'),
                datos.ProteccionActuada.alias('obsr_proteccion_actuada'),
                datos.ElementoDisparado.alias('obsr_elemen_disp_distrib'),
                datos.DetalleConsecuencia.alias('obsr_consecuencias'),
                datos.NotaDetalle.alias('obsr_obs_generales')).distinct()
        
        return df_fallas #.filter(col('agt_id_fk').isNull()==True)
        