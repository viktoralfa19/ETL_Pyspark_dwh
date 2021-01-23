from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit, to_date, substring, trim
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarConsumoDiario():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Consumo Diario de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Consumo Diario ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_demanda_diaria \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarConsumoDiario.Extract_Transform_data(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarConsumoDiario.Load_data(transform_data,'cen_dws.fact_demanda_diaria',accesoDatos)
            
            if(deteleTmpQuery is not None):
                query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
                accesoDatos.Delete(query)
        
            return True
            
        except Exception as error:
            ExceptionManager.Treatment(error)
            
    def Extract_Transform_data (fecha_inicio,accesoDatos):
        """Método que realiza la extracción de datos de Asignacion desde el HDFS"""        
        genericDataFrame=GenericDataFrame(HDFSContext(DataBase='SIVO')) 
        fecha = fecha_inicio.strftime('%Y_%m_%d')
        dventrega = genericDataFrame.GetDataHdfs('DV_Entrega','file_DV_Entrega_'+fecha)
        dvgeneracion = genericDataFrame.GetDataHdfs('DV_Generacion','file_DV_Generacion_'+fecha)
        
        if(dventrega is None):
            return None
        
        if(dvgeneracion is None):
            return None
        
        empresa = genericDataFrame.GetDataHdfs('CFG_Empresa','file_CFG_Empresa')
        unegocio = genericDataFrame.GetDataHdfs('CFG_UnidadNegocio','file_CFG_UnidadNegocio')
        subestacion = genericDataFrame.GetDataHdfs('CFG_SubEstacion','file_CFG_SubEstacion')
        posicion = genericDataFrame.GetDataHdfs('CFG_Posicion','file_CFG_Posicion')
        pos_conect = genericDataFrame.GetDataHdfs('CFG_PosicionesConectadas','file_CFG_PosicionesConectadas')
        pos_uni = genericDataFrame.GetDataHdfs('CFG_PosicionUnidad','file_CFG_PosicionUnidad')
        elem_conect = genericDataFrame.GetDataHdfs('CFG_ElementosConectados','file_CFG_ElementosConectados')
        carga = genericDataFrame.GetDataHdfs('CFG_Carga','file_CFG_Carga')
        unidad = genericDataFrame.GetDataHdfs('CFG_Unidad','file_CFG_Unidad')
        tipo_unegocio = genericDataFrame.GetDataHdfs('CFG_TipoUnidadNegocio','file_CFG_TipoUnidadNegocio')
        voltaje = genericDataFrame.GetDataHdfs('CFG_NivelVoltaje','file_CFG_NivelVoltaje')
        
        agente_dist = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'DIS') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 0))
        elemento_dem = accesoDatos.GetAllData('cen_dws.dim_agt_elemento_demanda')
        componente = accesoDatos.GetAllData('cen_dws.dim_componente_demanda')
        
        consumos = LimpiarConsumoDiario\
        .Procesamiento(empresa,unegocio,subestacion,posicion,pos_conect,pos_uni,elem_conect,carga,unidad,tipo_unegocio,voltaje,
                       dvgeneracion,dventrega,fecha_inicio)
        
        #consumos=consumos.filter(col('Posicion')!='P_Refineria2_Esmera') #### SOLO PARA VALIDAR FUNCIONAMIENTO
        
        fecha_actual = datetime.datetime.now()
        
        consumos = consumos\
        .join(agente_dist, 
              (consumos.Empresa==agente_dist.agt_empresa_id_bk) &\
              (consumos.UNegocio==agente_dist.agt_und_negocio_id_bk) &\
              (consumos.Subestacion==agente_dist.agt_estacion_id_bk) &\
              (consumos.Posicion==agente_dist.agt_elemento_id_bk), how='left')\
        .join(elemento_dem, consumos.Elemento==elemento_dem.eldem_elemento_id_bk, how='left')\
        .join(componente, consumos.Componente==componente.compdem_componente, how='left')\
        .select(regexp_replace(substring(consumos.FechaHora.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                trim(regexp_replace(substring(consumos.FechaHora.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_id_fk'),
                substring(consumos.FechaHora.cast('string'),11,6).alias('hora'),
                agente_dist.agt_id_pk.alias('agt_id_fk'),
                elemento_dem.eldem_id_pk.alias('eldem_id_fk'),
                componente.compdem_id_pk.alias('compdem_id_fk'),
                consumos.MV_Validado.cast('float').alias('demdr_potencia')
               )
        
        faltantes = consumos.filter((consumos.agt_id_fk.isNull()) | (consumos.tmpo_id_fk.isNull()) |\
                                    (consumos.eldem_id_fk.isNull()) | (consumos.compdem_id_fk.isNull()))
        #faltantes.show()
        if(len(faltantes.head(1))!=0):
            return False 
        
        consumos = consumos.orderBy('agt_id_fk','eldem_id_fk','compdem_id_fk','hora_id_fk')
        consumos_rdd = consumos.rdd.map(lambda x: (str(x.tmpo_id_fk)+'-'+str(x.agt_id_fk)+'-'+str(x.eldem_id_fk)+'-'+str(x.compdem_id_fk),
                                                   [x.hora,x.hora_id_fk,x.demdr_potencia]))
        consumos_rdd = consumos_rdd.groupByKey()
        consumos_rdd = consumos_rdd.map(lambda x: LimpiarConsumoDiario.CalculoEnergetico(x)) 
            
        consumos_rdd = consumos_rdd.flatMap(lambda x: LimpiarConsumoDiario.SepararDatos(x))
        
        consumos = consumos_rdd.toDF(['tmpo_id_fk','agt_id_fk','eldem_id_fk','compdem_id_fk','demdr_potencia'])\
        .select(col('tmpo_id_fk').cast('integer').alias('tmpo_id_fk'),
                col('agt_id_fk').cast('integer').alias('agt_id_fk'),
                col('eldem_id_fk').cast('integer').alias('eldem_id_fk'),
                col('compdem_id_fk').cast('integer').alias('compdem_id_fk'),
                col('demdr_potencia').alias('demdr_potencia'))\
        .withColumn('fecha_carga',lit(fecha_actual))
        
        return consumos
    
    def CalculoEnergetico(x):
        id = x[0]
        datos = list(x[1])
        suma = 0
        pot_ant = 0
        
        for dato in datos:
            if(dato[1]==0):
                pot_ant = dato[2]
            elif(dato[0].split(':')[1]=='30'):
                suma += ((dato[2] + pot_ant)/4)
            else:
                suma += ((dato[2] + pot_ant)/2)
            pot_ant = dato[2]
         
        return (id,suma)
    
    def SepararDatos(x):
        id = x[0].split('-') 
        dato = float(x[1])
        splitDatos = [(id[0],id[1],id[2],id[3],dato)]
        return splitDatos
        
    
    def Procesamiento(df_cat_empresa,df_cat_unegocio,df_cat_subestacion,df_cat_posicion,df_cat_pos_conectadas,
                      df_cat_pos_unidad,df_cat_elem_conectados,df_cat_carga,df_cat_unidad,df_cat_tipo_unegocio,
                      df_cat_voltajes,df_generacion_media_horaria,df_entrega_medio_horario,dt_fecha):
        """Método que realiza el procesamiento de los DataFrames necesarios"""    
        dt_fecha_inicio = datetime.datetime.combine(dt_fecha,datetime.datetime.min.time())
        dt_fecha_fin = (dt_fecha_inicio + timedelta(minutes=1439) + timedelta(seconds=59)) 

        #Armamos el DataFrame de posiciones unidades vigentes
        df_pos_unidad = df_cat_pos_unidad.filter(((dt_fecha_inicio>=col('FechaInicio')) &\
                                                ((dt_fecha_inicio<=col('FechaFin')))) |
                                                ((dt_fecha_fin>=col('FechaInicio')) &\
                                                ((dt_fecha_fin<=col('FechaFin')))) |
                                                ((col('FechaInicio')>=dt_fecha_inicio) &\
                                                ((col('FechaInicio')<=dt_fecha_fin))) |
                                                ((col('FechaFin')>=dt_fecha_inicio) &\
                                                ((col('FechaFin')<=dt_fecha_fin))))

        #Completamos el DataFrame de Entregas
        df_entrega = df_entrega_medio_horario\
        .join(df_cat_posicion, df_entrega_medio_horario.Posicion == df_cat_posicion.Codigo)\
        .join(df_cat_subestacion, df_cat_posicion.IdSubestacion == df_cat_subestacion.IdSubestacion)\
        .join(df_cat_pos_conectadas, df_cat_posicion.IdPosicion == df_cat_pos_conectadas.IdPosicion)\
        .join(df_cat_elem_conectados, df_cat_pos_conectadas.IdElementoConectado == \
              df_cat_elem_conectados.IdElementoConectado)\
        .join(df_cat_carga, df_cat_elem_conectados.IdCarga == df_cat_carga.IdCarga)\
        .join(df_cat_voltajes, df_cat_carga.IdNivelVoltaje == df_cat_voltajes.IdNivelVoltaje)\
        .join(df_cat_unegocio, df_cat_carga.IdUNegocio == df_cat_unegocio.IdUNegocio)\
        .join(df_cat_empresa, df_cat_unegocio.IdEmpresa == df_cat_empresa.IdEmpresa)\
        .select(df_cat_empresa.Codigo.alias('EmpresaCodigo'),df_cat_empresa.Nombre.alias('Empresa'),
                df_cat_unegocio.Codigo.alias('UNegocioCodigo'),df_cat_unegocio.Nombre.alias('UNegocio'),
                df_cat_subestacion.Codigo.alias('SubestacionCodigo'),df_cat_subestacion.Nombre.alias('Subestacion'),
                df_cat_voltajes.Codigo.alias('VoltajeCodigo'),df_cat_voltajes.Descripcion.alias('Voltaje'),
                df_cat_posicion.IdPosicion,
                df_cat_posicion.Codigo.alias('PosicionCodigo'),df_cat_posicion.Nombre.alias('Posicion'),
                df_cat_posicion.Codigo.alias('ElementoCodigo'),df_cat_posicion.Nombre.alias('Elemento'),
                df_entrega_medio_horario.Fecha,df_entrega_medio_horario.Hora, df_entrega_medio_horario.MV_Validado,
                to_date(df_cat_unegocio.FechaAlta,'yyyy-MM-dd').alias('FechaAlta'),
                when(df_cat_unegocio.FechaBaja.isNull(), dt_fecha)\
                .otherwise(to_date(df_cat_unegocio.FechaBaja,'yyyy-MM-dd')).alias('FechaBaja'),
                to_date(df_cat_pos_conectadas.FechaAlta,'yyyy-MM-dd').alias('FechaAltaPC'),
                when(df_cat_pos_conectadas.FechaBaja.isNull(), dt_fecha)\
                .otherwise(to_date(df_cat_pos_conectadas.FechaBaja,'yyyy-MM-dd')).alias('FechaBajaPC'),
                lit('ENTREGA').alias('ComponenteCodigo'),
                lit('ENTREGAS').alias('Componente'))\
        .filter(((col('FechaAlta')<=dt_fecha) & (col('FechaBaja')>=dt_fecha)) &
                ((col('FechaAltaPC')<=dt_fecha) & (col('FechaBajaPC')>=dt_fecha))).drop('FechaAlta',
                                                                                        'FechaBaja',
                                                                                        'FechaAltaPC',
                                                                                        'FechaBajaPC').distinct()           

        #Armamos el DataFrame de Posiciones Unidades
        df_posiciones_unidad = df_cat_posicion\
        .join(df_cat_subestacion, df_cat_posicion.IdSubestacion == df_cat_subestacion.IdSubestacion)\
        .join(df_cat_pos_conectadas, df_cat_posicion.IdPosicion == df_cat_pos_conectadas.IdPosicion)\
        .join(df_cat_elem_conectados, df_cat_pos_conectadas.IdElementoConectado == \
              df_cat_elem_conectados.IdElementoConectado)\
        .join(df_cat_carga, df_cat_elem_conectados.IdCarga == df_cat_carga.IdCarga)\
        .join(df_cat_voltajes, df_cat_carga.IdNivelVoltaje == df_cat_voltajes.IdNivelVoltaje)\
        .join(df_cat_unegocio, df_cat_carga.IdUNegocio == df_cat_unegocio.IdUNegocio)\
        .join(df_cat_empresa, df_cat_unegocio.IdEmpresa == df_cat_empresa.IdEmpresa)\
        .join(df_pos_unidad, df_cat_posicion.IdPosicion == df_pos_unidad.IdPosicion)\
        .join(df_cat_unidad, df_pos_unidad.IdUnidad == df_cat_unidad.IdUnidad)\
        .select(df_cat_empresa.Codigo.alias('EmpresaCodigo'),df_cat_empresa.Nombre.alias('Empresa'),
                df_cat_unegocio.Codigo.alias('UNegocioCodigo'),df_cat_unegocio.Nombre.alias('UNegocio'),
                df_cat_subestacion.Codigo.alias('SubestacionCodigo'),df_cat_subestacion.Nombre.alias('Subestacion'),
                df_cat_voltajes.Codigo.alias('VoltajeCodigo'),df_cat_voltajes.Descripcion.alias('Voltaje'),
                df_cat_posicion.IdPosicion,
                df_cat_posicion.Codigo.alias('PosicionCodigo'),df_cat_posicion.Nombre.alias('Posicion'),
                to_date(df_cat_unegocio.FechaAlta,'yyyy-MM-dd').alias('FechaAltaUN'),
                when(df_cat_unegocio.FechaBaja.isNull(), dt_fecha)\
                .otherwise(to_date(df_cat_unegocio.FechaBaja,'yyyy-MM-dd')).alias('FechaBajaUN'),
                to_date(df_cat_pos_conectadas.FechaAlta,'yyyy-MM-dd').alias('FechaAltaPC'),
                when(df_cat_pos_conectadas.FechaBaja.isNull(), dt_fecha)\
                .otherwise(to_date(df_cat_pos_conectadas.FechaBaja,'yyyy-MM-dd')).alias('FechaBajaPC'))\
        .filter(((col('FechaAltaUN')<=dt_fecha) & (col('FechaBajaUN')>=dt_fecha)) &
                ((col('FechaAltaPC')<=dt_fecha) & (col('FechaBajaPC')>=dt_fecha))).drop('FechaAltaUN',
                                                                                        'FechaBajaUN',
                                                                                        'FechaAltaPC',
                                                                                        'FechaBajaPC').distinct()

        #Armamos el DataFrame de Posiciones Unidades Completas con fecha Inicio y Fin
        df_posiciones_unidad_fechas = df_posiciones_unidad\
        .join(df_pos_unidad, df_posiciones_unidad.IdPosicion == df_pos_unidad.IdPosicion)\
        .join(df_cat_unidad, df_pos_unidad.IdUnidad == df_cat_unidad.IdUnidad)\
        .select(df_posiciones_unidad.IdPosicion,df_cat_unidad.Codigo.alias('CodigoUnidad'),
                           df_pos_unidad.FechaInicio,df_pos_unidad.FechaFin).distinct()

        #Armamos el DataFrame de Posiciones Unidades Completas con fecha Inicio y Fin y Codigo Posición
        df_posiciones_unidad_cod_fechas = df_pos_unidad\
        .join(df_cat_posicion, df_pos_unidad.IdPosicion == df_cat_posicion.IdPosicion)\
        .join(df_cat_unidad, df_pos_unidad.IdUnidad == df_cat_unidad.IdUnidad)\
        .join(df_posiciones_unidad_fechas, (df_cat_unidad.Codigo == df_posiciones_unidad_fechas.CodigoUnidad) &
              (df_pos_unidad.IdPosicion == df_posiciones_unidad_fechas.IdPosicion))\
        .select(df_cat_posicion.Codigo,df_posiciones_unidad_fechas.CodigoUnidad,
                df_posiciones_unidad_fechas.FechaInicio,df_posiciones_unidad_fechas.FechaFin).distinct()

        #Armamos el DataFrame de generación inmersa distinta de cero
        df_generacion_inmersa = df_generacion_media_horaria\
        .join(df_posiciones_unidad_cod_fechas, df_generacion_media_horaria.Unidad == \
              df_posiciones_unidad_cod_fechas.CodigoUnidad)\
        .withColumn('FechaHora',func.concat(col('Fecha'),lit(' '),col('Hora')))\
        .filter((to_timestamp(col('FechaInicio'),'yyyy-MM-dd HH:mm')<=\
                 to_timestamp(col('FechaHora'),'yyyy-MM-dd HH:mm')) & \
                (to_timestamp(col('FechaFin'),'yyyy-MM-dd HH:mm')>=\
                 to_timestamp(col('FechaHora'),'yyyy-MM-dd HH:mm')))\
        .select(df_posiciones_unidad_cod_fechas.Codigo.alias('Posicion'),
                df_generacion_media_horaria.Unidad,df_generacion_media_horaria.Fecha,
                df_generacion_media_horaria.Hora,df_generacion_media_horaria.MV_Validado).distinct()

        #Armamos el DataFrame de generación inmersa distinta de cero sin fecha de Inicio y Fin
        df_generacion_inmersa_sin_flitro = df_generacion_media_horaria\
        .join(df_posiciones_unidad_cod_fechas, df_generacion_media_horaria.Unidad == \
              df_posiciones_unidad_cod_fechas.CodigoUnidad)\
        .select(df_posiciones_unidad_cod_fechas.Codigo.alias('Posicion'),
                df_generacion_media_horaria.Unidad,df_generacion_media_horaria.Fecha,
                df_generacion_media_horaria.Hora,df_generacion_media_horaria.MV_Validado).distinct()

        #Armamos el DataFrame que resta entre la generación inmersa total y la que si se filtra
        df_generacion_inmersa_ceros = df_generacion_inmersa_sin_flitro.exceptAll(df_generacion_inmersa)\
        .select('Posicion','Unidad','Fecha','Hora',lit('0').cast(FloatType()).alias('MV_Validado')).distinct()

        df_generacion_inmersa_total = df_generacion_inmersa.union(df_generacion_inmersa_ceros)

        df_generacion = df_posiciones_unidad\
        .join(df_generacion_inmersa_total, df_posiciones_unidad.PosicionCodigo == \
              df_generacion_inmersa_total.Posicion)\
        .join(df_cat_unidad, df_generacion_inmersa_total.Unidad == df_cat_unidad.Codigo)\
        .select(df_posiciones_unidad.EmpresaCodigo,df_posiciones_unidad.Empresa,
                df_posiciones_unidad.UNegocioCodigo,df_posiciones_unidad.UNegocio,
                df_posiciones_unidad.SubestacionCodigo,df_posiciones_unidad.Subestacion,
                df_posiciones_unidad.VoltajeCodigo,df_posiciones_unidad.Voltaje,
                df_posiciones_unidad.IdPosicion,
                df_posiciones_unidad.PosicionCodigo,df_posiciones_unidad.Posicion,
                df_generacion_inmersa_total.Unidad.alias('ElementoCodigo'),
                df_cat_unidad.Nombre.alias('Elemento'),
                df_generacion_inmersa_total.Fecha,df_generacion_inmersa_total.Hora,
                df_generacion_inmersa_total.MV_Validado,
                lit('GENERACION').alias('ComponenteCodigo'),
                lit('GENERACIÓN INMERSA').alias('Componente'))\
        .distinct()
        
        df_entrega_generacion = df_entrega.union(df_generacion)\
        .select(col('EmpresaCodigo').alias('Empresa'),col('UNegocioCodigo').alias('UNegocio'),
                col('SubestacionCodigo').alias('Subestacion'),col('PosicionCodigo').alias('Posicion'),
                col('ElementoCodigo').alias('Elemento'),col('Fecha'),col('Hora'),col('MV_Validado'),col('Componente'))\
        .withColumn('FechaHora',(func.concat(col('Fecha'),lit(' '),col('Hora'))).cast('timestamp'))

        return df_entrega_generacion        
    
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
    