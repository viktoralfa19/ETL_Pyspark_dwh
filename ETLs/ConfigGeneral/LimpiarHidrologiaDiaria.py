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

class LimpiarHidrologiaDiaria():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Hidrología Diaria de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Hidrología Diaria ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_hidrologia \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarHidrologiaDiaria.Extract_Transform_data(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarHidrologiaDiaria.Load_data(transform_data,'cen_dws.fact_hidrologia',accesoDatos)
            
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
        
        dvhidrologia = genericDataFrame.GetDataHdfs('TMP_System_Embalse','file_TMP_System_Embalse_'+fecha)
        
        if(dvhidrologia is None):
            return None
        
        agente = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'GEN') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 8))
        parametros = accesoDatos.GetAllData('cen_dws.dim_param_hidrologicos')
        
        #Calculamos el valor del Caudal Lateral Complejo
        amaluza = dvhidrologia.filter(dvhidrologia.Embalse == 'AMALE')
        mazar = dvhidrologia.filter(dvhidrologia.Embalse == 'MAZAE')
        
        uneg = amaluza.select('UNegocio').first()[0]
        
        amaluza_caudal_lat_prom = amaluza.select(amaluza.CaudalLateralProm.alias('Caudal_Prom'))
        mazar_caudal_prom = mazar.select(mazar.Caudal_Prom)

        mazar_amaluza = amaluza_caudal_lat_prom.union(mazar_caudal_prom)
        
        schema = StructType([StructField('agt_id_fk', IntegerType(), False),
                             StructField('paramhid_id_fk', IntegerType(), False),
                             StructField('tmpo_id_fk', IntegerType(), False),
                             StructField('hidro_valor', FloatType(), False)])
        
        #Caudal Complejo
        caudal_promedio_complejo = round(mazar_amaluza.agg({'Caudal_Prom':'sum'}).collect()[0][0],2)
        
        #Fecha del caudal complejo
        fecha_complejo = dvhidrologia.select(regexp_replace('Fecha', '-', '').cast('integer').alias('Fecha')).first()[0]
        
        #Parámetro del caudal complejo
        parametro_complejo = parametros.filter(parametros.paramhid_id_bk == 'Caudal_Prom')\
        .select(parametros.paramhid_id_pk).first()[0]
        
        #Agente del caudal complejo
        agente_complejo = agente.filter((agente.agt_elemento_id_bk == 'COMPLEJO') &\
                                        (agente.agt_und_negocio_id_bk == uneg))\
        .select(agente.agt_id_pk).first()[0]

        dvhidrologia = dvhidrologia\
        .join(agente, 
              (dvhidrologia.Empresa==agente.agt_empresa_id_bk) &\
              (dvhidrologia.UNegocio==agente.agt_und_negocio_id_bk) &\
              (dvhidrologia.Central==agente.agt_estacion_id_bk) &\
              (dvhidrologia.Embalse==agente.agt_elemento_id_bk), how='left')\
        .select(agente.agt_id_pk.alias('agt_id_fk'),
                regexp_replace(substring(dvhidrologia.Fecha.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                dvhidrologia.AlturaNeta.cast('float'),
                dvhidrologia.CaudalDesFondo.cast('float'),
                dvhidrologia.CaudalEntradaHora.cast('float'),
                dvhidrologia.CaudalEntradaProm.cast('float'),
                dvhidrologia.CaudalLateralProm.cast('float'),
                dvhidrologia.Caudal_Prom.cast('float'),
                dvhidrologia.CaudalTurbinado.cast('float'),
                dvhidrologia.CaudalVertido.cast('float'),
                dvhidrologia.CotaDescarga.cast('float'),
                dvhidrologia.Energia_Emergencia_Emb_Destino.cast('float'),
                dvhidrologia.Energia_Rem_Almacenada.cast('float'),
                dvhidrologia.Energia_Rem_Alm_Emb_Destino.cast('float'),
                dvhidrologia.Energia_Rem_Total_Almacenada.cast('float'),
                dvhidrologia.Horas_Aper_Desfondo_dia.cast('float'),
                dvhidrologia.Nivel.cast('float'),
                dvhidrologia.Nivel_Prom.cast('float'),
                dvhidrologia.Res_Energetica.cast('float'),
                dvhidrologia.Vol_Almacenado.cast('float'),
                dvhidrologia.Vol_Des_Fondo.cast('float'),
                dvhidrologia.Vol_Turbinado.cast('float'),
                dvhidrologia.Vol_Vertido.cast('float'))\
        .fillna(0,subset=['AlturaNeta','CaudalDesFondo','CaudalEntradaHora','CaudalEntradaProm','CaudalLateralProm','Caudal_Prom',
                          'CaudalTurbinado','CaudalVertido','CotaDescarga','Energia_Emergencia_Emb_Destino','Energia_Rem_Almacenada',
                          'Energia_Rem_Alm_Emb_Destino','Energia_Rem_Total_Almacenada','Horas_Aper_Desfondo_dia','Nivel','Nivel_Prom',
                          'Res_Energetica','Vol_Almacenado','Vol_Des_Fondo','Vol_Turbinado','Vol_Vertido'])
        
        faltantes = dvhidrologia.filter((dvhidrologia.agt_id_fk.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False

        fecha_actual = datetime.datetime.now()
        
        alturaNeta = dvhidrologia.select('agt_id_fk',lit('AlturaNeta').alias('parametro'),
                                         'tmpo_id_fk',col('AlturaNeta').cast('float').alias('hidro_valor'))
        caudalDesFondo = dvhidrologia.select('agt_id_fk',lit('CaudalDesFondo').alias('parametro'),
                                         'tmpo_id_fk',col('CaudalDesFondo').cast('float').alias('hidro_valor'))
        caudalEntradaHora = dvhidrologia.select('agt_id_fk',lit('CaudalEntradaHora').alias('parametro'),
                                         'tmpo_id_fk',col('CaudalEntradaHora').cast('float').alias('hidro_valor'))
        caudalEntradaProm = dvhidrologia.select('agt_id_fk',lit('CaudalEntradaProm').alias('parametro'),
                                         'tmpo_id_fk',col('CaudalEntradaProm').cast('float').alias('hidro_valor'))
        caudalLateralProm = dvhidrologia.select('agt_id_fk',lit('CaudalLateralProm').alias('parametro'),
                                         'tmpo_id_fk',col('CaudalLateralProm').cast('float').alias('hidro_valor'))
        caudalProm = dvhidrologia.select('agt_id_fk',lit('Caudal_Prom').alias('parametro'),
                                         'tmpo_id_fk',col('Caudal_Prom').cast('float').alias('hidro_valor'))
        caudalTurbinado = dvhidrologia.select('agt_id_fk',lit('CaudalTurbinado').alias('parametro'),
                                         'tmpo_id_fk',col('CaudalTurbinado').cast('float').alias('hidro_valor'))
        caudalVertido = dvhidrologia.select('agt_id_fk',lit('CaudalVertido').alias('parametro'),
                                         'tmpo_id_fk',col('CaudalVertido').cast('float').alias('hidro_valor'))
        cotaDescarga = dvhidrologia.select('agt_id_fk',lit('CotaDescarga').alias('parametro'),
                                         'tmpo_id_fk',col('CotaDescarga').cast('float').alias('hidro_valor'))
        energiaEmergenciaEmbDestino = dvhidrologia.select('agt_id_fk',lit('Energia_Emergencia_Emb_Destino').alias('parametro'),
                                                          'tmpo_id_fk',
                                                          col('Energia_Emergencia_Emb_Destino').cast('float').alias('hidro_valor'))
        energiaRemAlmacenada = dvhidrologia.select('agt_id_fk',lit('Energia_Rem_Almacenada').alias('parametro'),
                                         'tmpo_id_fk',col('Energia_Rem_Almacenada').cast('float').alias('hidro_valor'))
        energiaRemAlmEmbDestino = dvhidrologia.select('agt_id_fk',lit('Energia_Rem_Alm_Emb_Destino').alias('parametro'),
                                         'tmpo_id_fk',col('Energia_Rem_Alm_Emb_Destino').cast('float').alias('hidro_valor'))
        energiaRemTotalAlmacenada = dvhidrologia.select('agt_id_fk',lit('Energia_Rem_Total_Almacenada').alias('parametro'),
                                         'tmpo_id_fk',col('Energia_Rem_Total_Almacenada').cast('float').alias('hidro_valor'))
        horasAperDesfondodia = dvhidrologia.select('agt_id_fk',lit('Horas_Aper_Desfondo_dia').alias('parametro'),
                                         'tmpo_id_fk',col('Horas_Aper_Desfondo_dia').cast('float').alias('hidro_valor'))
        nivel = dvhidrologia.select('agt_id_fk',lit('Nivel').alias('parametro'),
                                         'tmpo_id_fk',col('Nivel').cast('float').alias('hidro_valor'))
        nivelProm = dvhidrologia.select('agt_id_fk',lit('Nivel_Prom').alias('parametro'),
                                         'tmpo_id_fk',col('Nivel_Prom').cast('float').alias('hidro_valor'))
        resEnergetica = dvhidrologia.select('agt_id_fk',lit('Res_Energetica').alias('parametro'),
                                         'tmpo_id_fk',col('Res_Energetica').cast('float').alias('hidro_valor'))
        volAlmacenado = dvhidrologia.select('agt_id_fk',lit('Vol_Almacenado').alias('parametro'),
                                         'tmpo_id_fk',col('Vol_Almacenado').cast('float').alias('hidro_valor'))
        volDesFondo = dvhidrologia.select('agt_id_fk',lit('Vol_Des_Fondo').alias('parametro'),
                                         'tmpo_id_fk',col('Vol_Des_Fondo').cast('float').alias('hidro_valor'))
        volTurbinado = dvhidrologia.select('agt_id_fk',lit('Vol_Turbinado').alias('parametro'),
                                         'tmpo_id_fk',col('Vol_Turbinado').cast('float').alias('hidro_valor'))
        volVertido = dvhidrologia.select('agt_id_fk',lit('Vol_Vertido').alias('parametro'),
                                         'tmpo_id_fk',col('Vol_Vertido').cast('float').alias('hidro_valor'))
        
        dvhidrologia = alturaNeta.union(caudalDesFondo).union(caudalEntradaHora).union(caudalEntradaProm)\
        .union(caudalLateralProm).union(caudalProm).union(caudalTurbinado).union(caudalVertido).union(cotaDescarga)\
        .union(energiaEmergenciaEmbDestino).union(energiaRemAlmacenada).union(energiaRemAlmEmbDestino)\
        .union(energiaRemTotalAlmacenada).union(horasAperDesfondodia).union(nivel).union(nivelProm)\
        .union(resEnergetica).union(volAlmacenado).union(volDesFondo).union(volTurbinado).union(volVertido)\
        .withColumn('fecha_carga',lit(fecha_actual))
        
        dvhidrologia = dvhidrologia\
        .join(parametros, dvhidrologia.parametro == parametros.paramhid_id_bk, how='left')\
        .select('agt_id_fk',parametros.paramhid_id_pk.alias('paramhid_id_fk'),'tmpo_id_fk','hidro_valor',
                dvhidrologia.fecha_carga)
        
        
        faltantes = dvhidrologia.filter((dvhidrologia.paramhid_id_fk.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False 

         #Agrego el agente hidro Caudal Complejo
        caudalComplejo = accesoDatos._dBContextDw.spark.createDataFrame([(agente_complejo,parametro_complejo,
                                                                          fecha_complejo,caudal_promedio_complejo)],schema)\
        .withColumn('fecha_carga',lit(fecha_actual))
        
        dvhidrologia = dvhidrologia.union(caudalComplejo)
        
        return dvhidrologia
    
    
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
    