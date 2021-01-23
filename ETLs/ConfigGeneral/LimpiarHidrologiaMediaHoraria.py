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

class LimpiarHidrologiaMediaHoraria():
    """Clase que permite realizar la extracción, limpieza y validación de datos de Hidrología Media Horaria de SIVO."""
    genericDataFrame = None
    accesoDatos = None
    
    def Elt_main(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        try:
            """Método que procesa todo el flujo de proceso del ETL."""
            print('---- Proceso de ETL de Hechos de Hidrología Media Horaria ---- \n')
            print('DATAWAREHOUSE: dwh_sirio')
            print('DIMENSIÓN: fact_hidrologia_horaria \n')

            dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
            accesoDatos = EtlAL(dbContext) 
        
            print('1. Extracción de datos y Transformación de datos')
            transform_data = LimpiarHidrologiaMediaHoraria.Extract_Transform_data(fecha_inicio,accesoDatos)
            
            print('2. Cargar  datos\n')
            LimpiarHidrologiaMediaHoraria.Load_data(transform_data,'cen_dws.fact_hidrologia_horaria',accesoDatos)
            
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
        
        dvhidrologia = genericDataFrame.GetDataHdfs('AGT_CaudalesNivelesHorarios','file_AGT_CaudalesNivelesHorarios_'+fecha)
        
        if(dvhidrologia is None):
            return None
        
        agente = accesoDatos.GetAllData('cen_dws.dim_agente').filter((col('agt_clase_unegocio_id_bk') == 'GEN') &\
                                                                     (col('agt_tipo_elemento_id_bk') == 8))
        parametros = accesoDatos.GetAllData('cen_dws.dim_param_hidrologicos')
        
        fecha_actual = datetime.datetime.now()
        
        dvhidrologia = dvhidrologia\
        .withColumnRenamed('Cau_Ent_Prom','CaudalEntradaProm')\
        .withColumnRenamed('Cau_Ent_Hora','CaudalEntradaHora')\
        .withColumnRenamed('Cau_Lat_Prom','CaudalLateralProm')\
        .withColumnRenamed('Cau_Turbinado','CaudalTurbinado')\
        .withColumnRenamed('Cau_Vertido','CaudalVertido')\
        .withColumnRenamed('Cau_Des_Fondo','CaudalDesFondo')\
        .withColumnRenamed('Nivel','Nivel_Prom')\
        .withColumnRenamed('Cota_Descarga','CotaDescarga')\
        .withColumnRenamed('Altura_Neta','AlturaNeta')\
        .withColumn('FechaHora',(func.concat(col('Fecha'),lit(' '),col('Hora'))).cast('timestamp'))
        
        dvhidrologia = dvhidrologia\
        .join(agente, 
              (dvhidrologia.Empresa==agente.agt_empresa_id_bk) &\
              (dvhidrologia.UNegocio==agente.agt_und_negocio_id_bk) &\
              (dvhidrologia.Central==agente.agt_estacion_id_bk) &\
              (dvhidrologia.Embalse==agente.agt_elemento_id_bk), how='left')\
        .select(agente.agt_id_pk.alias('agt_id_fk'),
                regexp_replace(substring(dvhidrologia.FechaHora.cast('string'),0,10),'-','').cast('int').alias('tmpo_id_fk'),
                trim(regexp_replace(substring(dvhidrologia.FechaHora.cast('string'),11,6),':','')).cast(IntegerType()).alias('hora_id_fk'),
                dvhidrologia.AlturaNeta,
                dvhidrologia.CaudalDesFondo,
                dvhidrologia.CaudalEntradaHora,
                dvhidrologia.CaudalEntradaProm,
                dvhidrologia.CaudalLateralProm,
                dvhidrologia.CaudalTurbinado,
                dvhidrologia.CaudalVertido,
                dvhidrologia.CotaDescarga,
                dvhidrologia.Nivel_Prom)
        
        faltantes = dvhidrologia.filter((dvhidrologia.agt_id_fk.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False  
        
        alturaNeta = dvhidrologia.select('agt_id_fk',lit('AlturaNeta').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('AlturaNeta').cast('float').alias('hidroh_valor'))
        caudalDesFondo = dvhidrologia.select('agt_id_fk',lit('CaudalDesFondo').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('CaudalDesFondo').cast('float').alias('hidroh_valor'))
        caudalEntradaHora = dvhidrologia.select('agt_id_fk',lit('CaudalEntradaHora').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('CaudalEntradaHora').cast('float').alias('hidroh_valor'))
        caudalEntradaProm = dvhidrologia.select('agt_id_fk',lit('CaudalEntradaProm').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('CaudalEntradaProm').cast('float').alias('hidroh_valor'))
        caudalLateralProm = dvhidrologia.select('agt_id_fk',lit('CaudalLateralProm').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('CaudalLateralProm').cast('float').alias('hidroh_valor'))
        caudalTurbinado = dvhidrologia.select('agt_id_fk',lit('CaudalTurbinado').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('CaudalTurbinado').cast('float').alias('hidroh_valor'))
        caudalVertido = dvhidrologia.select('agt_id_fk',lit('CaudalVertido').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('CaudalVertido').cast('float').alias('hidroh_valor'))
        cotaDescarga = dvhidrologia.select('agt_id_fk',lit('CotaDescarga').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('CotaDescarga').cast('float').alias('hidroh_valor'))
        nivelProm = dvhidrologia.select('agt_id_fk',lit('Nivel_Prom').alias('parametro'),
                                         'hora_id_fk','tmpo_id_fk',col('Nivel_Prom').cast('float').alias('hidroh_valor'))
        
        dvhidrologia = alturaNeta.union(caudalDesFondo).union(caudalEntradaHora).union(caudalEntradaProm)\
        .union(caudalLateralProm).union(caudalTurbinado).union(caudalVertido).union(cotaDescarga).union(nivelProm)\
        .withColumn('fecha_carga',lit(fecha_actual))
        
        dvhidrologia = dvhidrologia\
        .join(parametros, dvhidrologia.parametro == parametros.paramhid_id_bk, how='left')\
        .select('agt_id_fk',parametros.paramhid_id_pk.alias('paramhid_id_fk'),'hora_id_fk','tmpo_id_fk','hidroh_valor',
                dvhidrologia.fecha_carga)
        
        
        faltantes = dvhidrologia.filter((dvhidrologia.paramhid_id_fk.isNull()))
        
        if(len(faltantes.head(1))!=0):
            return False 
        
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
    