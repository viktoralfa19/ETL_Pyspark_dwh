from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from GetCircuitos import GetCircuitos
from Dbscan import Dbscan

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarCircuitos():
    """Clase que permite realizar la extracción, limpieza y validación de datos de potencia aparente de circuitos."""
        
    def Obtener_Circuitos(fecha_inicio,fecha_fin,circuito=None,destino=False):
        getCircuitos = GetCircuitos(TableName = '')
        if(destino):
            circuitos = getCircuitos.GetDataDestino(fecha_inicio,fecha_fin,fileName='circuitos_destino_*',circuito=circuito)
        else:
            circuitos = getCircuitos.GetDataOrigen(fecha_inicio,fecha_fin,fileName='circuitos_origen_*',circuito=circuito)
        return circuitos

    def Obtener_Calidad_Circuitos(fecha_inicio,fecha_fin,circuito=None,destino=False):
        # Datos De circuitos (DataFrame Pyspark)
        getCircuitos = GetCircuitos(TableName = '')
        if(destino):
            circuitos = getCircuitos.GetDataQualityDestino(fecha_inicio,fecha_fin,fileName='calidad_circuitos_destino_*',circuito=circuito)
        else:
            circuitos = getCircuitos.GetDataQualityOrigen(fecha_inicio,fecha_fin,fileName='calidad_circuitos_origen_*',circuito=circuito)
        return circuitos
    
    def Limpiar_Datos(datosDestino):
        dbscan = Dbscan(datosDestino)
        dbscan.Limpiar_outliers()
        dbscan.Resumen_Datos()
        return dbscan.outliers,dbscan.datos_limpios
        
    def Limpiar(x):
        id = x[0]
        datos = list(x[1])
        salida = pd.DataFrame()
        ids = []
        fechas = []
        potencia = []
        calidad = []
        for i in range(0,len(datos)):
            ids.append(datos[i][0])
            fechas.append(datos[i][1])
            potencia.append(datos[i][2])
            calidad.append(datos[i][3])

        salida['Id'] = ids
        salida['Fecha'] = fechas
        salida['Potencia'] = potencia
        salida['Calidad'] = calidad

        outliers,datos_limpios = LimpiarCircuitos.Limpiar_Datos(salida.set_index('Fecha'))

        return (id, [outliers,datos_limpios,salida])
    
    
    
    def Completar_Datos(x,tipo,fecha_inicio,fecha_fin):
        id = x[0]
        datos = list(x[1])

        outliers = datos[0].reset_index()
        datos_limpios = datos[1].reset_index()
        datos_originales = datos[2]


        if(datos_limpios['Id'].count()==0):
            datos_limpios = outliers

        resultado = pd.merge(datos_originales,datos_limpios,on='Id')
        resultado = resultado[resultado['Calidad'].isin(['Normal','AL','AL.L5','AL.L6','L1'])]

        if(resultado['Id'].count()==0):
            resultado = pd.merge(datos_originales,datos_limpios,on='Id')


        puntoInicial = np.array([datetime.datetime(fecha_inicio.year,fecha_inicio.month,fecha_inicio.day,0,0,0) - timedelta(seconds=1)])
        puntoInicial = pd.DataFrame(puntoInicial,columns=['Fecha'])

        puntoFinal = np.array([datetime.datetime(fecha_inicio.year,fecha_inicio.month,fecha_inicio.day,23,59,59) + timedelta(seconds=1)])
        puntoFinal = pd.DataFrame(puntoFinal,columns=['Fecha'])

        resultado = resultado[['Id','Fecha_x','Potencia_x','Calidad']].rename(columns={'Fecha_x':'Fecha','Potencia_x':'Potencia'})

        maximo = resultado['Id'].max()#.values[-1]

        resultado = pd.merge(puntoInicial,resultado,on='Fecha',how='outer')
        resultado = pd.merge(puntoFinal,resultado,on='Fecha',how='outer')

        values = {'Fecha':datetime.datetime(1970,1,1,0,0,0),'Id':maximo,'Potencia':0,'Calidad':'Normal'}
        resultado = resultado.fillna(value=values)
        resultado = resultado.set_index('Fecha').resample('S').mean()
        resultado = resultado[1:-1]
        resultado = resultado.interpolate(method='values')
        resultado = resultado.interpolate(method='values',limit_direction='backward')

        resultado_segundo = resultado.copy()

        resultado = resultado.resample('T').max()
        resultado['Elemento'] = id
        resultado['Tipo'] = tipo

        if(tipo=='Origen'):
            calidad_circuitos = LimpiarCircuitos.Obtener_Calidad_Circuitos(fecha_inicio,fecha_fin,circuito=id,destino=False)
        else:
            calidad_circuitos = LimpiarCircuitos.Obtener_Calidad_Circuitos(fecha_inicio,fecha_fin,circuito=id,destino=True)

        calidad_circuitos = calidad_circuitos.toPandas().set_index('Fecha')[['Calidad','LimMaxOperacion',
                                                                             'LimOperacionContinuo','LimTermico',
                                                                             'TagCalidad','TagPotencia']]

        resultado = pd.merge(calidad_circuitos,resultado,on='Fecha')

        resultado_segundo = resultado_segundo.reset_index().groupby('Potencia')['Fecha'].min().reset_index()\
        .set_index('Potencia').rename(columns={'Fecha':'FechaMaximo'})
        resultado = resultado.reset_index().set_index('Potencia')
        resultado = pd.merge(resultado_segundo,resultado,on='Potencia')
        resultado = resultado.reset_index().set_index('Fecha').sort_index().reset_index()

        return (id,resultado)
    
    
    def Algoritmo_Validacion_Datos(df,deltaDiffCircuito):         
        df_origen = df.filter(col('Tipo')=='Origen')\
        .select(col('Fecha').alias('FechaOrigen'),
                col('Potencia').alias('PotenciaOrigen'),
                col('Calidad').alias('CalidadOrigen'),
                col('LimMaxOperacion').alias('LimMaxOperacionOrigen'),
                col('LimOperacionContinuo').alias('LimOperacionContinuoOrigen'),
                col('LimTermico').alias('LimTermicoOrigen'),
                col('TagCalidad').alias('TagCalidadOrigen'),
                col('TagPotencia').alias('TagPotenciaOrigen'),
                col('Elemento').alias('ElementoOrigen'),
                col('FechaMaximo').alias('FechaMaximoOrigen'))\
        .withColumn('SuperiorOrigen',func.round(col('PotenciaOrigen')/col('LimTermicoOrigen'),5))\
        .withColumn('CargabilidadOrigen',func.round(col('PotenciaOrigen')/col('LimOperacionContinuoOrigen'),5))

        df_destino = df.filter(col('Tipo')=='Destino')\
        .select(col('Fecha').alias('FechaDestino'),
                col('Potencia').alias('PotenciaDestino'),
                col('Calidad').alias('CalidadDestino'),
                col('LimMaxOperacion').alias('LimMaxOperacionDestino'),
                col('LimOperacionContinuo').alias('LimOperacionContinuoDestino'),
                col('LimTermico').alias('LimTermicoDestino'),
                col('TagCalidad').alias('TagCalidadDestino'),
                col('TagPotencia').alias('TagPotenciaDestino'),
                col('Elemento').alias('ElementoDestino'),
                col('FechaMaximo').alias('FechaMaximoDestino'))\
        .withColumn('SuperiorDestino',func.round(col('PotenciaDestino')/col('LimTermicoDestino'),5).cast('double'))\
        .withColumn('CargabilidadDestino',func.round(col('PotenciaDestino')/col('LimOperacionContinuoDestino'),5).cast('double'))

        df = df_origen.join(df_destino,
                            (df_origen.FechaOrigen==df_destino.FechaDestino) &\
                            (df_origen.ElementoOrigen==df_destino.ElementoDestino))\
        .select(when(col('FechaOrigen').isNull(),col('FechaDestino')).otherwise(col('FechaOrigen')).alias('Fecha'),
                
                when(col('ElementoOrigen').isNull(),col('ElementoDestino')).otherwise(col('ElementoOrigen')).alias('Elemento'),
                
                when(col('LimOperacionContinuoOrigen').isNull(),col('LimOperacionContinuoDestino'))\
                .otherwise(col('LimOperacionContinuoOrigen')).alias('LimOperacionContinuo'),
                
                when(col('LimTermicoOrigen').isNull(),col('LimTermicoDestino'))\
                .otherwise(col('LimTermicoOrigen')).alias('LimTermico'),
                
                when(col('LimMaxOperacionOrigen').isNull(),col('LimMaxOperacionDestino'))\
                .otherwise(col('LimMaxOperacionOrigen')).alias('LimMaxOperacion'),

                when(col('PotenciaOrigen')>=col('PotenciaDestino'),
                     when((col('PotenciaOrigen')-col('PotenciaDestino'))<deltaDiffCircuito,col('TagPotenciaOrigen'))\
                     .otherwise(col('TagPotenciaDestino')))\
                .otherwise(col('TagPotenciaDestino')).alias('TagPotencia'),

                when(col('PotenciaOrigen')>=col('PotenciaDestino'),
                     when((col('PotenciaOrigen')-col('PotenciaDestino'))<deltaDiffCircuito,col('PotenciaOrigen'))\
                     .otherwise(col('PotenciaDestino')))\
                .otherwise(col('PotenciaDestino')).alias('Potencia'),

                when(col('PotenciaOrigen')>=col('PotenciaDestino'),
                     when((col('PotenciaOrigen')-col('PotenciaDestino'))<deltaDiffCircuito,col('SuperiorOrigen'))\
                     .otherwise(col('SuperiorDestino')))\
                .otherwise(col('SuperiorDestino')).alias('Superior'),

                when(col('PotenciaOrigen')>=col('PotenciaDestino'),
                     when((col('PotenciaOrigen')-col('PotenciaDestino'))<deltaDiffCircuito,col('CargabilidadOrigen'))\
                     .otherwise(col('CargabilidadDestino')))\
                .otherwise(col('CargabilidadDestino')).alias('Cargabilidad'),

                when(col('PotenciaOrigen')>=col('PotenciaDestino'),
                     when((col('PotenciaOrigen')-col('PotenciaDestino'))<deltaDiffCircuito,col('FechaMaximoOrigen'))\
                     .otherwise(col('FechaMaximoDestino')))\
                .otherwise(col('FechaMaximoDestino')).alias('FechaMaximo'),

                when(col('PotenciaOrigen')>=col('PotenciaDestino'),
                     when((col('PotenciaOrigen')-col('PotenciaDestino'))<deltaDiffCircuito,col('CalidadOrigen'))\
                     .otherwise(col('CalidadDestino')))\
                .otherwise(col('CalidadDestino')).alias('Calidad')).orderBy('Elemento','Fecha')

        return df
    
    def Integridad_Datos(df,agentes,tiempo,hora,fecha_inicio,fecha_fin,accesoDatos,table):
        agentes = agentes.filter((col('agt_clase_unegocio_id_bk')=='TRA') & (col('agt_tipo_elemento_id_bk')==3))
        fecha_actual = datetime.datetime.now()
        df = df\
        .join(tiempo, date_format(df.Fecha,'yyyyMMdd').cast('int')==tiempo.tmpo_id_pk)\
        .join(hora, date_format(df.Fecha,'HHmm').cast('smallint')==hora.hora_id_pk)\
        .join(agentes, df.Elemento == agentes.agt_elemento_id_bk,how='left')\
        .select(date_format(df.Fecha,'yyyyMMdd').cast('int').alias('tmpo_id_fk'),
                date_format(df.Fecha,'HHmm').cast('smallint').alias('hora_id_fk'),
                agentes.agt_id_pk.alias('agt_id_fk'),
                df.Potencia.alias('crk_potencia_aparente'),
                df.Superior.alias('crk_superior'),
                df.Cargabilidad.alias('crk_cargabilidad'),
                df.LimTermico.alias('crk_limite_termico'),
                df.LimMaxOperacion.alias('crk_limite_max_operacion'),
                df.LimOperacionContinuo.alias('crk_limite_operacion_cont'),
                df.FechaMaximo.alias('crk_fecha_maximo'),
                df.TagPotencia.alias('crk_tag_potencia'),
                df.Calidad.alias('crk_calidad'),
                lit(fecha_actual).alias('fecha_carga'))

        faltantes = df.filter(agentes.agt_id_pk.isNull())

        #if(~faltantes.rdd.isEmpty()):
        #    return None

        # Almacenamiento
        result = accesoDatos.Insert(df,table)

        return result
    
    def ConvertirArray(x):
        return (x[1].to_numpy())
        
    def SepararDatos(x):
        return [(datetime.datetime.strptime(datetime.datetime.strftime(d[0],'%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S'),
                 d[1],datetime.datetime.strptime(datetime.datetime.strftime(d[2],'%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S'),
                 d[3],d[4],d[5],d[6],d[7],d[8],d[9],d[10]) for d in list(x)]
    
    #def Procesamiento(fecha_inicio,fecha_fin,accesoDatos,genericDataFrame,sc,deltaDiffCircuito,table):
    def Procesamiento(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        """Lógica de negocio para realizar la limpieza de datos de circuitos."""
        dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
        accesoDatos = EtlAL(dbContext)
        genericDataFrame = GenericDataFrame(HDFSContext(Path='PI_INTEGRATOR',DataBase='TRANSMISION',Schema=''))
        sc = genericDataFrame.spark.sparkContext
        deltaDiffCircuito = 110
        tableName = 'cen_dws.fact_circuito'

        _accesoDatos = accesoDatos
        _genericDataFrame = genericDataFrame
        _sc = sc
        _deltaDiffCircuito = deltaDiffCircuito
        _table = tableName

        if(fecha_inicio is None or fecha_fin is None):
            fecha_inicio = datetime.datetime.combine(datetime.date.today(),datetime.datetime.min.time()) 
            fecha_fin = fecha_inicio + timedelta(seconds=86399)

        circuitos_origen = LimpiarCircuitos.Obtener_Circuitos(fecha_inicio,fecha_fin,destino=False)
        circuitos_destino = LimpiarCircuitos.Obtener_Circuitos(fecha_inicio,fecha_fin,destino=True)

        if((circuitos_origen.count()==0) or (circuitos_destino.count()==0)):
            print('No existe Datos a Procesar. '+str(datetime.datetime.now()))
            return False

        #Procesamos por cada circuito para limpiar - Datos Origen
        circuitos_map_origen = circuitos_origen.rdd.map(lambda x: (x.Circuito,[x.Id,x.Fecha,x.Potencia,x.Calidad]))
        circuitos_map_group_origen = circuitos_map_origen.groupByKey()
        circuitos_pandas_origen = circuitos_map_group_origen.map(lambda x: LimpiarCircuitos.Limpiar(x))
        circuitos_pandas_origen = circuitos_pandas_origen.map(lambda x: LimpiarCircuitos.Completar_Datos(x,
                                                                                                         'Origen',
                                                                                                         fecha_inicio,
                                                                                                         fecha_fin))

        #Procesamos por cada circuito para limpiar - Datos Destino
        circuitos_map_destino = circuitos_destino.rdd.map(lambda x: (x.Circuito,[x.Id,x.Fecha,x.Potencia,x.Calidad]))
        circuitos_map_group_destino = circuitos_map_destino.groupByKey()
        circuitos_pandas_destino = circuitos_map_group_destino.map(lambda x: LimpiarCircuitos.Limpiar(x))
        circuitos_pandas_destino = circuitos_pandas_destino.map(lambda x: LimpiarCircuitos.Completar_Datos(x,
                                                                                                           'Destino',
                                                                                                           fecha_inicio,
                                                                                                           fecha_fin))

        #Unimos ambos RDDs
        datos_completos = _sc.union([circuitos_pandas_origen,circuitos_pandas_destino])
        datos_completos = datos_completos.map(lambda x: LimpiarCircuitos.ConvertirArray(x))
        datos_completos = datos_completos.flatMap(lambda x: LimpiarCircuitos.SepararDatos(x))

        schema = StructType([StructField('Fecha', TimestampType(), False),
                             StructField('Potencia', FloatType(), False),
                             StructField('FechaMaximo', TimestampType(), False),
                             StructField('Calidad', StringType(), False),
                             StructField('LimMaxOperacion', FloatType(), False),
                             StructField('LimOperacionContinuo', FloatType(), False),
                             StructField('LimTermico', FloatType(), False),
                             StructField('TagCalidad', StringType(), False),
                             StructField('TagPotencia', StringType(), False),
                             StructField('Elemento', StringType(), False),
                             StructField('Tipo', StringType(), False)])

        df = _genericDataFrame.spark.createDataFrame(datos_completos,schema)
        df = LimpiarCircuitos.Algoritmo_Validacion_Datos(df,_deltaDiffCircuito)
        df_elementos = _accesoDatos.GetAllData('cen_dws.dim_agente')
        df_tiempo = _accesoDatos.GetAllData('cen_dws.dim_tiempo')
        df_horas = _accesoDatos.GetAllData('cen_dws.dim_hora')
        
        # Validación de la existencia de agentes e ingreso de información
        result = LimpiarCircuitos.Integridad_Datos(df,df_elementos,df_tiempo,df_horas,fecha_inicio,fecha_fin,_accesoDatos,_table)
        
        if result == True: 
            mensaje = " **** EXITOSO: Datos insertados correctamente en la dimensión de {0}.**** ".format(_table)
        else: 
            mensaje = " **** ERROR: Error al insertar datos en la dimensión de {0}.****".format(_table)
            
        print(mensaje)
        
        if(deteleTmpQuery is not None):
            query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
            accesoDatos.Delete(query)
        
        return True
    