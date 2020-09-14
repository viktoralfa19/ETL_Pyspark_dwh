from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from GetPosiciones import GetPosiciones
from Dbscan import Dbscan

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarPosiciones():
    """Clase que permite realizar la extracción, limpieza y validación de datos de potencia aparente de posiciones."""
    
    def Obtener_Posiciones(fecha_inicio,fecha_fin,posicion=None):
        getPosiciones = GetPosiciones(TableName = '')
        posiciones = getPosiciones.GetData(fecha_inicio,fecha_fin,fileName='posiciones_*',posicion=posicion)
        return posiciones

    def Obtener_Calidad_Posiciones(fecha_inicio,fecha_fin,posicion=None):
        # Datos De posiciones (DataFrame Pyspark)
        getPosiciones = GetPosiciones(TableName = '')
        posiciones = getPosiciones.GetDataQuality(fecha_inicio,fecha_fin,fileName='calidad_posiciones_*',posicion=posicion)
        return posiciones
    
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

        outliers,datos_limpios = LimpiarPosiciones.Limpiar_Datos(salida.set_index('Fecha'))

        return (id, [outliers,datos_limpios,salida])
    
    
    
    def Completar_Datos(x,fecha_inicio,fecha_fin):
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

        calidad_posiciones = LimpiarPosiciones.Obtener_Calidad_Posiciones(fecha_inicio,fecha_fin,posicion=id)

        calidad_posiciones = calidad_posiciones.toPandas().set_index('Fecha')[['Calidad','Proteccion','TagCalidad','TagPotencia']]

        resultado = pd.merge(calidad_posiciones,resultado,on='Fecha')

        resultado_segundo = resultado_segundo.reset_index().groupby('Potencia')['Fecha'].min().reset_index()\
        .set_index('Potencia').rename(columns={'Fecha':'FechaMaximo'})
        resultado = resultado.reset_index().set_index('Potencia')
        resultado = pd.merge(resultado_segundo,resultado,on='Potencia')
        resultado = resultado.reset_index().set_index('Fecha').sort_index().reset_index()

        return (id,resultado)
    
    
    def Algoritmo_Validacion_Datos(df):         
        df = df\
        .select(col('Fecha'),
                col('Potencia'),
                col('Calidad'),
                when(col('Proteccion') == None ,0).otherwise(col('Proteccion')).alias('Proteccion'),
                col('TagCalidad'),
                col('TagPotencia'),
                col('Elemento'),
                col('FechaMaximo'))\
        .withColumn('Cargabilidad',when(col('Proteccion')==0,0).otherwise(func.round(col('Potencia')/col('Proteccion'),5)))
        
        return df
    
    def Integridad_Datos(df,agentes,tiempo,hora,fecha_inicio,fecha_fin,accesoDatos,table):
        agentes = agentes.filter((col('agt_clase_unegocio_id_bk')=='TRA') & (col('agt_tipo_elemento_id_bk')==0))
        fecha_actual = datetime.datetime.now()
        df = df\
        .join(tiempo, date_format(df.Fecha,'yyyyMMdd').cast('int')==tiempo.tmpo_id_pk)\
        .join(hora, date_format(df.Fecha,'HHmm').cast('smallint')==hora.hora_id_pk)\
        .join(agentes, df.Elemento == agentes.agt_elemento_id_bk,how='left')\
        .select(date_format(df.Fecha,'yyyyMMdd').cast('int').alias('tmpo_id_fk'),
                date_format(df.Fecha,'HHmm').cast('smallint').alias('hora_id_fk'),
                agentes.agt_id_pk.alias('agt_id_fk'),
                df.Potencia.alias('pos_potencia_aparente'),
                df.Cargabilidad.alias('pos_cargabilidad'),
                df.Proteccion.alias('pos_proteccion'),
                df.FechaMaximo.alias('pos_fecha_maximo'),
                df.TagPotencia.alias('pos_tag_potencia'),
                df.Calidad.alias('pos_calidad'),
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
                 d[3],d[4],d[5],d[6],d[7]) for d in list(x)]
    
    #def Procesamiento(fecha_inicio,fecha_fin,accesoDatos,genericDataFrame,sc,table):
    def Procesamiento(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        """Lógica de negocio para realizar la limpieza de datos de posiciones."""
        dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
        accesoDatos = EtlAL(dbContext)
        genericDataFrame = GenericDataFrame(HDFSContext(Path='PI_INTEGRATOR',DataBase='TRANSMISION',Schema=''))
        sc = genericDataFrame.spark.sparkContext
        tableName = 'cen_dws.fact_posicion'
        
        _accesoDatos = accesoDatos
        _genericDataFrame = genericDataFrame
        _sc = sc
        _table = tableName

        if(fecha_inicio is None or fecha_fin is None):
            fecha_inicio = datetime.datetime.combine(datetime.date.today(),datetime.datetime.min.time()) 
            fecha_fin = fecha_inicio + timedelta(seconds=86399)

        posiciones = LimpiarPosiciones.Obtener_Posiciones(fecha_inicio,fecha_fin)

        if((posiciones.count()==0)):
            print('No existe Datos a Procesar. '+str(datetime.datetime.now()))
            return False

        #Procesamos por cada posicion para limpiar - Datos Posicion
        posiciones_map_origen = posiciones.rdd.map(lambda x: (x.Posicion,[x.Id,x.Fecha,x.Potencia,x.Calidad]))
        posiciones_map_group_origen = posiciones_map_origen.groupByKey()
        posiciones_pandas_origen = posiciones_map_group_origen.map(lambda x: LimpiarPosiciones.Limpiar(x))
        posiciones_pandas_origen = posiciones_pandas_origen.map(lambda x: LimpiarPosiciones.Completar_Datos(x,
                                                                                                           fecha_inicio,
                                                                                                           fecha_fin))

        #Unimos ambos RDDs
        datos_completos = posiciones_pandas_origen.map(lambda x: LimpiarPosiciones.ConvertirArray(x))
        datos_completos = datos_completos.flatMap(lambda x: LimpiarPosiciones.SepararDatos(x))

        schema = StructType([StructField('Fecha', TimestampType(), False),
                             StructField('Potencia', FloatType(), False),
                             StructField('FechaMaximo', TimestampType(), False),
                             StructField('Calidad', StringType(), False),
                             StructField('Proteccion', FloatType(), False),
                             StructField('TagCalidad', StringType(), False),
                             StructField('TagPotencia', StringType(), False),
                             StructField('Elemento', StringType(), False)])

        df = _genericDataFrame.spark.createDataFrame(datos_completos,schema)
        df = LimpiarPosiciones.Algoritmo_Validacion_Datos(df)
        df_elementos = _accesoDatos.GetAllData('cen_dws.dim_agente')
        df_tiempo = _accesoDatos.GetAllData('cen_dws.dim_tiempo')
        df_horas = _accesoDatos.GetAllData('cen_dws.dim_hora')
        
        # Validación de la existencia de agentes e ingreso de información
        result = LimpiarPosiciones.Integridad_Datos(df,df_elementos,df_tiempo,df_horas,fecha_inicio,fecha_fin,_accesoDatos,_table)
        
        if result == True: 
            mensaje = " **** EXITOSO: Datos insertados correctamente en la dimensión de {0}.**** ".format(_table)
        else: 
            mensaje = " **** ERROR: Error al insertar datos en la dimensión de {0}.****".format(_table)
            
        print(mensaje)
        
        if(deteleTmpQuery is not None):
            query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
            accesoDatos.Delete(query)
        
        return True