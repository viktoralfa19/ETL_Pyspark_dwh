from ExceptionManager import ExceptionManager
from HDFSContext import HDFSContext
from GenericDataFrame import GenericDataFrame
from DBContextDw import DBContextDw
from EtlAL import EtlAL
from Queries import Queries
from GetBarras import GetBarras
from Dbscan import Dbscan

from pyspark.sql.types import StructType,StructField,TimestampType,FloatType,StringType,IntegerType
import pyspark.sql.functions as func
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, date_format, lit
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np

class LimpiarBarras():
    """Clase que permite realizar la extracción, limpieza y validación de datos de voltaje de barras."""
    
    def Obtener_Barras(fecha_inicio,fecha_fin,barra=None):
        getBarras = GetBarras(TableName = '')
        barras = getBarras.GetData(fecha_inicio,fecha_fin,fileName='barras_*',barra=barra)
        return barras

    def Obtener_Calidad_Barras(fecha_inicio,fecha_fin,barra=None):
        getBarras = GetBarras(TableName = '')
        barras = getBarras.GetDataQuality(fecha_inicio,fecha_fin,fileName='calidad_barras_*',barra=barra)
        return barras
    
    def Limpiar_Datos(datosDestino):
        dbscan = Dbscan(datosDestino)
        dbscan.Limpiar_outliers(dimension='Voltaje')
        dbscan.Resumen_Datos(dimension='Voltaje')
        return dbscan.outliers,dbscan.datos_limpios
        
    def Limpiar(x):
        id = x[0]
        datos = list(x[1])
        salida = pd.DataFrame()
        ids = []
        fechas = []
        voltaje = []
        calidad = []
        for i in range(0,len(datos)):
            ids.append(datos[i][0])
            fechas.append(datos[i][1])
            voltaje.append(datos[i][2])
            calidad.append(datos[i][3])

        salida['Id'] = ids
        salida['Fecha'] = fechas
        salida['Voltaje'] = voltaje
        salida['Calidad'] = calidad

        outliers,datos_limpios = LimpiarBarras.Limpiar_Datos(salida.set_index('Fecha'))

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

        resultado = resultado[['Id','Fecha_x','Voltaje_x','Calidad']].rename(columns={'Fecha_x':'Fecha','Voltaje_x':'Voltaje'})

        maximo = resultado['Id'].max()#.values[-1]

        resultado = pd.merge(puntoInicial,resultado,on='Fecha',how='outer')
        resultado = pd.merge(puntoFinal,resultado,on='Fecha',how='outer')

        values = {'Fecha':datetime.datetime(1970,1,1,0,0,0),'Id':maximo,'Voltaje':0,'Calidad':'Normal'}
        resultado = resultado.fillna(value=values)
        resultado = resultado.set_index('Fecha').resample('S').mean()
        resultado = resultado[1:-1]
        resultado = resultado.interpolate(method='values')
        resultado = resultado.interpolate(method='values',limit_direction='backward')

        resultado_segundo = resultado.copy()
        resultado_max_min = resultado.copy()
        
        resultado_max = resultado.resample('T').max()
        resultado_max['Elemento'] = id
        
        resultado_min = resultado.resample('T').min()
        resultado_min['Elemento'] = id

        calidad_barras = LimpiarBarras.Obtener_Calidad_Barras(fecha_inicio,fecha_fin,barra=id)
        calidad_barras = calidad_barras.toPandas().set_index('Fecha')[['Calidad','TagCalidad','TagVoltaje']]

        resultado_max = pd.merge(calidad_barras,resultado_max,on='Fecha')
        resultado_min = pd.merge(calidad_barras,resultado_min,on='Fecha')

        
        resultado_segundo = resultado_segundo.reset_index().groupby('Voltaje')['Fecha'].min().reset_index()\
        .set_index('Voltaje').rename(columns={'Fecha':'FechaMaximoMinimo'})
        
        resultado_max = resultado_max.reset_index().set_index('Voltaje')
        resultado_max = pd.merge(resultado_segundo,resultado_max,on='Voltaje')
        
        resultado_min = resultado_min.reset_index().set_index('Voltaje')
        resultado_min = pd.merge(resultado_segundo,resultado_min,on='Voltaje')
        
        resultado_max = resultado_max.reset_index().set_index('Fecha').sort_index().reset_index()
        resultado_min = resultado_min.reset_index().set_index('Fecha').sort_index().reset_index()

        return (id,[resultado_max,resultado_min])
    
    
    def Algoritmo_Validacion_Datos(df_max,df_min,barra,voltaje_max_min):    
        barras_max_min = barra.join(voltaje_max_min, barra.VoltajeID==voltaje_max_min.Voltaje_ID)\
        .filter(voltaje_max_min.Nombre.like('%Normal%'))\
        .select(barra.Codigo,
                voltaje_max_min.ValorMinimo,
                voltaje_max_min.ValorMaximo,
                voltaje_max_min.FechaInicial,
                voltaje_max_min.FechaCaducidad)
        
        schema_max = StructType([StructField('FechaMax', TimestampType(), False),
                             StructField('VoltajeMax', FloatType(), False),
                             StructField('FechaMaximo', TimestampType(), False),
                             StructField('CalidadMax', StringType(), False),
                             StructField('TagCalidadMax', StringType(), False),
                             StructField('TagVoltajeMax', StringType(), False),
                             StructField('ElementoMax', StringType(), False)])
        
        
        df = df_max.join(df_min, (df_max.FechaMax==df_min.FechaMin) & (df_max.ElementoMax==df_min.ElementoMin))\
        .select(when(col('FechaMax').isNull(),col('FechaMin')).otherwise(col('FechaMax')).alias('Fecha'),
                when(col('ElementoMax').isNull(),col('ElementoMin')).otherwise(col('ElementoMax')).alias('Elemento'),
                col('VoltajeMax'),col('VoltajeMin'),
                col('FechaMaximo').alias('FechaMax'),col('FechaMinimo').alias('FechaMin'),
                col('CalidadMax'),col('CalidadMin'),
                col('TagVoltajeMax'),col('TagVoltajeMin'))
        
        df = df.join(barras_max_min, (df.Elemento==barras_max_min.Codigo) &\
                     (df.Fecha>=barras_max_min.FechaInicial) &\
                     (df.Fecha<=barras_max_min.FechaCaducidad))\
        .select(df.Fecha,df.Elemento,df.VoltajeMax,df.VoltajeMin,df.FechaMax,df.FechaMin,
                df.CalidadMax,df.CalidadMin,df.TagVoltajeMax,df.TagVoltajeMin,
                barras_max_min.ValorMaximo.cast('float').alias('LimiteMax'),
                barras_max_min.ValorMinimo.cast('float').alias('LimiteMin'),
                when(df.VoltajeMax>barras_max_min.ValorMaximo,lit('S')).otherwise(lit('-')).alias('Superior'),
                when(df.VoltajeMin<barras_max_min.ValorMinimo,lit('I')).otherwise(lit('-')).alias('Inferior'))
        
        return df
    
    def Integridad_Datos(df,agentes,tiempo,hora,fecha_inicio,fecha_fin,accesoDatos,table):
        agentes = agentes.filter((col('agt_clase_unegocio_id_bk')=='TRA') & (col('agt_tipo_elemento_id_bk')==2))
        fecha_actual = datetime.datetime.now()
        df = df\
        .join(tiempo, date_format(df.Fecha,'yyyyMMdd').cast('int')==tiempo.tmpo_id_pk)\
        .join(hora, date_format(df.Fecha,'HHmm').cast('smallint')==hora.hora_id_pk)\
        .join(agentes, df.Elemento == agentes.agt_elemento_id_bk,how='left')\
        .select(date_format(df.Fecha,'yyyyMMdd').cast('int').alias('tmpo_id_fk'),
                date_format(df.Fecha,'HHmm').cast('smallint').alias('hora_id_fk'),
                agentes.agt_id_pk.alias('agt_id_fk'),
                
                df.VoltajeMax.alias('bar_voltaje_max'),
                df.VoltajeMin.alias('bar_voltaje_min'),
                
                df.LimiteMax.alias('bar_limite_max'),
                df.LimiteMin.alias('bar_limite_min'),

                df.FechaMax.alias('bar_fecha_maximo'),
                df.FechaMin.alias('bar_fecha_minimo'),
                
                df.TagVoltajeMax.alias('bar_tag_voltaje_max'),
                df.TagVoltajeMin.alias('bar_tag_voltaje_min'),
                
                df.CalidadMax.alias('bar_calidad_max'),
                df.CalidadMin.alias('bar_calidad_min'),
                
                df.Superior.alias('bar_superior'),
                df.Inferior.alias('bar_inferior'),
                
                lit(fecha_actual).alias('fecha_carga'))

        faltantes = df.filter(agentes.agt_id_pk.isNull())

        #if(~faltantes.rdd.isEmpty()):
        #    return None

        # Almacenamiento
        result = accesoDatos.Insert(df,table)

        return result
    
    def ConvertirArray(x):
        datos = list(x[1])
        maximos = datos[0]
        minimos = datos[1]
        return ([maximos.to_numpy(),minimos.to_numpy()])
        
    def SepararDatos(x):
        return [(datetime.datetime.strptime(datetime.datetime.strftime(d[0],'%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S'),
                 d[1],datetime.datetime.strptime(datetime.datetime.strftime(d[2],'%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S'),
                 d[3],d[4],d[5],d[6]) for d in list(x)]
    
    #def Procesamiento(fecha_inicio,fecha_fin,accesoDatos,genericDataFrame,sc,table):
    def Procesamiento(fecha_inicio,fecha_fin,modulo,deteleTmpQuery):
        """Lógica de negocio para realizar la limpieza de datos de posiciones."""
        dbContext = DBContextDw(Database='dwh_sirio',urlDriver='/home/jovyan/work/postgresql-42.2.12.jar')
        accesoDatos = EtlAL(dbContext)
        genericDataFrame = GenericDataFrame(HDFSContext(Path='PI_INTEGRATOR',DataBase='TRANSMISION',Schema=''))
        sc = genericDataFrame.spark.sparkContext
        tableName = 'cen_dws.fact_barra'
        
        _accesoDatos = accesoDatos
        _genericDataFrame = genericDataFrame
        _sc = sc
        _table = tableName

        if(fecha_inicio is None or fecha_fin is None):
            fecha_inicio = datetime.datetime.combine(datetime.date.today(),datetime.datetime.min.time()) 
            fecha_fin = fecha_inicio + timedelta(seconds=86399)

        barras = LimpiarBarras.Obtener_Barras(fecha_inicio,fecha_fin)
        genericDataFrame = GenericDataFrame(HDFSContext(DataBase='SIVO'))
        barra = genericDataFrame.GetDataHdfs('CFG_Barra','file_CFG_Barra')
        voltaje_max_min = genericDataFrame.GetDataHdfs('CFG_Voltajes_MAX_MIN','file_CFG_Voltajes_MAX_MIN')

        #barra.show()
        #voltaje_max_min.show()
        
        if((barras.count()==0)):
            print('No existe Datos a Procesar. '+str(datetime.datetime.now()))
            return False

        #Procesamos por cada posicion para limpiar - Datos Posicion
        posiciones_map_origen = barras.rdd.map(lambda x: (x.Barra,[x.Id,x.Fecha,x.Voltaje,x.Calidad]))
        posiciones_map_group_origen = posiciones_map_origen.groupByKey()
        posiciones_pandas_origen = posiciones_map_group_origen.map(lambda x: LimpiarBarras.Limpiar(x))
        posiciones_pandas_origen = posiciones_pandas_origen.map(lambda x: LimpiarBarras.Completar_Datos(x,fecha_inicio,fecha_fin))

        #for d in posiciones_pandas_origen.collect():
        #    print(d[0],d[1][0],d[1][1])
          
        
        #Unimos ambos RDDs
        datos_completos = posiciones_pandas_origen.map(lambda x: LimpiarBarras.ConvertirArray(x))
  
        datos_completos_max = datos_completos.flatMap(lambda x: LimpiarBarras.SepararDatos(x[0]))
        datos_completos_min = datos_completos.flatMap(lambda x: LimpiarBarras.SepararDatos(x[1]))

        
        schema_max = StructType([StructField('FechaMax', TimestampType(), False),
                             StructField('VoltajeMax', FloatType(), False),
                             StructField('FechaMaximo', TimestampType(), False),
                             StructField('CalidadMax', StringType(), False),
                             StructField('TagCalidadMax', StringType(), False),
                             StructField('TagVoltajeMax', StringType(), False),
                             StructField('ElementoMax', StringType(), False)])
        
        schema_min = StructType([StructField('FechaMin', TimestampType(), False),
                             StructField('VoltajeMin', FloatType(), False),
                             StructField('FechaMinimo', TimestampType(), False),
                             StructField('CalidadMin', StringType(), False),
                             StructField('TagCalidadMin', StringType(), False),
                             StructField('TagVoltajeMin', StringType(), False),
                             StructField('ElementoMin', StringType(), False)])

        
        df_max = _genericDataFrame.spark.createDataFrame(datos_completos_max,schema_max)
        df_min = _genericDataFrame.spark.createDataFrame(datos_completos_min,schema_min)
        
        df = LimpiarBarras.Algoritmo_Validacion_Datos(df_max,df_min,barra,voltaje_max_min)
        
        #df.printSchema()
        #print(df.columns)
        
        df_elementos = _accesoDatos.GetAllData('cen_dws.dim_agente')
        df_tiempo = _accesoDatos.GetAllData('cen_dws.dim_tiempo')
        df_horas = _accesoDatos.GetAllData('cen_dws.dim_hora')
        
        
        # Validación de la existencia de agentes e ingreso de información
        result = LimpiarBarras.Integridad_Datos(df,df_elementos,df_tiempo,df_horas,fecha_inicio,fecha_fin,_accesoDatos,_table)
        
        if result == True: 
            mensaje = " **** EXITOSO: Datos insertados correctamente en la dimensión de {0}.**** ".format(_table)
        else: 
            mensaje = " **** ERROR: Error al insertar datos en la dimensión de {0}.****".format(_table)
            
        print(mensaje)
        
        if(deteleTmpQuery is not None):
            query = deteleTmpQuery.format(fecha_inicio,fecha_fin,modulo)
            accesoDatos.Delete(query)
        
        return True