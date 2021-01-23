class Refactorizar:
    """Contiene metodos auxiliares del negocio"""

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

    def LimpiezaPeriodos(df_agentes,df_indisponibilidad_falla,df_indisponibilidad_mant):
        df_agentes  = df_agentes.filter((df_agentes.agtsni_clase=='TRANSMISIÓN')&\
                                        (df_agentes.agtsni_tipo_elemento!='BARRA'))

        formato = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        tiempoIndisponible = \
        (unix_timestamp(to_timestamp('FechaDispon','yyyy-MM-dd HH:mm:ss'), format=formato)
        - unix_timestamp(to_timestamp('Fecha','yyyy-MM-dd HH:mm:ss'), format=formato))

        df_indisponibilidad_falla = df_indisponibilidad_falla\
        .join(df_agentes, df_indisponibilidad_falla.agt_id_fk == df_agentes.agtsni_id_pk)\
        .select(df_agentes.agtsni_id_pk.alias('AgenteId'),
                df_agentes.agtsni_empresa_id_bk.alias('EmpresaCodigo'),
                df_agentes.agtsni_unidad_negocio_id_bk.alias('UNegocioCodigo'),
                df_agentes.agtsni_clase.alias('Clase'),
                df_agentes.agtsni_estacion_id_bk.alias('EstacionCodigo'),
                df_agentes.agtsni_nivel_voltaje_id_bk.alias('VoltajeCodigo'),
                df_agentes.agtsni_tipo_elemento_id_bk.alias('TipoCodigo'),
                df_agentes.agtsni_tipo_elemento.alias('Tipo'),
                when(df_agentes.agtsni_tipo_elemento_id_bk.isin({0,5}), 2)\
                .otherwise(when(df_agentes.agtsni_tipo_elemento_id_bk.isin({3,4}), 4)\
                .otherwise(0)).alias('LHI'),
                when(df_agentes.agtsni_tipo_elemento_id_bk.isin({0,5,4}), 1)\
                .otherwise(when(df_agentes.agtsni_tipo_elemento_id_bk.isin({3}), 2)\
                .otherwise(0)).alias('NDP'),
                df_agentes.agtsni_elemento_id_bk.alias('ElementoCodigo'),
                lit(0).alias('Mantenimiento'),
                df_indisponibilidad_falla.tmp_id_pk.alias('FechaId'),
                df_indisponibilidad_falla.tmp_fecha.alias('Fecha'),
                df_indisponibilidad_falla.tmp_anio.alias('Anio'),
                df_indisponibilidad_falla.tmp_semestre.alias('Semestre'),
                df_indisponibilidad_falla.tmpd_id_pk.alias('FechaDisponId'),
                df_indisponibilidad_falla.tmpd_fecha.alias('FechaDispon'),
                df_indisponibilidad_falla.falla_tmp_indisponibilidad.alias('Indispon'))\
        .withColumn('TiempoSegundos',tiempoIndisponible)


        df_indisponibilidad_mant = df_indisponibilidad_mant\
        .join(df_agentes,
              (df_indisponibilidad_mant.empresa_codigo == df_agentes.agtsni_empresa_id_bk)&\
              (df_indisponibilidad_mant.unegocio_codigo == df_agentes.agtsni_unidad_negocio_id_bk)&\
              (df_indisponibilidad_mant.clase == df_agentes.agtsni_clase)&\
              (df_indisponibilidad_mant.estacion_codigo == df_agentes.agtsni_estacion_id_bk)&\
              (df_indisponibilidad_mant.voltaje_codigo == df_agentes.agtsni_nivel_voltaje_id_bk)&\
              (df_indisponibilidad_mant.tipo_elemento_codigo == df_agentes.agtsni_tipo_elemento_id_bk)&\
              (df_indisponibilidad_mant.elemento_codigo == df_agentes.agtsni_elemento_id_bk))\
        .select(df_agentes.agtsni_id_pk.alias('AgenteId'),
                df_agentes.agtsni_empresa_id_bk.alias('EmpresaCodigo'),
                df_agentes.agtsni_unidad_negocio_id_bk.alias('UNegocioCodigo'),
                df_agentes.agtsni_clase.alias('Clase'),
                df_agentes.agtsni_estacion_id_bk.alias('EstacionCodigo'),
                df_agentes.agtsni_nivel_voltaje_id_bk.alias('VoltajeCodigo'),
                df_agentes.agtsni_tipo_elemento_id_bk.alias('TipoCodigo'),
                df_agentes.agtsni_tipo_elemento.alias('Tipo'),
                when(df_agentes.agtsni_tipo_elemento_id_bk.isin({0,5}), 2)\
                .otherwise(when(df_agentes.agtsni_tipo_elemento_id_bk.isin({3,4}), 4)\
                .otherwise(0)).alias('LHI'),
                when(df_agentes.agtsni_tipo_elemento_id_bk.isin({0,5,4}), 1)\
                .otherwise(when(df_agentes.agtsni_tipo_elemento_id_bk.isin({3}), 2)\
                .otherwise(0)).alias('NDP'),
                df_agentes.agtsni_elemento_id_bk.alias('ElementoCodigo'),
                df_indisponibilidad_mant.codigo_mantenimiento.alias('Mantenimiento'),
                df_indisponibilidad_mant.fecha_id.alias('FechaId'),
                df_indisponibilidad_mant.fecha.alias('Fecha'),
                df_indisponibilidad_mant.anio.alias('Anio'),
                df_indisponibilidad_mant.semestre.alias('Semestre'),
                df_indisponibilidad_mant.fechad_id.alias('FechaDisponId'),
                df_indisponibilidad_mant.fechad.alias('FechaDispon'),
                df_indisponibilidad_mant.tiempo_indispon.alias('Indispon'))\
        .withColumn('TiempoSegundos',tiempoIndisponible)


        df_indisponibilidad = df_indisponibilidad_falla.union(df_indisponibilidad_mant).orderBy('Anio','Semestre','AgenteId','Fecha')

        ############################ CALCULO DE LOS TIEMPOS DE INDISPONIBILIDAD DE FALLAS
        df_indisponibilidad_falla = df_indisponibilidad_falla\
        .groupby('Anio','Semestre','AgenteId','LHI','NDP')\
        .agg(func.sum('TiempoSegundos').alias('TiempoSegundos'),
             func.count('AgenteId').alias('NumeroDisparos'))

        ############################ CALCULO DE LOS TIEMPOS DE INDISPONIBILIDAD DE MANTENIMIENTOS
        df_indisponibilidad_mant = df_indisponibilidad_mant\
        .groupby('Anio','Semestre','AgenteId','LHI','NDP')\
        .agg(func.sum('TiempoSegundos').alias('TiempoSegundos'),
             func.count('Mantenimiento').alias('NumeroDisparos'))

        ############################ CALCULO DE LOS TIEMPOS DE INDISPONIBILIDAD TOTAL
        dispon_anual_semestral = df_indisponibilidad.rdd\
        .map(lambda x: (str(x.Anio)+'-'+str(x.Semestre)+'-'+str(x.AgenteId),[x.Fecha,x.FechaDispon]))

        dispon_anual_semestral_group = dispon_anual_semestral.groupByKey()

        datos_limpios = dispon_anual_semestral_group.map(lambda x: Refactorizar.Verificacion(x))

        datos_limpios_calculados = datos_limpios.map(lambda x: Refactorizar.CalcularSuma(x))

        datos_totales = datos_limpios_calculados.map(lambda x: (x[0].split('-')[0],x[0].split('-')[1],x[0].split('-')[2],x[1]))

        datos_totales_indisponibilidad = datos_totales.toDF(['Anio', 'Semestre', 'AgenteId', 'TiempoSegundos'])

        ############################ UNION DE TIEMPOS DE INDISPONIBILIDAD TOTAL
        datos_totales_indisponibilidad = datos_totales_indisponibilidad\
        .join(df_indisponibilidad_falla,
              (datos_totales_indisponibilidad.Anio==df_indisponibilidad_falla.Anio) & \
              (datos_totales_indisponibilidad.Semestre==df_indisponibilidad_falla.Semestre) & \
              (datos_totales_indisponibilidad.AgenteId==df_indisponibilidad_falla.AgenteId), how='left')\
        .join(df_indisponibilidad_mant,
              (datos_totales_indisponibilidad.Anio==df_indisponibilidad_mant.Anio) & \
              (datos_totales_indisponibilidad.Semestre==df_indisponibilidad_mant.Semestre) & \
              (datos_totales_indisponibilidad.AgenteId==df_indisponibilidad_mant.AgenteId), how='left')\
        .select(datos_totales_indisponibilidad.Anio,
                datos_totales_indisponibilidad.Semestre,
                datos_totales_indisponibilidad.AgenteId,
                when(datos_totales_indisponibilidad.TiempoSegundos.isNull(),0)\
                .otherwise(datos_totales_indisponibilidad.TiempoSegundos/3600.0).alias('TiempoTotal'),
                when(df_indisponibilidad_falla.TiempoSegundos.isNull(),0)\
                .otherwise((df_indisponibilidad_falla.TiempoSegundos/3600.0)).alias('TiempoFallas'),
                when(df_indisponibilidad_mant.TiempoSegundos.isNull(),0)\
                .otherwise((df_indisponibilidad_mant.TiempoSegundos/3600.0)).alias('TiempoMant'),
                when(df_indisponibilidad_falla.NumeroDisparos.isNull(),0)\
                .otherwise(df_indisponibilidad_falla.NumeroDisparos).alias('DisparosFallas'),
                when(df_indisponibilidad_mant.NumeroDisparos.isNull(),0)\
                .otherwise(df_indisponibilidad_mant.NumeroDisparos).alias('DisparosMant'),
                when(df_indisponibilidad_falla.LHI.isNull(),df_indisponibilidad_mant.LHI)\
                .otherwise(df_indisponibilidad_falla.LHI).alias('Lhi'),
                when(df_indisponibilidad_falla.NDP.isNull(),df_indisponibilidad_mant.NDP)\
                .otherwise(df_indisponibilidad_falla.NDP).alias('Ndp'))\
        .withColumn('FactorLhi',(col('TiempoTotal')-col('Lhi'))/col('Lhi'))\
        .withColumn('FactorNdp',((col('DisparosFallas')+col('DisparosMant'))-col('Ndp'))/col('Ndp'))\
        .withColumn('FCS',when((col('FactorLhi')<=0)&(col('FactorNdp')<=0),0)\
                    .otherwise(when((col('FactorLhi')<=0)&(col('FactorNdp')>0),1+col('FactorNdp'))\
                    .otherwise(when((col('FactorLhi')>0)&(col('FactorNdp')<=0),1+col('FactorLhi'))\
                    .otherwise(1+col('FactorLhi')+col('FactorNdp')))))

        return datos_totales_indisponibilidad
        
    def Verificacion(x):
        id = x[0]
        fechas = x[1]
        i=0
        for fecha in fechas:
            if (i==0):
                i+=1
                fecha_anterior = fecha
                continue
            else:
                if((fecha[0]>=fecha_anterior[0]) & (fecha[1]<=fecha_anterior[1])):
                    fecha[0]=fecha[1]
                    i+=1
                    continue
                elif((fecha[0]>=fecha_anterior[1])):
                    i+=1
                    continue
                else:
                    fecha[0]=fecha_anterior[1]
                    i+=1
        return (id,fechas)
            
    @staticmethod
    def CalcularSuma(x):
        id = x[0]
        fechas = x[1]
        tiempo = 0
        for fecha in fechas:
            tiempo += (fecha[1] - fecha[0]).total_seconds()
        return (id,tiempo)