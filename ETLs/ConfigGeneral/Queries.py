#!/usr/bin/env python
# coding: utf-8

# In[ ]:


class Queries: 

    @staticmethod
    def Upsert_Query_Catalogos_Mant(x):
        
        pk = x[0]
        values = x[1]
        
        result = []
        
        query = "INSERT INTO cen_dws.dim_catalogos_mant"                " VALUES({0}, '{1}', '{2}', '{3}', '{4}')"                " ON CONFLICT (cat_id_bk) "                " DO"                   " UPDATE"                   " SET cat_catalogo = EXCLUDED.cat_catalogo,"                   " cat_parametro = EXCLUDED.cat_parametro,"                   " fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3])
        
        return (pk,query)
    
    
    @staticmethod
    def Upsert_Query_Dim_Agente(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_agente\
        VALUES({0}, '{1}', '{2}', '{3}', '{4}','{5}',\
        '{6}', '{7}', '{8}', '{9}', '{10}',{11},\
        '{12}', '{13}', '{14}', '{15}', '{16}',{17},\
        '{18}', '{19}', '{20}', '{21}', '{22}')\
        ON CONFLICT (agt_empresa_id_bk,agt_und_negocio_id_bk,agt_clase_unegocio_id_bk,agt_estacion_id_bk,agt_elemento_id_bk) DO \
        UPDATE SET \
        agt_empresa_id_bk = EXCLUDED.agt_empresa_id_bk,\
        agt_empresa = EXCLUDED.agt_empresa,\
        agt_region_id_bk = EXCLUDED.agt_region_id_bk,\
        agt_region = EXCLUDED.agt_region,\
        agt_und_negocio_id_bk = EXCLUDED.agt_und_negocio_id_bk,\
        agt_und_negocio = EXCLUDED.agt_und_negocio,\
        agt_clase_unegocio_id_bk = EXCLUDED.agt_clase_unegocio_id_bk,\
        agt_clase_unegocio = EXCLUDED.agt_clase_unegocio,\
        agt_estacion_id_bk = EXCLUDED.agt_estacion_id_bk,\
        agt_estacion = EXCLUDED.agt_estacion,\
        agt_tipo_estacion_id_bk = EXCLUDED.agt_tipo_estacion_id_bk,\
        agt_tipo_estacion = EXCLUDED.agt_tipo_estacion,\
        agt_grupo_gen_id_bk = EXCLUDED.agt_grupo_gen_id_bk,\
        agt_grupo_gen = EXCLUDED.agt_grupo_gen,\
        agt_voltaje_id_bk = EXCLUDED.agt_voltaje_id_bk,\
        agt_voltaje = EXCLUDED.agt_voltaje,\
        agt_tipo_elemento_id_bk = EXCLUDED.agt_tipo_elemento_id_bk,\
        agt_tipo_elemento = EXCLUDED.agt_tipo_elemento,\
        agt_elemento_id_bk = EXCLUDED.agt_elemento_id_bk,\
        agt_elemento = EXCLUDED.agt_elemento,\
        agt_operacion_comercial = EXCLUDED.agt_operacion_comercial,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3],values[4],values[5],\
                                                    values[6],values[7],values[8],values[9],values[10],values[11],\
                                                    values[12],values[13],values[14],values[15],values[16],values[17],\
                                                    values[18],values[19],values[20],values[21])
        
        return (pk,query)

    @staticmethod
    def Upsert_Query_Dim_Elemento_Demanda(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_agt_elemento_demanda\
        VALUES({0}, '{1}', '{2}', '{3}')\
        ON CONFLICT (eldem_elemento_id_bk) DO \
        UPDATE SET \
        eldem_elemento_id_bk = EXCLUDED.eldem_elemento_id_bk,\
        eldem_elemento = EXCLUDED.eldem_elemento,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Agente_Central(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_agt_central\
        VALUES({0}, '{1}', '{2}', '{3}', '{4}','{5}',\
        '{6}', '{7}', '{8}', '{9}')\
        ON CONFLICT (agt_cent_empresa_id_bk,agt_cent_unegocio_id_bk,agt_cent_central_id_bk) DO \
        UPDATE SET \
        agt_cent_empresa_id_bk = EXCLUDED.agt_cent_empresa_id_bk,\
        agt_cent_empresa = EXCLUDED.agt_cent_empresa,\
        agt_cent_region_id_bk = EXCLUDED.agt_cent_region_id_bk,\
        agt_cent_region = EXCLUDED.agt_cent_region,\
        agt_cent_unegocio_id_bk = EXCLUDED.agt_cent_unegocio_id_bk,\
        agt_cent_unegocio = EXCLUDED.agt_cent_unegocio,\
        agt_cent_central_id_bk = EXCLUDED.agt_cent_central_id_bk,\
        agt_cent_central = EXCLUDED.agt_cent_central,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3],values[4],values[5],\
                                                    values[6],values[7],values[8])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Agente_Interconexion(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_agt_interconexion\
        VALUES({0}, '{1}', '{2}', '{3}', '{4}','{5}',\
        '{6}', '{7}', '{8}', '{9}')\
        ON CONFLICT (agt_int_empresa_id_bk,agt_int_unegocio_id_bk,agt_int_linea_id_bk) DO \
        UPDATE SET \
        agt_int_empresa_id_bk = EXCLUDED.agt_int_empresa_id_bk,\
        agt_int_empresa = EXCLUDED.agt_int_empresa,\
        agt_int_region_id_bk = EXCLUDED.agt_int_region_id_bk,\
        agt_int_region = EXCLUDED.agt_int_region,\
        agt_int_unegocio_id_bk = EXCLUDED.agt_int_unegocio_id_bk,\
        agt_int_unegocio = EXCLUDED.agt_int_unegocio,\
        agt_int_linea_id_bk = EXCLUDED.agt_int_linea_id_bk,\
        agt_int_linea = EXCLUDED.agt_int_linea,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3],values[4],values[5],\
                                                    values[6],values[7],values[8])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Origen(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_agt_origen\
        VALUES({0}, '{1}', '{2}', '{3}', '{4}','{5}',\
        '{6}', '{7}', '{8}', '{9}')\
        ON CONFLICT (agtorg_empresa_id_bk,agtorg_und_negocio_id_bk,agtorg_clase_unegocio_id_bk) DO \
        UPDATE SET \
        agtorg_empresa_id_bk = EXCLUDED.agtorg_empresa_id_bk,\
        agtorg_empresa = EXCLUDED.agtorg_empresa,\
        agtorg_region_id_bk = EXCLUDED.agtorg_region_id_bk,\
        agtorg_region = EXCLUDED.agtorg_region,\
        agtorg_und_negocio_id_bk = EXCLUDED.agtorg_und_negocio_id_bk,\
        agtorg_und_negocio = EXCLUDED.agtorg_und_negocio,\
        agtorg_clase_unegocio_id_bk = EXCLUDED.agtorg_clase_unegocio_id_bk,\
        agtorg_clase_unegocio = EXCLUDED.agtorg_clase_unegocio,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3],values[4],values[5],\
                                                    values[6],values[7],values[8])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Asignacion_Origen(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_asignacion_origen\
        VALUES({0}, '{1}', '{2}', '{3}','{4}')\
        ON CONFLICT (asigorg_origen_id_bk) DO \
        UPDATE SET \
        asigorg_origen_id_bk = EXCLUDED.asigorg_origen_id_bk,\
        asigorg_origen = EXCLUDED.asigorg_origen,\
        asigorg_nombre = EXCLUDED.asigorg_nombre,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Clasificacion_Falla(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_clasificacion_fallas\
        VALUES({0}, {1}, '{2}', {3}, '{4}', '{5}', '{6}', '{7}', '{8}', '{9}')\
        ON CONFLICT (clsf_clasif_id_bk,clsf_grupo_id_bk,clsf_nivel_dos_id_bk,clsf_nivel_uno_id_bk) DO \
        UPDATE SET \
        clsf_clasif_id_bk = EXCLUDED.clsf_clasif_id_bk,\
        clsf_clasif = EXCLUDED.clsf_clasif,\
        clsf_grupo_id_bk = EXCLUDED.clsf_grupo_id_bk,\
        clsf_grupo = EXCLUDED.clsf_grupo,\
        clsf_nivel_dos_id_bk = EXCLUDED.clsf_nivel_dos_id_bk,\
        clsf_nivel_dos = EXCLUDED.clsf_nivel_dos,\
        clsf_nivel_uno_id_bk = EXCLUDED.clsf_nivel_uno_id_bk,\
        clsf_nivel_uno = EXCLUDED.clsf_nivel_uno,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],
                                                    values[8])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Componente_Demanda(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_componente_demanda\
        VALUES({0}, '{1}', '{2}')\
        ON CONFLICT (compdem_componente) DO \
        UPDATE SET \
        compdem_componente = EXCLUDED.compdem_componente,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Grupo_Combustible(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_grupo_combustible\
        VALUES({0}, '{1}', '{2}', '{3}')\
        ON CONFLICT (grcomb_id_bk) DO \
        UPDATE SET \
        grcomb_id_bk = EXCLUDED.grcomb_id_bk,\
        grcomb_nombre = EXCLUDED.grcomb_nombre,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1], values[2])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Modo_Agc(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_modo_agc\
        VALUES({0}, '{1}', '{2}')\
        ON CONFLICT (agcmodo_nombre) DO \
        UPDATE SET \
        agcmodo_nombre = EXCLUDED.agcmodo_nombre,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Nro_Redespacho(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_nro_redespacho\
        VALUES({0}, {1}, '{2}', '{3}')\
        ON CONFLICT (nro_valor) DO \
        UPDATE SET \
        nro_valor = EXCLUDED.nro_valor,\
        nro_nombre = EXCLUDED.nro_nombre,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Parametros_Hidro(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_param_hidrologicos\
        VALUES({0}, '{1}', '{2}', '{3}')\
        ON CONFLICT (paramhid_id_bk) DO \
        UPDATE SET \
        paramhid_id_bk = EXCLUDED.paramhid_id_bk,\
        paramhid_nombre = EXCLUDED.paramhid_nombre,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Pasos(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_pasos\
        VALUES({0}, '{1}', '{2}', '{3}')\
        ON CONFLICT (pasos_numero) DO \
        UPDATE SET \
        pasos_numero = EXCLUDED.pasos_numero,\
        pasos_nombre_paso = EXCLUDED.pasos_nombre_paso,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Tipo_Combustible(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_tipo_combustible\
        VALUES({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')\
        ON CONFLICT (tipcomb_gpl_id_bk,tipcomb_gop_id_bk,tipcomb_gtc_id_bk) DO \
        UPDATE SET \
        tipcomb_gpl_id_bk = EXCLUDED.tipcomb_gpl_id_bk,\
        tipcomb_gpl_combustible = EXCLUDED.tipcomb_gpl_combustible,\
        tipcomb_gop_id_bk = EXCLUDED.tipcomb_gop_id_bk,\
        tipcomb_gop_combustible = EXCLUDED.tipcomb_gop_combustible,\
        tipcomb_gtc_id_bk = EXCLUDED.tipcomb_gtc_id_bk,\
        tipcomb_gtc_combustible = EXCLUDED.tipcomb_gtc_combustible,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1],values[2],values[3],values[4],values[5],values[6])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Tipo_Generacion(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_tipo_generacion\
        VALUES({0}, '{1}', '{2}', '{3}')\
        ON CONFLICT (tipgen_id_bk) DO \
        UPDATE SET \
        tipgen_id_bk = EXCLUDED.tipgen_id_bk,\
        tipgen_tipo_generacion = EXCLUDED.tipgen_tipo_generacion,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1], values[2])
        
        return (pk,query)
    
    
    @staticmethod
    def Upsert_Query_Dim_Tipo_Tecnologia(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_tipo_tecnologia\
        VALUES({0}, '{1}', '{2}', '{3}')\
        ON CONFLICT (tiptec_id_bk) DO \
        UPDATE SET \
        tiptec_id_bk = EXCLUDED.tiptec_id_bk,\
        tiptec_tipo_tecnologia = EXCLUDED.tiptec_tipo_tecnologia,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0],values[1], values[2])
        
        return (pk,query)
    
    @staticmethod
    def Upsert_Query_Dim_Ltc(x):
        
        pk = x[0]
        values = x[1]
                
        query = "INSERT INTO cen_dws.dim_agt_ltc\
        VALUES({0}, '{1}', '{2}', '{3}')\
        ON CONFLICT (ltc_elemento_id_bk) DO \
        UPDATE SET \
        ltc_elemento_id_bk = EXCLUDED.ltc_elemento_id_bk,\
        ltc_elemento = EXCLUDED.ltc_elemento,\
        fecha_carga = EXCLUDED.fecha_carga;".format(pk, values[0], values[1], values[2])
        
        return (pk,query)
