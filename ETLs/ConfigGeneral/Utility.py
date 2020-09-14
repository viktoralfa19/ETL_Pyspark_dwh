## Utilitarios
import datetime
import time

class Utility:
    """Clase para usar métodos bastante genéricos y en muchos casos estáticos"""
    
    @staticmethod       
    def CalcularTiempoHoras(datetime_start,datetime_end):
        """Método que realiza la direnciación de horas entre dos fechas""" 
    
        minutos = (datetime_end - datetime_start).total_seconds() / 3600.0
        return minutos
    
    @staticmethod
    def EstablecerDesfaseFecha(dateCon, dateEje): 
        """Método que establece si una fecha está desfasada de otra o es igual."""    
        
        if dateEje is None:
            dateEje = datetime.datetime.today() 

        if dateCon is None:
            dateCon = datetime.datetime.today() 
            
        if dateEje < dateCon :
            return 'A'
        elif dateEje > dateCon :
            return 'D'
        elif dateEje == dateCon :
            return 'T'        
            
    @staticmethod
    def CalcularDesfaseDesconCarga(x):
        """Método que realiza el calculo los desfases de tiempo, carga y ENS de las deconexiones de carga.""" 
        
        y=x[0]
        x=x[1]        
        _none = 0.0
        _noneStr = None
        
        '''Posicion [0][1] = None --> Datos de ejecucion'''
        '''Posicion [0][0] = None --> Datos de consignacion'''
        if (x[0][0]!=None) :
            cargasConsig= x[0][0]  
            cargasEjec = x[1][1]
        else: 
            cargasConsig= x[1][0]  
            cargasEjec = x[0][1]
        p=[]
            
        if (len(cargasConsig)==len(cargasEjec)):  
            
            for i in  range(len(cargasConsig)):
                
                s = []
                
                '''Datos de carga programado '''
                fecha_ini_crg_planif = cargasConsig[i][0]
                fecha_fin_crg_planif = cargasConsig[i][1]
                
                if cargasConsig[i][2] is not None:
                    carga_planif = cargasConsig[i][2]
                else: 
                    carga_planif = cargasEjec[i][2]
                    
                tiempo_planif = Utility.CalcularTiempoHoras(fecha_ini_crg_planif, fecha_fin_crg_planif)
                ens_planif = carga_planif * tiempo_planif
                
                '''Datos de carga ejecutada '''
                fecha_ini_crg_ejec = cargasEjec[i][0]
                fecha_fin_crg_ejec = cargasEjec[i][1]
                carga_ejec = cargasEjec[i][2]
                tiempo_ejec = cargasEjec[i][3]
                ens_ejec = cargasEjec[i][4]
                                
                '''Desfases'''                
                desfase_ini = Utility.EstablecerDesfaseFecha(fecha_ini_crg_planif,fecha_ini_crg_ejec)
                desfase_fin = Utility.EstablecerDesfaseFecha(fecha_fin_crg_planif,fecha_fin_crg_ejec)
                
                desfase_hora_ini = abs(Utility.CalcularTiempoHoras(fecha_ini_crg_planif,fecha_ini_crg_ejec))
                desfase_hora_fin = abs(Utility.CalcularTiempoHoras(fecha_fin_crg_planif,fecha_fin_crg_ejec))
                
                desfase_carga = abs(carga_planif-carga_ejec)
                desfase_tiempo = abs(tiempo_planif-tiempo_ejec)
                desfase_ens = abs(ens_planif-ens_ejec)
                trazabilidad = "FCE"
                
                '''Datos de catálogos'''
                descarga_id = cargasConsig[i][3]
                ps_codigo = cargasConsig[i][4]                
                tipconsig_id_bk = cargasConsig[i][5]
                tipconsig_consig = cargasConsig[i][6]
                tipmant_id_bk = cargasConsig[i][7]
                tipmant_tipomantenimiento = cargasConsig[i][8]
                dst_empresa_id_bk = cargasConsig[i][9]
                dst_empresa= cargasConsig[i][10]
                org_empresa_id_bk = cargasConsig[i][11]
                org_empresa = cargasConsig[i][12]
                dst_tipo_empresa_id_bk = cargasConsig[i][13]
                dst_tipo_empresa= cargasConsig[i][14]
                org_tipo_empresa_id_bk = cargasConsig[i][15]
                org_tipo_empresa= cargasConsig[i][16]                
                dst_unegocio_id_bk = cargasConsig[i][17]
                dst_unegocio = cargasConsig[i][18]
                org_unegocio_id_bk = cargasConsig[i][19]
                org_unegocio = cargasConsig[i][20]
                dst_tipo_unegocio_id_bk = cargasConsig[i][21]
                dst_tipo_unegocio= cargasConsig[i][22]
                org_tipo_unegocio_id_bk = cargasConsig[i][23]
                org_tipo_unegocio= cargasConsig[i][24]
                dst_provincia_id_bk = cargasConsig[i][25]
                dst_provincia = cargasConsig[i][26]
                org_provincia_id_bk = cargasConsig[i][27]
                org_provincia = cargasConsig[i][28]
                dst_region_id_bk = cargasConsig[i][29]
                dst_region = cargasConsig[i][30]
                org_region_id_bk = cargasConsig[i][31]
                org_region = cargasConsig[i][32]
                
                
                
                s.extend([fecha_ini_crg_planif,fecha_fin_crg_planif, carga_planif, tiempo_planif, ens_planif,
                          fecha_ini_crg_ejec, fecha_fin_crg_ejec, carga_ejec, tiempo_ejec, ens_ejec, 
                          desfase_ini,desfase_fin, desfase_hora_ini,desfase_hora_fin,desfase_carga,desfase_tiempo,
                          desfase_ens,trazabilidad, descarga_id, ps_codigo, tipconsig_id_bk,tipconsig_consig,tipmant_id_bk,
                          tipmant_tipomantenimiento,dst_empresa_id_bk,dst_empresa,org_empresa_id_bk,org_empresa,
                          dst_tipo_empresa_id_bk,dst_tipo_empresa,org_tipo_empresa_id_bk,org_tipo_empresa,
                          dst_unegocio_id_bk,dst_unegocio,org_unegocio_id_bk, org_unegocio,
                          dst_tipo_unegocio_id_bk,dst_tipo_unegocio,org_tipo_unegocio_id_bk,org_tipo_unegocio,
                          dst_provincia_id_bk,dst_provincia,org_provincia_id_bk,org_provincia,
                          dst_region_id_bk,dst_region,org_region_id_bk,org_region
                         ])
                
                p.append(s)
                
        else:                         
           
            '''Completar el número de registros faltantes para los peridos que no son coincidentes.'''
            if len(cargasConsig) > len(cargasEjec):                 
                for i in range(len(cargasConsig) - len(cargasEjec)):
                    cargasEjec.append(None)
                    
            elif len(cargasConsig) < len(cargasEjec): 
                for i in range(len(cargasEjec) - len(cargasConsig)):
                    cargasConsig.append(None)
            
            for i in  range(len(cargasConsig)): 
                s=[]
                
                if (cargasConsig[i]!=None) & (cargasEjec[i]!=None):
                    
                    '''Datos de carga programado '''
                    fecha_ini_crg_planif = cargasConsig[i][0]
                    fecha_fin_crg_planif = cargasConsig[i][1]

                    if cargasConsig[i][2] is not None:
                        carga_planif = cargasConsig[i][2]
                    else: 
                        carga_planif = cargasEjec[i][2]

                    tiempo_planif = Utility.CalcularTiempoHoras(fecha_ini_crg_planif, fecha_fin_crg_planif)
                    ens_planif = carga_planif * tiempo_planif

                    '''Datos de carga ejecutada '''
                    fecha_ini_crg_ejec = cargasEjec[i][0]
                    fecha_fin_crg_ejec = cargasEjec[i][1]
                    carga_ejec = cargasEjec[i][2]
                    tiempo_ejec = cargasEjec[i][3]
                    ens_ejec = cargasEjec[i][4]

                    '''Desfases'''                
                    desfase_ini = Utility.EstablecerDesfaseFecha(fecha_ini_crg_planif,fecha_ini_crg_ejec)
                    desfase_fin = Utility.EstablecerDesfaseFecha(fecha_fin_crg_planif,fecha_fin_crg_ejec)

                    desfase_hora_ini = abs(Utility.CalcularTiempoHoras(fecha_ini_crg_planif,fecha_ini_crg_ejec))
                    desfase_hora_fin = abs(Utility.CalcularTiempoHoras(fecha_fin_crg_planif,fecha_fin_crg_ejec))

                    desfase_carga = abs(carga_planif-carga_ejec)
                    desfase_tiempo = abs(tiempo_planif-tiempo_ejec)
                    desfase_ens = abs(ens_planif-ens_ejec)
                    trazabilidad = "FCE"

                    '''Datos de catálogos'''
                    descarga_id = cargasConsig[i][3]
                    ps_codigo = cargasConsig[i][4]                
                    tipconsig_id_bk = cargasConsig[i][5]
                    tipconsig_consig = cargasConsig[i][6]
                    tipmant_id_bk = cargasConsig[i][7]
                    tipmant_tipomantenimiento = cargasConsig[i][8]
                    dst_empresa_id_bk = cargasConsig[i][9]
                    dst_empresa= cargasConsig[i][10]
                    org_empresa_id_bk = cargasConsig[i][11]
                    org_empresa = cargasConsig[i][12]
                    dst_tipo_empresa_id_bk = cargasConsig[i][13]
                    dst_tipo_empresa= cargasConsig[i][14]
                    org_tipo_empresa_id_bk = cargasConsig[i][15]
                    org_tipo_empresa= cargasConsig[i][16]                
                    dst_unegocio_id_bk = cargasConsig[i][17]
                    dst_unegocio = cargasConsig[i][18]
                    org_unegocio_id_bk = cargasConsig[i][19]
                    org_unegocio = cargasConsig[i][20]
                    dst_tipo_unegocio_id_bk = cargasConsig[i][21]
                    dst_tipo_unegocio= cargasConsig[i][22]
                    org_tipo_unegocio_id_bk = cargasConsig[i][23]
                    org_tipo_unegocio= cargasConsig[i][24]
                    dst_provincia_id_bk = cargasConsig[i][25]
                    dst_provincia = cargasConsig[i][26]
                    org_provincia_id_bk = cargasConsig[i][27]
                    org_provincia = cargasConsig[i][28]
                    dst_region_id_bk = cargasConsig[i][29]
                    dst_region = cargasConsig[i][30]
                    org_region_id_bk = cargasConsig[i][31]
                    org_region = cargasConsig[i][32]



                    s.extend([fecha_ini_crg_planif,fecha_fin_crg_planif, carga_planif, tiempo_planif, ens_planif,
                              fecha_ini_crg_ejec, fecha_fin_crg_ejec, carga_ejec, tiempo_ejec, ens_ejec, 
                              desfase_ini,desfase_fin, desfase_hora_ini,desfase_hora_fin,desfase_carga,desfase_tiempo,
                              desfase_ens,trazabilidad, descarga_id, ps_codigo, tipconsig_id_bk,tipconsig_consig,tipmant_id_bk,
                              tipmant_tipomantenimiento,dst_empresa_id_bk,dst_empresa,org_empresa_id_bk,org_empresa,
                              dst_tipo_empresa_id_bk,dst_tipo_empresa,org_tipo_empresa_id_bk,org_tipo_empresa,
                              dst_unegocio_id_bk,dst_unegocio,org_unegocio_id_bk, org_unegocio,
                              dst_tipo_unegocio_id_bk,dst_tipo_unegocio,org_tipo_unegocio_id_bk,org_tipo_unegocio,
                              dst_provincia_id_bk,dst_provincia,org_provincia_id_bk,org_provincia,
                              dst_region_id_bk,dst_region,org_region_id_bk,org_region
                             ])

                    p.append(s)
                    
                else:
                    
                    if (cargasConsig[i]==None) & (cargasEjec[i]!=None): 
                        
                        '''Datos de carga programado '''
                        fecha_ini_crg_planif=None
                        fecha_fin_crg_planif=None
                        carga_planif=_none
                        tiempo_planif=_none
                        ens_planif=_none

                        '''Datos de carga ejecutada '''
                        fecha_ini_crg_ejec=cargasEjec[i][0]
                        fecha_fin_crg_ejec=cargasEjec[i][1]
                        carga_ejec=cargasEjec[i][2]
                        tiempo_ejec=cargasEjec[i][3]
                        ens_ejec=cargasEjec[i][4]

                        '''Desfases'''                
                        desfase_ini = _noneStr
                        desfase_fin = _noneStr
                        desfase_min_ini = _none
                        desfase_min_fin = _none
                        desfase_carga=_none
                        desfase_tiempo=_none
                        desfase_ens=_none
                        trazabilidad="SFE"
                        
                        '''Datos de catálogos'''
                        descarga_id = cargasEjec[i][5]
                        ps_codigo = cargasEjec[i][6]                
                        tipconsig_id_bk = cargasEjec[i][7]
                        tipconsig_consig = cargasEjec[i][8]
                        tipmant_id_bk = cargasEjec[i][9]
                        tipmant_tipomantenimiento = cargasEjec[i][10]
                        dst_empresa_id_bk = cargasEjec[i][11]
                        dst_empresa=  cargasEjec[i][12]
                        org_empresa_id_bk = cargasEjec[i][13]
                        org_empresa = cargasEjec[i][14]
                        dst_tipo_empresa_id_bk = cargasEjec[i][15]
                        dst_tipo_empresa= cargasEjec[i][16]
                        org_tipo_empresa_id_bk = cargasEjec[i][17]
                        org_tipo_empresa= cargasEjec[i][18]                
                        dst_unegocio_id_bk = cargasEjec[i][19]
                        dst_unegocio = cargasEjec[i][20]
                        org_unegocio_id_bk = cargasEjec[i][21]
                        org_unegocio = cargasEjec[i][22]
                        dst_tipo_unegocio_id_bk = cargasEjec[i][23]
                        dst_tipo_unegocio= cargasEjec[i][24]
                        org_tipo_unegocio_id_bk = cargasEjec[i][25]
                        org_tipo_unegocio= cargasEjec[i][26]
                        dst_provincia_id_bk = cargasEjec[i][27]
                        dst_provincia = cargasEjec[i][28]
                        org_provincia_id_bk = cargasEjec[i][29]
                        org_provincia = cargasEjec[i][30]
                        dst_region_id_bk = cargasEjec[i][31]
                        dst_region = cargasEjec[i][32]
                        org_region_id_bk = cargasEjec[i][33]
                        org_region = cargasEjec[i][34]
                        
                        s.extend([fecha_ini_crg_planif,fecha_fin_crg_planif, carga_planif, tiempo_planif, ens_planif,
                                  fecha_ini_crg_ejec, fecha_fin_crg_ejec, carga_ejec, tiempo_ejec, ens_ejec, 
                                  desfase_ini,desfase_fin, desfase_hora_ini,desfase_hora_fin,desfase_carga,desfase_tiempo,
                                  desfase_ens,trazabilidad, descarga_id, ps_codigo, tipconsig_id_bk,tipconsig_consig,tipmant_id_bk,
                                  tipmant_tipomantenimiento,dst_empresa_id_bk,dst_empresa,org_empresa_id_bk,org_empresa,
                                  dst_tipo_empresa_id_bk,dst_tipo_empresa,org_tipo_empresa_id_bk,org_tipo_empresa,
                                  dst_unegocio_id_bk,dst_unegocio,org_unegocio_id_bk, org_unegocio,
                                  dst_tipo_unegocio_id_bk,dst_tipo_unegocio,org_tipo_unegocio_id_bk,org_tipo_unegocio,
                                  dst_provincia_id_bk,dst_provincia,org_provincia_id_bk,org_provincia,
                                  dst_region_id_bk,dst_region,org_region_id_bk,org_region
                                 ])
                    
                        
                        p.append(s)
                        
                    elif (cargasConsig[i]!=None) & (cargasEjec[i]==None):                       
                        
                        '''Datos de carga programado '''
                        fecha_ini_crg_planif = cargasConsig[i][0]
                        fecha_fin_crg_planif = cargasConsig[i][1]

                        if cargasConsig[i][2] is not None:
                            carga_planif = cargasConsig[i][2]
                        else: 
                            carga_planif = 0
                            
                        tiempo_planif = Utility.CalcularTiempoHoras(fecha_ini_crg_planif, fecha_fin_crg_planif)
                        ens_planif = carga_planif * tiempo_planif
                
                        '''Datos de carga ejecutada '''
                        fecha_ini_crg_ejec=None
                        fecha_fin_crg_ejec=None
                        carga_ejec=_none
                        tiempo_ejec=_none
                        ens_ejec=_none

                        '''Desfases'''                
                        desfase_ini = _noneStr
                        desfase_fin = _noneStr
                        desfase_min_ini = _none
                        desfase_min_fin = _none
                        desfase_carga=_none
                        desfase_tiempo=_none
                        desfase_ens=_none
                        trazabilidad="SFC" 
                        
                        '''Datos de catálogos'''
                        descarga_id = cargasConsig[i][3]
                        ps_codigo = cargasConsig[i][4]                
                        tipconsig_id_bk = cargasConsig[i][5]
                        tipconsig_consig = cargasConsig[i][6]
                        tipmant_id_bk = cargasConsig[i][7]
                        tipmant_tipomantenimiento = cargasConsig[i][8]
                        dst_empresa_id_bk = cargasConsig[i][9]
                        dst_empresa= cargasConsig[i][10]
                        org_empresa_id_bk = cargasConsig[i][11]
                        org_empresa = cargasConsig[i][12]
                        dst_tipo_empresa_id_bk = cargasConsig[i][13]
                        dst_tipo_empresa= cargasConsig[i][14]
                        org_tipo_empresa_id_bk = cargasConsig[i][15]
                        org_tipo_empresa= cargasConsig[i][16]                
                        dst_unegocio_id_bk = cargasConsig[i][17]
                        dst_unegocio = cargasConsig[i][18]
                        org_unegocio_id_bk = cargasConsig[i][19]
                        org_unegocio = cargasConsig[i][20]
                        dst_tipo_unegocio_id_bk = cargasConsig[i][21]
                        dst_tipo_unegocio= cargasConsig[i][22]
                        org_tipo_unegocio_id_bk = cargasConsig[i][23]
                        org_tipo_unegocio= cargasConsig[i][24]
                        dst_provincia_id_bk = cargasConsig[i][25]
                        dst_provincia = cargasConsig[i][26]
                        org_provincia_id_bk = cargasConsig[i][27]
                        org_provincia = cargasConsig[i][28]
                        dst_region_id_bk = cargasConsig[i][29]
                        dst_region = cargasConsig[i][30]
                        org_region_id_bk = cargasConsig[i][31]
                        org_region = cargasConsig[i][32]



                        s.extend([fecha_ini_crg_planif,fecha_fin_crg_planif, carga_planif, tiempo_planif, ens_planif,
                                  fecha_ini_crg_ejec, fecha_fin_crg_ejec, carga_ejec, tiempo_ejec, ens_ejec, 
                                  desfase_ini,desfase_fin, desfase_hora_ini,desfase_hora_fin,desfase_carga,desfase_tiempo,
                                  desfase_ens,trazabilidad, descarga_id, ps_codigo, tipconsig_id_bk,tipconsig_consig,tipmant_id_bk,
                                  tipmant_tipomantenimiento,dst_empresa_id_bk,dst_empresa,org_empresa_id_bk,org_empresa,
                                  dst_tipo_empresa_id_bk,dst_tipo_empresa,org_tipo_empresa_id_bk,org_tipo_empresa,
                                  dst_unegocio_id_bk,dst_unegocio,org_unegocio_id_bk, org_unegocio,
                                  dst_tipo_unegocio_id_bk,dst_tipo_unegocio,org_tipo_unegocio_id_bk,org_tipo_unegocio,
                                  dst_provincia_id_bk,dst_provincia,org_provincia_id_bk,org_provincia,
                                  dst_region_id_bk,dst_region,org_region_id_bk,org_region
                                 ])
                        p.append(s)

                               
        return p
    
    @staticmethod   
    def SepararClaveValor(x):   
        """Método que realiza la separación de los objetos clave valor de map reduce.""" 
        q=[]        
        for i in x:
            q.append(i)            
        return q