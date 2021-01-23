from ExceptionManager import ExceptionManager

class EtlAL:
    
    """Clase que permite el acceso a datos y persistencia de la dimensión del Catálogo de Mantenimientos. """
    def __init__(self,DBContextDw):
        self._dBContextDw = DBContextDw
        
    # ------ GET METHODS -----------------#
    
    def GetData(self,query):
        """Método que obtiene todos los datos de cualquier consulta del DW"""
        try:            
            data = self._dBContextDw.GetDataDW(query)
            return data
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
        
    def GetAllData(self,tableName):
        """Método que obtiene todos los datos de cualquier objeto del DW"""
        try:            
            data = self._dBContextDw.GetAllDataTableDW(tableName)
            return data
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
            
    def GetMaxPkTable(self, query):
        """Método que obtiene la clave máxima de cualquier tabla hechos o dimensión de la bodega de datos """
        try:            
            maxIdPk = self._dBContextDw.GetDataDW(query)
            maxIdPk=maxIdPk.first()
            maxIdPk = maxIdPk.pk
            if maxIdPk is None:
                maxIdPk=0
            return maxIdPk
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise            
    
    # ------ Upsert METHODS -----------------#
    
    def UpsertDimension(self, query):
        """Método para actualizar e insertar los datos de la dimensiones."""
        result = False 
        try:
            result = self._dBContextDw.UpsertDataDW(query)
            return result
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
    
    # ------ Delete METHODS -----------------#
    
    def Delete(self, query):
        """Método para eliminar datos de una tabla del DWH."""
        result = False 
        try:
            result = self._dBContextDw.UpsertDataDW(query)
            return result
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
            
    # ------ Delete METHODS -----------------#
    
    def Update(self, query):
        """Método para actualizar datos de una tabla del DWH"""
        result = False 
        try:
            result = self._dBContextDw.UpsertDataDW(query)
            return result
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
    
    # ------ INSERT METHODS -----------------#
    
    def Insert(self, df_datos, table):
        """Método para insertar los datos de la dimensión."""
        result = False 
        try:
            result = self._dBContextDw.InsertDataDW(df_datos, table)
            return result
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
        