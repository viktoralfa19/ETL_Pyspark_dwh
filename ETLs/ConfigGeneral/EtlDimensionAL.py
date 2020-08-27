from ExceptionManager import ExceptionManager

class EtlDimensionAL:
    
    """Clase que permite el acceso a datos y persistencia de la dimensión del Catálogo de Mantenimientos. """
    def __init__(self,DBContextDw):
        self._dBContextDw = DBContextDw
        
    # ------ GET METHODS -----------------#
        
    def GetAllData(self,tableName):
        """Método que obtiene todos los datos de cualquier objeto del DW"""
        try:            
            data = self._dBContextDw.GetAllDataTableDW(tableName)
            return data
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
            
    def GetMaxPkDimension(self, query):
        """Método que obtiene la clave máxima de cualquier dimensión """
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
    
    # ------ INSERT METHODS -----------------#
    
    def UpsertDimension(self, query):
        """Método para actualizar einsertar los datos de la dimensión de catálogo."""
        result = False 
        try:
            result = self._dBContextDw.UpsertDataDW(query)
            return result
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
    
    # ------ INSERT METHODS -----------------#
    
    def InsertDimension(self, df_datos, table):
        """Método para insertar los datos de la dimensión."""
        result = False 
        try:
            result = self._dBContextDw.InsertDataDW(df_datos, table)
            return result
        except Exception as error:
            ExceptionManager.Treatment(error)
            raise
        