## Manejo de Excepciones
import sys, traceback
class ExceptionManager:
    """Clase para manejar las excepciones."""
    def __init__(self):
        pass
    
    @staticmethod
    def Treatment(exception):
        try:
            print(exception)
        except Py4JNetworkError as error:
            print(error)
            
    @staticmethod
    def TraceTreatment(exception):
        try:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            print(exception)
        except Py4JNetworkError as error:
            print(error)