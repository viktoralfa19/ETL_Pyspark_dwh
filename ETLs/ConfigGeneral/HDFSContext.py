## Configuracion de Contextos HDFS
class HDFSContext: 
    """La clase ContextHdfs establece la configuración con el datalake para acceder a los diferentes archivos 
    almacenados de la base de datos de origen en el HDFS."""
    def __init__ (self,Host='10.30.80.3',Port='9000',Path='DATABASE/RDBMS',DataBase='SIVO',Schema='dbo'):
        self.HostHdfs= Host
        self.Port = Port
        self.Path = Path
        self.DataBase = DataBase
        self.Schema = Schema
        
    def HdfsPath(self,tName, fName):        
        """Método que establece el path de búsqueda de un archivo específico."""  
        pathDir = "hdfs://{0}:{1}/{2}/{3}/{4}/{5}/{6}".format(self.HostHdfs,self.Port,self.Path,self.DataBase,
                                                              self.Schema,tName,fName);
        return pathDir;