import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.decomposition import PCA
import sklearn.neighbors
from sklearn.neighbors import kneighbors_graph
from sklearn import preprocessing
from sklearn.cluster import DBSCAN

class Dbscan():
    """Clase que permite realizar el proceso de la limpieza de datos."""  
    
    def __init__ (self, datosDestino):
        self.clusters = None
        self.df_escalado = None
        self.outliers = None
        self.datos_limpios = None
        self.datosDestino = datosDestino
    
    def Limpiar_outliers(self, epsilon = 0.01, samples = 5, dimension='Potencia'):
        datos_totales = self.datosDestino[['Id',dimension]].copy()
        
        if(datos_totales['Id'].count()<samples):
            samples = datos_totales['Id'].count()
            epsilon = 0.5
            
        # Normalizando los datos con MaxMin()
        min_max_scaler = preprocessing.MinMaxScaler()
        self.df_escalado = min_max_scaler.fit_transform(datos_totales)
        self.df_escalado = pd.DataFrame(self.df_escalado)
        self.df_escalado = self.df_escalado.rename(columns={0:'Id',1:dimension})

        # Aplicación del DBSCAN
        dbscan = DBSCAN(eps=epsilon, min_samples=samples, metric='euclidean').fit(self.df_escalado)
        self.clusters = dbscan.fit_predict(self.df_escalado)
        
        return self.clusters,self.df_escalado
    
    
    def Dibujar_Elbow(self, plt):
        if self.df_escalado is None:
            return None
        estimador = PCA(n_components = 2)
        x_pca = estimador.fit_transform(self.df_escalado)
        dist = sklearn.neighbors.DistanceMetric.get_metric('euclidean')
        matsim = dist.pairwise(x_pca)

        minpts = 5
        if(minpts>df_escalado['Id'].count()):
            minpts = df_escalado['Id'].count()-1

        a = kneighbors_graph(x_pca, minpts, include_self=False)

        ar = a.toarray()
        seq = []

        for i,s in enumerate(x_pca):
            for j in range(len(x_pca)):
                if ar[i][j] != 0:
                    seq.append(matsim[i][j])
        seq.sort()
        plt.plot(seq)
        plt.show()
        
        
    def Dibujar_Grupos(self, plt, circuito = '',tag = '', xlabel = 'Tiempo', ylabel = 'Potencia Aparente',dimension='Potencia'):
        datos = self.datosDestino[['Id',dimension]].copy()
        plt.scatter(datos['Id'],datos[dimension],c=self.clusters,cmap='plasma',marker='.')
        plt.title(circuito+' - '+tag)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.show()
            
    
    def Resumen_Datos(self,dimension='Potencia'):
        datos = self.datosDestino[['Id',dimension]].copy()
        resumen = pd.DataFrame()
        resumen['Id'] = datos['Id']
        resumen[dimension] = datos[dimension]
        resumen['Label'] = self.clusters
        grupos = pd.DataFrame()
        grupos['Cantidad'] = resumen.groupby('Label').size()
        
        self.outliers = resumen[resumen['Label']==-1]
        self.datos_limpios = resumen.drop(self.outliers.index)

        return grupos
    
    def Dibujar_Resultados(self, plt, circuito = '',tag = '', xlabel = 'Tiempo', ylabel = 'Potencia Aparente',dimension='Potencia'):
        datos = self.datosDestino[['Id',dimension]].copy()
        plt.plot(datos['Id'],datos[dimension],color="red",markersize=0.5,marker='.')
        plt.plot(self.datos_limpios['Id'],self.datos_limpios[dimension],color="blue",markersize=0.05,marker='.')
        plt.title(circuito+' - '+tag)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.show()
    