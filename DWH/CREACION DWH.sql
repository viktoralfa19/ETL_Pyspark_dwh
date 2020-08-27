/*==============================================================*/
/* DBMS name:      PostgreSQL 8                                 */
/* Created on:     25/8/2020 10:51:36                           */
/*==============================================================*/
drop table CEN_DWS.FACT_AGC;
drop table CEN_DWS.FACT_BGEN_DIARIO;
drop table CEN_DWS.FACT_BGEN_MEDIOHORARIO;
drop table CEN_DWS.FACT_DEMANDA_DIARIA;
drop table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA;
drop table CEN_DWS.FACT_DESP_REDESP_HORARIO;
drop table CEN_DWS.FACT_DIST_ENS;
drop table CEN_DWS.FACT_ENS_ES;
drop table CEN_DWS.FACT_FACTOR_CARGA;
drop table CEN_DWS.FACT_FACTOR_PLANTA;
drop table CEN_DWS.FACT_FALLAS_SNI;
drop table CEN_DWS.FACT_HIDROLOGIA;
drop table CEN_DWS.FACT_HIDROLOGIA_HORARIA;
drop table CEN_DWS.FACT_IMPORTACION_EXPORTACION;
drop table CEN_DWS.FACT_INTERCONEXION_DIARIA;
drop table CEN_DWS.FACT_LIM_FCS;
drop table CEN_DWS.FACT_POT_DISPONIBLE;
drop table CEN_DWS.FACT_PRODUCCION_DIARIA;
drop table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA;
drop table CEN_DWS.FACT_STOCK_COMBUSTIBLE;
drop table CEN_DWS.FACT_TIEMPO_OPERACION;
drop table CEN_DWS.FACT_TMP_DESCON_CARGA;

drop table CEN_DWS.DIM_AGENTE;
drop table CEN_DWS.DIM_AGT_CENTRAL;
drop table CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA;
drop table CEN_DWS.DIM_AGT_INTERCONEXION;
drop table CEN_DWS.DIM_AGT_ORIGEN;
drop table CEN_DWS.DIM_AGT_ORIGEN_DESTINO;
drop table CEN_DWS.DIM_ASIGNACION_ORIGEN;
drop table CEN_DWS.DIM_CATALOGOS_MANT;
drop table CEN_DWS.DIM_CLASIFICACION_FALLAS;
drop table CEN_DWS.DIM_COMPONENTE_DEMANDA;
drop table CEN_DWS.DIM_GRUPO_COMBUSTIBLE;
drop table CEN_DWS.DIM_HORA;
drop table CEN_DWS.DIM_MODO_AGC;
drop table CEN_DWS.DIM_NRO_REDESPACHO;
drop table CEN_DWS.DIM_NUMERO;
drop table CEN_DWS.DIM_OBSERVACIONES_FALLA;
drop table CEN_DWS.DIM_PARAM_HIDROLOGICOS;
drop table CEN_DWS.DIM_PASOS;
drop table CEN_DWS.DIM_TIEMPO;
drop table CEN_DWS.DIM_TIPO_COMBUSTIBLE;
drop table CEN_DWS.DIM_TIPO_GENERACION;
drop table CEN_DWS.DIM_TIPO_TECNOLOGIA;

/*==============================================================*/
/* Table: CEN_DWS.DIM_AGENTE                                            */
/*==============================================================*/
create table CEN_DWS.DIM_AGENTE (
   AGT_ID_PK            INT4                 not null,
   AGT_EMPRESA_ID_BK    VARCHAR(20)          not null,
   AGT_EMPRESA          VARCHAR(100)         not null,
   AGT_REGION_ID_BK     VARCHAR(20)          not null,
   AGT_REGION           VARCHAR(50)          not null,
   AGT_UND_NEGOCIO_ID_BK VARCHAR(20)          not null,
   AGT_UND_NEGOCIO      VARCHAR(100)         not null,
   AGT_CLASE_UNEGOCIO_ID_BK VARCHAR(20)                 not null,
   AGT_CLASE_UNEGOCIO   VARCHAR(50)          not null,
   AGT_ESTACION_ID_BK   VARCHAR(20)          not null,
   AGT_ESTACION         VARCHAR(100)         not null,
   AGT_TIPO_ESTACION_ID_BK INT2                 not null,
   AGT_TIPO_ESTACION    VARCHAR(100)         not null,
   AGT_GRUPO_GEN_ID_BK  VARCHAR(20)          not null,
   AGT_GRUPO_GEN        VARCHAR(50)          not null,
   AGT_VOLTAJE_ID_BK    VARCHAR(20)          not null,
   AGT_VOLTAJE          VARCHAR(50)          not null,
   AGT_TIPO_ELEMENTO_ID_BK INT2                 not null,
   AGT_TIPO_ELEMENTO    VARCHAR(50)          not null,
   AGT_ELEMENTO_ID_BK   VARCHAR(20)          not null,
   AGT_ELEMENTO         VARCHAR(100)         not null,
   AGT_OPERACION_COMERCIAL TIMESTAMP WITHOUT TIME ZONE                 not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_AGENTE primary key (AGT_ID_PK)
);

comment on table CEN_DWS.DIM_AGENTE is
'Tabla de dimensión con información de gentes unificados historicos.';

comment on column CEN_DWS.DIM_AGENTE.AGT_ID_PK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.DIM_AGENTE.AGT_EMPRESA_ID_BK is
'Código del negocio de la empresa ';

comment on column CEN_DWS.DIM_AGENTE.AGT_EMPRESA is
'Nombre de las empresas.';

comment on column CEN_DWS.DIM_AGENTE.AGT_REGION_ID_BK is
'Código de la región correspondiente a la unidad de negocio.';

comment on column CEN_DWS.DIM_AGENTE.AGT_REGION is
'Nombre de la región correspondiente a la unidad de negocio.';

comment on column CEN_DWS.DIM_AGENTE.AGT_UND_NEGOCIO_ID_BK is
'Identificador del negocio para las unidades de negocio.';

comment on column CEN_DWS.DIM_AGENTE.AGT_UND_NEGOCIO is
'Nombre de la unidad de negocio.';

comment on column CEN_DWS.DIM_AGENTE.AGT_CLASE_UNEGOCIO_ID_BK is
'Codigo de la clase de unidad de negocio.';

comment on column CEN_DWS.DIM_AGENTE.AGT_CLASE_UNEGOCIO is
'Nombre de la clase de unidad de negocio.';

comment on column CEN_DWS.DIM_AGENTE.AGT_ESTACION_ID_BK is
'Identificador del negocio para las estaciones que pueden ser central, subestacion o línea.';

comment on column CEN_DWS.DIM_AGENTE.AGT_ESTACION is
'Nombre de la estación.';

comment on column CEN_DWS.DIM_AGENTE.AGT_TIPO_ESTACION_ID_BK is
'Código del tipo de estación.';

comment on column CEN_DWS.DIM_AGENTE.AGT_TIPO_ESTACION is
'Nombre del tipo de estación, puede ser central, subestación o línea.';

comment on column CEN_DWS.DIM_AGENTE.AGT_GRUPO_GEN_ID_BK is
'Código del grupo de generación existe, solamente para las unidades generardoras.';

comment on column CEN_DWS.DIM_AGENTE.AGT_GRUPO_GEN is
'Nombre del grupo de generación.';

comment on column CEN_DWS.DIM_AGENTE.AGT_VOLTAJE_ID_BK is
'Código del nivel de voltaje asociado.';

comment on column CEN_DWS.DIM_AGENTE.AGT_VOLTAJE is
'Nombre del nivel de voltaje asociado.';

comment on column CEN_DWS.DIM_AGENTE.AGT_TIPO_ELEMENTO_ID_BK is
'Código del tipo de elemento.';

comment on column CEN_DWS.DIM_AGENTE.AGT_TIPO_ELEMENTO is
'Nombre del tipo de elemento.';

comment on column CEN_DWS.DIM_AGENTE.AGT_ELEMENTO_ID_BK is
'Identificador del negocio para los elementos.';

comment on column CEN_DWS.DIM_AGENTE.AGT_ELEMENTO is
'Nombre del elemento.';

comment on column CEN_DWS.DIM_AGENTE.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_AGENTE owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_AGENTE_PK                                         */
/*==============================================================*/
create unique index DIM_AGENTE_PK on CEN_DWS.DIM_AGENTE (
AGT_ID_PK
);

/*==============================================================*/
/* Index: AGENTE_UK                                             */
/*==============================================================*/
create unique index AGENTE_UK on CEN_DWS.DIM_AGENTE (
( AGT_EMPRESA_ID_BK ),
( AGT_UND_NEGOCIO_ID_BK ),
( AGT_CLASE_UNEGOCIO_ID_BK ),
( AGT_ESTACION_ID_BK ),
( AGT_ELEMENTO_ID_BK )
);

/*==============================================================*/
/* Table: DIM_AGT_CENTRAL                                       */
/*==============================================================*/
create table CEN_DWS.DIM_AGT_CENTRAL (
   AGT_CENT_ID_PK       INT2                 not null,
   AGT_CENT_EMPRESA_ID_BK VARCHAR(20)          not null,
   AGT_CENT_EMPRESA     VARCHAR(100)         not null,
   AGT_CENT_REGION_ID_BK VARCHAR(20)          null,
   AGT_CENT_REGION      VARCHAR(100)         null,
   AGT_CENT_UNEGOCIO_ID_BK VARCHAR(20)          not null,
   AGT_CENT_UNEGOCIO    VARCHAR(100)         not null,
   AGT_CENT_CENTRAL_ID_BK VARCHAR(20)          not null,
   AGT_CENT_CENTRAL     VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_AGT_CENTRAL primary key (AGT_CENT_ID_PK)
);

comment on table CEN_DWS.DIM_AGT_CENTRAL is
'Dimensión exclusivamente para el analisis de factor de planta y carga, para identificar solamente las centrales de generación';

comment on column CEN_DWS.DIM_AGT_CENTRAL.AGT_CENT_ID_PK is
'Identificador de la tabla de agentes central.';

comment on column CEN_DWS.DIM_AGT_CENTRAL.AGT_CENT_EMPRESA_ID_BK is
'Código de la empresa de egeneración.';

comment on column CEN_DWS.DIM_AGT_CENTRAL.AGT_CENT_EMPRESA is
'Nombre de la empresa de generación.';

comment on column CEN_DWS.DIM_AGT_CENTRAL.AGT_CENT_UNEGOCIO_ID_BK is
'Código de la unidad de negocio de generación.';

comment on column CEN_DWS.DIM_AGT_CENTRAL.AGT_CENT_UNEGOCIO is
'Nombre de la unidad de negocio.';

comment on column CEN_DWS.DIM_AGT_CENTRAL.AGT_CENT_CENTRAL_ID_BK is
'Código de la central generadora.';

comment on column CEN_DWS.DIM_AGT_CENTRAL.AGT_CENT_CENTRAL is
'Nombre de la central';

comment on column CEN_DWS.DIM_AGT_CENTRAL.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_AGT_CENTRAL owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_AGT_CENTRAL_PK                                    */
/*==============================================================*/
create unique index DIM_AGT_CENTRAL_PK on CEN_DWS.DIM_AGT_CENTRAL (
AGT_CENT_ID_PK
);

/*==============================================================*/
/* Index: AGT_CENT_UK                                           */
/*==============================================================*/
create unique index AGT_CENT_UK on CEN_DWS.DIM_AGT_CENTRAL (
( AGT_CENT_EMPRESA_ID_BK ),
( AGT_CENT_UNEGOCIO_ID_BK ),
( AGT_CENT_CENTRAL_ID_BK )
);

/*==============================================================*/
/* Table: DIM_AGT_ELEMENTO_DEMANDA                              */
/*==============================================================*/
create table CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA (
   ELDEM_ID_PK          INT2                 not null,
   ELDEM_ELEMENTO_ID_BK VARCHAR(20)          not null,
   ELDEM_ELEMENTO       VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_AGT_ELEMENTO_DEMANDA primary key (ELDEM_ID_PK)
);

comment on table CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA is
'Esta dimensión permite adicionar un nivel más a parte del elemento común de analisis general, al analisis de la demanda de potencia y energía de SIVO.';

comment on column CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA.ELDEM_ID_PK is
'Identificador de la tabla de elementos de demanda.';

comment on column CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA.ELDEM_ELEMENTO_ID_BK is
'Código del elemento de demanda. Permite al usuario reconocer el elemento.';

comment on column CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA.ELDEM_ELEMENTO is
'Nombre del elemento de demanda.';

comment on column CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA owner to CEN_DWS
;
/*==============================================================*/
/* Index: AGT_ELEM_DEM_PK                                       */
/*==============================================================*/
create unique index AGT_ELEM_DEM_PK on CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA (
ELDEM_ID_PK
);

/*==============================================================*/
/* Index: ELEM_DEM_UK                                           */
/*==============================================================*/
create unique index ELEM_DEM_UK on CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA (
( ELDEM_ELEMENTO_ID_BK )
);

/*==============================================================*/
/* Table: DIM_AGT_INTERCONEXION                                 */
/*==============================================================*/
create table CEN_DWS.DIM_AGT_INTERCONEXION (
   AGT_INT_ID_PK        INT2                 not null,
   AGT_INT_EMPRESA_ID_BK VARCHAR(20)          not null,
   AGT_INT_EMPRESA      VARCHAR(100)         not null,
   AGT_INT_REGION_ID_BK VARCHAR(20)          not null,
   AGT_INT_REGION       VARCHAR(100)         not null,
   AGT_INT_UNEGOCIO_ID_BK VARCHAR(20)          not null,
   AGT_INT_UNEGOCIO     VARCHAR(100)         not null,
   AGT_INT_LINEA_ID_BK  VARCHAR(20)          not null,
   AGT_INT_LINEA        VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_AGT_INTERCONEXION primary key (AGT_INT_ID_PK)
);

comment on table CEN_DWS.DIM_AGT_INTERCONEXION is
'Dimensión donde el agente de interconexión solo debe estar a nivel de línea de transmisión';

comment on column CEN_DWS.DIM_AGT_INTERCONEXION.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_AGT_INTERCONEXION owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_AGT_INTERCONEXION_PK                              */
/*==============================================================*/
create unique index DIM_AGT_INTERCONEXION_PK on CEN_DWS.DIM_AGT_INTERCONEXION (
AGT_INT_ID_PK
);

/*==============================================================*/
/* Index: AGT_INTER_UK                                          */
/*==============================================================*/
create unique index AGT_INTER_UK on CEN_DWS.DIM_AGT_INTERCONEXION (
( AGT_INT_EMPRESA_ID_BK ),
( AGT_INT_UNEGOCIO_ID_BK ),
( AGT_INT_LINEA_ID_BK )
);

/*==============================================================*/
/* Table: DIM_AGT_ORIGEN                                        */
/*==============================================================*/
create table CEN_DWS.DIM_AGT_ORIGEN (
   AGTORG_ID_PK         INT2                 not null,
   AGTORG_EMPRESA_ID_BK VARCHAR(20)          not null,
   AGTORG_EMPRESA       VARCHAR(100)         not null,
   AGTORG_REGION_ID_BK  VARCHAR(20)          not null,
   AGTORG_REGION        VARCHAR(100)         not null,
   AGTORG_UND_NEGOCIO_ID_BK VARCHAR(20)          not null,
   AGTORG_UND_NEGOCIO   VARCHAR(100)         not null,
   AGTORG_CLASE_UNEGOCIO_ID_BK INT2                 null,
   AGTORG_CLASE_UNEGOCIO VARCHAR(50)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_AGT_ORIGEN primary key (AGTORG_ID_PK)
);

comment on table CEN_DWS.DIM_AGT_ORIGEN is
'Representa la unidad de negocio que son el origen de muchas fallas.';

comment on column CEN_DWS.DIM_AGT_ORIGEN.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_AGT_ORIGEN owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_AGT_ORIGEN_PK                                     */
/*==============================================================*/
create unique index DIM_AGT_ORIGEN_PK on CEN_DWS.DIM_AGT_ORIGEN (
AGTORG_ID_PK
);

/*==============================================================*/
/* Index: AGT_ORIGEN_UK                                         */
/*==============================================================*/
create unique index AGT_ORIGEN_UK on CEN_DWS.DIM_AGT_ORIGEN (
( AGTORG_EMPRESA_ID_BK ),
( AGTORG_UND_NEGOCIO_ID_BK ),
( AGTORG_CLASE_UNEGOCIO_ID_BK )
);

/*==============================================================*/
/* Table: DIM_AGT_ORIGEN_DESTINO                                */
/*==============================================================*/
create table CEN_DWS.DIM_AGT_ORIGEN_DESTINO (
   AGTORGDES_ID_PK      INT2                 not null,
   AGTORGDES_TIPO_EMPRESA_ID_BK VARCHAR(32)          not null,
   AGTORGDES_TIPO_EMPRESA VARCHAR(128)         not null,
   AGTORGDES_EMPRESA_ID_BK VARCHAR(32)          not null,
   AGTORGDES_EMPRESA    VARCHAR(128)         not null,
   AGTORGDES_TIPO_UNEGOCIO_ID_BK VARCHAR(32)          not null,
   AGTORGDES_TIPO_UNEGOCIO VARCHAR(128)         not null,
   AGTORGDES_UNEGOCIO_ID_BK VARCHAR(32)          not null,
   AGTORGDES_UNEGOCIO   VARCHAR(128)         not null,
   AGTORGDES_PROVINCIA_ID_BK VARCHAR(32)          not null,
   AGTORGDES_PROVINCIA  VARCHAR(128)         not null,
   AGTORGDES_REGION_ID_BK VARCHAR(32)          not null,
   AGTORGDES_REGION     VARCHAR(128)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_AGT_ORIGEN_DESTINO primary key (AGTORGDES_ID_PK)
);

comment on table CEN_DWS.DIM_AGT_ORIGEN_DESTINO is
'Corresponde al agente que genera la desconexión de carga en fallas y mantenimientos. ';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_ID_PK is
'Corresponde al código de identificación del regidtro del agente ';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_TIPO_EMPRESA_ID_BK is
'Corresponde al código del negocio que identifica el tipo de empresa';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_TIPO_EMPRESA is
'Corresponde al nombre que identifica el tipo de empresa';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_EMPRESA_ID_BK is
'Corresponde al código del negocio que identifica a la empresa';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_EMPRESA is
'Corresponde al nombre que identifica a la empresa';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_TIPO_UNEGOCIO_ID_BK is
'Corresponde al codigo de negocio que identifica el tipo de unidad de negocio de los agentes destino origen';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_TIPO_UNEGOCIO is
'Corresponde al nombre del tipo de unidad de negocio de los agentes destino origen';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_UNEGOCIO_ID_BK is
'Corresponde al código del negocio que identifica a la unidad de negocio';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_UNEGOCIO is
'Corresponde al nombre que identifica a la unidad de negocio';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_PROVINCIA_ID_BK is
'Corresponde al codigo de la provincia';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_PROVINCIA is
'Nombre de la provincia';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_REGION_ID_BK is
'Corresponde al código del negocio que identifica a la región';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.AGTORGDES_REGION is
'Corresponde al nombre que identifica a la región';

comment on column CEN_DWS.DIM_AGT_ORIGEN_DESTINO.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_AGT_ORIGEN_DESTINO owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_AGT_ORIGEN_DESTINO_PK                             */
/*==============================================================*/
create unique index DIM_AGT_ORIGEN_DESTINO_PK on CEN_DWS.DIM_AGT_ORIGEN_DESTINO (
AGTORGDES_ID_PK
);

/*==============================================================*/
/* Index: UK_AGT_ORIGEN_DESTINO                                 */
/*==============================================================*/
create unique index UK_AGT_ORIGEN_DESTINO on CEN_DWS.DIM_AGT_ORIGEN_DESTINO (
AGTORGDES_EMPRESA_ID_BK,
AGTORGDES_EMPRESA,
AGTORGDES_UNEGOCIO_ID_BK,
AGTORGDES_UNEGOCIO
);

/*==============================================================*/
/* Table: DIM_ASIGNACION_ORIGEN                                 */
/*==============================================================*/
create table CEN_DWS.DIM_ASIGNACION_ORIGEN (
   ASIGORG_ID_PK        INT2                 not null,
   ASIGORG_ORIGEN_ID_BK VARCHAR(30)          not null,
   ASIGORG_ORIGEN       VARCHAR(70)          not null,
   ASIGORG_NOMBRE       VARCHAR(70)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_ASIGNACION_ORIGEN primary key (ASIGORG_ID_PK)
);

comment on table CEN_DWS.DIM_ASIGNACION_ORIGEN is
'Permite distinguir cual ha sido el origen de la falla.';

comment on column CEN_DWS.DIM_ASIGNACION_ORIGEN.ASIGORG_ID_PK is
'Identificador de la falla.';

comment on column CEN_DWS.DIM_ASIGNACION_ORIGEN.ASIGORG_ORIGEN_ID_BK is
'Código del Origen de Falla.';

comment on column CEN_DWS.DIM_ASIGNACION_ORIGEN.ASIGORG_ORIGEN is
'Código para reporte de Interrupciones.';

comment on column CEN_DWS.DIM_ASIGNACION_ORIGEN.ASIGORG_NOMBRE is
'Nombre del origen de Falla.';

comment on column CEN_DWS.DIM_ASIGNACION_ORIGEN.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_ASIGNACION_ORIGEN owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_ASIGNACION_ORIGEN_PK                              */
/*==============================================================*/
create unique index DIM_ASIGNACION_ORIGEN_PK on CEN_DWS.DIM_ASIGNACION_ORIGEN (
ASIGORG_ID_PK
);

/*==============================================================*/
/* Index: ASIG_UK                                               */
/*==============================================================*/
create unique index ASIG_UK on CEN_DWS.DIM_ASIGNACION_ORIGEN (
( ASIGORG_ORIGEN_ID_BK )
);

/*==============================================================*/
/* Table: DIM_CATALOGOS_MANT                                    */
/*==============================================================*/
create table CEN_DWS.DIM_CATALOGOS_MANT (
   CAT_ID_PK            INT2                 not null,
   CAT_ID_BK            VARCHAR(16)          not null,
   CAT_CATALOGO         VARCHAR(32)          not null,
   CAT_PARAMETRO        VARCHAR(32)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_CATALOGOS_MANT primary key (CAT_ID_PK)
);

comment on table CEN_DWS.DIM_CATALOGOS_MANT is
'Dimensión no deseada que contiene datos de los tipos de mantenimiento, tipos de consignación y estados de la ejecución y consignación ';

comment on column CEN_DWS.DIM_CATALOGOS_MANT.CAT_ID_PK is
'Clave primaria de la tabla que contiene todos los catálogos';

comment on column CEN_DWS.DIM_CATALOGOS_MANT.CAT_ID_BK is
'Clave de negocio que contiene datos la identificación del registro del catálogo ';

comment on column CEN_DWS.DIM_CATALOGOS_MANT.CAT_CATALOGO is
'Corresponde al nombre del catálogo al que se hace referencia';

comment on column CEN_DWS.DIM_CATALOGOS_MANT.CAT_PARAMETRO is
'Corresponde al valor ya sea estado o nombre de algun tipo o clasificación. ';

comment on column CEN_DWS.DIM_CATALOGOS_MANT.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_CATALOGOS_MANT owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_CATALOGOS_MANT_PK                                 */
/*==============================================================*/
create unique index DIM_CATALOGOS_MANT_PK on CEN_DWS.DIM_CATALOGOS_MANT (
CAT_ID_PK
);

/*==============================================================*/
/* Index: UK_DIM_CATALOGOS                                      */
/*==============================================================*/
create unique index UK_DIM_CATALOGOS on CEN_DWS.DIM_CATALOGOS_MANT (
CAT_ID_BK,
CAT_PARAMETRO
);

/*==============================================================*/
/* Table: DIM_CLASIFICACION_FALLAS                              */
/*==============================================================*/
create table CEN_DWS.DIM_CLASIFICACION_FALLAS (
   CLASF_ID_PK          INT4                 not null,
   CLSF_CLASIF_ID_BK    INT2                 not null,
   CLSF_CLASIF          VARCHAR(200)         not null,
   CLSF_GRUPO_ID_BK     INT2                 not null,
   CLSF_GRUPO           VARCHAR(150)         not null,
   CLSF_NIVEL_DOS_ID_BK VARCHAR(20)          not null,
   CLSF_NIVEL_DOS       VARCHAR(50)          not null,
   CLSF_NIVEL_UNO_ID_BK VARCHAR(20)          not null,
   CLSF_NIVEL_UNO       VARCHAR(50)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_CLASIFICACION_FALLAS primary key (CLASF_ID_PK)
);

comment on table CEN_DWS.DIM_CLASIFICACION_FALLAS is
'Esta tabla identifica la clasificación de las fallas.';

comment on column CEN_DWS.DIM_CLASIFICACION_FALLAS.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_CLASIFICACION_FALLAS owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_CLASIFICACION_FALLAS_PK                           */
/*==============================================================*/
create unique index DIM_CLASIFICACION_FALLAS_PK on CEN_DWS.DIM_CLASIFICACION_FALLAS (
CLASF_ID_PK
);

/*==============================================================*/
/* Index: CLASIF_UK                                             */
/*==============================================================*/
create unique index CLASIF_UK on CEN_DWS.DIM_CLASIFICACION_FALLAS (
( CLSF_NIVEL_UNO_ID_BK ),
( CLSF_NIVEL_DOS_ID_BK ),
( CLSF_GRUPO_ID_BK ),
( CLSF_CLASIF_ID_BK )
);

/*==============================================================*/
/* Table: DIM_COMPONENTE_DEMANDA                                */
/*==============================================================*/
create table CEN_DWS.DIM_COMPONENTE_DEMANDA (
   COMPDEM_ID_PK        INT2                 not null,
   COMPDEM_COMPONENTE   VARCHAR(32)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_COMPONENTE_DEMANDA primary key (COMPDEM_ID_PK)
);

comment on table CEN_DWS.DIM_COMPONENTE_DEMANDA is
'Corrresponde a la tabla que tiene datos del componente de demanda';

comment on column CEN_DWS.DIM_COMPONENTE_DEMANDA.COMPDEM_ID_PK is
'Corresponde a la clave primaria de la tabla para identificar el componente de demanda';

comment on column CEN_DWS.DIM_COMPONENTE_DEMANDA.COMPDEM_COMPONENTE is
'Corresponde al nombre del componente ';

comment on column CEN_DWS.DIM_COMPONENTE_DEMANDA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_COMPONENTE_DEMANDA owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_COMP_DEM_PK                                       */
/*==============================================================*/
create unique index DIM_COMP_DEM_PK on CEN_DWS.DIM_COMPONENTE_DEMANDA (
COMPDEM_ID_PK
);

/*==============================================================*/
/* Index: COMP_DEM_UK                                           */
/*==============================================================*/
create unique index COMP_DEM_UK on CEN_DWS.DIM_COMPONENTE_DEMANDA (
( COMPDEM_COMPONENTE )
);

/*==============================================================*/
/* Table: DIM_GRUPO_COMBUSTIBLE                                 */
/*==============================================================*/
create table CEN_DWS.DIM_GRUPO_COMBUSTIBLE (
   GRCOMB_ID_PK         INT2                 not null,
   GRCOMB_ID_BK         VARCHAR(20)          not null,
   GRCOMB_NOMBRE        VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_GRUPO_COMBUSTIBLE primary key (GRCOMB_ID_PK)
);

comment on table CEN_DWS.DIM_GRUPO_COMBUSTIBLE is
'Dimensión que permite determinar el grupo de combustible al aque pertenece el agente.';

comment on column CEN_DWS.DIM_GRUPO_COMBUSTIBLE.GRCOMB_ID_PK is
'Llave primaria del grupo de combustible';

comment on column CEN_DWS.DIM_GRUPO_COMBUSTIBLE.GRCOMB_ID_BK is
'Código identificatorio del grupo de combustible.';

comment on column CEN_DWS.DIM_GRUPO_COMBUSTIBLE.GRCOMB_NOMBRE is
'Nombre del grupo de combustible';

comment on column CEN_DWS.DIM_GRUPO_COMBUSTIBLE.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_GRUPO_COMBUSTIBLE owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_GRP_COMB_PK                                       */
/*==============================================================*/
create unique index DIM_GRP_COMB_PK on CEN_DWS.DIM_GRUPO_COMBUSTIBLE (
GRCOMB_ID_PK
);

/*==============================================================*/
/* Index: GRP_COMB_UK                                           */
/*==============================================================*/
create unique index GRP_COMB_UK on CEN_DWS.DIM_GRUPO_COMBUSTIBLE (
( GRCOMB_ID_BK )
);

/*==============================================================*/
/* Table: DIM_HORA                                              */
/*==============================================================*/
create table CEN_DWS.DIM_HORA (
   HORA_ID_PK           INT2                 not null,
   HR_TIEMPO            TIME                 not null,
   HR_HORA              INT2                 not null,
   HR_MINUTO            INT2                 not null,
   HR_HORA_DESCRIPCION  VARCHAR(8)           not null,
   HR_MINUTO_DESCRIPCION VARCHAR(4)           not null,
   HR_HORA_12           VARCHAR(8)           not null,
   HR_HORA_24           VARCHAR(8)           not null,
   FECHA_CARGA        TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_HORA primary key (HORA_ID_PK)
);

comment on table CEN_DWS.DIM_HORA is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.DIM_HORA.HORA_ID_PK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.DIM_HORA.HR_HORA is
'Valor numérico de la hora';

comment on column CEN_DWS.DIM_HORA.HR_MINUTO is
'Corresponde a la descripción de minuto';

comment on column CEN_DWS.DIM_HORA.HR_HORA_DESCRIPCION is
'Corresponde a la descripción de la hora en formato HH:mm';

comment on column CEN_DWS.DIM_HORA.HR_MINUTO_DESCRIPCION is
'Correponde a la descripción de los minutos';

comment on column CEN_DWS.DIM_HORA.HR_HORA_12 is
'Corresponde al formato de las 12 horas AM o PM';

comment on column CEN_DWS.DIM_HORA.HR_HORA_24 is
'Corresponde al formato de las 24 horas.';

comment on column CEN_DWS.DIM_HORA.H_FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_HORA owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_HORA_PK                                           */
/*==============================================================*/
create unique index DIM_HORA_PK on CEN_DWS.DIM_HORA (
HORA_ID_PK
);

/*==============================================================*/
/* Index: UK_HORA                                               */
/*==============================================================*/
create unique index UK_HORA on CEN_DWS.DIM_HORA (
( HR_TIEMPO )
);

/*==============================================================*/
/* Table: DIM_MODO_AGC                                          */
/*==============================================================*/
create table CEN_DWS.DIM_MODO_AGC (
   AGCMODO_ID_PK        INT2                 not null,
   AGCMODO_NOMBRE       VARCHAR(30)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_MODO_AGC primary key (AGCMODO_ID_PK)
);

comment on table CEN_DWS.DIM_MODO_AGC is
'Dimensión para identificar el modo registrado en la pestaña AGC.';

comment on column CEN_DWS.DIM_MODO_AGC.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_MODO_AGC owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_MODO_AGC_PK                                       */
/*==============================================================*/
create unique index DIM_MODO_AGC_PK on CEN_DWS.DIM_MODO_AGC (
AGCMODO_ID_PK
);

/*==============================================================*/
/* Index: MODO_UK                                               */
/*==============================================================*/
create unique index MODO_UK on CEN_DWS.DIM_MODO_AGC (
( AGCMODO_NOMBRE )
);

/*==============================================================*/
/* Table: DIM_NRO_REDESPACHO                                    */
/*==============================================================*/
create table CEN_DWS.DIM_NRO_REDESPACHO (
   NRO_ID_PK            INT4                 not null,
   NRO_VALOR            INT2                 not null,
   NRO_NOMBRE           VARCHAR(30)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_NRO_REDESPACHO primary key (NRO_ID_PK)
);

comment on table CEN_DWS.DIM_NRO_REDESPACHO is
'Tabla que contiene infromación del número de redespacho que se ejecutó en un período de tiempo. Si es cero corresponde al valor del despacho ';

comment on column CEN_DWS.DIM_NRO_REDESPACHO.NRO_ID_PK is
'Corresponde a la clave primaria de la tabla, para identificar los registro únicos del número de redespachos';

comment on column CEN_DWS.DIM_NRO_REDESPACHO.NRO_VALOR is
'Corresponde al número de redespacho ejecutado';

comment on column CEN_DWS.DIM_NRO_REDESPACHO.NRO_NOMBRE is
'Corresponde al nombre del despacho y redespacho ';

comment on column CEN_DWS.DIM_NRO_REDESPACHO.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_NRO_REDESPACHO owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_NRO_REDESPACHO_PK                                 */
/*==============================================================*/
create unique index DIM_NRO_REDESPACHO_PK on CEN_DWS.DIM_NRO_REDESPACHO (
NRO_ID_PK
);

/*==============================================================*/
/* Index: NRO_REDESP_UK                                         */
/*==============================================================*/
create unique index NRO_REDESP_UK on CEN_DWS.DIM_NRO_REDESPACHO (
( NRO_VALOR )
);

/*==============================================================*/
/* Table: DIM_NUMERO                                            */
/*==============================================================*/
create table CEN_DWS.DIM_NUMERO (
   NUM_ID_PK            INT4                 not null,
   NUM_ANIO             INT2                 not null,
   NUM_NUMERO           INT2                 not null,
   NUM_CODIGO           VARCHAR(9)           not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_NUMERO primary key (NUM_ID_PK)
);

comment on table CEN_DWS.DIM_NUMERO is
'Dimensión que permite identificar los números de fallas de reportes.';

comment on column CEN_DWS.DIM_NUMERO.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_NUMERO owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_NUMERO_PK                                         */
/*==============================================================*/
create unique index DIM_NUMERO_PK on CEN_DWS.DIM_NUMERO (
NUM_ID_PK
);

/*==============================================================*/
/* Index: NUM_UK                                                */
/*==============================================================*/
create unique index NUM_UK on CEN_DWS.DIM_NUMERO (
( NUM_CODIGO )
);

/*==============================================================*/
/* Table: DIM_OBSERVACIONES_FALLA                               */
/*==============================================================*/
create table CEN_DWS.DIM_OBSERVACIONES_FALLA (
   OBSR_ID_PK           INT8                 not null,
   OBSR_ID_FALLA        INT8                 not null,
   OBSR_EVENTO_DTL_ID   INT4                 not null,
   OBSR_CAUSA           VARCHAR(1024)        not null,
   OBSR_PROTECCION_ACTUADA VARCHAR(500)         not null,
   OBSR_ELEMEN_DISP_DISTRIB VARCHAR(500)         not null,
   OBSR_OBS_GEN_PERDIDA VARCHAR(1024)        not null,
   OBSR_OBS_SNT_PERDIDA VARCHAR(1024)        not null,
   OBSR_CONSECUENCIAS   VARCHAR(2048)        not null,
   OBSR_OBS_GENERALES   VARCHAR(2048)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_OBSERVACIONES_FALLA primary key (OBSR_ID_PK)
);

comment on table CEN_DWS.DIM_OBSERVACIONES_FALLA is
'Dimensión que recopila todos los campos de texto de observaciones que son generales y relacionadas a la falla en global.';

comment on column CEN_DWS.DIM_OBSERVACIONES_FALLA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_OBSERVACIONES_FALLA owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_OBSERVACIONES_FALLA_PK                            */
/*==============================================================*/
create unique index DIM_OBSERVACIONES_FALLA_PK on CEN_DWS.DIM_OBSERVACIONES_FALLA (
OBSR_ID_PK
);

/*==============================================================*/
/* Index: OBSERV_UK                                             */
/*==============================================================*/
create unique index OBSERV_UK on CEN_DWS.DIM_OBSERVACIONES_FALLA (
( OBSR_ID_FALLA ),
( OBSR_EVENTO_DTL_ID )
);

/*==============================================================*/
/* Table: DIM_PARAM_HIDROLOGICOS                                */
/*==============================================================*/
create table CEN_DWS.DIM_PARAM_HIDROLOGICOS (
   PARAMHID_ID_PK       INT2                 not null,
   PARAMHID_ID_BK       VARCHAR(35)          not null,
   PARAMHID_NOMBRE      VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_PARAM_HIDROLOGICOS primary key (PARAMHID_ID_PK)
);

comment on table CEN_DWS.DIM_PARAM_HIDROLOGICOS is
'Dimensión de parametros hidrológicos. Permite deterrminar el valor de que tipo.';

comment on column CEN_DWS.DIM_PARAM_HIDROLOGICOS.PARAMHID_ID_PK is
'Corresponde a la calve primaria de parámetros de hidrología';

comment on column CEN_DWS.DIM_PARAM_HIDROLOGICOS.PARAMHID_ID_BK is
'Código de identificación del negocio del tipo de parámetro hidrológico';

comment on column CEN_DWS.DIM_PARAM_HIDROLOGICOS.PARAMHID_NOMBRE is
'Corresponde al nombre del parámetro de hidrología';

comment on column CEN_DWS.DIM_PARAM_HIDROLOGICOS.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_PARAM_HIDROLOGICOS owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_PARAM_HIDROLOGICOS_PK                             */
/*==============================================================*/
create unique index DIM_PARAM_HIDROLOGICOS_PK on CEN_DWS.DIM_PARAM_HIDROLOGICOS (
PARAMHID_ID_PK
);

/*==============================================================*/
/* Index: UK_PARAM_HIDRO                                        */
/*==============================================================*/
create unique index UK_PARAM_HIDRO on CEN_DWS.DIM_PARAM_HIDROLOGICOS (
PARAMHID_ID_BK
);

/*==============================================================*/
/* Table: DIM_PASOS                                             */
/*==============================================================*/
create table CEN_DWS.DIM_PASOS (
   PASOS_ID_PK          INT2                 not null,
   PASOS_NUMERO         INT2                 not null,
   PASOS_NOMBRE_PASO    VARCHAR(25)          not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_PASOS primary key (PASOS_ID_PK)
);

comment on table CEN_DWS.DIM_PASOS is
'Dimensión que permite identificar en que paso se dió la reconexión.';

comment on column CEN_DWS.DIM_PASOS.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_PASOS owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_PASOS_PK                                          */
/*==============================================================*/
create unique index DIM_PASOS_PK on CEN_DWS.DIM_PASOS (
PASOS_ID_PK
);

/*==============================================================*/
/* Index: PASOS_UK                                              */
/*==============================================================*/
create unique index PASOS_UK on CEN_DWS.DIM_PASOS (
( PASOS_NUMERO )
);

/*==============================================================*/
/* Table: DIM_TIEMPO                                            */
/*==============================================================*/
create table CEN_DWS.DIM_TIEMPO (
   TMPO_ID_PK           INT4                 not null,
   TMPO_FECHA           DATE                 not null,
   TMPO_FECHA_DESCRIPCION VARCHAR(32)          not null,
   TMPO_ANIO            INT2                 not null,
   TMPO_MES_ID          INT2                 not null,
   TMPO_MES             VARCHAR(16)          not null,
   TMPO_DIA_ID          INT2                 not null,
   TMPO_DIA             VARCHAR(16)          not null,
   TMPO_SEMESTRE        INT2                 not null,
   TMPO_SEMANA_OPERATIVA INT2                 not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_TIEMPO primary key (TMPO_ID_PK)
);

comment on table CEN_DWS.DIM_TIEMPO is
'Corresponde a los datos de todas las fechas en formato yyyy-MM-dd';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_ID_PK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_FECHA is
'Campo fecha de tipo Datetime, corresponde a la fecha del análisis.';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_FECHA_DESCRIPCION is
'Descripción de la fecha de análisis en palabras.';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_ANIO is
'Corresponde al año de análisis.';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_MES_ID is
'Corresponde al número del mes de análisis';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_MES is
'Corresponde al nombre del mes de análisis';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_DIA_ID is
'Corresponde al día de análisis';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_DIA is
'Corresponde al nombre del día del análisis.';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_SEMESTRE is
'Corresponde al semestre del año del análisis.';

comment on column CEN_DWS.DIM_TIEMPO.TMPO_SEMANA_OPERATIVA is
'Columna que contiene la semana que inicianoperaciones CENACE, donde jueves es el día de inicio y miércoles es el día de fin.';

comment on column CEN_DWS.DIM_TIEMPO.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_TIEMPO owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_TIEMPO_PK                                         */
/*==============================================================*/
create unique index DIM_TIEMPO_PK on CEN_DWS.DIM_TIEMPO (
TMPO_ID_PK
);

/*==============================================================*/
/* Index: UK_TIEMPO                                             */
/*==============================================================*/
create unique index UK_TIEMPO on CEN_DWS.DIM_TIEMPO (
TMPO_FECHA
);

/*==============================================================*/
/* Table: DIM_TIPO_COMBUSTIBLE                                  */
/*==============================================================*/
create table CEN_DWS.DIM_TIPO_COMBUSTIBLE (
   TIPCOMB_ID_PK        INT2                 not null,
   TIPCOMB_GPL_ID_BK    VARCHAR(20)          not null,
   TIPCOMB_GPL_COMBUSTIBLE VARCHAR(100)         not null,
   TIPCOMB_GOP_ID_BK    VARCHAR(20)          not null,
   TIPCOMB_GOP_COMBUSTIBLE VARCHAR(100)         not null,
   TIPCOMB_GTC_ID_BK    VARCHAR(20)          not null,
   TIPCOMB_GTC_COMBUSTIBLE VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_TIPO_COMBUSTIBLE primary key (TIPCOMB_ID_PK)
);

comment on table CEN_DWS.DIM_TIPO_COMBUSTIBLE is
'Representa el tipo de conbustible con el que operan las unidades';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.TIPCOMB_ID_PK is
'Clave principal del tipo de combustible';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.TIPCOMB_GPL_ID_BK is
'Clave del negocio del tipo de combustible de GPL';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.TIPCOMB_GPL_COMBUSTIBLE is
'Corresponde al nombre del tipo de combustible DE GPL';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.TIPCOMB_GOP_ID_BK is
'Clave del negocio del tipo de combustible de GOP';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.TIPCOMB_GOP_COMBUSTIBLE is
'Corresponde al nombre del tipo de combustible DE GOP';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.TIPCOMB_GTC_ID_BK is
'Clave del negocio del tipo de combustible de GTC';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.TIPCOMB_GTC_COMBUSTIBLE is
'Corresponde al nombre del tipo de combustible DE GTC';

comment on column CEN_DWS.DIM_TIPO_COMBUSTIBLE.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_TIPO_COMBUSTIBLE owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_TIPO_COMBUSTIBLE_PK                               */
/*==============================================================*/
create unique index DIM_TIPO_COMBUSTIBLE_PK on CEN_DWS.DIM_TIPO_COMBUSTIBLE (
TIPCOMB_ID_PK
);

/*==============================================================*/
/* Index: TIP_COMB_UK                                           */
/*==============================================================*/
create unique index TIP_COMB_UK on CEN_DWS.DIM_TIPO_COMBUSTIBLE (
( TIPCOMB_GOP_ID_BK ),
( TIPCOMB_GPL_ID_BK ),
( TIPCOMB_GTC_ID_BK )
);

/*==============================================================*/
/* Table: DIM_TIPO_GENERACION                                   */
/*==============================================================*/
create table CEN_DWS.DIM_TIPO_GENERACION (
   TIPGEN_ID_PK         INT2                 not null,
   TIPGEN_ID_BK         VARCHAR(20)          not null,
   TIPGEN_TIPO_GENERACION VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_TIPO_GENERACION primary key (TIPGEN_ID_PK)
);

comment on table CEN_DWS.DIM_TIPO_GENERACION is
'Representa el tipo de generación ';

comment on column CEN_DWS.DIM_TIPO_GENERACION.TIPGEN_ID_PK is
'Corresponde a la clave principal del tipo de generación';

comment on column CEN_DWS.DIM_TIPO_GENERACION.TIPGEN_ID_BK is
'Corresponde al código del tipo de generación';

comment on column CEN_DWS.DIM_TIPO_GENERACION.TIPGEN_TIPO_GENERACION is
'Corresponde al nombre del tipo de generación ';

comment on column CEN_DWS.DIM_TIPO_GENERACION.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_TIPO_GENERACION owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_TIPO_GENERACION_PK                                */
/*==============================================================*/
create unique index DIM_TIPO_GENERACION_PK on CEN_DWS.DIM_TIPO_GENERACION (
TIPGEN_ID_PK
);

/*==============================================================*/
/* Index: TIP_GEN_UK                                            */
/*==============================================================*/
create unique index TIP_GEN_UK on CEN_DWS.DIM_TIPO_GENERACION (
( TIPGEN_ID_BK )
);

/*==============================================================*/
/* Table: DIM_TIPO_TECNOLOGIA                                   */
/*==============================================================*/
create table CEN_DWS.DIM_TIPO_TECNOLOGIA (
   TIPTEC_ID_PK         INT2                 not null,
   TIPTEC_ID_BK         VARCHAR(20)          not null,
   TIPTEC_TIPO_TECNOLOGIA VARCHAR(100)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_DIM_TIPO_TECNOLOGIA primary key (TIPTEC_ID_PK)
);

comment on table CEN_DWS.DIM_TIPO_TECNOLOGIA is
'Representa el tipo de tecnología que se usa para producir energía de un cierto elemento. ';

comment on column CEN_DWS.DIM_TIPO_TECNOLOGIA.TIPTEC_ID_PK is
'Corresponde a la clave primaria del tipo de tecnología';

comment on column CEN_DWS.DIM_TIPO_TECNOLOGIA.TIPTEC_ID_BK is
'Corresponde al código del negocio del tipo de tecnología';

comment on column CEN_DWS.DIM_TIPO_TECNOLOGIA.TIPTEC_TIPO_TECNOLOGIA is
'Corresponde al nombre del tipo de tecnología';

comment on column CEN_DWS.DIM_TIPO_TECNOLOGIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.DIM_TIPO_TECNOLOGIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: DIM_TIPO_TECNOLOGIA_PK                                */
/*==============================================================*/
create unique index DIM_TIPO_TECNOLOGIA_PK on CEN_DWS.DIM_TIPO_TECNOLOGIA (
TIPTEC_ID_PK
);

/*==============================================================*/
/* Index: TIP_TEC_UK                                            */
/*==============================================================*/
create unique index TIP_TEC_UK on CEN_DWS.DIM_TIPO_TECNOLOGIA (
( TIPTEC_ID_BK )
);

/*==============================================================*/
/* Table: FACT_AGC                                              */
/*==============================================================*/
create table CEN_DWS.FACT_AGC (
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   AGT_CENT_ID_FK       INT2                 not null,
   AGT_ID_FK            INT4                 not null,
   NUM_ID_FK            INT4                 not null,
   PASOS_ID_FK          INT2                 not null,
   AGCMODO_ID_FK        INT2                 not null,
   AGC_BIAS             NUMERIC(10,2)        not null,
   AGC_FRECUENCIA       NUMERIC(5,2)         not null,
   AGC_RPF_ANTES        NUMERIC(10,2)        not null,
   AGC_RPF_DESPUES      NUMERIC(10,2)        not null,
   AGC_INTERCAMBIO_PREVIO NUMERIC(10,2)        not null,
   AGC_INTERCAMBIO_PROGRAMADO NUMERIC(10,2)        not null,
   AGC_INTERCAMBIO_POST NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_AGC primary key (TMPO_ID_FK, HORA_ID_FK, AGT_CENT_ID_FK, AGT_ID_FK, NUM_ID_FK, PASOS_ID_FK, AGCMODO_ID_FK)
);

comment on table CEN_DWS.FACT_AGC is
'Permite determinar otros indicadores refrentes a AGC.';

comment on column CEN_DWS.FACT_AGC.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_AGC.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_AGC.AGT_CENT_ID_FK is
'Identificador de la tabla de agentes central.';

comment on column CEN_DWS.FACT_AGC.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_AGC.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_AGC owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_AGC_PK                                           */
/*==============================================================*/
create unique index FACT_AGC_PK on CEN_DWS.FACT_AGC (
TMPO_ID_FK,
HORA_ID_FK,
AGT_CENT_ID_FK,
AGT_ID_FK,
NUM_ID_FK,
PASOS_ID_FK,
AGCMODO_ID_FK
);

/*==============================================================*/
/* Index: AGT_AGC_NK_FK                                         */
/*==============================================================*/
create  index AGT_AGC_NK_FK on CEN_DWS.FACT_AGC (
AGT_ID_FK
);

/*==============================================================*/
/* Index: AGT_CEN_AGC_NK_FK                                     */
/*==============================================================*/
create  index AGT_CEN_AGC_NK_FK on CEN_DWS.FACT_AGC (
AGT_CENT_ID_FK
);

/*==============================================================*/
/* Index: HORA_FALLA_AGC_NK_FK                                  */
/*==============================================================*/
create  index HORA_FALLA_AGC_NK_FK on CEN_DWS.FACT_AGC (
HORA_ID_FK
);

/*==============================================================*/
/* Index: MODO_AGC_NK_FK                                        */
/*==============================================================*/
create  index MODO_AGC_NK_FK on CEN_DWS.FACT_AGC (
AGCMODO_ID_FK
);

/*==============================================================*/
/* Index: NUM_AGC_NK_FK                                         */
/*==============================================================*/
create  index NUM_AGC_NK_FK on CEN_DWS.FACT_AGC (
NUM_ID_FK
);

/*==============================================================*/
/* Index: PASO_AGC_NK_FK                                        */
/*==============================================================*/
create  index PASO_AGC_NK_FK on CEN_DWS.FACT_AGC (
PASOS_ID_FK
);

/*==============================================================*/
/* Index: TMPO_FALLA_AGC_NK_FK                                  */
/*==============================================================*/
create  index TMPO_FALLA_AGC_NK_FK on CEN_DWS.FACT_AGC (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_BGEN_DIARIO                                      */
/*==============================================================*/
create table CEN_DWS.FACT_BGEN_DIARIO (
   TMPO_ID_FK           INT4                 not null,
   ENERG_DEMANDA        NUMERIC(10,2)        not null,
   ENERG_PRODUCCION     NUMERIC(10,2)        not null,
   ENERG_CONSUMO        NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_BGEN_DIARIO primary key (TMPO_ID_FK)
);

comment on table CEN_DWS.FACT_BGEN_DIARIO is
'Tabla que contiene información de la demanda en bornes de generación, producción y consumo diario.';

comment on column CEN_DWS.FACT_BGEN_DIARIO.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_BGEN_DIARIO.ENERG_DEMANDA is
'Valor de medida de demanda en bornes de generación diaria';

comment on column CEN_DWS.FACT_BGEN_DIARIO.ENERG_PRODUCCION is
'Corresponde al valor de producción en bornes de generación diaria';

comment on column CEN_DWS.FACT_BGEN_DIARIO.ENERG_CONSUMO is
'Corresponde al consumo de exportaciones diarias';

comment on column CEN_DWS.FACT_BGEN_DIARIO.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_BGEN_DIARIO owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_BGEN_DIARIO_PK                                   */
/*==============================================================*/
create unique index FACT_BGEN_DIARIO_PK on CEN_DWS.FACT_BGEN_DIARIO (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_BGEN_MEDIOHORARIO                                */
/*==============================================================*/
create table CEN_DWS.FACT_BGEN_MEDIOHORARIO (
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   PROD_DEMANDA         NUMERIC(10,2)        not null,
   PROD_PRODUCCION      NUMERIC(10,2)        not null,
   PROD_CONSUMO         NUMERIC(10,2)        not null,
   PROD_IMPORTACION     NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_BGEN_MEDIOHORARIO primary key (TMPO_ID_FK, HORA_ID_FK)
);

comment on table CEN_DWS.FACT_BGEN_MEDIOHORARIO is
'Tabla que contiene información de la demanda en bornes de generación, producción y consumo medio horario';

comment on column CEN_DWS.FACT_BGEN_MEDIOHORARIO.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_BGEN_MEDIOHORARIO.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_BGEN_MEDIOHORARIO.PROD_DEMANDA is
'Valor de medida de demanda en bornes de generación medio horaria';

comment on column CEN_DWS.FACT_BGEN_MEDIOHORARIO.PROD_PRODUCCION is
'Corresponde al valor de producción en bornes de generación medio horaria';

comment on column CEN_DWS.FACT_BGEN_MEDIOHORARIO.PROD_CONSUMO is
'Corresponde al consumo de exportaciones medio horarias';

comment on column CEN_DWS.FACT_BGEN_MEDIOHORARIO.PROD_IMPORTACION is
'Corresponde al valor de la importaciones mediohoraria';

comment on column CEN_DWS.FACT_BGEN_MEDIOHORARIO.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_BGEN_MEDIOHORARIO owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_BGEN_MH_PK                                       */
/*==============================================================*/
create unique index FACT_BGEN_MH_PK on CEN_DWS.FACT_BGEN_MEDIOHORARIO (
TMPO_ID_FK,
HORA_ID_FK
);

/*==============================================================*/
/* Index: HOR_BGEN_MH_NK_FK                                     */
/*==============================================================*/
create  index HOR_BGEN_MH_NK_FK on CEN_DWS.FACT_BGEN_MEDIOHORARIO (
HORA_ID_FK
);

/*==============================================================*/
/* Index: TMP_BGEN_MH_NK_FK                                     */
/*==============================================================*/
create  index TMP_BGEN_MH_NK_FK on CEN_DWS.FACT_BGEN_MEDIOHORARIO (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_DEMANDA_DIARIA                                   */
/*==============================================================*/
create table CEN_DWS.FACT_DEMANDA_DIARIA (
   TMPO_ID_FK           INT4                 not null,
   ELDEM_ID_FK          INT2                 not null,
   COMPDEM_ID_FK        INT2                 not null,
   AGT_ID_FK            INT4                 not null,
   DEMDR_POTENCIA       NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_DEMANDA_DIARIA primary key (TMPO_ID_FK, ELDEM_ID_FK, COMPDEM_ID_FK, AGT_ID_FK)
);

comment on table CEN_DWS.FACT_DEMANDA_DIARIA is
'Corresponde al análisis de demanda diaria';

comment on column CEN_DWS.FACT_DEMANDA_DIARIA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_DEMANDA_DIARIA.ELDEM_ID_FK is
'Identificador de la tabla de elementos de demanda.';

comment on column CEN_DWS.FACT_DEMANDA_DIARIA.COMPDEM_ID_FK is
'Corresponde a la clave primaria de la tabla para identificar el componente de demanda';

comment on column CEN_DWS.FACT_DEMANDA_DIARIA.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_DEMANDA_DIARIA.DEMDR_POTENCIA is
'Corresponde al valor de la demanda de potencia diaria';

comment on column CEN_DWS.FACT_DEMANDA_DIARIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_DEMANDA_DIARIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_DEM_DR_PK                                        */
/*==============================================================*/
create unique index FACT_DEM_DR_PK on CEN_DWS.FACT_DEMANDA_DIARIA (
TMPO_ID_FK,
ELDEM_ID_FK,
COMPDEM_ID_FK,
AGT_ID_FK
);

/*==============================================================*/
/* Index: AGT_DEM_NK_FK                                         */
/*==============================================================*/
create  index AGT_DEM_NK_FK on CEN_DWS.FACT_DEMANDA_DIARIA (
AGT_ID_FK
);

/*==============================================================*/
/* Index: COMP_DEM_NK_FK                                        */
/*==============================================================*/
create  index COMP_DEM_NK_FK on CEN_DWS.FACT_DEMANDA_DIARIA (
COMPDEM_ID_FK
);

/*==============================================================*/
/* Index: ELEM_DEM_NK_FK                                        */
/*==============================================================*/
create  index ELEM_DEM_NK_FK on CEN_DWS.FACT_DEMANDA_DIARIA (
ELDEM_ID_FK
);

/*==============================================================*/
/* Index: TMP_DEM_NK_FK                                         */
/*==============================================================*/
create  index TMP_DEM_NK_FK on CEN_DWS.FACT_DEMANDA_DIARIA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_DEMANDA_MEDIO_HORARIA                            */
/*==============================================================*/
create table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA (
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   AGT_ID_FK            INT4                 not null,
   ELDEM_ID_FK          INT2                 not null,
   COMPDEM_ID_FK        INT2                 not null,
   DEMMH_POTENCIA       NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_DEMANDA_MEDIO_HORARIA primary key (TMPO_ID_FK, HORA_ID_FK, AGT_ID_FK, ELDEM_ID_FK, COMPDEM_ID_FK)
);

comment on table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA is
'Corresponde a la demanda en puntos de entrega y en unidades asociadas a cada punto de entrega correspondiente a generación inmersa';

comment on column CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA.ELDEM_ID_FK is
'Identificador de la tabla de elementos de demanda.';

comment on column CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA.COMPDEM_ID_FK is
'Corresponde a la clave primaria de la tabla para identificar el componente de demanda';

comment on column CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA.DEMMH_POTENCIA is
'Corresponde al valor de potencia para entregas y generación inmersa. ';

comment on column CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_DEM_MH_PK                                        */
/*==============================================================*/
create unique index FACT_DEM_MH_PK on CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA (
TMPO_ID_FK,
HORA_ID_FK,
AGT_ID_FK,
ELDEM_ID_FK,
COMPDEM_ID_FK
);

/*==============================================================*/
/* Index: AGT_DEM_MH_NK_FK                                      */
/*==============================================================*/
create  index AGT_DEM_MH_NK_FK on CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA (
AGT_ID_FK
);

/*==============================================================*/
/* Index: COMP_DEM_MH_NK_FK                                     */
/*==============================================================*/
create  index COMP_DEM_MH_NK_FK on CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA (
COMPDEM_ID_FK
);

/*==============================================================*/
/* Index: ELEM_DEM_MH_NK_FK                                     */
/*==============================================================*/
create  index ELEM_DEM_MH_NK_FK on CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA (
ELDEM_ID_FK
);

/*==============================================================*/
/* Index: HORA_DEM_NK_FK                                        */
/*==============================================================*/
create  index HORA_DEM_NK_FK on CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA (
HORA_ID_FK
);

/*==============================================================*/
/* Index: TMP_DEM_MH_NK_FK                                      */
/*==============================================================*/
create  index TMP_DEM_MH_NK_FK on CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_DESP_REDESP_HORARIO                              */
/*==============================================================*/
create table CEN_DWS.FACT_DESP_REDESP_HORARIO (
   AGT_ID_FK            INT4                 not null,
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   TIPCOMB_ID_FK        INT2                 not null,
   TIPGEN_ID_FK         INT2                 not null,
   TIPTEC_ID_FK         INT2                 not null,
   NRO_ID_FK            INT4                 not null,
   REDESP_MV            NUMERIC(10,2)        not null,
   REDESP_PRECIO        NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_DESP_REDESP_HORARIO primary key (AGT_ID_FK, TMPO_ID_FK, HORA_ID_FK, TIPCOMB_ID_FK, TIPGEN_ID_FK, TIPTEC_ID_FK, NRO_ID_FK)
);

comment on table CEN_DWS.FACT_DESP_REDESP_HORARIO is
'Contiene información del despacho y redespacho horario ';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.TIPCOMB_ID_FK is
'Clave principal del tipo de combustible';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.TIPGEN_ID_FK is
'Corresponde a la clave principal del tipo de generación';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.TIPTEC_ID_FK is
'Corresponde a la clave primaria del tipo de tecnología';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.NRO_ID_FK is
'Corresponde a la clave primaria de la tabla, para identificar los registro únicos del número de redespachos';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.REDESP_MV is
'Valor de potencia activa del redespacho';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.REDESP_PRECIO is
'Corresponde al valor monetario vigente que establece el precio de la generación';

comment on column CEN_DWS.FACT_DESP_REDESP_HORARIO.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_DESP_REDESP_HORARIO owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_DESP_REDESP_PK                                   */
/*==============================================================*/
create unique index FACT_DESP_REDESP_PK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
AGT_ID_FK,
TMPO_ID_FK,
HORA_ID_FK,
TIPCOMB_ID_FK,
TIPGEN_ID_FK,
TIPTEC_ID_FK,
NRO_ID_FK
);

/*==============================================================*/
/* Index: AGT_DESP_NK_FK                                        */
/*==============================================================*/
create  index AGT_DESP_NK_FK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
AGT_ID_FK
);

/*==============================================================*/
/* Index: HORA_DESP_NK_FK                                       */
/*==============================================================*/
create  index HORA_DESP_NK_FK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
HORA_ID_FK
);

/*==============================================================*/
/* Index: NRO_DESP_NK_FK                                        */
/*==============================================================*/
create  index NRO_DESP_NK_FK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
NRO_ID_FK
);

/*==============================================================*/
/* Index: TIP_COMB_DESP_NK_FK                                   */
/*==============================================================*/
create  index TIP_COMB_DESP_NK_FK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: TIP_GEN_DESP_NK_FK                                    */
/*==============================================================*/
create  index TIP_GEN_DESP_NK_FK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
TIPGEN_ID_FK
);

/*==============================================================*/
/* Index: TIP_TEC_DESP_NK_FK                                    */
/*==============================================================*/
create  index TIP_TEC_DESP_NK_FK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
TIPTEC_ID_FK
);

/*==============================================================*/
/* Index: TMP_DESP_NK_FK                                        */
/*==============================================================*/
create  index TMP_DESP_NK_FK on CEN_DWS.FACT_DESP_REDESP_HORARIO (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_DIST_ENS                                         */
/*==============================================================*/
create table CEN_DWS.FACT_DIST_ENS (
   AGT_ID_FK            INT4                 not null,
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   AGTORG_ID_FK         INT2                 not null,
   ASIGORG_ID_FK        INT2                 not null,
   NUM_ID_FK            INT4                 not null,
   PASOS_ID_FK          INT2                 not null,
   ENSD_INTERNA_TMP     NUMERIC(10,3)        not null,
   ENSD_INTERNA_CRG     NUMERIC(10,2)        not null,
   ENSD_INTERNA         NUMERIC(10,2)        not null,
   ENSD_AFECT_SNT_TMP   NUMERIC(10,3)        not null,
   ENSD_AFECT_SNT_CRG   NUMERIC(10,2)        not null,
   ENSD_AFECT_SNT       NUMERIC(10,2)        not null,
   ENSD_EXT_TRANS_TMP   NUMERIC(10,3)        not null,
   ENSD_EXT_TRANS_CRG   NUMERIC(10,2)        not null,
   ENSD_EXT_TRANS       NUMERIC(10,2)        not null,
   ENSD_EXT_GEN_TMP     NUMERIC(10,3)        not null,
   ENSD_EXT_GEN_CRG     NUMERIC(10,2)        not null,
   ENSD_EXT_GEN         NUMERIC(10,2)        not null,
   ENSD_EAC_TMP         NUMERIC(10,3)        not null,
   ENSD_EAC_CRG         NUMERIC(10,2)        not null,
   ENSD_EAC             NUMERIC(10,2)        not null,
   ENSD_SISTEMICO_SPS_TMP NUMERIC(10,3)        not null,
   ENSD_SISTEMICO_SPS_CRG NUMERIC(10,2)        not null,
   ENSD_SISTEMICO_SPS   NUMERIC(10,2)        not null,
   ENSD_OTROS_TMP       NUMERIC(10,3)        not null,
   ENSD_OTROS_CRG       NUMERIC(10,2)        not null,
   ENSD_OTROS           NUMERIC(10,2)        not null,
   ENSD_NO_DEFINIDO_TMP NUMERIC(10,2)        not null,
   ENSD_NO_DEFINIDO_CRG NUMERIC(10,2)        not null,
   ENSD_NO_DEFINIDO     NUMERIC(10,2)        not null,
   ENSD_MANUAL          NUMERIC(10,2)        not null,
   ENSD_ES              NUMERIC(10,2)        not null,
   ENSD_ELEMENTO_PRINCIPAL BOOL                 not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_DIST_ENS primary key (AGT_ID_FK, TMPO_ID_FK, HORA_ID_FK, AGTORG_ID_FK, ASIGORG_ID_FK, NUM_ID_FK, PASOS_ID_FK)
);

comment on table CEN_DWS.FACT_DIST_ENS is
'Tabla de hechos para realizar el análisis de la energía no suministrada de empresas distribuidoras.';

comment on column CEN_DWS.FACT_DIST_ENS.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_DIST_ENS.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_DIST_ENS.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_DIST_ENS.ASIGORG_ID_FK is
'Identificador de la falla.';

comment on column CEN_DWS.FACT_DIST_ENS.ENSD_MANUAL is
'Energía no suministrada manual.';

comment on column CEN_DWS.FACT_DIST_ENS.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_DIST_ENS owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_DIST_ENS_PK                                      */
/*==============================================================*/
create unique index FACT_DIST_ENS_PK on CEN_DWS.FACT_DIST_ENS (
AGT_ID_FK,
TMPO_ID_FK,
HORA_ID_FK,
AGTORG_ID_FK,
ASIGORG_ID_FK,
NUM_ID_FK,
PASOS_ID_FK
);

/*==============================================================*/
/* Index: AGTORG_DIST_NK_FK                                     */
/*==============================================================*/
create  index AGTORG_DIST_NK_FK on CEN_DWS.FACT_DIST_ENS (
AGTORG_ID_FK
);

/*==============================================================*/
/* Index: AGT_DIST_NK_FK                                        */
/*==============================================================*/
create  index AGT_DIST_NK_FK on CEN_DWS.FACT_DIST_ENS (
AGT_ID_FK
);

/*==============================================================*/
/* Index: HORA_FALLA_DIST_NK_FK                                 */
/*==============================================================*/
create  index HORA_FALLA_DIST_NK_FK on CEN_DWS.FACT_DIST_ENS (
HORA_ID_FK
);

/*==============================================================*/
/* Index: NUM_DIST_NK_FK                                        */
/*==============================================================*/
create  index NUM_DIST_NK_FK on CEN_DWS.FACT_DIST_ENS (
NUM_ID_FK
);

/*==============================================================*/
/* Index: ORG_DIST_NK_FK                                        */
/*==============================================================*/
create  index ORG_DIST_NK_FK on CEN_DWS.FACT_DIST_ENS (
ASIGORG_ID_FK
);

/*==============================================================*/
/* Index: PASO_DIST_NK_FK                                       */
/*==============================================================*/
create  index PASO_DIST_NK_FK on CEN_DWS.FACT_DIST_ENS (
PASOS_ID_FK
);

/*==============================================================*/
/* Index: TMPO_FALLA_DIST_NK_FK                                 */
/*==============================================================*/
create  index TMPO_FALLA_DIST_NK_FK on CEN_DWS.FACT_DIST_ENS (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_ENS_ES                                           */
/*==============================================================*/
create table CEN_DWS.FACT_ENS_ES (
   TMPO_ID_FK           INT4                 not null,
   AGT_ID_FK            INT4                 not null,
   AGTORG_ID_FK         INT2                 not null,
   ASIGORG_ID_FK        INT2                 not null,
   ENSES_ENSM           NUMERIC(10,2)        not null,
   ENSES_ES             NUMERIC(10,2)        not null,
   constraint PK_FACT_ENS_ES primary key (TMPO_ID_FK, AGT_ID_FK, AGTORG_ID_FK, ASIGORG_ID_FK)
);

comment on table CEN_DWS.FACT_ENS_ES is
'Tabla de hechos que permite realizar un análisis de la Energía No Suministrada versus Energía Suministrada.';

comment on column CEN_DWS.FACT_ENS_ES.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_ENS_ES.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_ENS_ES.ASIGORG_ID_FK is
'Identificador de la falla.';

-- set table ownership
--alter table CEN_DWS.FACT_ENS_ES owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_ENS_ES_PK                                        */
/*==============================================================*/
create unique index FACT_ENS_ES_PK on CEN_DWS.FACT_ENS_ES (
TMPO_ID_FK,
AGT_ID_FK,
AGTORG_ID_FK,
ASIGORG_ID_FK
);

/*==============================================================*/
/* Index: AGTARG_ENS_ES_NK_FK                                   */
/*==============================================================*/
create  index AGTARG_ENS_ES_NK_FK on CEN_DWS.FACT_ENS_ES (
AGTORG_ID_FK
);

/*==============================================================*/
/* Index: AGT_ENS_ES_NK_FK                                      */
/*==============================================================*/
create  index AGT_ENS_ES_NK_FK on CEN_DWS.FACT_ENS_ES (
AGT_ID_FK
);

/*==============================================================*/
/* Index: ORG_ENS_ES_NK_FK                                      */
/*==============================================================*/
create  index ORG_ENS_ES_NK_FK on CEN_DWS.FACT_ENS_ES (
ASIGORG_ID_FK
);

/*==============================================================*/
/* Index: TMPO_ENS_ES_NK_FK                                     */
/*==============================================================*/
create  index TMPO_ENS_ES_NK_FK on CEN_DWS.FACT_ENS_ES (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_FACTOR_CARGA                                     */
/*==============================================================*/
create table CEN_DWS.FACT_FACTOR_CARGA (
   TMPO_ID_FK           INT4                 not null,
   FACTCRG_FACT_CARGA   DECIMAL(5,2)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_FACTOR_CARGA primary key (TMPO_ID_FK)
);

comment on table CEN_DWS.FACT_FACTOR_CARGA is
'Corresponde al valor de cálculo del factor de carga anual segregado por central. ';

comment on column CEN_DWS.FACT_FACTOR_CARGA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_FACTOR_CARGA.FACTCRG_FACT_CARGA is
'Valor de la medida de factor de carga';

comment on column CEN_DWS.FACT_FACTOR_CARGA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_FACTOR_CARGA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_FACTR_CAR_PK                                     */
/*==============================================================*/
create unique index FACT_FACTR_CAR_PK on CEN_DWS.FACT_FACTOR_CARGA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_FACTOR_PLANTA                                    */
/*==============================================================*/
create table CEN_DWS.FACT_FACTOR_PLANTA (
   TMPO_ID_FK           INT4                 not null,
   AGT_CENT_ID_FK       INT2                 not null,
   FACTPLAN_FACT_PLANTA DECIMAL(5,2)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_FACTOR_PLANTA primary key (TMPO_ID_FK, AGT_CENT_ID_FK)
);

comment on table CEN_DWS.FACT_FACTOR_PLANTA is
'Tabla histórica con los valores de factor de planta ';

comment on column CEN_DWS.FACT_FACTOR_PLANTA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_FACTOR_PLANTA.AGT_CENT_ID_FK is
'Identificador de la tabla de agentes central.';

comment on column CEN_DWS.FACT_FACTOR_PLANTA.FACTPLAN_FACT_PLANTA is
'Valor de la medida del factor de planta';

comment on column CEN_DWS.FACT_FACTOR_PLANTA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_FACTOR_PLANTA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_FACTR_PLAN_PK                                    */
/*==============================================================*/
create unique index FACT_FACTR_PLAN_PK on CEN_DWS.FACT_FACTOR_PLANTA (
TMPO_ID_FK,
AGT_CENT_ID_FK
);

/*==============================================================*/
/* Index: AGT_CEN_FACT_PLAN_NK_FK                               */
/*==============================================================*/
create  index AGT_CEN_FACT_PLAN_NK_FK on CEN_DWS.FACT_FACTOR_PLANTA (
AGT_CENT_ID_FK
);

/*==============================================================*/
/* Index: TMP_FACT_PLAN_NK_FK                                   */
/*==============================================================*/
create  index TMP_FACT_PLAN_NK_FK on CEN_DWS.FACT_FACTOR_PLANTA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_FALLAS_SNI                                       */
/*==============================================================*/
create table CEN_DWS.FACT_FALLAS_SNI (
   AGT_ID_FK            INT4                 not null,
   TMPO_FALLA_ID_FK     INT4                 not null,
   HORA_FALLA_ID_FK     INT2                 not null,
   TMPO_DISPON_ID_FK    INT4                 not null,
   TMPO_CIERRE_ID_FK    INT4                 not null,
   TMPO_MAX_NORM_ID_FK  INT4                 not null,
   TMPO_FIN_ID_FK       INT4                 not null,
   HORA_DISPON_ID_FK    INT2                 not null,
   HORA_FIN_ID_FK       INT2                 not null,
   HORA_CIERRE_ID_FK    INT2                 not null,
   HORA_MAX_NORM_ID_FK  INT2                 not null,
   ASIGORG_ID_FK        INT2                 not null,
   AGTORG_ID_FK         INT2                 not null,
   AGTEQV_ID_FK         INT4                 not null,
   CLASF_ID_FK          INT4                 not null,
   NUM_ID_FK            INT4                 not null,
   OBSR_ID_FK           INT8                 not null,
   FALLA_TMP_INDISPONIBILIDAD NUMERIC(10,2)        not null,
   FALLA_TMP_NORMALIZACION_CARGA NUMERIC(10,2)        not null,
   FALLA_POTENCIA_DISPONIBLE NUMERIC(10,2)        not null,
   FALLA_POTENCIA_DISPARADA NUMERIC(10,2)        not null,
   FALLA_CARGA_DESCONECTADA NUMERIC(10,2)        not null,
   FALLA_CARGA_DESCONECTADA_EAC NUMERIC(10,2)        not null,
   FALLA_ENS_TRN        NUMERIC(10,2)        not null,
   FALLA_ENS_TRN_MAN    NUMERIC(10,2)        not null,
   FALLA_ENS_SISTEMA    NUMERIC(10,2)        not null,
   FALLA_ENS_SISTEMA_EAC NUMERIC(10,2)        not null,
   FALLA_ENS_SISTEMA_MAN NUMERIC(10,2)        not null,
   FALLA_ELEMENTO_PRINCIPAL BOOL                 not null,
   FALLA_CONSECUENCIA   BOOL                 not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_FALLAS_SNI primary key (AGT_ID_FK, TMPO_FALLA_ID_FK, HORA_FALLA_ID_FK, ASIGORG_ID_FK, AGTORG_ID_FK, AGTEQV_ID_FK, CLASF_ID_FK, NUM_ID_FK)
);

comment on table CEN_DWS.FACT_FALLAS_SNI is
'Tabla de hechos para realizar análisis de fallas por interrupciones.';

comment on column CEN_DWS.FACT_FALLAS_SNI.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_FALLAS_SNI.TMPO_FALLA_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_FALLAS_SNI.HORA_FALLA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_FALLAS_SNI.TMPO_DISPON_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_FALLAS_SNI.TMPO_CIERRE_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_FALLAS_SNI.TMPO_MAX_NORM_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_FALLAS_SNI.TMPO_FIN_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_FALLAS_SNI.HORA_DISPON_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_FALLAS_SNI.HORA_FIN_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_FALLAS_SNI.HORA_CIERRE_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_FALLAS_SNI.HORA_MAX_NORM_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_FALLAS_SNI.ASIGORG_ID_FK is
'Identificador de la falla.';

comment on column CEN_DWS.FACT_FALLAS_SNI.AGTEQV_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_TMP_INDISPONIBILIDAD is
'Tiempo de indisponibilidad del elemento';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_TMP_NORMALIZACION_CARGA is
'Tiempo que dura la normalización de la carga.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_POTENCIA_DISPONIBLE is
'Potencia con la que se disponibilizó el elemento.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_POTENCIA_DISPARADA is
'Potencia disparada del elemento.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_CARGA_DESCONECTADA is
'Carga desconectada por toda la falla, aplicada solamente al elemento principal de la falla.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_CARGA_DESCONECTADA_EAC is
'Carga desconectada por EAC de toda la falla aplicada solamente al registro principal de la misma.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_ENS_TRN is
'Es la ENS de transmisión, donde se multiplica la potencia dsiparada por la diferencia de fechas entre fecha de falla y la fecha de normalización en el caso de que sea menor a fecha fin y menor a fecha disponible, sino se resta entre la fecha disponible.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_ENS_TRN_MAN is
'ENS de transmisión ingresada manualmente desde SAF.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_ENS_SISTEMA is
'ENS del sistema calculado por falla y aplicado solamente al elemento principal de la falla.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_ENS_SISTEMA_EAC is
'ENS del sistema asignado a EAC, esquema de alivio de carga.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_ENS_SISTEMA_MAN is
'ENS ingresada manualmente desde SAF, Área bajo la curva.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FALLA_ELEMENTO_PRINCIPAL is
'Permite identificar cual es el elemento principal de la falla.';

comment on column CEN_DWS.FACT_FALLAS_SNI.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_FALLAS_SNI owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_FALLAS_SNI_PK                                    */
/*==============================================================*/
create unique index FACT_FALLAS_SNI_PK on CEN_DWS.FACT_FALLAS_SNI (
AGT_ID_FK,
TMPO_FALLA_ID_FK,
HORA_FALLA_ID_FK,
ASIGORG_ID_FK,
AGTORG_ID_FK,
AGTEQV_ID_FK,
CLASF_ID_FK,
NUM_ID_FK
);

/*==============================================================*/
/* Index: AGT_EQV_FALLA_NK_FK                                   */
/*==============================================================*/
create  index AGT_EQV_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
AGTEQV_ID_FK
);

/*==============================================================*/
/* Index: AGT_FALLA_NK_FK                                       */
/*==============================================================*/
create  index AGT_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
AGT_ID_FK
);

/*==============================================================*/
/* Index: AGT_ORG_FALLA_NK_FK                                   */
/*==============================================================*/
create  index AGT_ORG_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
AGTORG_ID_FK
);

/*==============================================================*/
/* Index: CLASE_FALLA_NK_FK                                     */
/*==============================================================*/
create  index CLASE_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
CLASF_ID_FK
);

/*==============================================================*/
/* Index: FECHA_CIERRE_NK_FK                                    */
/*==============================================================*/
create  index FECHA_CIERRE_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
TMPO_CIERRE_ID_FK
);

/*==============================================================*/
/* Index: FECHA_DISPON_NK_FK                                    */
/*==============================================================*/
create  index FECHA_DISPON_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
TMPO_DISPON_ID_FK
);

/*==============================================================*/
/* Index: FECHA_FALLA_NK_FK                                     */
/*==============================================================*/
create  index FECHA_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
TMPO_FALLA_ID_FK
);

/*==============================================================*/
/* Index: FECHA_FIN_NK_FK                                       */
/*==============================================================*/
create  index FECHA_FIN_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
TMPO_FIN_ID_FK
);

/*==============================================================*/
/* Index: FECHA_MAX_NORM_NK_FK                                  */
/*==============================================================*/
create  index FECHA_MAX_NORM_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
TMPO_MAX_NORM_ID_FK
);

/*==============================================================*/
/* Index: HORA_CIERRE_NK_FK                                     */
/*==============================================================*/
create  index HORA_CIERRE_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
HORA_CIERRE_ID_FK
);

/*==============================================================*/
/* Index: HORA_DISPON_NK_FK                                     */
/*==============================================================*/
create  index HORA_DISPON_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
HORA_DISPON_ID_FK
);

/*==============================================================*/
/* Index: HORA_FALLA_NK_FK                                      */
/*==============================================================*/
create  index HORA_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
HORA_FALLA_ID_FK
);

/*==============================================================*/
/* Index: HORA_FIN_NK_FK                                        */
/*==============================================================*/
create  index HORA_FIN_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
HORA_FIN_ID_FK
);

/*==============================================================*/
/* Index: HORA_MAX_NORM_NK_FK                                   */
/*==============================================================*/
create  index HORA_MAX_NORM_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
HORA_MAX_NORM_ID_FK
);

/*==============================================================*/
/* Index: NUM_FALLA_NK_FK                                       */
/*==============================================================*/
create  index NUM_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
NUM_ID_FK
);

/*==============================================================*/
/* Index: OBSR_FALLA_NK_FK                                      */
/*==============================================================*/
create  index OBSR_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
OBSR_ID_FK
);

/*==============================================================*/
/* Index: ORIG_FALLA_NK_FK                                      */
/*==============================================================*/
create  index ORIG_FALLA_NK_FK on CEN_DWS.FACT_FALLAS_SNI (
ASIGORG_ID_FK
);

/*==============================================================*/
/* Table: FACT_HIDROLOGIA                                       */
/*==============================================================*/
create table CEN_DWS.FACT_HIDROLOGIA (
   AGT_ID_FK            INT4                 not null,
   PARAMHID_ID_FK       INT2                 not null,
   TMPO_ID_FK           INT4                 not null,
   HIDRO_VALOR          NUMERIC(8,2)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_HIDROLOGIA primary key (AGT_ID_FK, PARAMHID_ID_FK, TMPO_ID_FK)
);

comment on table CEN_DWS.FACT_HIDROLOGIA is
'Modelo de hechos de Información Hidrológica.';

comment on column CEN_DWS.FACT_HIDROLOGIA.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_HIDROLOGIA.PARAMHID_ID_FK is
'Corresponde a la calve primaria de parámetros de hidrología';

comment on column CEN_DWS.FACT_HIDROLOGIA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_HIDROLOGIA.HIDRO_VALOR is
'Corresponde al valor de hidrología';

comment on column CEN_DWS.FACT_HIDROLOGIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_HIDROLOGIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_HIDROLOGIA_PK                                    */
/*==============================================================*/
create unique index FACT_HIDROLOGIA_PK on CEN_DWS.FACT_HIDROLOGIA (
AGT_ID_FK,
PARAMHID_ID_FK,
TMPO_ID_FK
);

/*==============================================================*/
/* Index: AGT_HIDRO_NK_FK                                       */
/*==============================================================*/
create  index AGT_HIDRO_NK_FK on CEN_DWS.FACT_HIDROLOGIA (
AGT_ID_FK
);

/*==============================================================*/
/* Index: TMPO_HIDRO_NK_FK                                      */
/*==============================================================*/
create  index TMPO_HIDRO_NK_FK on CEN_DWS.FACT_HIDROLOGIA (
TMPO_ID_FK
);

/*==============================================================*/
/* Index: PARAM_HIDRO_NK_FK                                     */
/*==============================================================*/
create  index PARAM_HIDRO_NK_FK on CEN_DWS.FACT_HIDROLOGIA (
PARAMHID_ID_FK
);

/*==============================================================*/
/* Table: FACT_HIDROLOGIA_HORARIA                               */
/*==============================================================*/
create table CEN_DWS.FACT_HIDROLOGIA_HORARIA (
   AGT_ID_FK            INT4                 not null,
   PARAMHID_ID_FK       INT2                 not null,
   HORA_ID_FK           INT2                 not null,
   TMPO_ID_FK           INT4                 not null,
   HIDROH_VALOR         NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_HIDROLOGIA_HORARIA primary key (AGT_ID_FK, PARAMHID_ID_FK, HORA_ID_FK, TMPO_ID_FK)
);

comment on table CEN_DWS.FACT_HIDROLOGIA_HORARIA is
'Corresponde al análisis de hidrología horario ';

comment on column CEN_DWS.FACT_HIDROLOGIA_HORARIA.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_HIDROLOGIA_HORARIA.PARAMHID_ID_FK is
'Corresponde a la calve primaria de parámetros de hidrología';

comment on column CEN_DWS.FACT_HIDROLOGIA_HORARIA.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_HIDROLOGIA_HORARIA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_HIDROLOGIA_HORARIA.HIDROH_VALOR is
'Corresponde al valor de hidrología';

comment on column CEN_DWS.FACT_HIDROLOGIA_HORARIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_HIDROLOGIA_HORARIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_HIDROLOGIA_HR_PK                                 */
/*==============================================================*/
create unique index FACT_HIDROLOGIA_HR_PK on CEN_DWS.FACT_HIDROLOGIA_HORARIA (
AGT_ID_FK,
PARAMHID_ID_FK,
HORA_ID_FK,
TMPO_ID_FK
);

/*==============================================================*/
/* Index: AGT_HIDRO_HOR_NK_FK                                   */
/*==============================================================*/
create  index AGT_HIDRO_HOR_NK_FK on CEN_DWS.FACT_HIDROLOGIA_HORARIA (
AGT_ID_FK
);

/*==============================================================*/
/* Index: PARAM_HIDRO_HOR_NK_FK                                 */
/*==============================================================*/
create  index PARAM_HIDRO_HOR_NK_FK on CEN_DWS.FACT_HIDROLOGIA_HORARIA (
PARAMHID_ID_FK
);

/*==============================================================*/
/* Index: HORA_HIDRO_HOR_NK_FK                                  */
/*==============================================================*/
create  index HORA_HIDRO_HOR_NK_FK on CEN_DWS.FACT_HIDROLOGIA_HORARIA (
HORA_ID_FK
);

/*==============================================================*/
/* Index: TMP_HIDRO_HR_NK_FK                                    */
/*==============================================================*/
create  index TMP_HIDRO_HR_NK_FK on CEN_DWS.FACT_HIDROLOGIA_HORARIA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_IMPORTACION_EXPORTACION                          */
/*==============================================================*/
create table CEN_DWS.FACT_IMPORTACION_EXPORTACION (
   AGT_ID_FK            INT4                 not null,
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   IMPEXP_MV            NUMERIC(6,2)         not null,
   IMPEXP_MVAR          NUMERIC(6,2)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_IMPORTACION_EXPORTACIO primary key (AGT_ID_FK, TMPO_ID_FK, HORA_ID_FK)
);

comment on table CEN_DWS.FACT_IMPORTACION_EXPORTACION is
'Tabla que contiene información de importación y exportación ';

comment on column CEN_DWS.FACT_IMPORTACION_EXPORTACION.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_IMPORTACION_EXPORTACION.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_IMPORTACION_EXPORTACION.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_IMPORTACION_EXPORTACION.IMPEXP_MV is
'Valor de potencia activa de importación y exportación';

comment on column CEN_DWS.FACT_IMPORTACION_EXPORTACION.IMPEXP_MVAR is
'Corresponde a los valores de potencia reactiva de importación y exportación';

comment on column CEN_DWS.FACT_IMPORTACION_EXPORTACION.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_IMPORTACION_EXPORTACION owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_IMPOR_EXPOR_PK                                   */
/*==============================================================*/
create unique index FACT_IMPOR_EXPOR_PK on CEN_DWS.FACT_IMPORTACION_EXPORTACION (
AGT_ID_FK,
TMPO_ID_FK,
HORA_ID_FK
);

/*==============================================================*/
/* Index: AGT_INT_NK_FK                                         */
/*==============================================================*/
create  index AGT_INT_NK_FK on CEN_DWS.FACT_IMPORTACION_EXPORTACION (
AGT_ID_FK
);

/*==============================================================*/
/* Index: HORA_INT_NK_FK                                        */
/*==============================================================*/
create  index HORA_INT_NK_FK on CEN_DWS.FACT_IMPORTACION_EXPORTACION (
HORA_ID_FK
);

/*==============================================================*/
/* Index: TMPO_INT_NK_FK                                        */
/*==============================================================*/
create  index TMPO_INT_NK_FK on CEN_DWS.FACT_IMPORTACION_EXPORTACION (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_INTERCONEXION_DIARIA                             */
/*==============================================================*/
create table CEN_DWS.FACT_INTERCONEXION_DIARIA (
   TMPO_ID_FK           INT4                 not null,
   TIPCOMB_ID_FK        INT2                 not null,
   TIPGEN_ID_FK         INT2                 not null,
   TIPTEC_ID_FK         INT2                 not null,
   AGT_INT_ID_FK        INT2                 not null,
   INTERDR_ENERGIA      NUMERIC(10,2)        not null,
   INTERDR_CLASE        VARCHAR(1)           not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_INTERCONEXION_DIARIA primary key (TMPO_ID_FK, TIPCOMB_ID_FK, TIPGEN_ID_FK, TIPTEC_ID_FK, AGT_INT_ID_FK)
);

comment on table CEN_DWS.FACT_INTERCONEXION_DIARIA is
'Tabla de hechos para registrar la importación y exportación de manera separada.';

comment on column CEN_DWS.FACT_INTERCONEXION_DIARIA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_INTERCONEXION_DIARIA.TIPCOMB_ID_FK is
'Clave principal del tipo de combustible';

comment on column CEN_DWS.FACT_INTERCONEXION_DIARIA.TIPGEN_ID_FK is
'Corresponde a la clave principal del tipo de generación';

comment on column CEN_DWS.FACT_INTERCONEXION_DIARIA.TIPTEC_ID_FK is
'Corresponde a la clave primaria del tipo de tecnología';

comment on column CEN_DWS.FACT_INTERCONEXION_DIARIA.INTERDR_ENERGIA is
'Energía de inteconexión.';

comment on column CEN_DWS.FACT_INTERCONEXION_DIARIA.INTERDR_CLASE is
'Clase de interconexión';

comment on column CEN_DWS.FACT_INTERCONEXION_DIARIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_INTERCONEXION_DIARIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_INTER_DR_PK                                      */
/*==============================================================*/
create unique index FACT_INTER_DR_PK on CEN_DWS.FACT_INTERCONEXION_DIARIA (
TMPO_ID_FK,
TIPCOMB_ID_FK,
TIPGEN_ID_FK,
TIPTEC_ID_FK,
AGT_INT_ID_FK
);

/*==============================================================*/
/* Index: AGT_INT_DR_NK_FK                                      */
/*==============================================================*/
create  index AGT_INT_DR_NK_FK on CEN_DWS.FACT_INTERCONEXION_DIARIA (
AGT_INT_ID_FK
);

/*==============================================================*/
/* Index: TIP_COMB_INT_NK_FK                                    */
/*==============================================================*/
create  index TIP_COMB_INT_NK_FK on CEN_DWS.FACT_INTERCONEXION_DIARIA (
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: TIP_GEN_INT_BK_FK                                     */
/*==============================================================*/
create  index TIP_GEN_INT_BK_FK on CEN_DWS.FACT_INTERCONEXION_DIARIA (
TIPGEN_ID_FK
);

/*==============================================================*/
/* Index: TIP_TEC_INT_NK_FK                                     */
/*==============================================================*/
create  index TIP_TEC_INT_NK_FK on CEN_DWS.FACT_INTERCONEXION_DIARIA (
TIPTEC_ID_FK
);

/*==============================================================*/
/* Index: TMPO_INT_DR_NK_FK                                     */
/*==============================================================*/
create  index TMPO_INT_DR_NK_FK on CEN_DWS.FACT_INTERCONEXION_DIARIA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_LIM_FCS                                          */
/*==============================================================*/
create table CEN_DWS.FACT_LIM_FCS (
   TMPO_ID_FK           INT4                 not null,
   AGT_ID_FK            INT4                 not null,
   FCS_HORAS_INDISPON_FALLA NUMERIC(8,2)         not null,
   FCS_HORAS_INDISPON_MANT NUMERIC(8,2)         not null,
   FCS_HORAS_INDISPON_TOT NUMERIC(8,2)         not null,
   FCS_NIT_FALLA        INT2                 not null,
   FCS_NIT_MANT         INT2                 not null,
   FCS_LHI              NUMERIC(8,2)         not null,
   FCS_NDP              INT2                 not null,
   FCS_FACTOR_LHI       NUMERIC(12,4)        not null,
   FCS_FACTOR_NDP       NUMERIC(12,4)        not null,
   FCS_VALOR            NUMERIC(8,2)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_LIM_FCS primary key (TMPO_ID_FK, AGT_ID_FK)
);

comment on table CEN_DWS.FACT_LIM_FCS is
'Tabla de hechos instantaneas, en un rango de tiempo de 6 meses, en función de la ocurrencia de fallas, definido por ahora cada seis meses.';

comment on column CEN_DWS.FACT_LIM_FCS.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_LIM_FCS.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_LIM_FCS.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_LIM_FCS owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_LIM_FCS_PK                                       */
/*==============================================================*/
create unique index FACT_LIM_FCS_PK on CEN_DWS.FACT_LIM_FCS (
TMPO_ID_FK,
AGT_ID_FK
);

/*==============================================================*/
/* Index: AGT_LIM_NK_FK                                         */
/*==============================================================*/
create  index AGT_LIM_NK_FK on CEN_DWS.FACT_LIM_FCS (
AGT_ID_FK
);

/*==============================================================*/
/* Index: TMPO_LIM_NK_FK                                        */
/*==============================================================*/
create  index TMPO_LIM_NK_FK on CEN_DWS.FACT_LIM_FCS (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_POT_DISPONIBLE                                   */
/*==============================================================*/
create table CEN_DWS.FACT_POT_DISPONIBLE (
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   AGT_ID_FK            INT4                 not null,
   POTDISP_POT_DISPON   NUMERIC(8,2)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_POT_DISPONIBLE primary key (TMPO_ID_FK, HORA_ID_FK, AGT_ID_FK)
);

comment on table CEN_DWS.FACT_POT_DISPONIBLE is
'Es la tabla de hechos para almacenar las potencias disponibles horarias de acuerdo con las novedades on-off.';

comment on column CEN_DWS.FACT_POT_DISPONIBLE.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_POT_DISPONIBLE.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_POT_DISPONIBLE.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_POT_DISPONIBLE.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_POT_DISPONIBLE owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_POT_DISPO_PK                                     */
/*==============================================================*/
create unique index FACT_POT_DISPO_PK on CEN_DWS.FACT_POT_DISPONIBLE (
TMPO_ID_FK,
HORA_ID_FK,
AGT_ID_FK
);

/*==============================================================*/
/* Index: AGT_POT_DP_NK_FK                                      */
/*==============================================================*/
create  index AGT_POT_DP_NK_FK on CEN_DWS.FACT_POT_DISPONIBLE (
AGT_ID_FK
);

/*==============================================================*/
/* Index: HORA_POT_DP_NK_FK                                     */
/*==============================================================*/
create  index HORA_POT_DP_NK_FK on CEN_DWS.FACT_POT_DISPONIBLE (
HORA_ID_FK
);

/*==============================================================*/
/* Index: TMPO_POT_DP_NK_FK                                     */
/*==============================================================*/
create  index TMPO_POT_DP_NK_FK on CEN_DWS.FACT_POT_DISPONIBLE (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_PRODUCCION_DIARIA                                */
/*==============================================================*/
create table CEN_DWS.FACT_PRODUCCION_DIARIA (
   TMPO_ID_FK           INT4                 not null,
   AGT_ID_FK            INT4                 not null,
   TIPTEC_ID_FK         INT2                 not null,
   TIPGEN_ID_FK         INT2                 not null,
   TIPCOMB_ID_FK        INT2                 not null,
   PRODD_ENERGIA        NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_PRODUCCION_DIARIA primary key (TMPO_ID_FK, AGT_ID_FK, TIPTEC_ID_FK, TIPGEN_ID_FK, TIPCOMB_ID_FK)
);

comment on table CEN_DWS.FACT_PRODUCCION_DIARIA is
'Tabla de hechos para análisis de energía. (MWh)';

comment on column CEN_DWS.FACT_PRODUCCION_DIARIA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_PRODUCCION_DIARIA.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_PRODUCCION_DIARIA.TIPTEC_ID_FK is
'Corresponde a la clave primaria del tipo de tecnología';

comment on column CEN_DWS.FACT_PRODUCCION_DIARIA.TIPGEN_ID_FK is
'Corresponde a la clave principal del tipo de generación';

comment on column CEN_DWS.FACT_PRODUCCION_DIARIA.TIPCOMB_ID_FK is
'Clave principal del tipo de combustible';

comment on column CEN_DWS.FACT_PRODUCCION_DIARIA.PRODD_ENERGIA is
'Medida de producción diaria.';

comment on column CEN_DWS.FACT_PRODUCCION_DIARIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_PRODUCCION_DIARIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_PROD_DR_PK                                       */
/*==============================================================*/
create unique index FACT_PROD_DR_PK on CEN_DWS.FACT_PRODUCCION_DIARIA (
TMPO_ID_FK,
AGT_ID_FK,
TIPTEC_ID_FK,
TIPGEN_ID_FK,
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: AGT_PROD_NK_FK                                        */
/*==============================================================*/
create  index AGT_PROD_NK_FK on CEN_DWS.FACT_PRODUCCION_DIARIA (
AGT_ID_FK
);

/*==============================================================*/
/* Index: TIP_COMB_PROD_NK_FK                                   */
/*==============================================================*/
create  index TIP_COMB_PROD_NK_FK on CEN_DWS.FACT_PRODUCCION_DIARIA (
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: TIP_GEN_PROD_NK_FK                                    */
/*==============================================================*/
create  index TIP_GEN_PROD_NK_FK on CEN_DWS.FACT_PRODUCCION_DIARIA (
TIPGEN_ID_FK
);

/*==============================================================*/
/* Index: TIP_TEC_PROD_NK_FK                                    */
/*==============================================================*/
create  index TIP_TEC_PROD_NK_FK on CEN_DWS.FACT_PRODUCCION_DIARIA (
TIPTEC_ID_FK
);

/*==============================================================*/
/* Index: TMP_PROD_NK_FK                                        */
/*==============================================================*/
create  index TMP_PROD_NK_FK on CEN_DWS.FACT_PRODUCCION_DIARIA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_PRODUCCION_MEDIOHORARIA                          */
/*==============================================================*/
create table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
   TMPO_ID_FK           INT4                 not null,
   AGT_ID_FK            INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   TIPTEC_ID_FK         INT2                 not null,
   TIPGEN_ID_FK         INT2                 not null,
   TIPCOMB_ID_FK        INT2                 not null,
   PRODMH_POTENCIA_ACTIVA NUMERIC(10,2)        not null,
   PRODMH_POTENCIA_REACTIVA NUMERIC(10,2)        not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_PRODUCCION_MEDIOHORARI primary key (TMPO_ID_FK, AGT_ID_FK, HORA_ID_FK, TIPTEC_ID_FK, TIPGEN_ID_FK, TIPCOMB_ID_FK)
);

comment on table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA is
'Tabla que almacena la producción horaria';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.TIPTEC_ID_FK is
'Corresponde a la clave primaria del tipo de tecnología';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.TIPGEN_ID_FK is
'Corresponde a la clave principal del tipo de generación';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.TIPCOMB_ID_FK is
'Clave principal del tipo de combustible';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.PRODMH_POTENCIA_ACTIVA is
'Medida que contiene información de potencia activa';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.PRODMH_POTENCIA_REACTIVA is
'Media que contiene información de potencia reactiva';

comment on column CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_PROD_MH_PK                                       */
/*==============================================================*/
create unique index FACT_PROD_MH_PK on CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
TMPO_ID_FK,
AGT_ID_FK,
HORA_ID_FK,
TIPTEC_ID_FK,
TIPGEN_ID_FK,
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: AGT_PROD_MH_NK_FK                                     */
/*==============================================================*/
create  index AGT_PROD_MH_NK_FK on CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
AGT_ID_FK
);

/*==============================================================*/
/* Index: HORA_PROD_MH_NK_FK                                    */
/*==============================================================*/
create  index HORA_PROD_MH_NK_FK on CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
HORA_ID_FK
);

/*==============================================================*/
/* Index: TIP_COMB_PROD_MH_NK_FK                                */
/*==============================================================*/
create  index TIP_COMB_PROD_MH_NK_FK on CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: TIP_GEN_PROD_MH_NK_FK                                 */
/*==============================================================*/
create  index TIP_GEN_PROD_MH_NK_FK on CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
TIPGEN_ID_FK
);

/*==============================================================*/
/* Index: TIP_TEC_PROD_MH_NK_FK                                 */
/*==============================================================*/
create  index TIP_TEC_PROD_MH_NK_FK on CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
TIPTEC_ID_FK
);

/*==============================================================*/
/* Index: TMP_PROD_MH_NK_FK                                     */
/*==============================================================*/
create  index TMP_PROD_MH_NK_FK on CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_STOCK_COMBUSTIBLE                                */
/*==============================================================*/
create table CEN_DWS.FACT_STOCK_COMBUSTIBLE (
   GRCOMB_ID_FK         INT2                 not null,
   AGT_ID_FK            INT4                 not null,
   TMPO_ID_FK           INT4                 not null,
   TIPCOMB_ID_FK        INT2                 not null,
   COMB_CLASE           VARCHAR(1)           not null
      constraint CKC_COMB_CLASE_FACT_STO check (COMB_CLASE in ('A','O') and COMB_CLASE = upper(COMB_CLASE)),
   COMB_STOCK_COMB      NUMERIC(10,2)        not null,
   COMB_DIAS_OPERACION  NUMERIC(8,1)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_STOCK_COMBUSTIBLE primary key (GRCOMB_ID_FK, AGT_ID_FK, TMPO_ID_FK, TIPCOMB_ID_FK)
);

comment on table CEN_DWS.FACT_STOCK_COMBUSTIBLE is
'Tabla de hechos que contiene datos de stock de combustibles';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.GRCOMB_ID_FK is
'Llave primaria del grupo de combustible';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.TIPCOMB_ID_FK is
'Clave principal del tipo de combustible';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.COMB_CLASE is
'Permite conocer la clase stock, para saber si es code Operación "O", o es de Arranque "A".';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.COMB_STOCK_COMB is
'Corresponde al valor del stock de combustible ';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.COMB_DIAS_OPERACION is
'Días de operación de la máquina';

comment on column CEN_DWS.FACT_STOCK_COMBUSTIBLE.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_STOCK_COMBUSTIBLE owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_STOCK_COMB_PK                                    */
/*==============================================================*/
create unique index FACT_STOCK_COMB_PK on CEN_DWS.FACT_STOCK_COMBUSTIBLE (
GRCOMB_ID_FK,
AGT_ID_FK,
TMPO_ID_FK,
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: AGT_STOCK_NK_FK                                       */
/*==============================================================*/
create  index AGT_STOCK_NK_FK on CEN_DWS.FACT_STOCK_COMBUSTIBLE (
AGT_ID_FK
);

/*==============================================================*/
/* Index: GRCOMB_STOCK_NK_FK                                    */
/*==============================================================*/
create  index GRCOMB_STOCK_NK_FK on CEN_DWS.FACT_STOCK_COMBUSTIBLE (
GRCOMB_ID_FK
);

/*==============================================================*/
/* Index: TIP_COMB_STOCK_NK_FK                                  */
/*==============================================================*/
create  index TIP_COMB_STOCK_NK_FK on CEN_DWS.FACT_STOCK_COMBUSTIBLE (
TIPCOMB_ID_FK
);

/*==============================================================*/
/* Index: TMPO_STOCK_NK_FK                                      */
/*==============================================================*/
create  index TMPO_STOCK_NK_FK on CEN_DWS.FACT_STOCK_COMBUSTIBLE (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_TIEMPO_OPERACION                                 */
/*==============================================================*/
create table CEN_DWS.FACT_TIEMPO_OPERACION (
   TMPO_ID_FK           INT4                 not null,
   HORA_ID_FK           INT2                 not null,
   AGT_ID_FK            INT4                 not null,
   EVT_TIEMPO_DISPONIBLE NUMERIC(8,2)         not null,
   EVT_TIEMPO_INDISPONIBLE NUMERIC(8,2)         not null,
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_TIEMPO_OPERACION primary key (TMPO_ID_FK, HORA_ID_FK, AGT_ID_FK)
);

comment on table CEN_DWS.FACT_TIEMPO_OPERACION is
'Tabla de hechos, donde se almacenarán tiempos de operación en función de los eventos que activan calculos ON y OFF de las unidades de generación.';

comment on column CEN_DWS.FACT_TIEMPO_OPERACION.TMPO_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_TIEMPO_OPERACION.HORA_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_TIEMPO_OPERACION.AGT_ID_FK is
'Llave primaria de los agentes de hidrología.';

comment on column CEN_DWS.FACT_TIEMPO_OPERACION.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_TIEMPO_OPERACION owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_TIEMPO_OPER_PK                                   */
/*==============================================================*/
create unique index FACT_TIEMPO_OPER_PK on CEN_DWS.FACT_TIEMPO_OPERACION (
TMPO_ID_FK,
HORA_ID_FK,
AGT_ID_FK
);

/*==============================================================*/
/* Index: AGT_TMP_OPER_NK_FK                                    */
/*==============================================================*/
create  index AGT_TMP_OPER_NK_FK on CEN_DWS.FACT_TIEMPO_OPERACION (
AGT_ID_FK
);

/*==============================================================*/
/* Index: HORA_TMP_OPER_NK_FK                                   */
/*==============================================================*/
create  index HORA_TMP_OPER_NK_FK on CEN_DWS.FACT_TIEMPO_OPERACION (
HORA_ID_FK
);

/*==============================================================*/
/* Index: TMP_OPER_NK_FK                                        */
/*==============================================================*/
create  index TMP_OPER_NK_FK on CEN_DWS.FACT_TIEMPO_OPERACION (
TMPO_ID_FK
);

/*==============================================================*/
/* Table: FACT_TMP_DESCON_CARGA                                 */
/*==============================================================*/
create table CEN_DWS.FACT_TMP_DESCON_CARGA (
   AGTORG_ORIG_ID_FK    INT2                 not null,
   AGTORG_DEST_ID_FK    INT2                 not null,
   CAT_TIP_MANT_ID_FK   INT2                 not null,
   CAT_TIP_CONSIG_ID_FK INT2                 not null,
   TMPO_INI_PLANIF_ID_FK INT4                 not null,
   HORA_INI_PLANIF_ID_FK INT2                 not null,
   TMPO_FIN_PLANIF_ID_FK INT4                 not null,
   HORA_FIN_PLANIF_ID_FK INT2                 not null,
   TMPO_INI_EJEC_ID_FK  INT4                 not null,
   HORA_INI_EJEC_ID_FK  INT2                 not null,
   TMPO_FIN_EJEC_ID_FK  INT4                 not null,
   HORA_FIN_EJEC_ID_FK  INT2                 not null,
   DESCCRG_DESCARGA_ID  INT4                 not null,
   DESCCRG_PS_CODIGO    VARCHAR(16)          not null,
   DESCCRG_CARGA_PLANIF DECIMAL(12,2)        not null,
   DESCCRG_TIEMPO_PLANIF DECIMAL(12,2)        not null,
   DESCCRG_ENS_PLANIF   DECIMAL(12,2)        not null,
   DESCCRG_CARGA_EJEC   DECIMAL(12,2)        not null,
   DESCCRG_TIEMPO_EJEC  DECIMAL(12,2)        not null,
   DESCCRG_ENS_EJEC     DECIMAL(12,2)        not null,
   DESCCRG_DESFASE_INI  VARCHAR(2)           not null
      constraint CKC_DESCCRG_DESFASE_I_FACT_TMP check (DESCCRG_DESFASE_INI in ('A','D','T') and DESCCRG_DESFASE_INI = upper(DESCCRG_DESFASE_INI)),
   DESCCRG_DESFASE_FIN  VARCHAR(2)           not null
      constraint CKC_DESCCRG_DESFASE_F_FACT_TMP check (DESCCRG_DESFASE_FIN in ('A','D','T') and DESCCRG_DESFASE_FIN = upper(DESCCRG_DESFASE_FIN)),
   DESCCRG_DESFASE_HORA_INI DECIMAL(12,2)        not null,
   DESCCRG_DESFASE_HORA_FIN DECIMAL(12,2)        not null,
   DESCCRG_DESFASE_CARGA DECIMAL(12,2)        not null,
   DESCCRG_DESFASE_TIEMPO DECIMAL(12,2)        not null,
   DESCCRG_DESFASE_ENS  DECIMAL(12,2)        not null,
   DESCCRG_TRAZABILIDAD VARCHAR(4)           not null
      constraint CKC_DESCCRG_TRAZABILI_FACT_TMP check (DESCCRG_TRAZABILIDAD in ('SFC','SFE','FCE') and DESCCRG_TRAZABILIDAD = upper(DESCCRG_TRAZABILIDAD)),
   FECHA_CARGA          TIMESTAMP WITHOUT TIME ZONE                 not null,
   constraint PK_FACT_TMP_DESCON_CARGA primary key (AGTORGDES_ID_FK, CAT_TIP_MANT_ID_FK, CAT_TIP_CONSIG_ID_FK, TMPO_INI_PLANIF_ID_FK, HORA_INI_PLANIF_ID_FK, TMPO_FIN_PLANIF_ID_FK, HORA_FIN_PLANIF_ID_FK, TMPO_INI_EJEC_ID_FK, HORA_INI_EJEC_ID_FK, TMPO_FIN_EJEC_ID_FK, HORA_FIN_EJEC_ID_FK)
);

comment on table CEN_DWS.FACT_TMP_DESCON_CARGA is
'Tabla que almacena los datos de tiempos de deconexión de carga.';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.AGTORG_ORIG_ID_FK is
'Corresponde al código de identificación del regidtro del agente ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.AGTORG_DEST_ID_FK is
'Corresponde al código de identificación del regidtro del agente ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.CAT_TIP_MANT_ID_FK is
'Clave primaria de la tabla que contiene todos los catálogos';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.CAT_TIP_CONSIG_ID_FK is
'Clave primaria de la tabla que contiene todos los catálogos';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.TMPO_INI_PLANIF_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.HORA_INI_PLANIF_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.TMPO_FIN_PLANIF_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.HORA_FIN_PLANIF_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.TMPO_INI_EJEC_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.HORA_INI_EJEC_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.TMPO_FIN_EJEC_ID_FK is
'Clave primaria de la tabla tiempo';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.HORA_FIN_EJEC_ID_FK is
'Tabla que contiene todas las horas de un día. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_PS_CODIGO is
'Código del mantenimiento';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_CARGA_PLANIF is
'Corresponde al valor de la carga planificada';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_TIEMPO_PLANIF is
'Corresponde al tiempo de desconexión de carga planificado';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_ENS_PLANIF is
'Corresponde al valor de energía no suministrada planificada. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_CARGA_EJEC is
'Corresponde al valor de la carga real que se desconectó en un mantenimiento';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_TIEMPO_EJEC is
'Corresponde al tiempo de desconexión de carga real desconectada. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_ENS_EJEC is
'Corresponde al valor de energía no suministrada ejecutada.';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_DESFASE_INI is
'Columna que representa si existió desfases en la fecha de planificación de inicio';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_DESFASE_FIN is
'Columna que representa si existió desfases en la fecha de planificación de fin de desconexión de carga';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_DESFASE_HORA_INI is
'Corresponde al número de horas de desfase de inicio de desconexión de carga ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_DESFASE_HORA_FIN is
'Corresponde al número de horas de desfase de fin de desconexión de carga ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_DESFASE_CARGA is
'Corresponde al valos de desfase de carga entre lo planificado y lo ejecutado. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_DESFASE_TIEMPO is
'Corresponde al defase de tiempo entre lo planificado vs lo ejecutado. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_DESFASE_ENS is
'Corresponde al valor de desfase entre ENS planificada versus la ejecutada. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.DESCCRG_TRAZABILIDAD is
'Indica que tipo de ingreso se realizó. 
1. Si la carga sólo quedó en fase de consignacion, es decir que no se ejecutó. 
2. Si la carga no pasó por la fase de consignacióm, es decir no se tiene fechas de planificación. 
3. La carga tiene planificación y se ejecutó. ';

comment on column CEN_DWS.FACT_TMP_DESCON_CARGA.FECHA_CARGA is
'Corresponde a la fecha de carga del registro.';

-- set table ownership
--alter table CEN_DWS.FACT_TMP_DESCON_CARGA owner to CEN_DWS
;
/*==============================================================*/
/* Index: FACT_TMP_DESCON_CARGA_PK                              */
/*==============================================================*/
create unique index FACT_TMP_DESCON_CARGA_PK on CEN_DWS.FACT_TMP_DESCON_CARGA (
AGTORGDES_ID_FK,
CAT_TIP_MANT_ID_FK,
CAT_TIP_CONSIG_ID_FK,
TMPO_INI_PLANIF_ID_FK,
HORA_INI_PLANIF_ID_FK,
TMPO_FIN_PLANIF_ID_FK,
HORA_FIN_PLANIF_ID_FK,
TMPO_INI_EJEC_ID_FK,
HORA_INI_EJEC_ID_FK,
TMPO_FIN_EJEC_ID_FK,
HORA_FIN_EJEC_ID_FK
);

/*==============================================================*/
/* Index: AGT_ORG_DES_NK_FK                                     */
/*==============================================================*/
create  index AGT_ORG_DES_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
AGTORGDES_ID_FK
);

/*==============================================================*/
/* Index: CAT_TIPCONSIG_DENCONCRG_NK_FK                         */
/*==============================================================*/
create  index CAT_TIPCONSIG_DENCONCRG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
CAT_TIP_CONSIG_ID_FK
);

/*==============================================================*/
/* Index: CAT_TIPMANT_DENCONCRG_NK_FK                           */
/*==============================================================*/
create  index CAT_TIPMANT_DENCONCRG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
CAT_TIP_MANT_ID_FK
);

/*==============================================================*/
/* Index: HORA_FINPLANIF_DESCARG_NK_FK                          */
/*==============================================================*/
create  index HORA_FINPLANIF_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
HORA_FIN_PLANIF_ID_FK
);

/*==============================================================*/
/* Index: HORA_FINEJEC_DESCARG_NK_FK                            */
/*==============================================================*/
create  index HORA_FINEJEC_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
HORA_FIN_EJEC_ID_FK
);

/*==============================================================*/
/* Index: HORA_INIPLANIF_DESCARG_NK_FK                          */
/*==============================================================*/
create  index HORA_INIPLANIF_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
HORA_INI_PLANIF_ID_FK
);

/*==============================================================*/
/* Index: HORA_INIEJEC_DESCARG_NK_FK                            */
/*==============================================================*/
create  index HORA_INIEJEC_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
HORA_INI_EJEC_ID_FK
);

/*==============================================================*/
/* Index: TMP_FINEJEC_DESCARG_NK_FK                             */
/*==============================================================*/
create  index TMP_FINEJEC_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
TMPO_FIN_EJEC_ID_FK
);

/*==============================================================*/
/* Index: TMP_FINPLANIF_DESCARG_NK_FK                           */
/*==============================================================*/
create  index TMP_FINPLANIF_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
TMPO_FIN_PLANIF_ID_FK
);

/*==============================================================*/
/* Index: TMP_INIEJEC_DESCARG_NK_FK                             */
/*==============================================================*/
create  index TMP_INIEJEC_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
TMPO_INI_EJEC_ID_FK
);

/*==============================================================*/
/* Index: TMP_INIPLANIF_DESCARG_NK_FK                           */
/*==============================================================*/
create  index TMP_INIPLANIF_DESCARG_NK_FK on CEN_DWS.FACT_TMP_DESCON_CARGA (
TMPO_INI_PLANIF_ID_FK
);

alter table CEN_DWS.FACT_AGC
   add constraint FK_FACT_AGC_AGT_AGC_N_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_AGC
   add constraint FK_FACT_AGC_AGT_CEN_A_DIM_AGT_ foreign key (AGT_CENT_ID_FK)
      references CEN_DWS.DIM_AGT_CENTRAL (AGT_CENT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_AGC
   add constraint FK_FACT_AGC_HORA_FALL_DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_AGC
   add constraint FK_FACT_AGC_MODO_AGC__DIM_MODO foreign key (AGCMODO_ID_FK)
      references CEN_DWS.DIM_MODO_AGC (AGCMODO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_AGC
   add constraint FK_FACT_AGC_NUM_AGC_N_DIM_NUME foreign key (NUM_ID_FK)
      references CEN_DWS.DIM_NUMERO (NUM_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_AGC
   add constraint FK_FACT_AGC_PASO_AGC__DIM_PASO foreign key (PASOS_ID_FK)
      references CEN_DWS.DIM_PASOS (PASOS_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_AGC
   add constraint FK_FACT_AGC_TMPO_FALL_DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_BGEN_DIARIO
   add constraint FK_FACT_BGE_TMP_BGEN__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_BGEN_MEDIOHORARIO
   add constraint FK_FACT_BGE_HOR_BGEN__DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_BGEN_MEDIOHORARIO
   add constraint FK_FACT_BGE_TMP_BGEN__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_DIARIA
   add constraint FK_FACT_DEM_AGT_DEM_N_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_DIARIA
   add constraint FK_FACT_DEM_COMP_DEM__DIM_COMP foreign key (COMPDEM_ID_FK)
      references CEN_DWS.DIM_COMPONENTE_DEMANDA (COMPDEM_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_DIARIA
   add constraint FK_FACT_DEM_ELEM_DEM__DIM_AGT_ foreign key (ELDEM_ID_FK)
      references CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA (ELDEM_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_DIARIA
   add constraint FK_FACT_DEM_TMP_DEM_N_DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA
   add constraint FK_FACT_DEM_AGT_DEM_M_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA
   add constraint FK_FACT_DEM_COMP_DEM__DIM_COMP foreign key (COMPDEM_ID_FK)
      references CEN_DWS.DIM_COMPONENTE_DEMANDA (COMPDEM_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA
   add constraint FK_FACT_DEM_ELEM_DEM__DIM_AGT_ foreign key (ELDEM_ID_FK)
      references CEN_DWS.DIM_AGT_ELEMENTO_DEMANDA (ELDEM_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA
   add constraint FK_FACT_DEM_HORA_DEM__DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DEMANDA_MEDIO_HORARIA
   add constraint FK_FACT_DEM_TMP_DEM_M_DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DESP_REDESP_HORARIO
   add constraint FK_FACT_DES_AGT_DESP__DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DESP_REDESP_HORARIO
   add constraint FK_FACT_DES_HORA_DESP_DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DESP_REDESP_HORARIO
   add constraint FK_FACT_DES_NRO_DESP__DIM_NRO_ foreign key (NRO_ID_FK)
      references CEN_DWS.DIM_NRO_REDESPACHO (NRO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DESP_REDESP_HORARIO
   add constraint FK_FACT_DES_TIP_COMB__DIM_TIPO foreign key (TIPCOMB_ID_FK)
      references CEN_DWS.DIM_TIPO_COMBUSTIBLE (TIPCOMB_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DESP_REDESP_HORARIO
   add constraint FK_FACT_DES_TIP_GEN_D_DIM_TIPO foreign key (TIPGEN_ID_FK)
      references CEN_DWS.DIM_TIPO_GENERACION (TIPGEN_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DESP_REDESP_HORARIO
   add constraint FK_FACT_DES_TIP_TEC_D_DIM_TIPO foreign key (TIPTEC_ID_FK)
      references CEN_DWS.DIM_TIPO_TECNOLOGIA (TIPTEC_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DESP_REDESP_HORARIO
   add constraint FK_FACT_DES_TMP_DESP__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DIST_ENS
   add constraint FK_FACT_DIS_AGTORG_DI_DIM_AGT_ foreign key (AGTORG_ID_FK)
      references CEN_DWS.DIM_AGT_ORIGEN (AGTORG_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DIST_ENS
   add constraint FK_FACT_DIS_AGT_DIST__DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DIST_ENS
   add constraint FK_FACT_DIS_HORA_FALL_DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DIST_ENS
   add constraint FK_FACT_DIS_NUM_DIST__DIM_NUME foreign key (NUM_ID_FK)
      references CEN_DWS.DIM_NUMERO (NUM_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DIST_ENS
   add constraint FK_FACT_DIS_ORG_DIST__DIM_ASIG foreign key (ASIGORG_ID_FK)
      references CEN_DWS.DIM_ASIGNACION_ORIGEN (ASIGORG_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DIST_ENS
   add constraint FK_FACT_DIS_PASO_DIST_DIM_PASO foreign key (PASOS_ID_FK)
      references CEN_DWS.DIM_PASOS (PASOS_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_DIST_ENS
   add constraint FK_FACT_DIS_TMPO_FALL_DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_ENS_ES
   add constraint FK_FACT_ENS_AGTARG_EN_DIM_AGT_ foreign key (AGTORG_ID_FK)
      references CEN_DWS.DIM_AGT_ORIGEN (AGTORG_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_ENS_ES
   add constraint FK_FACT_ENS_AGT_ENS_E_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_ENS_ES
   add constraint FK_FACT_ENS_ORG_ENS_E_DIM_ASIG foreign key (ASIGORG_ID_FK)
      references CEN_DWS.DIM_ASIGNACION_ORIGEN (ASIGORG_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_ENS_ES
   add constraint FK_FACT_ENS_TMPO_ENS__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FACTOR_CARGA
   add constraint FK_FACT_FAC_TMP_FACT__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FACTOR_PLANTA
   add constraint FK_FACT_FAC_AGT_CEN_F_DIM_AGT_ foreign key (AGT_CENT_ID_FK)
      references CEN_DWS.DIM_AGT_CENTRAL (AGT_CENT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FACTOR_PLANTA
   add constraint FK_FACT_FAC_TMP_FACT__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_AGT_EQV_F_DIM_AGEN foreign key (AGTEQV_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_AGT_FALLA_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_AGT_ORG_F_DIM_AGT_ foreign key (AGTORG_ID_FK)
      references CEN_DWS.DIM_AGT_ORIGEN (AGTORG_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_CLASE_FAL_DIM_CLAS foreign key (CLASF_ID_FK)
      references CEN_DWS.DIM_CLASIFICACION_FALLAS (CLASF_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_FECHA_CIE_DIM_TIEM foreign key (TMPO_CIERRE_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_FECHA_DIS_DIM_TIEM foreign key (TMPO_DISPON_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_FECHA_FAL_DIM_TIEM foreign key (TMPO_FALLA_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_FECHA_FIN_DIM_TIEM foreign key (TMPO_FIN_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_FECHA_MAX_DIM_TIEM foreign key (TMPO_MAX_NORM_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_HORA_CIER_DIM_HORA foreign key (HORA_CIERRE_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_HORA_DISP_DIM_HORA foreign key (HORA_DISPON_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_HORA_FALL_DIM_HORA foreign key (HORA_FALLA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_HORA_FIN__DIM_HORA foreign key (HORA_FIN_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_HORA_MAX__DIM_HORA foreign key (HORA_MAX_NORM_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_NUM_FALLA_DIM_NUME foreign key (NUM_ID_FK)
      references CEN_DWS.DIM_NUMERO (NUM_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_OBSR_FALL_DIM_OBSE foreign key (OBSR_ID_FK)
      references CEN_DWS.DIM_OBSERVACIONES_FALLA (OBSR_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_FALLAS_SNI
   add constraint FK_FACT_FAL_ORIG_FALL_DIM_ASIG foreign key (ASIGORG_ID_FK)
      references CEN_DWS.DIM_ASIGNACION_ORIGEN (ASIGORG_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_HIDROLOGIA
   add constraint FK_FACT_HID_AGT_HIDRO_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_HIDROLOGIA
   add constraint FK_FACT_HID_PARAM_HID_DIM_PARA foreign key (PARAMHID_ID_FK)
      references CEN_DWS.DIM_PARAM_HIDROLOGICOS (PARAMHID_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_HIDROLOGIA
   add constraint FK_FACT_HID_TMP_HIDRO_DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_HIDROLOGIA_HORARIA
   add constraint FK_FACT_HID_AGT_HIDRO_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_HIDROLOGIA_HORARIA
   add constraint FK_FACT_HID_HORA_HIDR_DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_HIDROLOGIA_HORARIA
   add constraint FK_FACT_HID_PARAM_HID_DIM_PARA foreign key (PARAMHID_ID_FK)
      references CEN_DWS.DIM_PARAM_HIDROLOGICOS (PARAMHID_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_HIDROLOGIA_HORARIA
   add constraint FK_FACT_HID_TMP_HIDRO_DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_IMPORTACION_EXPORTACION
   add constraint FK_FACT_IMP_AGT_INT_N_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_IMPORTACION_EXPORTACION
   add constraint FK_FACT_IMP_HORA_INT__DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_IMPORTACION_EXPORTACION
   add constraint FK_FACT_IMP_TMPO_INT__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_INTERCONEXION_DIARIA
   add constraint FK_FACT_INT_AGT_INT_D_DIM_AGT_ foreign key (AGT_INT_ID_FK)
      references CEN_DWS.DIM_AGT_INTERCONEXION (AGT_INT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_INTERCONEXION_DIARIA
   add constraint FK_FACT_INT_TIP_COMB__DIM_TIPO foreign key (TIPCOMB_ID_FK)
      references CEN_DWS.DIM_TIPO_COMBUSTIBLE (TIPCOMB_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_INTERCONEXION_DIARIA
   add constraint FK_FACT_INT_TIP_GEN_I_DIM_TIPO foreign key (TIPGEN_ID_FK)
      references CEN_DWS.DIM_TIPO_GENERACION (TIPGEN_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_INTERCONEXION_DIARIA
   add constraint FK_FACT_INT_TIP_TEC_I_DIM_TIPO foreign key (TIPTEC_ID_FK)
      references CEN_DWS.DIM_TIPO_TECNOLOGIA (TIPTEC_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_INTERCONEXION_DIARIA
   add constraint FK_FACT_INT_TMPO_INT__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_LIM_FCS
   add constraint FK_FACT_LIM_AGT_LIM_N_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_LIM_FCS
   add constraint FK_FACT_LIM_TMPO_LIM__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_POT_DISPONIBLE
   add constraint FK_FACT_POT_AGT_POT_D_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_POT_DISPONIBLE
   add constraint FK_FACT_POT_HORA_POT__DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_POT_DISPONIBLE
   add constraint FK_FACT_POT_TMPO_POT__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_DIARIA
   add constraint FK_FACT_PRO_AGT_PROD__DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_DIARIA
   add constraint FK_FACT_PRO_TIP_COMB__DIM_TIPO foreign key (TIPCOMB_ID_FK)
      references CEN_DWS.DIM_TIPO_COMBUSTIBLE (TIPCOMB_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_DIARIA
   add constraint FK_FACT_PRO_TIP_GEN_P_DIM_TIPO foreign key (TIPGEN_ID_FK)
      references CEN_DWS.DIM_TIPO_GENERACION (TIPGEN_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_DIARIA
   add constraint FK_FACT_PRO_TIP_TEC_P_DIM_TIPO foreign key (TIPTEC_ID_FK)
      references CEN_DWS.DIM_TIPO_TECNOLOGIA (TIPTEC_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_DIARIA
   add constraint FK_FACT_PRO_TMP_PROD__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA
   add constraint FK_FACT_PRO_AGT_PROD__DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA
   add constraint FK_FACT_PRO_HORA_PROD_DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA
   add constraint FK_FACT_PRO_TIP_COMB__DIM_TIPO foreign key (TIPCOMB_ID_FK)
      references CEN_DWS.DIM_TIPO_COMBUSTIBLE (TIPCOMB_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA
   add constraint FK_FACT_PRO_TIP_GEN_P_DIM_TIPO foreign key (TIPGEN_ID_FK)
      references CEN_DWS.DIM_TIPO_GENERACION (TIPGEN_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA
   add constraint FK_FACT_PRO_TIP_TEC_P_DIM_TIPO foreign key (TIPTEC_ID_FK)
      references CEN_DWS.DIM_TIPO_TECNOLOGIA (TIPTEC_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_PRODUCCION_MEDIOHORARIA
   add constraint FK_FACT_PRO_TMP_PROD__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_STOCK_COMBUSTIBLE
   add constraint FK_FACT_STO_AGT_STOCK_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_STOCK_COMBUSTIBLE
   add constraint FK_FACT_STO_GRCOMB_ST_DIM_GRUP foreign key (GRCOMB_ID_FK)
      references CEN_DWS.DIM_GRUPO_COMBUSTIBLE (GRCOMB_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_STOCK_COMBUSTIBLE
   add constraint FK_FACT_STO_TIP_COMB__DIM_TIPO foreign key (TIPCOMB_ID_FK)
      references CEN_DWS.DIM_TIPO_COMBUSTIBLE (TIPCOMB_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_STOCK_COMBUSTIBLE
   add constraint FK_FACT_STO_TMPO_STOC_DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TIEMPO_OPERACION
   add constraint FK_FACT_TIE_AGT_TMP_O_DIM_AGEN foreign key (AGT_ID_FK)
      references CEN_DWS.DIM_AGENTE (AGT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TIEMPO_OPERACION
   add constraint FK_FACT_TIE_HORA_TMP__DIM_HORA foreign key (HORA_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TIEMPO_OPERACION
   add constraint FK_FACT_TIE_TMP_OPER__DIM_TIEM foreign key (TMPO_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_AGT_DEST__DIM_AGT_ foreign key (AGTORG_DEST_ID_FK)
      references CEN_DWS.DIM_AGT_ORIGEN_DESTINO (AGTORGDES_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_AGT_ORIG__DIM_AGT_ foreign key (AGTORG_ORIG_ID_FK)
      references CEN_DWS.DIM_AGT_ORIGEN_DESTINO (AGTORGDES_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_CAT_TIPCO_DIM_CATA foreign key (CAT_TIP_CONSIG_ID_FK)
      references CEN_DWS.DIM_CATALOGOS_MANT (CAT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_CAT_TIPMA_DIM_CATA foreign key (CAT_TIP_MANT_ID_FK)
      references CEN_DWS.DIM_CATALOGOS_MANT (CAT_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_HORA_FINE_DIM_HORA foreign key (HORA_FIN_EJEC_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_HORA_FINP_DIM_HORA foreign key (HORA_FIN_PLANIF_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_HORA_INIE_DIM_HORA foreign key (HORA_INI_EJEC_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_HORA_INIP_DIM_HORA foreign key (HORA_INI_PLANIF_ID_FK)
      references CEN_DWS.DIM_HORA (HORA_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_TMP_FINEJ_DIM_TIEM foreign key (TMPO_FIN_EJEC_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_TMP_FINPL_DIM_TIEM foreign key (TMPO_FIN_PLANIF_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_TMP_INIEJ_DIM_TIEM foreign key (TMPO_INI_EJEC_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

alter table CEN_DWS.FACT_TMP_DESCON_CARGA
   add constraint FK_FACT_TMP_TMP_INIPL_DIM_TIEM foreign key (TMPO_INI_PLANIF_ID_FK)
      references CEN_DWS.DIM_TIEMPO (TMPO_ID_PK)
      on delete restrict on update restrict;

