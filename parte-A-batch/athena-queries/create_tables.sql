-- TABLA PRINCIPAL PROCESADA (Dataset General Particionado)
CREATE EXTERNAL TABLE IF NOT EXISTS ideam_processed (
    CodigoEstacion STRING,
    NombreEstacion STRING,
    Variable STRING,
    Parametro STRING,
    Fecha STRING,
    Unidad STRING,
    Valor DOUBLE,
    NivelAprobacion STRING,
    Fecha_Timestamp TIMESTAMP,
    Anio INT,
    Mes INT,
    municipio STRING,
    latitud DOUBLE,
    longitud DOUBLE,
    altitud DOUBLE
)
PARTITIONED BY (
    departamento STRING
)
STORED AS PARQUET
LOCATION 's3://apgd-trabajo2-jr-jj-jv/processed/';


-- REPARAR LA TABLA PROCESADA
MSCK REPAIR TABLE ideam_processed;


-- TABLA RAMA 1: EVOLUCIÓN TEMPORAL (Año y Mes)
CREATE EXTERNAL TABLE IF NOT EXISTS ideam_rama1_temporal (
    Anio INT,
    Mes INT,
    Promedio_Wh_m2 DOUBLE,
    Maximo_Wh_m2 DOUBLE,
    Total_Lecturas BIGINT
)
STORED AS PARQUET
LOCATION 's3://apgd-trabajo2-jr-jj-jv/analytics-ideam/rama-1/';


-- TABLA RAMA 2: GEOGRAFÍA (Departamento y Municipio)
CREATE EXTERNAL TABLE IF NOT EXISTS ideam_rama2_geografica (
    departamento STRING,
    municipio STRING,
    Promedio_Wh_m2 DOUBLE,
    Maximo_Wh_m2 DOUBLE,
    Total_Lecturas BIGINT
)
STORED AS PARQUET
LOCATION 's3://apgd-trabajo2-jr-jj-jv/analytics-ideam/rama-2/';