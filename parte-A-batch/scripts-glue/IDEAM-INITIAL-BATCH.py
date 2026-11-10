import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RUTA_S3_RADIACION = "s3://apgd-trabajo2-jr-jj-jv/raw/main_dataset/"
RUTA_S3_ESTACIONES = "s3://apgd-trabajo2-jr-jj-jv/raw/secondary_dataset/"
RUTA_S3_PROCESADO = "s3://apgd-trabajo2-jr-jj-jv/processed/"

# Cargamos los datos de radiación solar desde S3
df_radiacion = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("recursiveFileLookup", "true")
    .load(RUTA_S3_RADIACION)
)

# Cargamos el catalogo de estaciones desde S3
df_estaciones = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .option("recursiveFileLookup", "true")
    .load(RUTA_S3_ESTACIONES)
)

# Estandarizamos el formato de fecha y hora
df_radiacion_limpio = df_radiacion.withColumn(
    "Fecha_Timestamp",
    F.coalesce(
        F.try_to_timestamp(
            F.trim(F.col("Fecha")),
            F.lit("yyyy-MM-dd HH:mm")
        ),
        F.try_to_timestamp(
            F.trim(F.col("Fecha")),
            F.lit("dd/MM/yyyy HH:mm")
        ),
        F.try_to_timestamp(
            F.trim(F.col("Fecha")),
            F.lit("yyyy-MM-dd")
        ),
        F.try_to_timestamp(
            F.trim(F.col("Fecha")),
            F.lit("dd/MM/yyyy")
        )
    )
)

# Extraemos Año y Mes a partir de la fecha estandarizada
df_radiacion_limpio = (
    df_radiacion_limpio
    .withColumn("Anio", F.year(F.col("Fecha_Timestamp")))
    .withColumn("Mes", F.month(F.col("Fecha_Timestamp")))
)

# Convertimos el valor de radiación a tipo numérico
df_radiacion_limpio = df_radiacion_limpio.withColumn(
    "Valor",
    F.col("Valor").cast("double")
)

# Filtramos los datos no válidos
df_radiacion_limpio = df_radiacion_limpio.filter(
    F.col("Fecha_Timestamp").isNotNull() &
    F.col("Valor").isNotNull() &
    (F.col("Valor") >= 0) &
    (F.col("Valor") <= 1500)
)

# Limpiamos espacios y renombramos la llave en el DataFrame de estaciones
df_estaciones_clean = (
    df_estaciones
    .withColumnRenamed("codigo", "CodigoEstacion_sec")
    .withColumn(
        "CodigoEstacion_sec",
        F.trim(F.col("CodigoEstacion_sec").cast(StringType()))
    )
    .withColumn(
        "latitud",
        F.regexp_replace(F.trim(F.col("latitud")), ",", ".").cast("double")
    )
    .withColumn(
        "longitud",
        F.regexp_replace(F.trim(F.col("longitud")), ",", ".").cast("double")
    )
    .withColumn(
        "altitud",
        F.regexp_replace(F.trim(F.col("altitud")), ",", ".").cast("double")
    )
)

# Convertir la llave a String en el DataFrame principal
df_radiacion_limpio = (
    df_radiacion_limpio
    .withColumn(
        "CodigoEstacion",
        F.trim(F.col("CodigoEstacion").cast(StringType()))
    )
)

# Realizar el LEFT JOIN con el catálogo de estaciones
df_enriquecido = df_radiacion_limpio.join(
    df_estaciones_clean.select(
        F.col("CodigoEstacion_sec").alias("CodigoEstacion"),
        "departamento",
        "municipio",
        "latitud",
        "longitud",
        "altitud"
    ),
    on="CodigoEstacion",
    how="left"
)

# Escribimos en formato Parquet particionado por departamento
df_enriquecido.printSchema()

(
    df_enriquecido.write
    .mode("overwrite")
    .partitionBy("departamento")
    .parquet(RUTA_S3_PROCESADO)
)

job.commit()