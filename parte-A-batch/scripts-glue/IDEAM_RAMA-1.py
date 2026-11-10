import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RUTA_S3_PROCESADO = (
    "s3://apgd-trabajo2-jr-jj-jv/processed/"
)

RUTA_S3_RAMA_2 = (
    "s3://apgd-trabajo2-jr-jj-jv/analytics-ideam/rama-2/"
)

df = spark.read.parquet(RUTA_S3_PROCESADO)

# Agregación de la Rama 1: Evolución Temporal por Año y Mes
rama_1 = (
    df.groupBy("Anio", "Mes")
    .agg(
        F.avg("Valor").alias("Promedio_Wh_m2"),
        F.max("Valor").alias("Maximo_Wh_m2"),
        F.count("Valor").alias("Total_Lecturas")
    )
)

# Guardamos en Parquet en S3
(   rama_1.write
    .mode("overwrite")
    .parquet(RUTA_S3_RAMA_2)
)

job.commit()