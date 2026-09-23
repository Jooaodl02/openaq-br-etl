import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# vem do default_arguments do job, em infra/bronze.tf
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_NAME", "API_PREFIX", "DATABASE_BRONZE", "TABLE_NAME"],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")


def data_hoje():
    return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")


# le so a particao do dia que a Lambda gravou
s3_path = "s3://{bucket}/{prefix}/ingestion_date={date}/".format(
    bucket=args["BUCKET_NAME"],
    prefix=args["API_PREFIX"],
    date=data_hoje(),
)

# schema fixo: sem ele o Spark infere e o tipo muda conforme o dia,
# quebrando o append na tabela do Catalog
schema_fixo = StructType([
    StructField("location_id", LongType(), nullable=False),
    StructField("sensors_id", LongType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("datetime", TimestampType(), nullable=True),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lon", DoubleType(), nullable=True),
    StructField("parameter", StringType(), nullable=True),
    StructField("units", StringType(), nullable=True),
    StructField("value", DoubleType(), nullable=True),
])

df = (
    spark.read.format("json")
    .schema(schema_fixo)
    .load(s3_path)
)

df = df.withColumns({
    "tipo_ingestao": F.lit("api"),
    "data_ingestao": F.current_date(),
})

df = df.select(
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value",
    "tipo_ingestao", "data_ingestao",
)

df.write \
  .mode("append") \
  .format("hive") \
  .partitionBy("data_ingestao") \
  .saveAsTable("{db}.{table}".format(db=args["DATABASE_BRONZE"], table=args["TABLE_NAME"]))

job.commit()
