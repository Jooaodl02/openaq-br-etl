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
    [
        "JOB_NAME",
        "BUCKET_NAME",
        "API_PREFIX",
        "BRONZE_PREFIX",
        "DATABASE_BRONZE",
        "TABLE_NAME",
    ],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# so a particao do dia e reescrita; o resto da tabela fica intacto.
# com isso rodar o job duas vezes no mesmo dia nao duplica registro
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def data_hoje():
    return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")


# a mesma data manda na leitura, no valor da coluna e no ALTER TABLE:
# calcular de novo em cada ponto arrisca virar o dia no meio do job
hoje = data_hoje()

# le so a particao do dia que a Lambda gravou
s3_path = "s3://{bucket}/{prefix}/ingestion_date={date}/".format(
    bucket=args["BUCKET_NAME"],
    prefix=args["API_PREFIX"],
    date=hoje,
)

tabela_path = "s3://{bucket}/{prefix}/".format(
    bucket=args["BUCKET_NAME"],
    prefix=args["BRONZE_PREFIX"],
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
    "data_ingestao": F.lit(hoje).cast("date"),
})

df = df.select(
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value",
    "tipo_ingestao", "data_ingestao",
)

# escrita direta no path da tabela, sem saveAsTable/format("hive"): aquele
# caminho grava num .hive-staging e depois pede pro Hive mover os arquivos
# pra particao final, e o move no S3 contra o Glue Catalog e o que quebrava
df.write \
  .mode("overwrite") \
  .format("parquet") \
  .partitionBy("data_ingestao") \
  .save(tabela_path)

# a escrita acima nao fala com o Catalog, entao a particao e registrada aqui
spark.sql(
    "ALTER TABLE {db}.{table} ADD IF NOT EXISTS PARTITION (data_ingestao = '{date}')".format(
        db=args["DATABASE_BRONZE"],
        table=args["TABLE_NAME"],
        date=hoje,
    )
)

job.commit()
