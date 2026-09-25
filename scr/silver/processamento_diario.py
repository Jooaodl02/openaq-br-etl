import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# vem do default_arguments do job, em infra/silver.tf
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_NAME", "DATABASE_BRONZE", "DATABASE_SILVER", "TABLE_NAME"],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

TABELA_BRONZE = "{db}.{table}".format(db=args["DATABASE_BRONZE"], table=args["TABLE_NAME"])
TABELA_SILVER = "{db}.{table}".format(db=args["DATABASE_SILVER"], table=args["TABLE_NAME"])


def data_hoje():
    return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")


df_bronze = glueContext.create_dynamic_frame.from_catalog(
    database=args["DATABASE_BRONZE"],
    table_name=args["TABLE_NAME"],
    push_down_predicate="",
).toDF()

##tira duplicados da tabela bronze, caso haja algum
df_bronze = df_bronze.dropDuplicates(["sensors_id", "datetime"])


## seleciona os id's e datetimes que já estão na tabela silver
df_silver = spark.table(f"{args['DATABASE_SILVER']}.{args['TABLE_NAME']}") \
                 .select("sensors_id", "datetime")

## traz só quem esta na bronze (sem duplicados) e não esta na silver
df_novos = df_bronze.join(df_silver, on=["sensors_id", "datetime"], how="left_anti")


    
df_novos.write \
  .mode("append") \
  .format("hive") \
  .partitionBy("data_ingestao") \
  .saveAsTable(TABELA_SILVER)

job.commit()
