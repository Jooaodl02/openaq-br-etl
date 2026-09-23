import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, TimestampType, DoubleType
from datetime import datetime
from zoneinfo import ZoneInfo
# 2. Ler todos os arquivos CSV de um diretório

def data_hoje():
    return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")

s3_path = "s3://bucket-openaq-etl-jooaodl02/arquivos_api/ingestion_date={date}/".format(date=data_hoje())


schema_fixo = StructType([
    StructField("location_id", LongType(), nullable=False),
    StructField("sensors_id", LongType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("datetime", TimestampType(), nullable=True),
    
    StructField("lat", DoubleType(), nullable=True),
    StructField("lon", DoubleType(), nullable=True),
    StructField("parameter", StringType(), nullable=True),
    StructField("units", StringType(), nullable=True),
    StructField("value", DoubleType(), nullable=True)
    
])

df = (
    spark.read.format("json")
    .option("header", "true")
    .schema(schema_fixo)
    .load(s3_path)
)  # ou apenas o caminho da pasta: "/caminho/para/sua/pasta/"

df = df.withColumns({
    "tipo_ingestao": F.lit("historico_csv"),
    "data_ingestao": F.current_date()
})


# df.filter(df["location_id"].isNull()).show()

df = df.select(
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value",
    "tipo_ingestao", "data_ingestao",
)

# df.createOrReplaceTempView("df")
# df = spark.sql("SELECT COUNT(*), location_id FROM df GROUP BY location_id")
# df.show()

df.write \
  .mode("append") \
  .format("hive") \
  .partitionBy("data_ingestao") \
  .saveAsTable("bronze_openaq.openaq")

# df.printSchema()
# 3. Exibir os dados
# df.show(n=20)
job.commit()