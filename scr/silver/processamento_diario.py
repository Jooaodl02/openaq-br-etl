import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# vem do default_arguments do job, em infra/silver.tf
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BUCKET_NAME",
        "DATABASE_BRONZE",
        "DATABASE_SILVER",
        "TABLE_NAME",
        "JANELA_DIAS",
    ],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# a silver e Iceberg e vive no catalogo glue_catalog, montado no --conf do job.
# sem esse prefixo o Spark procura no catalogo Hive e nao acha a tabela
TABELA_SILVER = "glue_catalog.{db}.{table}".format(
    db=args["DATABASE_SILVER"], table=args["TABLE_NAME"]
)

# quantos dias de data_ingestao da bronze entram nesta execucao. 0 le a bronze
# inteira, que e o caso da primeira carga:
#   aws glue start-job-run --job-name silver-processamento-diario \
#     --arguments '--JANELA_DIAS=0'
JANELA_DIAS = int(args["JANELA_DIAS"])


def predicado_ingestao():
    """Recorte de particao usado na leitura da bronze.

    A bronze e particionada por data_ingestao, entao o filtro entra como
    push_down_predicate e corta particao antes da leitura, nao depois. Quem
    garante que nada entre duas vezes continua sendo o MERGE; a janela so
    evita reler todo o historico a cada execucao.
    """
    if JANELA_DIAS <= 0:
        return ""

    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    limite = hoje - timedelta(days=JANELA_DIAS - 1)
    return "data_ingestao >= '{d}'".format(d=limite.isoformat())


df_bronze = glueContext.create_dynamic_frame.from_catalog(
    database=args["DATABASE_BRONZE"],
    table_name=args["TABLE_NAME"],
    push_down_predicate=predicado_ingestao(),
).toDF()

##tira duplicados da tabela bronze, caso haja algum
df_bronze = df_bronze.dropDuplicates(["sensors_id", "datetime"])


# o id 3911519 traz leituras de Caji (BA) e de Toronto sob o mesmo id; so a
# estacao brasileira fica, o resto do id sai da silver. o filtro olha o nome cru
# porque a partir daqui o nome passa a vir do id, nao do que a API mandou
df_lote = df_bronze.filter(
    (F.col("location_id") != 3911519)
    | F.col("location").isin("Parque Vida Nova, Caji", "Parque Vida Nova, Caji-3892884")
)

#tratamento para garantir 1 localizacao por id, mesmo que a API mude o nome da localizacao
localizacao = (
    F.when(F.col("location_id") == 6152580, "Abolição")
     .when(F.col("location_id") == 820321, "Bangu")
     .when(F.col("location_id") == 6152581, "Barra da Tijuca")
     .when(F.col("location_id") == 6152582, "Caju")
     .when(F.col("location_id") == 5110472, "Campinho")
     .when(F.col("location_id") == 820322, "Campo Grande")
     .when(F.col("location_id") == 6152583, "Campo Grande Aurassure")
     .when(F.col("location_id") == 5110511, "Campo de Santana")
     .when(F.col("location_id") == 5110513, "Cascadura")
     .when(F.col("location_id") == 5110473, "Cascadura 2")
     .when(F.col("location_id") == 3047759, "Caxias do Sul")
     .when(F.col("location_id") == 820323, "Centro")
     .when(F.col("location_id") == 820324, "Copacabana")
     .when(F.col("location_id") == 3852932, "GUAMÁ")
     .when(F.col("location_id") == 6152584, "Guaratiba")
     .when(F.col("location_id") == 6152585, "Ilha do Governador")
     .when(F.col("location_id") == 6404277, "Ilha do Governador")
     .when(F.col("location_id") == 6102319, "Imperatriz")
     .when(F.col("location_id") == 820326, "Irajá")
     .when(F.col("location_id") == 6152586, "Irajá Aurassure")
     .when(F.col("location_id") == 6199572, "Jacarepaguá")
     .when(F.col("location_id") == 6152587, "Lagoa")
     .when(F.col("location_id") == 5616694, "Lábrea Caititu")
     .when(F.col("location_id") == 5636852, "MAM")
     .when(F.col("location_id") == 5110514, "Madureira")
     .when(F.col("location_id") == 5040030, "Manaus")
     .when(F.col("location_id") == 5648811, "Parque Madureira")
     .when(F.col("location_id") == 3911519, "Parque Vida Nova, Caji")
     .when(F.col("location_id") == 6152588, "Pavuna")
     .when(F.col("location_id") == 6152590, "Penha")
     .when(F.col("location_id") == 5616182, "Porto Velho - Brux")
     .when(F.col("location_id") == 5636888, "Praça XV")
     .when(F.col("location_id") == 5110474, "Presidente Vargas")
     .when(F.col("location_id") == 6126992, "Quality01")
     .when(F.col("location_id") == 6126993, "Quality02")
     .when(F.col("location_id") == 5110512, "Quintino")
     .when(F.col("location_id") == 6152591, "Ramos")
     .when(F.col("location_id") == 6152592, "Realengo")
     .when(F.col("location_id") == 6418495, "Recreio dos Bandeirantes")
     .when(F.col("location_id") == 6152593, "Recreio dos Bandeirantes")
     .when(F.col("location_id") == 6152594, "Rocha")
     .when(F.col("location_id") == 6152595, "Rocinha")
     .when(F.col("location_id") == 6285443, "Santa Cruz 1")
     .when(F.col("location_id") == 6285444, "Santa Cruz 2")
     .when(F.col("location_id") == 589967, "Sao Paulo")
     .when(F.col("location_id") == 6460996, "Sao Paulo")
     .when(F.col("location_id") == 820328, "São Cristóvão")
     .when(F.col("location_id") == 6139516, "São Paulo")
     .when(F.col("location_id") == 6152598, "Tanque")
     .when(F.col("location_id") == 820329, "Tijuca")
     .when(F.col("location_id") == 6178275, "Tijuca Aurassure")
     .when(F.col("location_id") == 3107138, "teste1")
     .otherwise(F.col("location"))
)


# codigo do parametro normalizado, porque a API ja veio com caixa mista
parametro = F.lower(F.trim(F.col("parameter")))

# otherwise no proprio codigo: parametro novo na API nao vira null silencioso
descricao = (
    F.when(parametro == "o3", "Ozônio")
     .when(parametro == "no", "Óxido nítrico")
     .when(parametro == "no2", "Dióxido de nitrogênio")
     .when(parametro == "nox", "Óxidos de nitrogênio")
     .when(parametro == "so2", "Dióxido de enxofre")
     .when(parametro == "co", "Monóxido de carbono")
     .when(parametro == "pm1", "Partículas ultrafinas")
     .when(parametro == "pm25", "Partículas finas")
     .when(parametro == "pm10", "Partículas grossas")
     .when(parametro == "um003", "Contagem 0.3µm")
     .when(parametro == "temperature", "Temperatura")
     .when(parametro == "relativehumidity", "Umidade relativa")
     .otherwise(F.col("parameter"))
)

## renomeia as colunas da bronze para portugues e monta a descricao.
## os nomes tem que bater com os da tabela: o INSERT * do MERGE casa por nome
df_lote = df_lote.select(
    F.col("location_id").alias("id_localizacao"),
    F.col("sensors_id").alias("id_sensor"),
    localizacao.alias("localizacao"),
    F.col("datetime").alias("data_hora"),
    F.col("lat").alias("latitude"),
    F.col("lon").alias("longitude"),
    F.col("parameter").alias("parametro"),
    descricao.alias("parametro_descricao"),
    F.col("units").alias("unidade"),
    F.col("value").alias("valor"),
    F.col("tipo_ingestao"),
    F.date_format(F.col("datetime"), "yyyyMMdd").cast("int").alias("anomesdia"),
    F.col("data_ingestao").cast("date").alias("data_ingestao"),
)

df_lote.createOrReplaceTempView("lote")

# MERGE INTO no lugar de anti-join + append: a chave da silver e
# (id_sensor, data_hora), e so entra o que ainda nao esta la.
#
# anomesdia vem no ON junto da chave, mesmo sem fazer parte da identidade da
# linha (sai de data_hora, entao nao muda o resultado do casamento). ele esta
# ali pela poda: o Iceberg guarda min/max por coluna em cada arquivo, e com os
# valores de anomesdia do lote ele descarta no plano os arquivos do destino que
# nao podem colidir, em vez de ler a tabela inteira.
#
# so tem clausula NOT MATCHED, entao nenhuma linha existente e reescrita, o
# commit e um append de snapshot. o MERGE nao deduplica a origem: quem garante
# isso e o dropDuplicates la em cima
spark.sql(
    """
    MERGE INTO {tabela} AS destino
    USING lote AS origem
       ON destino.anomesdia = origem.anomesdia
      AND destino.id_sensor = origem.id_sensor
      AND destino.data_hora = origem.data_hora
    WHEN NOT MATCHED THEN INSERT *
    """.format(tabela=TABELA_SILVER)
)

job.commit()
