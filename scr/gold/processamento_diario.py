import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# vem do default_arguments do job, em infra/gold.tf
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BUCKET_NAME",
        "DATABASE_SILVER",
        "DATABASE_GOLD",
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

# silver e gold sao Iceberg e vivem no catalogo glue_catalog, montado no --conf
# do job. sem esse prefixo o Spark procura no catalogo Hive e nao acha a tabela
TABELA_SILVER = "glue_catalog.{db}.{table}".format(
    db=args["DATABASE_SILVER"], table=args["TABLE_NAME"]
)
TABELA_GOLD = "glue_catalog.{db}.{table}".format(
    db=args["DATABASE_GOLD"], table=args["TABLE_NAME"]
)

# quantos dias de anomesdia da silver entram nesta execucao. 0 le a silver
# inteira, que e o caso da primeira carga:
# o = depois de --arguments e obrigatorio: sem ele o CLI le o
# --JANELA_DIAS como uma opcao dele e nem chama a API
#   aws glue start-job-run --job-name gold-processamento-diario \
#     --arguments='--JANELA_DIAS=0'
JANELA_DIAS = int(args["JANELA_DIAS"])


def filtro_anomesdia():
    """Recorte de dias usado na leitura da silver.

    No SQL original esse recorte era um literal (anomesdia = 20260930). Aqui
    ele vira janela movel: o Iceberg guarda min/max por coluna em cada arquivo,
    entao o filtro descarta arquivo no plano, antes da leitura.

    A janela nao muda o resultado do ranking, porque as janelas do ROW_NUMBER
    sao particionadas por anomesdia: cada dia e ranqueado dentro dele mesmo,
    tanto lendo um dia quanto lendo trinta.
    """
    if JANELA_DIAS <= 0:
        return "1 = 1"

    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    limite = hoje - timedelta(days=JANELA_DIAS - 1)
    return "anomesdia >= {d}".format(d=int(limite.strftime("%Y%m%d")))


# a view no lugar do FROM silver_openaq.openaq do SQL original: o nome da
# tabela vem do argumento do job, e o recorte de dias fica num lugar so
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW silver_lote AS
    SELECT *
    FROM {tabela}
    WHERE {filtro}
    """.format(tabela=TABELA_SILVER, filtro=filtro_anomesdia())
)

# as regras de negocio ficam no SQL como estavam escritas: mesmas CTEs, mesmas
# janelas, mesmas chaves de join. duas correcoes em relacao ao .sql:
#
#  - faltava a virgula depois de TOP3_VALOR no SELECT do CRUZAMENTO, o que
#    fazia "TOP3_VALOR BOTTOM1_VALOR" virar um alias: o bottom1 real sumia do
#    resultado e a coluna BOTTOM1_VALOR trazia o valor do top3.
#  - o WHERE do dia saiu para a view silver_lote, acima.
spark.sql(
    """
    WITH BASE_SILVER AS
    (
    SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_localizacao, id_sensor, localizacao, anomesdia ORDER BY valor DESC) AS rn_valor_desc,
    ROW_NUMBER() OVER (PARTITION BY id_localizacao, id_sensor, localizacao, anomesdia ORDER BY valor ASC) AS rn_valor_asc
    FROM silver_lote
    )

    ,TOP1_VALOR AS
    (
    SELECT
    id_localizacao,
    id_sensor,
    localizacao,
    valor as TOP1_VALOR,
    anomesdia
    FROM BASE_SILVER
    WHERE rn_valor_desc = 1
    )

    ,TOP2_VALOR AS
    (
    SELECT
    id_localizacao,
    id_sensor,
    localizacao,
    valor as TOP2_VALOR,
    anomesdia
    FROM BASE_SILVER
    WHERE rn_valor_desc = 2
    )

    ,TOP3_VALOR AS
    (
    SELECT
    id_localizacao,
    id_sensor,
    localizacao,
    valor as TOP3_VALOR,
    anomesdia
    FROM BASE_SILVER
    WHERE rn_valor_desc = 3
    )

    ,BOTTOM1_VALOR AS
    (
    SELECT
    id_localizacao,
    id_sensor,
    localizacao,
    valor as BOTTOM1_VALOR,
    anomesdia
    FROM BASE_SILVER
    WHERE rn_valor_asc = 1
    )

    ,BOTTOM2_VALOR AS
    (
    SELECT
    id_localizacao,
    id_sensor,
    localizacao,
    valor as BOTTOM2_VALOR,
    anomesdia
    FROM BASE_SILVER
    WHERE rn_valor_asc = 2
    )

    ,BOTTOM3_VALOR AS
    (
    SELECT
    id_localizacao,
    id_sensor,
    localizacao,
    valor as BOTTOM3_VALOR,
    anomesdia
    FROM BASE_SILVER
    WHERE rn_valor_asc = 3
    )

    ,BASE_SILVER_NO_DUP_POR_ANOMESDIA AS
    (
    SELECT
    id_localizacao,
    id_sensor,
    localizacao,
    parametro,
    parametro_descricao,
    unidade,
    anomesdia
    FROM BASE_SILVER
    GROUP BY
    id_localizacao,
    id_sensor,
    localizacao,
    parametro,
    parametro_descricao,
    unidade,
    anomesdia
    )

    ,CRUZAMENTO AS
    (
    SELECT
    BASE_SILVER_NO_DUP_POR_ANOMESDIA.*,
    TOP1_VALOR,
    TOP2_VALOR,
    TOP3_VALOR,
    BOTTOM1_VALOR,
    BOTTOM2_VALOR,
    BOTTOM3_VALOR
    FROM BASE_SILVER_NO_DUP_POR_ANOMESDIA

    LEFT JOIN TOP1_VALOR
    ON BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_localizacao = TOP1_VALOR.id_localizacao
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_sensor = TOP1_VALOR.id_sensor
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.anomesdia = TOP1_VALOR.anomesdia

    LEFT JOIN TOP2_VALOR
    ON BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_localizacao = TOP2_VALOR.id_localizacao
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_sensor = TOP2_VALOR.id_sensor
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.anomesdia = TOP2_VALOR.anomesdia

    LEFT JOIN TOP3_VALOR
    ON BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_localizacao = TOP3_VALOR.id_localizacao
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_sensor = TOP3_VALOR.id_sensor
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.anomesdia = TOP3_VALOR.anomesdia

    LEFT JOIN BOTTOM1_VALOR
    ON BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_localizacao = BOTTOM1_VALOR.id_localizacao
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_sensor = BOTTOM1_VALOR.id_sensor
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.anomesdia = BOTTOM1_VALOR.anomesdia

    LEFT JOIN BOTTOM2_VALOR
    ON BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_localizacao = BOTTOM2_VALOR.id_localizacao
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_sensor = BOTTOM2_VALOR.id_sensor
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.anomesdia = BOTTOM2_VALOR.anomesdia

    LEFT JOIN BOTTOM3_VALOR
    ON BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_localizacao = BOTTOM3_VALOR.id_localizacao
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.id_sensor = BOTTOM3_VALOR.id_sensor
    AND BASE_SILVER_NO_DUP_POR_ANOMESDIA.anomesdia = BOTTOM3_VALOR.anomesdia
    )

    SELECT
    *
    FROM CRUZAMENTO
    """
).createOrReplaceTempView("lote")

# MERGE INTO como na silver: a chave da gold e
# (anomesdia, id_localizacao, id_sensor), e so entra o que ainda nao esta la.
#
# so tem clausula NOT MATCHED, entao nenhuma linha existente e reescrita e o
# commit e um append de snapshot. reprocessar um dia que ja esta na gold nao
# duplica nem atualiza: a linha e ignorada. para recalcular um dia ja gravado,
# apagar as linhas dele antes (DELETE FROM ... WHERE anomesdia = ...) e rodar
# de novo com a janela que cubra o dia
spark.sql(
    """
    MERGE INTO {tabela} AS destino
    USING lote AS origem
       ON destino.anomesdia = origem.anomesdia
      AND destino.id_localizacao = origem.id_localizacao
      AND destino.id_sensor = origem.id_sensor
    WHEN NOT MATCHED THEN INSERT *
    """.format(tabela=TABELA_GOLD)
)

job.commit()
