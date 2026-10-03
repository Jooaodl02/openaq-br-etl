WITH BASE_SILVER AS
(
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY id_localizacao, id_sensor, localizacao, anomesdia ORDER BY valor DESC) AS rn_valor_desc,
ROW_NUMBER() OVER (PARTITION BY id_localizacao, id_sensor, localizacao, anomesdia ORDER BY valor ASC) AS rn_valor_asc
FROM silver_openaq.openaq
WHERE anomesdia = 20260930
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
TOP3_VALOR
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