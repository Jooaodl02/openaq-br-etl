"""
Ingestao da API OpenAQ v3 -> arquivos no S3 prontos para o Glue.

Para cada estacao (location_id), busca as medicoes dos ultimos DAYS_BACK dias
e grava um arquivo NDJSON gzipado, achatado no mesmo schema do CSV historico.

Saida:
  s3://<bucket>/arquivos_api/ingestion_date=YYYY-MM-DD/location-<id>.ndjson.gz

A chave da API vem do Secrets Manager, num segredo com o formato:
  {"api_key": "..."}

Usa apenas stdlib + boto3, que ja vem no runtime da Lambda.
"""

import gzip
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

API = "https://api.openaq.org/v3"

# a chave gratuita da OpenAQ permite ~60 req/min; ficamos um pouco abaixo disso.
# com 52 estacoes sao ~212 chamadas, entao a pausa domina o tempo de execucao.
INTERVALO = 60.0 / 55

BUCKET = os.environ["BUCKET_NAME"]
SECRET_NAME = os.environ["SECRET_NAME"]
LOCATION_IDS = os.environ["LOCATION_IDS"].split(",")
PREFIX = os.environ.get("PREFIX", "arquivos_api")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "3"))

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")

# guarda a chave entre invocacoes quentes, para nao pagar um GetSecretValue por execucao
_api_key = None


def get_api_key():
    """Le a chave da OpenAQ no Secrets Manager (uma vez por container)."""
    global _api_key
    if _api_key is None:
        segredo = secrets.get_secret_value(SecretId=SECRET_NAME)["SecretString"]
        _api_key = json.loads(segredo)["api_key"]
    return _api_key


def api_get(path, params=None):
    """GET na OpenAQ, devolvendo a lista de results.

    404 vira lista vazia: a estacao ou o sensor saiu do cadastro da API e o
    chamador apenas pula. Os outros erros sobem, para a execucao falhar alto.
    """
    url = f"{API}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    time.sleep(INTERVALO)  # respeita o rate limit da API

    req = urllib.request.Request(url, headers={"X-API-Key": get_api_key()})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp).get("results", [])
    except urllib.error.HTTPError as erro:
        if erro.code == 404:
            return []
        raise


def montar_linha(medicao, location, sensor):
    """Achata a medicao no schema do CSV historico."""
    periodo = medicao.get("period") or {}
    datahora = periodo.get("datetimeTo") or {}
    parametro = medicao.get("parameter") or {}
    coords = location.get("coordinates") or {}

    return {
        "location_id": location["id"],
        "sensors_id": sensor["id"],
        "location": location.get("name"),
        "datetime": datahora.get("local"),
        "lat": coords.get("latitude"),
        "lon": coords.get("longitude"),
        "parameter": parametro.get("name"),
        "units": parametro.get("units"),
        "value": medicao.get("value"),
    }


def salvar_no_s3(linhas, key):
    """Grava as linhas como NDJSON gzipado: um registro por linha."""
    corpo = "".join(json.dumps(linha, ensure_ascii=False) + "\n" for linha in linhas)
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(corpo.encode("utf-8")))


def lambda_handler(event, context):
    agora = datetime.now(timezone.utc)
    # janela deslizante: o CSV historico so atualiza a cada ~72h
    inicio = agora - timedelta(days=DAYS_BACK)

    janela = {
        "datetime_from": inicio.strftime("%Y-%m-%dT00:00:00Z"),
        "datetime_to": agora.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": 1000,
    }
    ingestion_date = agora.strftime("%Y-%m-%d")
    resumo = {}

    for location_id in LOCATION_IDS:
        location_id = location_id.strip()
        resultados = api_get(f"/locations/{location_id}")
        if not resultados:
            print(f"location {location_id}: nao encontrada, pulando")
            continue
        location = resultados[0]

        linhas = []
        for sensor in location.get("sensors", []):
            for medicao in api_get(f"/sensors/{sensor['id']}/measurements", janela):
                linhas.append(montar_linha(medicao, location, sensor))

        if not linhas:
            print(f"location {location_id}: nenhuma medicao na janela")
            continue

        key = f"{PREFIX}/ingestion_date={ingestion_date}/location-{location_id}.ndjson.gz"
        salvar_no_s3(linhas, key)

        print(f"location {location_id}: {len(linhas)} registros -> s3://{BUCKET}/{key}")
        resumo[location_id] = len(linhas)

    return resumo
