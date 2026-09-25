"""
Ingestao da API OpenAQ v3 -> NDJSON gzipado no S3, pronto para o Glue.

Saida: s3://<bucket>/arquivos_api/ingestion_date=YYYY-MM-DD/location-<id>.ndjson.gz
Segredo esperado: {"api_key": "..."}
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

try:
    from zoneinfo import ZoneInfo

    FUSO_BR = ZoneInfo("America/Sao_Paulo")
except Exception:
    # runtime sem tzdata: o Brasil nao usa mais horario de verao desde 2019
    FUSO_BR = timezone(timedelta(hours=-3))

API = "https://api.openaq.org/v3"

# a chave gratuita da OpenAQ da ~60 req/min, ficamos um pouco abaixo
INTERVALO = 60.0 / 55

BUCKET = os.environ["BUCKET_NAME"]
SECRET_NAME = os.environ["SECRET_NAME"]
LOCATION_IDS = os.environ["LOCATION_IDS"].split(",")
PREFIX = os.environ.get("PREFIX", "arquivos_api")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "3"))

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")

# guarda a chave entre invocacoes quentes, um GetSecretValue por container
_api_key = None


def get_api_key():
    global _api_key
    if _api_key is None:
        segredo = secrets.get_secret_value(SecretId=SECRET_NAME)["SecretString"]
        _api_key = json.loads(segredo)["api_key"]
    return _api_key


def api_get(path, params=None):
    """GET na OpenAQ. 404 vira lista vazia, o resto sobe e falha alto."""
    url = f"{API}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    time.sleep(INTERVALO)

    req = urllib.request.Request(url, headers={"X-API-Key": get_api_key()})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp).get("results", [])
    except urllib.error.HTTPError as erro:
        if erro.code == 404:
            return []
        raise


def montar_linha(medicao, location, sensor):
    """Achata a medicao no mesmo schema do CSV historico."""
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
    """NDJSON gzipado: um registro por linha."""
    corpo = "".join(json.dumps(linha, ensure_ascii=False) + "\n" for linha in linhas)
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(corpo.encode("utf-8")))


def lambda_handler(event, context):
    # tudo em horario de Sao Paulo: a janela e a particao seguem o dia local
    agora = datetime.now(FUSO_BR)
    # janela deslizante, a OpenAQ atrasa ~72h
    inicio = (agora - timedelta(days=DAYS_BACK)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    janela = {
        "datetime_from": inicio.isoformat(timespec="seconds"),
        "datetime_to": agora.isoformat(timespec="seconds"),
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
