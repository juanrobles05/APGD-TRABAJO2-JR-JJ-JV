"""Lambda que lee el stream y calcula, por ventanas de 1 minuto y por categoria, el conteo y el promedio.
- Valida cada evento; los invalidos van a calcusol_descartados con su motivo.
- Repetido = mismo event_id ya visto (tabla calcusol_vistos). No se cuenta dos veces.
- Cada evento nuevo se guarda con UNA transaccion: marca el event_id como visto + suma a su ventana + suma al contador.
"""
import base64
import json
import os
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

ddb = boto3.client("dynamodb")
T_VENT = os.environ.get("T_VENTANAS", "calcusol_ventanas")
T_VISTOS = os.environ.get("T_VISTOS", "calcusol_vistos")
T_DESC = os.environ.get("T_DESCARTADOS", "calcusol_descartados")
T_CONT = os.environ.get("T_CONTADORES", "calcusol_contadores")
CATEGORIAS = {"Antioquia", "Cundinamarca", "Valle del Cauca", "Atlantico", "Santander"}
MIN_VALOR, MAX_VALOR = 0, 5000


def es_numero(x):
    return isinstance(x, (int, float, Decimal)) and not isinstance(x, bool)


def validar(ev):
    """Devuelve el motivo si el evento es invalido, o None si esta bien."""
    if not isinstance(ev, dict):
        return "no_es_objeto_json"
    for campo in ("event_id", "categoria", "valor", "ts_ms"):
        if campo not in ev or ev[campo] is None or ev[campo] == "":
            return f"falta_{campo}"
    if not isinstance(ev["event_id"], str):
        return "event_id_invalido"
    if ev["categoria"] not in CATEGORIAS:
        return "categoria_desconocida"
    if not es_numero(ev["valor"]):
        return "valor_no_numerico"
    if not (MIN_VALOR <= ev["valor"] <= MAX_VALOR):
        return "valor_fuera_de_rango"
    if not isinstance(ev["ts_ms"], int) or isinstance(ev["ts_ms"], bool):
        return "ts_ms_invalido"
    return None


def iso(ms):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def cancelada_por_condicion(e):
    if e.response["Error"]["Code"] != "TransactionCanceledException":
        return False
    razones = e.response.get("CancellationReasons", [])
    return bool(razones) and razones[0].get("Code") == "ConditionalCheckFailed"


def sumar(nombre):
    return {"Update": {"TableName": T_CONT, "Key": {"nombre": {"S": nombre}},
                       "UpdateExpression": "ADD #n :uno",
                       "ExpressionAttributeNames": {"#n": "cantidad"},
                       "ExpressionAttributeValues": {":uno": {"N": "1"}}}}


def descartar(seq, tipo, motivo, raw, event_id=None):
    item = {"seq": {"S": seq}, "tipo": {"S": tipo}, "motivo": {"S": motivo},
            "payload": {"S": raw[:2000]}, "recibido_ms": {"N": str(int(time.time() * 1000))}}
    if event_id:
        item["event_id"] = {"S": event_id}
    try:
        ddb.transact_write_items(TransactItems=[
            {"Put": {"TableName": T_DESC, "Item": item, "ConditionExpression": "attribute_not_exists(seq)"}},
            sumar(tipo + "s")])                      # "invalidos" o "repetidos"
    except ClientError as e:
        if cancelada_por_condicion(e):
            return                                   # reintento del mismo registro: ya estaba contado
        raise


def procesar(seq, ev, raw, tocadas):
    cat = ev["categoria"]
    valor = Decimal(str(ev["valor"]))
    ventana = ev["ts_ms"] // 60000 * 60000
    try:
        ddb.transact_write_items(TransactItems=[
            {"Put": {"TableName": T_VISTOS,
                     "Item": {"event_id": {"S": ev["event_id"]}, "seq": {"S": seq}},
                     "ConditionExpression": "attribute_not_exists(event_id)",
                     "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}},
            {"Update": {"TableName": T_VENT,
                        "Key": {"categoria": {"S": cat}, "ventana_inicio": {"N": str(ventana)}},
                        "UpdateExpression": "SET #i = :iso ADD #c :uno, #s :v",
                        "ExpressionAttributeNames": {"#i": "ventana_iso", "#c": "conteo", "#s": "suma"},
                        "ExpressionAttributeValues": {":iso": {"S": iso(ventana)}, ":uno": {"N": "1"},
                                                      ":v": {"N": str(valor)}}}},
            sumar("unicos")])
        tocadas.add((cat, ventana))
    except ClientError as e:
        if not cancelada_por_condicion(e):
            raise
        viejo = (e.response["CancellationReasons"][0].get("Item") or {}).get("seq", {}).get("S")
        if viejo == seq:
            return                                   # es este mismo registro reintentado: ya contado
        descartar(seq, "repetido", "event_id_repetido", raw, ev["event_id"])


def actualizar_promedios(tocadas):
    for cat, ventana in tocadas:
        key = {"categoria": {"S": cat}, "ventana_inicio": {"N": str(ventana)}}
        item = ddb.get_item(TableName=T_VENT, Key=key, ConsistentRead=True)["Item"]
        conteo, suma = Decimal(item["conteo"]["N"]), Decimal(item["suma"]["N"])
        prom = (suma / conteo).quantize(Decimal("0.01"))
        ddb.update_item(TableName=T_VENT, Key=key, UpdateExpression="SET #p = :p",
                        ExpressionAttributeNames={"#p": "promedio"},
                        ExpressionAttributeValues={":p": {"N": str(prom)}})


def handler(event, context):
    tocadas = set()
    falla = None
    for rec in event["Records"]:
        seq = rec["kinesis"]["sequenceNumber"]
        try:
            raw = base64.b64decode(rec["kinesis"]["data"]).decode("utf-8", errors="replace")
            try:
                ev = json.loads(raw, parse_float=Decimal)
                motivo = validar(ev)
            except ValueError:
                ev, motivo = None, "json_invalido"
            if motivo:
                eid = ev.get("event_id") if isinstance(ev, dict) and isinstance(ev.get("event_id"), str) else None
                descartar(seq, "invalido", motivo, raw, eid)
            else:
                procesar(seq, ev, raw, tocadas)
        except Exception as e:                       # error de infraestructura: Lambda reintenta desde aqui
            print(f"ERROR en {seq}: {e!r}")
            falla = seq
            break
    actualizar_promedios(tocadas)
    return {"batchItemFailures": [{"itemIdentifier": falla}] if falla else []}
