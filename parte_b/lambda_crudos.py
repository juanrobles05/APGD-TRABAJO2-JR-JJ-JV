"""Segundo consumidor del stream: guarda TODOS los eventos tal como llegan en S3 (un archivo .jsonl por lote)."""
import base64
import os
import time

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]


def handler(event, context):
    recs = event["Records"]
    if not recs:
        return {}
    lineas = []
    for r in recs:
        txt = base64.b64decode(r["kinesis"]["data"]).decode("utf-8", errors="replace")
        lineas.append(txt.replace("\r", " ").replace("\n", " "))   # una linea por evento
    primero = recs[0]["kinesis"]
    llegada = time.gmtime(primero["approximateArrivalTimestamp"])
    key = time.strftime("raw/%Y/%m/%d/%H/", llegada) + primero["sequenceNumber"] + ".jsonl"
    s3.put_object(Bucket=BUCKET, Key=key, Body=("\n".join(lineas) + "\n").encode("utf-8"))
    return {}
