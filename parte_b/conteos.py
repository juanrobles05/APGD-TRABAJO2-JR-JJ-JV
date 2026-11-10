"""Muestra que los conteos cuadran: publicados = unicos + repetidos + invalidos = lineas crudas en S3.
Uso: BUCKET=tu-bucket python3 conteos.py   (en la misma carpeta que productor_resumen.json)"""
import json
import os

import boto3

BUCKET = os.environ["BUCKET"]
ddb, s3 = boto3.client("dynamodb"), boto3.client("s3")


def scan(tabla):
    for pagina in ddb.get_paginator("scan").paginate(TableName=tabla):
        yield from pagina["Items"]


pub = json.load(open("productor_resumen.json"))
cont = {i["nombre"]["S"]: int(i["cantidad"]["N"]) for i in scan("calcusol_contadores")}
unicos, repetidos, invalidos = (cont.get(k, 0) for k in ("unicos", "repetidos", "invalidos"))
en_ventanas = sum(int(i["conteo"]["N"]) for i in scan("calcusol_ventanas"))
descartados = sum(1 for _ in scan("calcusol_descartados"))
crudos = 0
for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="raw/"):
    for obj in pagina.get("Contents", []):
        cuerpo = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode("utf-8")
        crudos += sum(1 for linea in cuerpo.split("\n") if linea)

print(f"Publicados por el productor ........ {pub['publicados']}")
print(f"  (enviados a proposito: unicos={pub['unicos']} repetidos={pub['repetidos']} invalidos={pub['invalidos']})")
print(f"Unicos procesados (contador) ....... {unicos}")
print(f"Unicos sumados en las ventanas ..... {en_ventanas}")
print(f"Repetidos descartados .............. {repetidos}")
print(f"Invalidos descartados .............. {invalidos}")
print(f"Filas en la tabla de descartados ... {descartados}  (debe ser repetidos + invalidos)")
print(f"Eventos crudos en S3 ............... {crudos}")
suma = unicos + repetidos + invalidos
print()
print(f"unicos + repetidos + invalidos = {unicos} + {repetidos} + {invalidos} = {suma}")
print("CUADRA" if suma == pub["publicados"] == crudos and en_ventanas == unicos and descartados == repetidos + invalidos
      else "NO CUADRA: revisa errores de la Lambda y que el productor haya terminado")
