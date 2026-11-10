"""Vacia las 4 tablas de la Parte B y la carpeta raw/ del bucket, para repetir una corrida desde cero.
Uso: BUCKET=tu-bucket python3 reiniciar_parteB.py"""
import os

import boto3

BUCKET = os.environ["BUCKET"]
CLAVES = {"calcusol_ventanas": ["categoria", "ventana_inicio"], "calcusol_vistos": ["event_id"],
          "calcusol_descartados": ["seq"], "calcusol_contadores": ["nombre"]}
ddb, s3 = boto3.client("dynamodb"), boto3.client("s3")

for tabla, claves in CLAVES.items():
    n = 0
    for pagina in ddb.get_paginator("scan").paginate(TableName=tabla, ProjectionExpression=", ".join(f"#k{i}" for i in range(len(claves))),
                                                       ExpressionAttributeNames={f"#k{i}": c for i, c in enumerate(claves)}):
        items = pagina["Items"]
        for i in range(0, len(items), 25):
            ddb.batch_write_item(RequestItems={tabla: [{"DeleteRequest": {"Key": it}} for it in items[i:i + 25]]})
            n += len(items[i:i + 25])
    print(f"{tabla}: {n} filas borradas")

borrados = 0
for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="raw/"):
    objs = [{"Key": o["Key"]} for o in pagina.get("Contents", [])]
    if objs:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})
        borrados += len(objs)
print(f"s3://{BUCKET}/raw/: {borrados} archivos borrados")
