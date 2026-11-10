"""Consulta a DynamoDB que responde la pregunta del proyecto:
'Que departamento tuvo la mayor potencia promedio por evento durante la prueba y en que minuto fue mayor?'"""
import boto3
from collections import defaultdict
from decimal import Decimal

ddb = boto3.client("dynamodb")
filas = []
for pagina in ddb.get_paginator("scan").paginate(TableName="calcusol_ventanas"):
    filas += pagina["Items"]

por_depto = defaultdict(lambda: [Decimal(0), Decimal(0)])
for f in filas:
    c = por_depto[f["categoria"]["S"]]
    c[0] += Decimal(f["conteo"]["N"])
    c[1] += Decimal(f["suma"]["N"])

print("Promedio por departamento (todas las ventanas):")
for cat, (n, s) in sorted(por_depto.items(), key=lambda x: -(x[1][1] / x[1][0])):
    print(f"  {cat:<18} eventos={n:>6}  promedio={(s / n):.2f} W")

mejor = max(filas, key=lambda f: Decimal(f["promedio"]["N"]))
print(f"\nVentana con mayor promedio: {mejor['categoria']['S']} {mejor['ventana_iso']['S']} "
      f"-> {mejor['promedio']['N']} W ({mejor['conteo']['N']} eventos)")
