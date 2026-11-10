"""Productor de la Parte B: publica eventos en Kinesis durante DURACION_S segundos (por defecto 6 minutos).
Cada evento valido: {"event_id", "categoria", "valor", "ts_ms"}.
A proposito repite algunos eventos (mismo event_id) y envia otros invalidos.
Al terminar escribe productor_resumen.json con lo realmente publicado.
"""
import json
import os
import random
import time
import uuid

import boto3

STREAM = os.environ.get("STREAM", "calcusol-stream")
DURACION_S = int(os.environ.get("DURACION_S", 360))
EVENTOS_POR_S = int(os.environ.get("EVENTOS_POR_S", 100))   # maximo 500 por llamada
P_REPETIDO = float(os.environ.get("P_REPETIDO", 0.05))
P_INVALIDO = float(os.environ.get("P_INVALIDO", 0.04))
CATEGORIAS = ["Antioquia", "Cundinamarca", "Valle del Cauca", "Atlantico", "Santander"]

kin = boto3.client("kinesis")
cuenta = {"publicados": 0, "unicos": 0, "repetidos": 0, "invalidos": 0}


def evento_valido():
    return {"event_id": str(uuid.uuid4()), "categoria": random.choice(CATEGORIAS),
            "valor": round(random.uniform(0, 4000), 2), "ts_ms": int(time.time() * 1000)}


def evento_invalido():
    e = evento_valido()
    tipo = random.choice(["sin_categoria", "valor_fuera_de_rango", "valor_texto", "sin_valor"])
    if tipo == "sin_categoria":
        del e["categoria"]
    elif tipo == "valor_fuera_de_rango":
        e["valor"] = 99999
    elif tipo == "valor_texto":
        e["valor"] = "abc"
    else:
        del e["valor"]
    return e


def enviar(lote):
    """lote = lista de (evento, tipo). Reintenta los registros que Kinesis rechace."""
    pendientes = lote
    for intento in range(6):
        recs = [{"Data": json.dumps(ev).encode("utf-8"),
                 "PartitionKey": ev.get("categoria") or "sin-categoria"} for ev, _ in pendientes]
        resp = kin.put_records(StreamName=STREAM, Records=recs)
        fallidos = []
        for item, res in zip(pendientes, resp["Records"]):
            if "ErrorCode" in res:
                fallidos.append(item)
            else:
                cuenta["publicados"] += 1
                cuenta[item[1] + "s"] += 1
        pendientes = fallidos
        if not pendientes:
            return
        time.sleep(0.5 * (intento + 1))
    raise RuntimeError(f"No se pudieron publicar {len(pendientes)} eventos")


def main():
    assert EVENTOS_POR_S <= 500, "PutRecords admite maximo 500 registros por llamada"
    inicio = time.time()
    recientes = []
    while time.time() - inicio < DURACION_S:
        t_seg = time.time()
        lote = []
        for _ in range(EVENTOS_POR_S):
            r = random.random()
            if r < P_INVALIDO:
                lote.append((evento_invalido(), "invalido"))
            elif r < P_INVALIDO + P_REPETIDO and recientes:
                lote.append((random.choice(recientes), "repetido"))
            else:
                e = evento_valido()
                recientes = (recientes + [e])[-500:]
                lote.append((e, "unico"))
        enviar(lote)
        print(f"{int(time.time() - inicio):4d}s  publicados={cuenta['publicados']}", flush=True)
        resto = 1 - (time.time() - t_seg)
        if resto > 0:
            time.sleep(resto)
    resumen = dict(cuenta, inicio=inicio, fin=time.time(), duracion_s=round(time.time() - inicio, 1))
    with open("productor_resumen.json", "w") as f:
        json.dump(resumen, f, indent=2)
    print(json.dumps(resumen, indent=2))


if __name__ == "__main__":
    main()
