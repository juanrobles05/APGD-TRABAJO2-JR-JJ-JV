# Trabajo 2 – Parte B: streaming con Kinesis, DynamoDB y S3

Almacenamiento y Procesamiento de Grandes Datos (SI7006-262), EAFIT.
Integrantes: (nombres del equipo)

Hicimos un flujo de streaming en AWS Academy Learner Lab (`us-east-1`), con todos los recursos creados a mano en la consola, sin infraestructura como código.

## Arquitectura

```
productor.py → Kinesis (calcusol-stream, 1 shard) ─┬─► Lambda calcusol-ventanas → DynamoDB
                                                    └─► Lambda calcusol-crudos   → S3 (raw/*.jsonl)
```

Usamos Lambda para las ventanas en lugar de Flink, debido a que es más simple de operar. Cada Lambda es un consumidor independiente del mismo stream.

Tablas de DynamoDB: `calcusol_ventanas` (llave `categoria` + `ventana_inicio`), `calcusol_vistos` (`event_id`), `calcusol_descartados` (`seq`) y `calcusol_contadores` (`nombre`). Bucket de S3: `calcusol-t2-jjj-1234`. Capturas en [Imagenes](Imagenes/).

## Cómo clasificamos los eventos

- **Válido**: `event_id` texto, `categoria` entre las 5 definidas, `valor` entre 0 y 5000 y `ts_ms` entero.
- **Repetido**: su `event_id` ya se había visto. Solo cuenta el primero.
- **Inválido**: no cumple lo anterior. Se guarda en `calcusol_descartados` con su motivo.
- **Ventana**: el minuto de `ts_ms` (hora del evento, no de llegada).

Para no contar un repetido dos veces, la Lambda usa una transacción de DynamoDB que, a la vez, marca el `event_id` como visto (solo si no existía) y suma a la ventana y al contador. Si el `event_id` ya existía, la transacción se cancela y el evento se aparta como repetido.

## Resultados de la corrida (6 minutos)

| Publicados | Únicos | Repetidos | Inválidos |
| ---: | ---: | ---: | ---: |
| 36.000 | 32.835 | 1.745 | 1.420 |

Los conteos cuadran: 32.835 + 1.745 + 1.420 = 36.000, que es lo que publicó el productor y lo que hay en S3. Además, los únicos sumados en las ventanas son 32.835 y `calcusol_descartados` tiene 3.165 filas (repetidos + inválidos). Evidencia: [conteos](<Imagenes/Evidencia que cuadran los conteos.jpeg>) y [productor](<Imagenes/Ejecución parte B.jpeg>).

**Pregunta del proyecto: ¿qué departamento tuvo la mayor potencia promedio por evento?**

| Departamento | Eventos | Promedio (W) |
| --- | ---: | ---: |
| Antioquia | 3.276 | 2.030,81 |
| Atlantico | 3.386 | 1.997,28 |
| Valle del Cauca | 3.484 | 1.988,51 |
| Cundinamarca | 3.282 | 1.983,65 |
| Santander | 3.323 | 1.970,79 |

Antioquia. Su ventana con mayor promedio fue `2026-11-09T22:28:00Z`, con 2.060,71 W (1.073 eventos). Captura: [consulta](<Imagenes/Promedio por departamento y ventana con mayor promedio.jpeg>).

## Cómo reproducirlo

1. Iniciar el Learner Lab y abrir la consola en `us-east-1`.
2. Crear las 4 tablas de DynamoDB (on-demand). La sort key de `calcusol_ventanas` debe ser **Number**.
3. Crear el stream `calcusol-stream` (1 shard).
4. Crear la Lambda `calcusol-ventanas` (Python 3.12, rol `LabRole`) con `parte_b/lambda_ventanas.py`, hacer **Deploy** y ajustar el Handler a `lambda_ventanas.lambda_handler`. Agregar el trigger de Kinesis (batch 100, Trim horizon, Report batch item failures).
5. Crear el bucket de S3 y la Lambda `calcusol-crudos` con `parte_b/lambda_crudos.py`, la variable `BUCKET` y el mismo trigger.
6. En CloudShell: `python3 productor.py`, y al terminar `python3 consulta_dynamodb.py` y `BUCKET=<bucket> python3 conteos.py`.
7. Para repetir una corrida: `BUCKET=<bucket> python3 reiniciar_parteB.py`.

Dos errores que tuvimos: el Handler no coincidía con el nombre del archivo (`ImportModuleError`) y la sort key de `calcusol_ventanas` estaba como String (`ValidationError` en la transacción); recreamos la tabla con tipo Number.

## Uso de IA

Usamos Claude (Anthropic) para la guía de la consola y para la primera versión de `productor.py`, las dos Lambdas, `consulta_dynamodb.py`, `conteos.py` y `reiniciar_parteB.py`, y para redactar este README con las indicaciones de la explicacion de lo que hicimos. Nosotros creamos los recursos en la consola, ejecutamos todo, corregimos los dos errores de arriba y verificamos los resultados.
