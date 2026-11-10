# Trabajo 2 – Pipelines de datos en AWS (Parte A batch y Parte B streaming)

Almacenamiento y Procesamiento de Grandes Datos (SI7006-262), EAFIT.
Integrantes: (nombres del equipo)

Todo se hizo en AWS Academy Learner Lab (`us-east-1`), con los recursos creados a mano en la consola (sin infraestructura como código; `docs/cloudformation_zero_stacks.png` muestra 0 pilas).

## Estructura del repositorio

```
├── parte-A-batch/
│   ├── scripts-glue/      IDEAM-INITIAL-BATCH.py, IDEAM_RAMA-1.py, IDEAM_RAMA-2.py
│   ├── step-functions/    IDEAM-Step-Function.json
│   └── athena-queries/    create_tables.sql, analytics_queries.sql
├── parte_b/               productor, Lambdas, scripts de verificación e imágenes (ver parte_b/README.md)
└── docs/                  capturas de la Parte A
```

---

## Parte A – Pipeline batch (radiación solar IDEAM)

Procesamos históricos de radiación solar del IDEAM y los cruzamos con el catálogo de estaciones para analizar el potencial solar en el tiempo y por zona geográfica.

### Arquitectura

```
S3 raw/ (CSV radiación + catálogo de estaciones)
        │
        ▼
Glue IDEAM-INITIAL-BATCH ──► S3 processed/ (Parquet, partitionBy departamento)
        │
        ├─► Glue IDEAM_RAMA-1 (temporal)   ──► S3 analytics-ideam/ ──► Athena: ideam_rama1_temporal
        └─► Glue IDEAM_RAMA-2 (geográfica) ──► S3 analytics-ideam/rama-2/ ──► Athena: ideam_rama2_geografica

Step Functions (IDEAM-Step-Function) orquesta todo; EventBridge Scheduler (IDEAM-Schedule) lo dispara cada 1 hora.
```

Bucket: `apgd-trabajo2-jr-jj-jv`.

### Qué hicimos

1. **Ingesta y limpieza (`IDEAM-INITIAL-BATCH`)**: lee los CSV de radiación (`raw/main_dataset/`, recursivo) y el catálogo de estaciones (`raw/secondary_dataset/`). Estandariza `Fecha` probando cuatro formatos (`yyyy-MM-dd HH:mm`, `dd/MM/yyyy HH:mm`, `yyyy-MM-dd`, `dd/MM/yyyy`), extrae `Anio` y `Mes`, descarta fechas nulas y valores fuera de 0–1500 Wh/m², hace `LEFT JOIN` por código de estación para traer departamento, municipio, latitud, longitud y altitud, y escribe Parquet particionado por `departamento` en `processed/` (`docs/s3_parquet_partitioned.png`).
2. **Rama 1 – evolución temporal (`IDEAM_RAMA-1`)**: promedio, máximo y conteo de lecturas por año y mes.
3. **Rama 2 – geografía (`IDEAM_RAMA-2`)**: promedio, máximo y conteo de lecturas por departamento y municipio.
4. **Orquestación**: una máquina de estados de Step Functions ejecuta primero el job inicial y luego las dos ramas en paralelo (estado `Parallel`) usando `glue:startJobRun.sync`, de modo que espera a que cada job termine (`docs/step_functions_diagram.png`, `docs/glue_jobs_succeeded.png`).
5. **Programación**: EventBridge Scheduler `IDEAM-Schedule`, `rate(1 hours)`, zona horaria America/Bogota (`docs/eventbridge_scheduler.png`).
6. **Consulta**: tablas externas en Athena (`create_tables.sql`) y consultas de verificación y análisis (`analytics_queries.sql`): total de registros procesados, serie histórica mensual, top 5 meses con mayor radiación, top 10 municipios, consolidado por departamento y promedio ponderado por número de lecturas.

### Resultados

- Los tres jobs de Glue (`IDEAM-INITIAL-BATCH`, `IDEAM_RAMA-1`, `IDEAM_RAMA-2`) corrieron con éxito y la ejecución completa de Step Functions terminó en verde.
- `processed/` quedó particionado por departamento (una carpeta por departamento).
- Consulta geográfica (`docs/athena_rama2_results.png`): los municipios con mayor radiación promedio encabezan con **Repelón (Atlántico), 306,28 Wh/m²**, seguido de Uribia (La Guajira, 251,56), Santa Marta (Magdalena, 236,55) y El Paso (Cesar, 227,30).
- Consulta temporal (`docs/athena_rama1_results.png`): serie mensual desde 1997 con promedio, máximo y número de lecturas por mes.

---

## Parte B – Streaming (Kinesis, Lambda, DynamoDB y S3)

Simulamos eventos de potencia por departamento, los publicamos en Kinesis y los procesamos en tiempo real por ventanas de 1 minuto, descartando repetidos e inválidos, y guardando también todo el crudo en S3.

```
productor.py → Kinesis (calcusol-stream, 1 shard) ─┬─► Lambda calcusol-ventanas → DynamoDB
                                                    └─► Lambda calcusol-crudos   → S3 (raw/*.jsonl)
```

### Qué hicimos

- **Productor** (`productor.py`): 100 eventos/s durante 6 minutos, con ~5 % repetidos (mismo `event_id`) y ~4 % inválidos (sin categoría, valor fuera de rango, valor texto o sin valor).
- **Lambda `calcusol-ventanas`**: valida cada evento; guarda los inválidos con su motivo; detecta repetidos por `event_id`; y con **una transacción de DynamoDB** marca el evento como visto, suma a la ventana (categoría + minuto del `ts_ms`) y actualiza el contador. Así un reintento no cuenta dos veces.
- **Lambda `calcusol-crudos`**: segundo consumidor del mismo stream, guarda cada lote como `.jsonl` en S3.
- **Tablas DynamoDB**: `calcusol_ventanas`, `calcusol_vistos`, `calcusol_descartados`, `calcusol_contadores`.
- **Verificación**: `conteos.py` comprueba que publicados = únicos + repetidos + inválidos = líneas crudas en S3; `consulta_dynamodb.py` responde la pregunta del proyecto; `reiniciar_parteB.py` limpia todo para repetir la corrida.

### Resultados (corrida de 6 minutos)

| Publicados | Únicos | Repetidos | Inválidos |
| ---: | ---: | ---: | ---: |
| 36.000 | 32.835 | 1.745 | 1.420 |

Los conteos cuadran (32.835 + 1.745 + 1.420 = 36.000, igual a lo publicado y a lo que hay en S3).

**Departamento con mayor potencia promedio por evento: Antioquia, 2.030,81 W.** Su ventana de mayor promedio fue `2026-11-09T22:28:00Z` con 2.060,71 W (1.073 eventos).

Detalle completo, instrucciones para reproducir, errores encontrados y capturas: [`parte_b/README.md`](parte_b/README.md).

---

## Cómo reproducir

- **Parte A**: crear el bucket con `raw/main_dataset/` y `raw/secondary_dataset/`; crear los tres jobs de Glue con los scripts de `parte-A-batch/scripts-glue/`; crear la máquina de estados con `step-functions/IDEAM-Step-Function.json`; ejecutar los `CREATE TABLE` de `athena-queries/create_tables.sql` (y `MSCK REPAIR TABLE ideam_processed`); correr `analytics_queries.sql`; opcionalmente crear el schedule de EventBridge.
- **Parte B**: seguir la sección "Cómo reproducirlo" de `parte_b/README.md`.

## Uso de IA

Usamos Claude (Anthropic) como apoyo para la guía de la consola, los scripts y la redacción de los README. Nosotros creamos los recursos, ejecutamos los pipelines, corregimos los errores y verificamos los resultados.
