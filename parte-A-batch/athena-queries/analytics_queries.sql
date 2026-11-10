-- verificamos el total de registros procesados en la tabla ideam_processed
SELECT
    COUNT(*) AS total_registros_procesados
FROM ideam_processed;



-- Verificacion de la evolución temporal de la radiación solar
-- Histórico ordenado cronológicamente
SELECT
    Anio,
    Mes,
    ROUND(Promedio_Wh_m2, 2) AS Promedio_Radiacion_Wh_m2,
    ROUND(Maximo_Wh_m2, 2) AS Maxima_Radiacion_Wh_m2,
    Total_Lecturas
FROM ideam_rama1_temporal
ORDER BY Anio ASC, Mes ASC;


-- Top 5 meses históricos con mayor radiación promedio
SELECT
    Anio,
    Mes,
    ROUND(Promedio_Wh_m2, 2) AS Promedio_Radiacion_Wh_m2,
    ROUND(Maximo_Wh_m2, 2) AS Maxima_Radiacion_Wh_m2
FROM ideam_rama1_temporal
ORDER BY Promedio_Wh_m2 DESC
LIMIT 5;


-- Verificacion del potencial solar geográfico por municipio
-- Top 10 municipios con mayor radiación promedio
SELECT
    departamento,
    municipio,
    ROUND(Promedio_Wh_m2, 2) AS Promedio_Radiacion_Wh_m2,
    ROUND(Maximo_Wh_m2, 2) AS Maxima_Radiacion_Wh_m2,
    Total_Lecturas
FROM ideam_rama2_geografica
ORDER BY Promedio_Wh_m2 DESC
LIMIT 10;


-- Consolidado por departamento
SELECT
    departamento,
    ROUND(AVG(Promedio_Wh_m2), 2) AS Promedio_Departamento_Wh_m2,
    ROUND(MAX(Maximo_Wh_m2), 2) AS Maximo_Absoluto_Wh_m2,
    SUM(Total_Lecturas) AS Lecturas_Totales
FROM ideam_rama2_geografica
GROUP BY departamento
ORDER BY Promedio_Departamento_Wh_m2 DESC;


-- PROMEDIO PONDERADO POR DEPARTAMENTO
SELECT
    departamento,
    ROUND(
        SUM(Promedio_Wh_m2 * Total_Lecturas) /
        NULLIF(SUM(Total_Lecturas), 0),
        2
    ) AS Promedio_Ponderado_Wh_m2,
    MAX(Maximo_Wh_m2) AS Maximo_Absoluto_Wh_m2,
    SUM(Total_Lecturas) AS Lecturas_Totales
FROM ideam_rama2_geografica
GROUP BY departamento
ORDER BY Promedio_Ponderado_Wh_m2 DESC;