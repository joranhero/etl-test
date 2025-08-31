# Notas de Diseño - Pipeline ETL

## Objetivo

Procesar datos de órdenes, usuarios y productos eficientemente con Python y DuckDB, preparando datos para análisis posteriores.

## Diseño

### Extracción

- Datos locales en JSON y CSV simulando fuentes reales.

### Transformación

- Fechas normalizadas a timezone UTC (tz-aware).  
- Expansión de campos con listas en filas tabulares con sku, qty y precio.  
- Unión SQL eficiente con DuckDB para enriquecer datos.

### Carga

- Archivo Parquet particionado por fecha en la tabla de hechos para óptima consulta.  
- Ordenación lógica en carpetas por tabla y fecha.

### Incrementalidad

- Parámetro `--since` para procesar sólo datos nuevos o modificados desde una fecha controlada.

## Deduplicación en el proceso de transformación
Al expandir las órdenes y sus ítems, se eliminan filas duplicadas usando pandas y DuckDB, basádos en claves naturales (`order_id` y `sku`). Esto previene que registros repetidos se propaguen a la tabla de hechos final.

## Observabilidad

- Logging para auditoría y debug.  
- Manejo básico de errores.  
- metricas en la carpeta output con el siguiente esquema para trazabilidad de ingesta
{
  "input_orders": 3,
  "valid_orders": 2,
  "deduped_orders": 2,
  "items": 2,
  "dim_user": 2,
  "dim_product": 3
}.
la obserbabilidad en entornos de produccion dependerá de la arquitectura y la infraestructura implementada pero algunas opciones podrían ser Prometheus/Grafana o DataDog,CloudWatch

## Contenerización con Docker
- Se adiciona procedimiento de Contenerización para mejores practicas de reproducibilidad y demostración de dominio de conocimiento para el cargo junto con el diseño de la arquitectura del test.
- Imagen Docker basada en Python 3.12 con dependencias instaladas.  
- Contenedor configurado para ejecutar pipeline en cualquier entorno reproducible.  
- Montaje opcional de volúmenes para entrada/salida de datos.  
- Facilita despliegue en nube y orquestadores.

## Decisiones clave:
- Elección: pandas + DuckDB por ligereza local vs PySpark. DuckDB es rápido para joins, pero PySpark escala mejor en producción.
- Particionado: Por created_at en Parquet para queries eficientes en Redshift.
- Claves: PK en order_id para dedupe (drop_duplicates en pandas).
- Idempotencia: Dedupe por order_id + upsert implícito (sobrescribe Parquet si existe).
- Incremental: Filtrar por --since en transforms.
- Monitorización: logging, métricas.
- Carga a Redshift: por medio de COPY FROM S3 para curated/ Parquet. SELECT COUNT(*) FROM fact_order GROUP BY country.
- Trade-offs: Simplicidad local vs escalabilidad; no se implementó MSSQL para efectos practicos en el desarrollo de la prueba tecnica y para evitar dependencias.
- conterizacion para repruducibilidad en entornos de desarrollo, pruebas o produccion.
- DashBoard PowerBI: DashBoard con la data de curated_SCV no se realiza transformacion en los datos para efectos de visualizacion de los datos, a modo que el analista de datos pueda gestionar los insights. se crea tabla de medidas explicitas, total_amount, total_qty, modelo de datos relacional, graficos con filtros dinamicos.
