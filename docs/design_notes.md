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

- Imagen Docker basada en Python 3.12 con dependencias instaladas.  
- Contenedor configurado para ejecutar pipeline en cualquier entorno reproducible.  
- Montaje opcional de volúmenes para entrada/salida de datos.  
- Facilita despliegue en nube y orquestadores.

## Futuras Mejoras

- Automatizar alertas vía integración con Slack o email.  
- Añadir métricas con Prometheus o similar.  
- Orquestar con Airflow u otro scheduler.  
- Soporte para lectura directa desde APIs o bases de datos.

## Decisiones clave:
- Elección: pandas + DuckDB por ligereza local vs PySpark (más pesado para samples pequeños). Trade-off: DuckDB es rápido para joins, pero PySpark escala mejor en producción.
- Particionado: Por created_at en Parquet para queries eficientes en Redshift.
- Claves: PK en order_id para dedupe (drop_duplicates en pandas).
- Idempotencia: Dedupe por order_id + upsert implícito (sobrescribe Parquet si existe).
- Incremental: Filtrar por --since en transforms.
- Monitorización: logging, métricas.
- Carga a Redshift: Usar COPY FROM S3 para curated/ Parquet; queries ejemplo: SELECT COUNT(*) FROM fact_order GROUP BY country.
- Trade-offs: Simplicidad local vs escalabilidad; no se implementó MSSQL para efectos practicos en el desarrollo de la prueba tecnica y para evitar dependencias.
- conterizacion para repruducibilidad en entornos de desarrollo, pruebas o produccion.
