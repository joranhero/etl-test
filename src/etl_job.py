import argparse
import os
import json
import pandas as pd
from datetime import datetime
import logging

from api_client import fetch_orders
from db import load_users, load_products
from transforms import transform_orders

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

def main(since):
    # Paths
    base_dir = os.path.dirname(os.path.dirname(__file__))
    orders_path = os.path.join(base_dir, 'sample_data/api_orders.json')
    users_path = os.path.join(base_dir, 'sample_data/users.csv')
    products_path = os.path.join(base_dir, 'sample_data/products.csv')
    output_raw = os.path.join(base_dir, 'output/raw')
    output_curated = os.path.join(base_dir, 'output/curated')
    os.makedirs(output_raw, exist_ok=True)
    os.makedirs(output_curated, exist_ok=True)

    # Normalizar since a datetime tz-aware o None
    if since:
        since_dt = pd.to_datetime(since)
        if since_dt.tzinfo is None:
            since_dt = since_dt.tz_localize('UTC')
    else:
        since_dt = None

    logger.info(f"Iniciando pipeline ETL con parámetro --since={since_dt}")

    # Cargar datos de entrada
    try:
        orders_data = fetch_orders(orders_path)
        logger.info(f"Se cargaron {len(orders_data)} órdenes.")
    except Exception as e:
        logger.error(f"Error leyendo órdenes: {e}")
        return

    try:
        users_df = load_users(users_path)
        logger.info(f"Se cargaron {len(users_df)} usuarios.")
        products_df = load_products(products_path)
        logger.info(f"Se cargaron {len(products_df)} productos.")
    except Exception as e:
        logger.error(f"Error cargando usuarios/productos: {e}")
        return

    # Guardar copia raw
    raw_file = os.path.join(output_raw, 'orders.json')
    try:
        with open(raw_file, 'w') as f:
            json.dump(orders_data, f)
        logger.info(f"Archivo raw guardado en {raw_file}")
    except Exception as e:
        logger.error(f"Error guardando archivo raw: {e}")

    # Transformar datos
    try:
        dim_user, dim_product, fact_order = transform_orders(orders_data, users_df, products_df, since_dt)
        logger.info("Transformación completada.")
    except Exception as e:
        logger.error(f"Error en la transformación: {e}")
        return
    # metricas basicas
    metrics = {
        "input_orders": len(orders_data),  # Órdenes crudas de entrada
        "valid_orders": len(fact_order['order_id'].unique()),  # Órdenes únicas después de procesamiento (ajusta si necesitas filtrado previo)
        "deduped_orders": len(fact_order),  # Filas en fact_order después de deduplicado
        "items": fact_order.shape[0],  # Número de ítems expandidos (filas en fact_order)
        "dim_user": len(dim_user),
        "dim_product": len(dim_product),
    }
    logger.info("Metrics: %s", metrics)

    # guardar metricas en archivo
    metrics_dir = os.path.join(base_dir, "output/metrics")  # Ajusta a tu base_dir si es necesario
    os.makedirs(metrics_dir, exist_ok=True)
    with open(os.path.join(metrics_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2, default=str)    

    # Guardar datos curados particionados por fecha
    partition_date = datetime.now().strftime('%Y-%m-%d')

    for df, name, partition in [
        (fact_order, 'fact_order', True),
        (dim_user, 'dim_user', False),
        (dim_product, 'dim_product', False)
    ]:
        path = os.path.join(output_curated, f"{name}/{partition_date}")
        os.makedirs(path, exist_ok=True)
        try:
            if partition and 'created_at' in df.columns:
                df.to_parquet(os.path.join(path, 'data.parquet'), partition_cols=['created_at'])
            else:
                df.to_parquet(os.path.join(path, 'data.parquet'))
            logger.info(f"Datos guardados correctamente en {path}")
        except Exception as e:
            logger.error(f"Error guardando {name}: {e}")

    logger.info("ETL completado correctamente.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="Fecha mínima para procesamiento incremental (YYYY-MM-DD)")
    args = parser.parse_args()
    main(args.since)
