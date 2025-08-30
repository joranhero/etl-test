import duckdb
from datetime import datetime, timezone
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_orders(orders_data, users_df, products_df, since=None):
    logger.info("Iniciando transformación de ordenes.")
    orders_df = pd.DataFrame(orders_data)
    logger.info(f"Datos de entrada: {len(orders_df)} ordenes")

    # Convertir created_at a tz-aware datetime UTC y llenar nulos
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'], utc=True, errors='coerce')
    orders_df['created_at'] = orders_df['created_at'].fillna(datetime.now(timezone.utc))

    if since is not None:
        orders_before = len(orders_df)
        orders_df = orders_df[orders_df['created_at'] >= since]
        logger.info(f"Filtradas ordenes desde {since}: {len(orders_df)} (antes {orders_before})")

    # Explode items
    orders_df = orders_df.explode('items')

    orders_df['sku'] = orders_df['items'].apply(lambda x: x.get('sku') if isinstance(x, dict) else None)
    orders_df['qty'] = orders_df['items'].apply(lambda x: x.get('qty') if isinstance(x, dict) else 0)
    orders_df['price'] = orders_df['items'].apply(lambda x: x.get('price') if isinstance(x, dict) else 0.0)
    orders_df.drop(columns=['items'], inplace=True)

    # Deduplicar para evitar filas repetidas
    orders_df = orders_df.drop_duplicates(subset=['order_id', 'sku'])
    logger.info(f"Después de explotar y deduplicar: {len(orders_df)} filas")

    # Crear duckdb en memoria para hacer joins
    con = duckdb.connect(database=':memory:')
    con.register('orders', orders_df)
    con.register('users', users_df)
    con.register('products', products_df)

    fact_order = con.execute("""
        SELECT
            o.order_id,
            o.user_id,
            o.amount,
            o.currency,
            o.created_at,
            o.sku,
            o.qty,
            o.price,
            u.email,
            u.country,
            p.name AS product_name,
            p.category
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.user_id
        LEFT JOIN products p ON o.sku = p.sku
    """).df()

    dim_user = users_df.copy()
    dim_product = products_df.copy()

    logger.info("Transformación completada con creación de dimensiones y tabla de hechos.")

    return dim_user, dim_product, fact_order
