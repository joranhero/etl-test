import duckdb
from datetime import datetime, timezone
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_orders(orders_data, users_df, products_df, since=None):
    logger.info("Iniciando transformación de órdenes.")
    orders_df = pd.DataFrame(orders_data)
    logger.info(f"Datos de entrada: {len(orders_df)} órdenes")

    # Convertir created_at a tz-aware datetime UTC y llenar nulos
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'], utc=True, errors='coerce')
    orders_df['created_at'] = orders_df['created_at'].fillna(datetime.now(timezone.utc))

    if since is not None:
        orders_before = len(orders_df)
        orders_df = orders_df[orders_df['created_at'] >= since]
        logger.info(f"Filtradas órdenes desde {since}: {len(orders_df)} (antes {orders_before})")

    # Explode items
    orders_df = orders_df.explode('items')

    # Extraer campos sku, qty y price de cada item
    def extract_field(item, field, default):
        if not isinstance(item, dict):
            return default
        val = item.get(field)
        return val if val is not None else default

    orders_df['sku'] = orders_df['items'].apply(lambda x: extract_field(x, 'sku', None))
    orders_df['qty'] = orders_df['items'].apply(lambda x: extract_field(x, 'qty', 0))
    orders_df['price'] = orders_df['items'].apply(lambda x: extract_field(x, 'price', None))

    # Imputar precios faltantes desde products_df
    price_map = products_df.set_index('sku')['price'].to_dict()

    def impute_price(row):
        if pd.isna(row['price']):
            return price_map.get(row['sku'], 0.0)
        return row['price']

    orders_df['price'] = orders_df.apply(impute_price, axis=1)

    # Calcular price 0 a float 0.0 para evitar errores
    orders_df['price'] = orders_df['price'].fillna(0.0).astype(float)
    orders_df['qty'] = orders_df['qty'].fillna(0).astype(int)

    orders_df.drop(columns=['items'], inplace=True)

    # Deduplicar para evitar filas repetidas
    orders_df = orders_df.drop_duplicates(subset=['order_id', 'sku'])

    # Calcular amount por orden si viene nulo
    # Primero, detectar órdenes con amount nulo
    orders_amount_null = orders_df['amount'] = orders_df.get('amount', None)
    if orders_amount_null is None or orders_amount_null.isnull().any():
        # Como amount no está en orders_df directamente, se realiza con orders_data
        # Crear DataFrame con amount por order original, luego imputar
        amounts = {}
        for order in orders_data:
            oid = order.get('order_id')
            amt = order.get('amount')
            if amt is None:
                # Suma qty * price de items imputados en exploded DataFrame
                amt = orders_df.loc[orders_df['order_id'] == oid, ['qty', 'price']].eval('qty * price').sum()
            amounts[oid] = amt

        # Agregar columna amount a orders_df
        orders_df['amount'] = orders_df['order_id'].map(amounts)

    # DuckDB join para enriquecer datos
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
