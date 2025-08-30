import pytest
import pandas as pd
from transforms import transform_orders
from unittest.mock import patch
from src import api_client

@pytest.fixture
def sample_data():
    orders = [{"order_id": "o_1001", "user_id": "u_1", "amount": 125.50, "currency": "USD", "created_at": "2025-08-20T15:23:10Z", "items": [{"sku": "p_1", "qty": 2, "price": 60.0}]}]
    users = pd.DataFrame({"user_id": ["u_1"], "email": ["user1@example.com"], "country": ["US"]})
    products = pd.DataFrame({"sku": ["p_1"], "name": ["Product1"], "category": ["Electronics"], "price": [60.0]})
    return orders, users, products

def test_transform_orders(sample_data):
    orders, users, products = sample_data
    dim_user, dim_product, fact_order = transform_orders(orders, users, products)
    assert len(fact_order) == 1
    assert fact_order['order_id'][0] == "o_1001"

@patch('src.api_client.fetch_orders')
def test_fetch_with_mock(mock_fetch):
    mock_fetch.return_value = [{"order_id": "o_1001"}]
    result = api_client.fetch_orders("dummy_path")
    assert len(result) == 1