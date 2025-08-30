import pandas as pd

def load_users(file_path):
    return pd.read_csv(file_path)

def load_products(file_path):
    return pd.read_csv(file_path)
