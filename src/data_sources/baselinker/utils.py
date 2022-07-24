import pandas as pd

from typing import Sequence, Dict, List

from src.data_sources.baselinker.schema import OrderSchema


def flatten_products(baselinker_orders: List[Dict]) -> Dict:
    for order in baselinker_orders:
        order['products'] = [product for product in order['products'].values()]
    return baselinker_orders


def filter_orders_without_products(baselinker_orders: Dict) -> List[Dict]:
    return [order for order in baselinker_orders if order['products'] is not None]


def get_orders_from_baselinker_dict(baselinker_orders: Sequence[Dict]) -> pd.DataFrame:
    orders_df = pd.concat(
        [
            OrderSchema(**order).as_dataframe()
            for order
            in baselinker_orders
        ]
    )
    return orders_df
