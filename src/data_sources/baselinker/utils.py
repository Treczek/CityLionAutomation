import pandas as pd

from typing import Sequence, Dict, List

import pydantic
import structlog

from src.data_sources.baselinker.schema import OrderSchema

logger = structlog.getLogger()
def flatten_products(baselinker_orders: List[Dict]) -> Dict:
    for order in baselinker_orders:
        order['products'] = [product for product in order['products'].values()]
    return baselinker_orders


def filter_orders_without_products(baselinker_orders: Dict) -> List[Dict]:
    return [order for order in baselinker_orders if order['products'] is not None]


def get_orders_from_baselinker_dict(baselinker_orders: Sequence[Dict]) -> pd.DataFrame:
    orders_df = []
    for order in baselinker_orders:
        try:
            orders_df.append(OrderSchema(**order).as_dataframe())
        except pydantic.error_wrappers.ValidationError as exc:
            logger.error("Order with validation problems", order_url=order["order_page"], exc=exc.json())
    return pd.concat(orders_df)
