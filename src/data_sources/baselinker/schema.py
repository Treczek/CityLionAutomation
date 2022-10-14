from datetime import datetime
from typing import List, Union

import pandas as pd
from pydantic import BaseModel, validator, Field
from dateutil.parser import parser


class ProductSchema(BaseModel):
    order_product_id: int = None
    product_id: Union[int, str] = Field(default=999999)
    variant_id: str = None
    name: str
    attributes: str = None
    price_brutto: float
    quantity: int

    @classmethod
    @validator('product_id')
    def change_to_string(cls, value):
        return str(value)


class OrderSchema(BaseModel):
    order_id: int
    order_source: str
    date_confirmed: Union[int, str]
    currency: str
    payment_done: float
    delivery_country: str = None
    products: Union[List[ProductSchema], List[List[ProductSchema]]]

    @classmethod
    @validator('date_confirmed')
    def change_to_string(cls, value):
        if isinstance(value, str):
            return parser.parse(value, dayfirst=True)
        return datetime.fromtimestamp(value).strftime('%Y-%m-%d')

    @validator('products')
    def map_to_dict(cls, value):
        if isinstance(value[0], list):
            value = value[0]
        return [dict(single_value) for single_value in value]

    @validator('payment_done')
    def change_to_int(cls, value):
        return int(value)

    def as_dataframe(self):
        df = pd.DataFrame(dict(self))
        return pd.concat([df, df.products.apply(pd.Series)], axis=1).drop("products", axis=1)


