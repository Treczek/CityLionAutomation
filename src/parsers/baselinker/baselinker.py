from src.gdrive_connection.base import GSheetConnection
from src.data_sources.baselinker.utils import (
    filter_orders_without_products,
    flatten_products,
)
from src.utils.steps import apply_steps
from src.data_sources.baselinker.utils import get_orders_from_baselinker_dict
from src.data_sources import BaselinkerAPI
from src.parsers.baselinker.utils import convert_pandas_datetime_to_timestamps
from src.utils.gsheet_types import datetime_to_excel_date
from src.utils.utils import get_country_to_iso_code_map
import structlog
import xmltodict as xml
from pathlib import Path
import pandas as pd


class BaselinkerParser:

    def __init__(self, spreadsheet_name: str, api_key: str):
        self.logger = structlog.getLogger(__name__)

        self.api_key = api_key
        self.api = BaselinkerAPI()

        self.cached_orders = (
            pd.read_pickle("order_cached.pkl")
            if Path("order_cached.pkl").exists()
            else pd.DataFrame([])
        )

        self.spreadsheet = GSheetConnection(spreadsheet_name)
        self.warnings = []

    # TODO Parser jako interfejs
    def _warn_with_caching(self, message):
        self.warnings.append(message)
        self.logger.warning(message)

    def parse(self):

        steps = [
            (self.add_archive_xml_orders, {}),
            (self.add_newest_orders, {}),
            (self.process_the_data, {}),
            (self.add_cached_orders, {}),
            (self.cache_the_data, {}),
            (self.merge_mappings, {}),
            (self.refresh_mappings_with_new_products, {}),
            # (self.format_before_pushing, {}),
            (self.send_data, {})
        ]

        apply_steps(steps, logger=self.logger)

    def add_archive_xml_orders(self, dummy=None):
        # TODO ogarnąć dynamiczne ścieżki
        xml_path = Path(__file__).parent.joinpath('xml_orders.xml')
        if not xml_path.exists():
            self._warn_with_caching('Archive was not available. Some of the orders might be missing')
            return pd.DataFrame([])
        else:
            self.logger.info('Processing xml archive...')
            with open(xml_path, 'rb') as stream:
                baselinker_orders = xml.parse(stream)['orders']['order']
                baselinker_orders = filter_orders_without_products(baselinker_orders)
                baselinker_orders = flatten_products(baselinker_orders)

            baselinker_orders = get_orders_from_baselinker_dict(baselinker_orders)
            baselinker_orders = self._xml_orders_data_preparation(baselinker_orders)

            self.logger.info(f"Successfully parsed {len(baselinker_orders)} orders from xml archive.")
            return baselinker_orders

    def _xml_orders_data_preparation(self, archived: pd.DataFrame) -> pd.DataFrame:

        order_source_map = {
            'Osobiście/tel.': 'manual',
            'Allegro': 'allegro',
            'Sklep int.': 'shop',
            'eBay': 'ebay',
            'Amazon': 'amazon',
        }
        archived['order_source'] = archived['order_source'].map(order_source_map)
        archived['date_confirmed'] = convert_pandas_datetime_to_timestamps(archived['date_confirmed'])

        return archived

    def add_cached_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([orders, self.cached_orders]).drop_duplicates()

    def add_newest_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        recent_orders = get_orders_from_baselinker_dict(
            self.api.get_orders(self.api_key)
        )
        return pd.concat([orders, recent_orders])

    def cache_the_data(self, orders: pd.DataFrame) -> pd.DataFrame:
        # TODO prosta bazka lub cachowanie w jakieś konkretne miejsce
        orders.to_pickle("order_cached.pkl")
        return orders

    def process_the_data(self, orders: pd.DataFrame) -> pd.DataFrame:
        orders = orders.drop_duplicates()

        orders['date_confirmed'] = pd.to_datetime(orders['date_confirmed'], unit='s').copy()
        orders["year"] = orders["date_confirmed"].dt.year
        orders["month"] = orders["date_confirmed"].dt.month
        orders['year-month'] = orders['date_confirmed'].dt.strftime("%Y-%m")
        orders['date_confirmed'] = orders['date_confirmed'].apply(datetime_to_excel_date)
        orders['delivery_country'] = orders['delivery_country'].fillna("Polska")
        orders['country_iso_code'] = orders['delivery_country'].map(get_country_to_iso_code_map())
        orders['export'] = orders['country_iso_code'] != 'PL'

        return orders

    def merge_mappings(self, orders: pd.DataFrame) -> pd.DataFrame:
        product_map = self.spreadsheet["BaselinkerProductMap"].get_data()
        return orders.merge(product_map.drop(columns=['attributes', 'index']), how='left', on='name')

    def refresh_mappings_with_new_products(self, orders: pd.DataFrame) -> pd.DataFrame:
        current_map = self.spreadsheet["BaselinkerProductMap"].get_data()
        new_orders = (
            orders[['name', 'attributes', 'master_product', 'battery_type', 'battery_size']]
            .drop_duplicates()
            .reset_index()
        )

        new_map = (
            pd
            .concat(
                [
                    current_map, new_orders
                ])
            .sort_values('name', ascending=False)
            .drop_duplicates('name')
            .sort_values("master_product")
        )

        ws = self.spreadsheet["BaselinkerProductMap"]
        ws.worksheet.clear()
        ws.update_data(new_map.fillna(""))

        return orders

    def format_before_pushing(self, orders: pd.DataFrame) -> pd.DataFrame:
        return orders.drop_duplicates(subset=['order_id'])

    def send_data(self, orders: pd.DataFrame) -> None:
        ws = self.spreadsheet["BaselinkerData"]
        ws.worksheet.clear()
        ws.update_data(orders)

    def format_after_pushing(self, dummy=None):

        # Date with proper format
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "startColumnIndex": 2,
                        "endColumnIndex": 3,
                        "sheetId": self.spreadsheet["BaselinkerData"].worksheet._properties[
                            "sheetId"
                        ],
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        ]

        self.spreadsheet.spreadsheet.batch_update({"requests": requests})