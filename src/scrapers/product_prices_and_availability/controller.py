from dataclasses import asdict

import pandas as pd
import structlog

from src.scrapers.product_prices_and_availability.eu_nkon import scrape_eu_nkon, ScrapeException
from src.gdrive_connection.base import GSheetConnection, GWorksheet
from src.utils.steps import apply_steps


class PriceAndAvailabilityScraper:
    def __init__(self, spreadsheet_name: str):
        self.logger = structlog.getLogger(__name__)
        self.spreadsheet = GSheetConnection(spreadsheet_name)

    def scrape(self):
        steps = [
            (self.load_data, {}),
            (self.scrape_products, {}),
            (self.convert_to_dataframe, {}),
            (self.send_data, {})
        ]

        apply_steps(steps, logger=self.logger)

    def load_data(self, dummy=None):

        df = (
            pd.DataFrame(
                self.spreadsheet["Kalkulator kosztów"]
                .worksheet
                .get_all_values()
            )
            .loc[range(2, 14), [0, 5]]
            .reset_index(drop=True)
        )
        df.columns = ["product", "url"]
        return df

    def scrape_products(self, data):
        scraped_products = []
        for ix, row in data.iterrows():
            product, url = row
            if url:
                try:
                    scraped_products.append(scrape_eu_nkon(product, url))
                    self.logger.info(f"Success - {product} - {url}")
                except ScrapeException:
                    self.logger.warning(f"Failed - {product} - {url}")
        return scraped_products

    def convert_to_dataframe(self, data):
        return pd.DataFrame([asdict(product) for product in data])

    def send_data(self, data):
        ws = GWorksheet(self.spreadsheet.get_worksheet("ScrapedData", True))
        ws.worksheet.clear()
        ws.update_data(data)
        self.logger.info("All done!")

