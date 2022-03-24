import requests
from dateutil.parser import parse
import pandas as pd
from datetime import timedelta
from pathlib import Path
import structlog

from more_itertools import chunked


class NBPApi:

    def __init__(self):
        self.logger = structlog.getLogger(__name__)
        self.cached_rates = pd.read_pickle('rates_cached.pkl') if Path('rates_cached.pkl').exists() else None

    # TODO zrobić solidny cache z uwzglednieniem walut
    def get_rates(self, date_range, currency='EUR') -> pd.DataFrame:

        start_date, end_date = date_range[0], date_range[-1]
        if self.cached_rates is not None:
            if (self.cached_rates['date'].min() <= start_date) and (self.cached_rates['date'].max() >= end_date):
                self.logger.info("All rates already cached. Proceeding")
                return self.cached_rates

        results = []
        for batch in chunked(date_range, 350):
            start_date = batch[0].strftime('%Y-%m-%d')
            end_date = batch[-1].strftime('%Y-%m-%d')

            url = f"http://api.nbp.pl/api/exchangerates/rates/A/{currency}/{start_date}/{end_date}"
            results.append(pd.DataFrame(requests.get(url).json()['rates']))

        data = (
            pd
            .concat(results)
            .rename(columns={'no': 'table',
                             'effectiveDate': 'date',
                             'mid': 'rate'})
            .astype({'date': 'datetime64[ns]'})
            .drop('table', axis=1)
        )

        data.to_pickle('rates_cached.pkl')

        return data

    @staticmethod
    def _parse_date(date, offset=0):
        date = parse(date)
        if offset:
            date = date + timedelta(days=offset)
        return date.strftime('%Y-%m-%d')
