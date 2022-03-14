import requests
from dateutil.parser import parse
import pandas as pd
from datetime import timedelta


class NBPApi:
    # TODO Ogarnąć jak będziemy wyciągać więcej niż rok
    def get_rates(self, start_date, end_date, currency='EUR') -> pd.DataFrame:
        start_date = self._parse_date(start_date, offset=3)
        end_date = self._parse_date(end_date)

        url = f"http://api.nbp.pl/api/exchangerates/rates/A/{currency}/{start_date}/{end_date}"
        data = requests.get(url).json()

        data = (pd
                .DataFrame(data['rates'])
                .rename(columns={'no': 'table',
                                 'effectiveDate': 'date',
                                 'mid': 'PLNtoEUR'})
                .astype({'date': 'datetime64[ns]'})
                .assign(EURtoPLN=lambda df: 1 / df['PLNtoEUR'])
                .drop('table', axis=1))

        return data

    @staticmethod
    def _parse_date(date, offset=0):
        date = parse(date)
        if offset:
            date = date + timedelta(days=offset)
        return date.strftime('%Y-%m-%d')