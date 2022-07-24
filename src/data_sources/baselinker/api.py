import requests
import json
from src.exceptions import APIException


from typing import Optional, List, Sequence


class BaselinkerAPI:
    base_url = 'https://api.baselinker.com/connector.php'

    def get_orders(self, api_key: str, timestamp: Optional[int] = None) -> Sequence[dict]:

        all_orders = []

        header = {'X-BLToken': api_key}
        params = {'method': 'getOrders'}

        if timestamp:
            params.update(
                {'parameters': json.dumps({
                    "date_confirmed_from": timestamp,
                    "get_unconfirmed_orders": False})
                }
            )

        response = requests.post(self.base_url, data=params, headers=header)
        json_response = json.loads(response.content)

        if json_response.get('status') != 'SUCCESS':
            raise APIException(f'Orders could not be collected. Response: {json_response}')

        json_orders: List[dict] = json.loads(response.content).get("orders", [])
        all_orders.extend(json_orders)

        if len(json_orders) == 100:
            last_order_timestamp = json_orders[-1]['date_confirmed']
            all_orders.extend(self.get_orders(api_key, timestamp=last_order_timestamp + 1))

        return all_orders
