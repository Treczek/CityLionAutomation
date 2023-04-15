from dataclasses import dataclass, asdict
import re
import requests
from bs4 import BeautifulSoup


@dataclass
class ScrapedProduct:
    product: str
    url: str
    price: float
    availability: str


class ScrapeException(Exception):
    pass


def extract_price(price_text):
    matches = re.findall(r'(?<=€)\d+\.\d+', price_text)
    if len(matches) >= 2:
        return float(matches[1])
    else:
        raise ScrapeException()


def scrape_eu_nkon(product, url) -> ScrapedProduct:
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        price = scrape_price(soup)
        availability = scrape_availability(soup)
    else:
        raise ScrapeException()
    return ScrapedProduct(product, url, price, availability)


def scrape_price(soup):
    ul_element = soup.find('ul', {'class': "tier-prices product-pricing"})
    if ul_element:
        last_li_element = ul_element.find_all('li')[-1]
        return extract_price(last_li_element.text.strip())
    else:
        raise ScrapeException()


def scrape_availability(soup):
    p_element = soup.find('p', {'class': "availability in-stock"})
    if p_element:
        span_element = p_element.find_all('span')[-1]
        return span_element.text.strip()
