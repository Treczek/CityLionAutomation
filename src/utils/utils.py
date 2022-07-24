from pathlib import Path
import requests
import bs4 as bs


def get_project_structure():
    root = Path.cwd()
    while root.name != "CityLionParser":
        root = root.parent

    backups = root / "backups"
    fx_rates_cache = backups / "fx_rates_cache.pickle"

    return dict(fx_rates_cache=fx_rates_cache)


def get_country_to_iso_code_map():
    r = requests.get('http://pl.wikipedia.org/wiki/ISO_3166-1')
    tags = bs.BeautifulSoup(r.content)
    country_map = {}
    for row in tags.find_all('table')[0].find_all('tr')[1:]:
        polish_name = row.find_all('a')[0].get_text()
        iso_code = row.find_all('a')[1].get_text()
        country_map[polish_name] = iso_code
    return country_map
