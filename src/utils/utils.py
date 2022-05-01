from pathlib import Path


def get_project_structure():
    root = Path.cwd()
    while root.name != "CityLionParser":
        root = root.parent

    backups = root / "backups"
    fx_rates_cache = backups / "fx_rates_cache.pickle"

    return dict(fx_rates_cache=fx_rates_cache)


if __name__ == "__main__":
    print(get_project_structure())
