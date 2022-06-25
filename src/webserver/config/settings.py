from webserver.config.schema import AppConfig


environments_dict = dict(development=AppConfig(verbosity=2, debug=True))


def get_settings(environment):
    try:
        return environments_dict[environment]
    except KeyError:
        raise RuntimeError(f"{environment} environment was not found.")
