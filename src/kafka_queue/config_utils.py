from configparser import ConfigParser


def get_config():
    """Parses a config_files.ini file and returns the content as dictionary."""
    config_parser = ConfigParser()
    config_parser.read_file(open("config_files/kafka_config.ini", "r"))
    config = dict(config_parser["default"])
    return config
