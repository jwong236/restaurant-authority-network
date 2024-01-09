import configparser
class Config:
    def __init__(self, config_path, config_type):
        config_parser = configparser.ConfigParser()
        config_parser.read(config_path)
        if config_type == "CRAWLER":
            self.USER_AGENT = config_parser["CRAWLER"]["USER_AGENT"].strip()
            self.SEED_URLS = config_parser["CRAWLER"]["SEED_URLS"].split(",")
            self.POLITENESS = float(config_parser["CRAWLER"]["POLITENESS"])
            self.SAVE_PATH = config_parser["CRAWLER"]["SAVE_PATH"]
            self.THREAD_COUNT = int(config_parser["CRAWLER"]["THREAD_COUNT"])
            self.CORPUS_DIRECTORY = config_parser["CRAWLER"]["CORPUS_DIRECTORY"]
        else:
            raise ValueError(f"Invalid config_type: '{config_type}'")