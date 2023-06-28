import configparser
class Config:
    def __init__(self, config_path, config_type):
        config_parser = configparser.ConfigParser()
        config_parser.read(config_path)
        if config_type == "CRAWLER":
            self.user_agent = config_parser["CRAWLER"]["USERAGENT"].strip()
            self.threads_count = int(config_parser["CRAWLER"]["THREADCOUNT"])
            self.save_file = config_parser["CRAWLER"]["SAVE"]
            self.seed_urls = config_parser["CRAWLER"]["SEEDURL"].split(",")
            self.time_delay = float(config_parser["CRAWLER"]["POLITENESS"])
        elif config_type == "INDEXER":
            self.batch_size = int(config_parser["INDEXER"]["BATCH_SIZE"])
            self.max_file_size = int(config_parser["INDEXER"]["MAX_FILE_SIZE"])
        elif config_type == "SEARCHER": 
            pass
        elif config_type == "RANKER":
            pass
        else:
            raise ValueError(f"Invalid config_type: '{config_type}'")