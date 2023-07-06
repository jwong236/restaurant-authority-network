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
        elif config_type == "INDEXER":
            self.CORPUS_DIRECTORY = config_parser["INDEXER"]["CORPUS_DIRECTORY"]
            self.PARTIAL_INDEX_PATH = config_parser["INDEXER"]["PARTIAL_INDEX_PATH"]
            self.INVERTED_INDEX_PATH = config_parser["INDEXER"]["INVERTED_INDEX_PATH"]
            self.DOCID_MAP_PATH = config_parser["INDEXER"]["DOCID_MAP_PATH"]
            self.MASTER_INDEX_PATH = config_parser["INDEXER"]["MASTER_INDEX_PATH"]
            self.BATCH_SIZE = int(config_parser["INDEXER"]["BATCH_SIZE"])
        elif config_type == "SEARCHER": 
            self.PARTIAL_INDEX_PATH = config_parser["SEARCHER"]["PARTIAL_INDEX_PATH"]
            self.INVERTED_INDEX_PATH = config_parser["SEARCHER"]["INVERTED_INDEX_PATH"]
            self.DOCID_MAP_PATH = config_parser["SEARCHER"]["DOCID_MAP_PATH"]
        elif config_type == "RANKER":
            pass
        else:
            raise ValueError(f"Invalid config_type: '{config_type}'")