from utils.config import Config
from crawler import Crawler
#from indexer import Indexer
#from searcher import Searcher
#from ranker import Ranker

class SearchEngine:
    def crawl(self, args):
        configs = Config(args.config_path, "CRAWLER")
        crawler = Crawler(configs)
        crawler.start()
    def build(self, args):
        configs = Config(args.config_path, "build")
        #inverted_indexer = Indexer(configs)
        #inverted_indexer.build_index()
    def start(self, args):
        configs = Config(args.config_path, "start")
        #searcher = Searcher(configs)
        #ranker = Ranker(configs)
        """
        while(True):
            query = input("Enter: ")
            results = searcher.search(query)
            ranked_results = ranker.rank(results)
            for i, result in enumerate(ranked_results):
                print(f"{i+1}: {result}")
        """