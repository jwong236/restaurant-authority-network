from argparse import ArgumentParser
from utils.config import Config
from crawler import Crawler


def crawl(args):
    configs = Config(args.config_path, "CRAWLER")
    crawler = Crawler(configs, args.resume)
    crawler.start()
    
def main():
    # Initialize Objects
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()
    default_config_path = "config.ini"

    # Add crawl as a command
    crawl_parser = subparsers.add_parser('crawl')
    crawl_parser.set_defaults(func=crawl)
    crawl_parser.add_argument("--resume", action="store_true", default=False)
    crawl_parser.add_argument("--config_path", type=str, default=default_config_path)

    # Run command
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()