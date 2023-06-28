from search_engine import SearchEngine
from argparse import ArgumentParser

def main():
    # Initialize Objects
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()
    search_engine = SearchEngine()
    default_config_path = "config.ini"

    # Add crawl as a command
    crawl_parser = subparsers.add_parser('crawl')
    crawl_parser.set_defaults(func=search_engine.crawl)
    crawl_parser.add_argument("--resume", action="store_true", default=False)
    crawl_parser.add_argument("--config_path", type=str, default=default_config_path)

    # Add build as a command
    build_parser = subparsers.add_parser('build')
    build_parser.set_defaults(func=search_engine.build)
    build_parser.add_argument("--update", action="store_true", default=False)
    build_parser.add_argument("--resume", action="store_true", default=False)
    build_parser.add_argument("--config_path", type=str, default=default_config_path)

    # Add start as a command
    start_parser = subparsers.add_parser('start')
    start_parser.set_defaults(func=search_engine.start)
    start_parser.add_argument("--config_path", type=str, default=default_config_path)

    # Run command
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()