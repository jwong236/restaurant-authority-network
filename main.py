from utils.logging import setup_logging
from etl.etl_linked_list import ETLLinkedList


def main():
    """Main entry point for the ETL pipeline."""
    logger = setup_logging(console_output=False)

    logger.info("Starting ETL pipeline user prompt.")

    while True:
        print("\n=== TravelQuest ETL Pipeline ===")
        print("1. Start ETL Cycle")
        print("2. Continue ETL Cycle")
        print("3. Build Blog Corpus")
        print("4. Exit")

        choice = input("Enter your choice (1-3): ").strip()

        if choice == "1":
            logger.info("User selected Start ETL Cycle")
            etl_pipeline = ETLLinkedList(logger, continuous=True)
            etl_pipeline.run()
        elif choice == "2":
            logger.info("User selected Continue ETL Cycle")
            etl_pipeline = ETLLinkedList(logger, continuous=False)
            etl_pipeline.run()
        elif choice == "3":
            logger.info("User selected Build Blog Corpus")

        elif choice == "4":
            logger.info("Exiting ETL pipeline. Goodbye!")
            break
        else:
            logger.warning("Invalid choice. User prompted again.")
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
