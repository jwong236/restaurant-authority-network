# ./etl/etl_linked_list.py
import os
from dotenv import load_dotenv
import psycopg2
import yaml
from etl.etl_phase import PhaseFactory

load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


class ETLLinkedList:
    def __init__(self, logger):
        self.logger = logger
        self.config = self.load_config()
        self.shared_state = {}

        self.feedback = PhaseFactory.get_phase(
            name="feedback",
            package="phases.feedback",
            logger=self.logger,
            next_phase=None,
            shared_state=self.shared_state,
        )

        self.load = PhaseFactory.get_phase(
            name="load",
            package="phases.load",
            logger=self.logger,
            next_phase=self.feedback,
            shared_state=self.shared_state,
        )

        self.transform = PhaseFactory.get_phase(
            name="transform",
            package="phases.transform",
            logger=self.logger,
            next_phase=self.load,
            shared_state=self.shared_state,
        )

        self.extract = PhaseFactory.get_phase(
            name="extract",
            package="phases.extract",
            logger=self.logger,
            next_phase=self.transform,
            shared_state=self.shared_state,
        )

        self.explore = PhaseFactory.get_phase(
            name="explore",
            package="phases.explore",
            logger=self.logger,
            next_phase=self.extract,
            shared_state=self.shared_state,
        )

        self.initialize = PhaseFactory.get_phase(
            name="initialize",
            package="phases.initialize",
            logger=self.logger,
            next_phase=self.explore,
            shared_state=self.shared_state,
        )

        self.head = self.initialize
        self.cycle_count = 0
        self.max_cycles = self.config.get("pipeline", {}).get("max_cycles", 10)
        self.continuous = self.config.get("pipeline", {}).get("continuous", False)

    def has_pending_work(self):
        """Checks if any phase has pending work"""
        current = self.head
        while current:
            if current.has_pending_work():
                return True
            current = current.next_phase
        return False

    def run(self):
        """Main entry point for pipeline execution"""
        if self.continuous and self.has_pending_work():
            self.logger.warning("Resuming incomplete pipeline")
            print("Resuming existing work...")
        else:
            self.logger.info("Starting new pipeline execution")
            print("Starting fresh pipeline run...")

        self.execute_pipeline()

    def execute_pipeline(self):
        """Executes the full phase chain with a shared database connection"""
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cur = conn.cursor()
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return

        try:
            while self.cycle_count < self.max_cycles:
                self.logger.info(f"Starting cycle {self.cycle_count + 1}")
                print(f"\n=== CYCLE {self.cycle_count + 1}/{self.max_cycles} ===")

                current_phase = self.head
                while current_phase:
                    self.logger.debug(f"Executing {current_phase.name} phase")
                    print(f"▶ {current_phase.name.upper()} PHASE")

                    phase_config = self.config.get(current_phase.name, {})
                    current_phase.execute(phase_config, cur)

                    current_phase = current_phase.next_phase

                conn.commit()
                self.cycle_count += 1

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Pipeline execution failed: {e}")

        finally:
            cur.close()
            conn.close()
            self.logger.info("Pipeline execution completed")
            print("\nPipeline finished!")

    @staticmethod
    def load_config():
        """Loads the entire configuration from config.yml"""
        with open("config.yml", "r") as f:
            return yaml.safe_load(f)
