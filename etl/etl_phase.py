import os
import json
from abc import ABC, abstractmethod


class ETLPhase(ABC):
    """Abstract base class representing a phase in the ETL process."""

    def __init__(self, name):
        self.name = name
        self.stage_count = 0
        self.backup_dir = "backups"
        self.backup_file = os.path.join(
            self.backup_dir,
            f"backup_{self.name}.json",
        )
        os.makedirs(self.backup_dir, exist_ok=True)

    def has_pending_work(self):
        """Checks if this phase has pending work."""
        return (
            os.path.exists(self.backup_file) and os.path.getsize(self.backup_file) > 0
        )

    def save_backup(self):
        """Saves current progress to backup file"""
        backup_data = {"stage_count": self.stage_count}
        with open(self.backup_file, "w") as f:
            json.dump(backup_data, f)

    def load_backup(self):
        """Loads last progress from a saved backup file if it exists."""
        if os.path.exists(self.backup_file):
            with open(self.backup_file, "r") as f:
                data = json.load(f)
                self.stage_count = data.get("stage_count", 0)

    @abstractmethod
    def execute(self, logger):
        """Abstract method that each phase must implement."""
        pass


class ExtractPhase(ETLPhase):
    def __init__(self, extract_limit, next_phase=None):
        super().__init__(name="extract")
        self.next_phase = next_phase
        self.extract_limit = extract_limit

    def execute(self, logger):
        logger.info(f"Extracting data... Limit: {self.extract_limit}")
        print("Extracting data...")


class TransformPhase(ETLPhase):
    def __init__(self, transform_limit, next_phase=None):
        super().__init__(name="transform")
        self.transform_limit = transform_limit
        self.next_phase = next_phase

    def execute(self, logger):
        logger.info(f"Transforming data... Limit: {self.transform_limit}")
        print("Transforming data...")


class LoadPhase(ETLPhase):
    def __init__(self, next_phase=None):
        super().__init__(name="load")
        self.next_phase = next_phase

    def execute(self, logger):
        logger.info("Loading data into database...")
        print("Loading data into database...")
