# ./etl/etl_phase.py
import os
import json
from abc import ABC, abstractmethod

from etl.phases import initialize, explore, extract, transform, load, feedback


class ETLPhase(ABC):
    def __init__(self, name, package, logger=None, next_phase=None, shared_state=None):
        self.name = name
        self.package = package
        self.logger = logger
        self.next_phase = next_phase
        self.shared_state = shared_state if shared_state else {}

        self.stage_count = 0
        self.backup_dir = "backups"
        self.backup_file = os.path.join(self.backup_dir, f"backup_{self.name}.json")
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
    def execute(self, config, cur):
        """Execute phase logic"""
        self.logger.info(f"Executing phase: {self.name}")
        print(f"Executing phase: {self.name}")


class InitializePhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("initialize", **kwargs)

    def execute(self, config, cur):
        """Initialize the database and load Michelin data"""
        self.logger.info("Initialize phase started")
        print("Initialize phase started")

        initialize.execute_phase(self.logger, config, self.shared_state, cur)

        self.logger.info("Initialize phase completed")
        print("Initialize phase completed")


class ExplorePhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("explore", **kwargs)

    def execute(self, config, cur):
        """Discover new sources and restaurants"""
        self.logger.info("Explore phase started")
        print("Explore phase started")

        explore.execute_phase(self.logger, config, self.shared_state, cur)

        self.logger.info("Explore phase completed")
        print("Explore phase completed")


class ExtractPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("extract", **kwargs)

    def execute(self, config, cur):
        """Extract content from crawled URLs"""
        self.logger.info("Extract phase started")
        print("Extract phase started")
        extract.execute_phase(self.logger, config, self.shared_state, cur)
        self.logger.info("Extract phase completed")
        print("Extract phase completed")


class TransformPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("transform", **kwargs)

    def execute(self, config, cur):
        """Transform and analyze content"""
        self.logger.info("Transform phase started")
        print("Transform phase started")
        transform.execute_phase(self.logger, config, self.shared_state, cur)
        self.logger.info("Transform phase completed")
        print("Transform phase completed")


class LoadPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("load", **kwargs)

    def execute(self, config, cur):
        """Load processed data into database"""
        self.logger.info("Load phase started")
        print("Load phase started")
        load.execute_phase(self.logger, config, self.shared_state, cur)
        self.logger.info("Load phase completed")
        print("Load phase completed")


class FeedbackPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("feedback", **kwargs)

    def execute(self, config, cur):
        """Update credibility scores and analyze metrics"""
        self.logger.info("Feedback phase started")
        print("Feedback phase started")
        feedback.execute_phase(self.logger, config, self.shared_state, cur)
        self.logger.info("Feedback phase completed")
        print("Feedback phase completed")


class PhaseFactory:
    @staticmethod
    def get_phase(name, **kwargs):
        phase_classes = {
            "initialize": InitializePhase,
            "explore": ExplorePhase,
            "extract": ExtractPhase,
            "transform": TransformPhase,
            "load": LoadPhase,
            "feedback": FeedbackPhase,
        }
        return phase_classes[name](**kwargs)
