import os
import json
from abc import ABC, abstractmethod


class ETLPhase(ABC):
    """Base class"""

    def __init__(self, name, package, shared_state=None, next_phase=None):
        self.name = name
        self.package = package
        self.shared_state = shared_state or {}
        self.next_phase = next_phase

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
    def execute(self, logger):
        """Abstract method that each phase must implement."""
        print("simulating phase execution for phase", self.name)

    @abstractmethod
    def configure(self, config):
        """Configures the phase with the given config."""
        pass


class ExplorePhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("explore", **kwargs)

    def execute(self, logger):
        """Discover new sources and restaurants"""
        logger.info("Code Stub: Explore phase")
        print("Code Stub: Explore phase")

    def configure(self, config):
        """Configures the explore phase with the given config."""
        print("Code Stub: Configuring explore phase with", config)


class ExtractPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("extract", **kwargs)

    def execute(self, logger):
        """Now accesses shared crawl frontier"""
        logger.info("Code Stub: Extract phase")
        print("Code Stub: Extract phase")

    def configure(self, config):
        """Configures the explore phase with the given config."""
        print("Code Stub: Configuring explore phase with", config)


class TransformPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("transform", **kwargs)

    def execute(self, logger):
        """Now includes content hashing"""
        logger.info("Code Stub: Transform phase")
        print("Code Stub: Transform phase")

    def configure(self, config):
        """Configures the explore phase with the given config."""
        print("Code Stub: Configuring explore phase with", config)


class LoadPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("load", **kwargs)

    def execute(self, logger):
        """Bulk loading with conflict handling"""
        logger.info("Code Stub: Load phase")
        print("Code Stub: Load phase")

    def configure(self, config):
        """Configures the explore phase with the given config."""
        print("Code Stub: Configuring explore phase with", config)


class FeedbackPhase(ETLPhase):
    def __init__(self, **kwargs):
        super().__init__("feedback", **kwargs)

    def execute(self, logger):
        """Update priorities and credibility scores"""
        logger.info("Code Stub: Feedback phase")
        print("Code Stub: Feedback phase")

    def configure(self, config):
        """Configures the explore phase with the given config."""
        print("Code Stub: Configuring explore phase with", config)


class PhaseFactory:
    @staticmethod
    def get_phase(name, **kwargs):
        phase_classes = {
            "explore": ExplorePhase,
            "extract": ExtractPhase,
            "transform": TransformPhase,
            "load": LoadPhase,
            "feedback": FeedbackPhase,
        }
        return phase_classes[name](**kwargs)
