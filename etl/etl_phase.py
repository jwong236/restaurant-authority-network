import os
import json


class ETLPhase:
    """Represents a phase in the ETL process as a node in the ETL linked list."""

    VALID_PHASES = {"extract", "transform", "load"}

    def __init__(self, name, next_phase=None):
        if name not in self.VALID_PHASES:
            raise ValueError(
                f"Invalid phase name: {name}. Must be one of {self.VALID_PHASES}"
            )

        self.name = name
        self.next_phase = next_phase
        self.stage_count = 0
        self.backup_dir = "staging"
        self.backup_file = (
            os.path.join(self.backup_dir, f"backup_{self.name}.json")
            if self.name in {"extract", "transform"}
            else None
        )

        # Ensure staging directory exists
        os.makedirs(self.backup_dir, exist_ok=True)

        # Load backup if exists
        self.load_backup()

    def has_pending_work(self):
        """Checks if there is pending work by verifying if a backup file exists and has content."""
        return (
            self.backup_file
            and os.path.exists(self.backup_file)
            and os.path.getsize(self.backup_file) > 0
        )

    def save_backup(self):
        """Saves current stage progress to a backup file."""
        if self.backup_file and self.stage_count > 0:
            backup_data = {"stage_count": self.stage_count}
            with open(self.backup_file, "w") as f:
                json.dump(backup_data, f)

    def load_backup(self):
        """Loads stage count from a saved backup file if it exists."""
        if self.backup_file and os.path.exists(self.backup_file):
            with open(self.backup_file, "r") as f:
                data = json.load(f)
                self.stage_count = data.get("stage_count", 0)

    def execute(self, logger):
        """Executes the phase-specific logic."""
        if self.name == "extract":
            logger.info("[TEST] Extracting data...")
        elif self.name == "transform":
            logger.info("[TEST] Transforming data...")
        elif self.name == "load":
            logger.info("[TEST] Loading data...")

        # Save progress after execution
        self.save_backup()
