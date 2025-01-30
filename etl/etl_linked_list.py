from etl.etl_phase import PhaseFactory


class ETLLinkedList:
    def __init__(
        self,
        logger,
        max_cycles=10,
        continuous=False,
        extract_limit=100,
        transform_limit=100,
    ):
        self.logger = logger

        self.load = PhaseFactory.get_phase("load")
        self.transform = PhaseFactory.get_phase(
            "transform", transform_limit=transform_limit, next_phase=self.load
        )
        self.extract = PhaseFactory.get_phase(
            "extract", extract_limit=extract_limit, next_phase=self.transform
        )

        self.head = self.extract  # Starting phase
        self.cycle_count = 0

        # Configs
        self.max_cycles = max_cycles
        self.continuous = continuous

    def has_pending_work(self):
        """Checks if linked list has pending work by checking each phase."""
        current_phase = self.head
        while current_phase:
            if current_phase.has_pending_work():
                return True
            current_phase = current_phase.next_phase
        return False

    def run(self):
        """Executes the ETL pipeline based on user selection."""
        if self.continuous:
            # Check if any phase has pending work
            if self.has_pending_work():
                print(
                    "Warning: Some staging areas are not empty. Please continue the last phase."
                )
                self.logger.info(
                    "Warning: Some staging areas are not empty. Please continue the last phase."
                )
                return
            self.logger.info("Starting a new ETL cycle...")
            print("\n----------------------------")
            print("Starting a new ETL cycle...")
            print("----------------------------\n")
        else:
            if not self.has_pending_work():
                self.logger.info(
                    "No unfinished work detected. Start a new cycle instead."
                )
                print("No unfinished work detected. Start a new cycle instead.")
                return

            print("\n----------------------------")
            print("Resuming ETL Cycle...")
            print("----------------------------\n")
            self.logger.info("Resuming ETL Cycle...")

        self.execute_pipeline()

    def execute_pipeline(self):
        """Executes the ETL process through extract -> transform -> load as a full cycle."""
        while self.continuous and self.cycle_count < self.max_cycles:
            print(f"----------------------------")
            print(
                f"Cycle {self.cycle_count + 1}/{self.max_cycles}: Starting full ETL cycle"
            )
            print(f"----------------------------")
            self.logger.info(
                f"Cycle {self.cycle_count + 1}/{self.max_cycles}: Starting full ETL cycle"
            )

            phase = self.head
            while phase:
                print(f"Executing {phase.name} phase...")
                self.logger.info(f"Executing {phase.name} phase...")
                phase.execute(self.logger)
                phase = phase.next_phase

            self.cycle_count += 1

            if self.cycle_count >= self.max_cycles:
                self.logger.info("Max cycles reached. Stopping ETL process.")
                print("Max cycles reached. Stopping ETL process.")
                return
            print("Restarting ETL Cycle...")
            self.logger.info("Restarting ETL Cycle...")
