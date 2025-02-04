from etl.etl_phase import PhaseFactory


class ETLLinkedList:
    def __init__(self, logger, config):
        self.logger = logger

        # Initialize shared state
        self.shared_state = {
            "crawl_frontier": [],
            "priority_queue": {},
            "credibility_scores": {},
        }

        # Create phases in reverse order to properly link them
        self.feedback = PhaseFactory.get_phase(
            name="feedback",
            package="phases.feedback",
            shared_state=self.shared_state,
            next_phase=None,
        )

        self.load = PhaseFactory.get_phase(
            name="load",
            package="phases.load",
            shared_state=self.shared_state,
            next_phase=self.feedback,
        )

        self.transform = PhaseFactory.get_phase(
            name="transform",
            package="phases.transform",
            shared_state=self.shared_state,
            next_phase=self.load,
        )

        self.extract = PhaseFactory.get_phase(
            name="extract",
            package="phases.extract",
            shared_state=self.shared_state,
            next_phase=self.transform,
        )

        self.explore = PhaseFactory.get_phase(
            name="explore",
            package="phases.explore",
            shared_state=self.shared_state,
            next_phase=self.extract,
        )

        # Configure phases
        self.explore.configure(config.get("explore", {}))
        self.extract.configure(config.get("extract", {}))
        self.transform.configure(config.get("transform", {}))
        self.load.configure(config.get("load", {}))
        self.feedback.configure(config.get("feedback", {}))

        # Pipeline configuration
        self.head = self.explore
        self.cycle_count = 0
        self.max_cycles = config.get("max_cycles", 10)
        self.continuous = config.get("continuous", False)

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
        """Executes the full phase chain"""
        while self.cycle_count < self.max_cycles:
            self.logger.info(f"Starting cycle {self.cycle_count + 1}")
            print(f"\n=== CYCLE {self.cycle_count + 1}/{self.max_cycles} ===")

            current_phase = self.head
            while current_phase:
                self.logger.debug(f"Executing {current_phase.name} phase")
                print(f"▶ {current_phase.name.upper()} PHASE")
                current_phase.execute(self.logger)
                current_phase = current_phase.next_phase

            self.cycle_count += 1

            if not self.continuous:
                break

        self.logger.info("Pipeline execution completed")
        print("\nPipeline finished!")
