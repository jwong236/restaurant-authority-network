# ./tests/test_initialize.py
# Run with: python -m tests.test_initialize

import logging
import json
import os
from unittest.mock import MagicMock
from etl.phases.initialize import execute_phase

# --- Initialize Logger ---
logger = logging.getLogger("initialize_test")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# --- Dynamically Set the Correct Path for Michelin JSON ---
michelin_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "etl",
    "phases",
    "initialize",
    "michelin_restaurants.json",
)
michelin_file_path = os.path.abspath(michelin_file_path)

# --- Example Config ---
config = {
    "output_file": michelin_file_path,
}

# --- Example Shared State ---
shared_state = {
    "initialized": False,
    "michelin_loaded": False,
}

# --- Mock Database Connection ---
mock_conn = MagicMock()
mock_cur = MagicMock()

mock_conn.cursor.return_value = mock_cur

# --- Run Initialize Phase ---
execute_phase(logger, config, shared_state, mock_cur)

# --- Print Final Shared State ---
print("\n=== Initialization Phase Completed ===")
print("Final Shared State:")
print(json.dumps(shared_state, indent=2))

# --- Verify Michelin Data was Loaded ---
if os.path.exists(michelin_file_path):
    with open(michelin_file_path, "r", encoding="utf-8") as file:
        michelin_data = json.load(file)
        print(f"\n✅ Loaded {len(michelin_data)} Michelin restaurants from JSON.")
else:
    print("\n❌ Michelin JSON file was NOT created.")

# --- Print All Mock Calls for Debugging ---
print("\n=== Mocked SQL Calls ===")
for call in mock_cur.call_args_list:
    print(call)

# --- Validate Source Types Were Inserted ---
assert any(
    "INSERT INTO source_types" in str(call) for call in mock_cur.execute.call_args_list
), "❌ The source_types insert was never executed!"
print("\n✅ The source_types insert was executed!")

# --- Validate Restaurants Were Inserted ---
assert any(
    "INSERT INTO restaurants" in str(call) for call in mock_cur.execute.call_args_list
), "❌ The restaurants insert was never executed!"
print("\n✅ Michelin restaurants successfully loaded into the database.")

# --- Validate Priority Queue Entries ---
assert any(
    "INSERT INTO priority_queue" in str(call)
    for call in mock_cur.execute.call_args_list
), "❌ The priority queue insert was never executed!"
print("\n✅ Michelin URLs inserted into the priority queue.")
