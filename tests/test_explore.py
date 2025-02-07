# ./tests/test_explore.py
# Run with: python -m tests.test_explore

import logging
import json
import os
from unittest.mock import MagicMock
from etl.phases.explore import execute_phase

# --- Initialize Logger ---
logger = logging.getLogger("explore_test")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# --- Example Config ---
config = {
    "discovery_batch_size": 5,
    "max_crawl_attempts": 3,
}

# --- Example Shared State ---
shared_state = {
    "crawl_frontier": [],
    "credibility_scores": {},
    "initialized": True,
    "michelin_loaded": True,
    "crawl_attempts": {},
    "discovered_links": [],
}

# --- Mock Database Connection ---
mock_conn = MagicMock()
mock_cur = MagicMock()

mock_conn.cursor.return_value = mock_cur

# --- Mock Database Behavior ---
mock_cur.fetchall.return_value = [
    ("https://example.com/test1",),
    ("https://example.com/test2",),
    ("https://example.com/test3",),
]

# --- Run Explore Phase ---
execute_phase(logger, config, shared_state, mock_cur)

# --- Print Final Shared State ---
print("\n=== Exploration Phase Completed ===")
print("Final Shared State:")
print(json.dumps(shared_state, indent=2))

# --- Validate Database Calls ---
print("\n=== Mocked SQL Calls ===")
for call in mock_cur.execute.call_args_list:
    print(call)

# --- Validate Priority Queue Fetch ---
assert any(
    "SELECT url FROM priority_queue" in str(call)
    for call in mock_cur.execute.call_args_list
), "❌ The priority queue batch fetch was never executed!"
print("\n✅ Priority queue batch fetch executed!")

# --- Validate URL Status Update to 'processing' ---
assert any(
    "UPDATE priority_queue SET status = 'processing'" in str(call)
    for call in mock_cur.execute.call_args_list
), "❌ URLs were not marked as 'processing'!"
print("\n✅ URLs successfully marked as 'processing'.")

# --- Validate Discovered Links Added to Shared State ---
assert len(shared_state["discovered_links"]) > 0, "❌ No discovered links were added!"
print(
    f"\n✅ {len(shared_state['discovered_links'])} discovered links added to shared state."
)

# --- Validate URLs Marked as 'processed' ---
assert any(
    "UPDATE priority_queue SET status = 'processed'" in str(call)
    for call in mock_cur.execute.call_args_list
), "❌ URLs were not marked as 'processed'!"
print("\n✅ URLs successfully marked as 'processed'.")
