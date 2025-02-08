import pytest
import os
import json
from pipeline.initialize import (
    get_restaurant_batch,
    get_json_path,
    get_progress_tracker_path,
)


@pytest.fixture
def setup_mock_data():
    """Creates a temporary Michelin JSON file for testing."""
    test_json_filename = "test_michelin_data.json"
    test_json_path = get_json_path(test_json_filename)

    os.makedirs(os.path.dirname(test_json_path), exist_ok=True)

    mock_data = [
        {
            "name": f"Test Restaurant {i}",
            "location": f"City {chr(64 + i)}",
            "source_url": f"http://example.com/{i}",
        }
        for i in range(1, 21)
    ]

    with open(test_json_path, "w", encoding="utf-8") as f:
        json.dump(mock_data, f)

    yield test_json_filename

    if os.path.exists(test_json_path):
        os.remove(test_json_path)


@pytest.fixture
def setup_mock_progress_tracker():
    """Creates a temporary progress tracker file for testing with an initial last_index of 5."""
    test_progress_filename = "test_progress_tracker.json"
    test_progress_path = get_progress_tracker_path(test_progress_filename)

    with open(test_progress_path, "w", encoding="utf-8") as f:
        json.dump({"last_index": 5, "date_time": "2025-02-07T00:00:00"}, f)

    yield test_progress_filename

    if os.path.exists(test_progress_path):
        os.remove(test_progress_path)


def test_missing_restaurant_file():
    """Test that an exception is raised if the restaurant file does not exist."""
    with pytest.raises(FileNotFoundError):
        get_restaurant_batch("non_existent.json", "progress_tracker.json")


def test_missing_progress_tracker_creates_new_one(setup_mock_data):
    """Test that a missing progress tracker file is created and starts from index 0."""
    test_json_filename = setup_mock_data
    test_progress_filename = "missing_progress.json"
    test_progress_path = get_progress_tracker_path(test_progress_filename)

    if os.path.exists(test_progress_path):
        os.remove(test_progress_path)

    batch = get_restaurant_batch(
        test_json_filename, test_progress_filename, batch_size=5
    )

    assert len(batch) == 5
    assert batch[0]["name"] == "Test Restaurant 1"

    assert os.path.exists(test_progress_path)

    if os.path.exists(test_progress_path):
        os.remove(test_progress_path)


def test_first_batch(setup_mock_data, setup_mock_progress_tracker):
    """Test that the first batch is correctly retrieved based on the progress tracker."""
    test_json_filename = setup_mock_data
    test_progress_filename = setup_mock_progress_tracker

    batch = get_restaurant_batch(
        test_json_filename, test_progress_filename, batch_size=5
    )

    assert len(batch) == 5
    assert batch[0]["name"] == "Test Restaurant 6"


def test_three_batches(setup_mock_data, setup_mock_progress_tracker):
    """Test that three sequential batches can be retrieved correctly."""
    test_json_filename = setup_mock_data
    test_progress_filename = setup_mock_progress_tracker

    batch1 = get_restaurant_batch(
        test_json_filename, test_progress_filename, batch_size=5
    )
    batch2 = get_restaurant_batch(
        test_json_filename, test_progress_filename, batch_size=5
    )
    batch3 = get_restaurant_batch(
        test_json_filename, test_progress_filename, batch_size=5
    )
    assert batch1[0]["name"] == "Test Restaurant 6"
    assert batch2[0]["name"] == "Test Restaurant 11"
    assert batch3[0]["name"] == "Test Restaurant 16"


def test_last_batch_not_exceeding_end(setup_mock_data):
    """Test that the last batch does not exceed the available data."""
    test_json_filename = setup_mock_data
    test_progress_filename = "test_progress_tracker_end.json"
    test_progress_path = get_progress_tracker_path(test_progress_filename)

    with open(test_progress_path, "w", encoding="utf-8") as f:
        json.dump({"last_index": 19}, f)

    batch = get_restaurant_batch(
        test_json_filename, test_progress_filename, batch_size=3
    )

    assert len(batch) == 1
    assert batch[0]["name"] == "Test Restaurant 20"

    if os.path.exists(test_progress_path):
        os.remove(test_progress_path)


def test_invalid_json_files():
    """Test that invalid JSON formats raise errors."""
    test_json_filename = "invalid_test_data.json"
    test_progress_filename = "invalid_test_progress.json"
    test_json_path = get_json_path(test_json_filename)
    test_progress_path = get_progress_tracker_path(test_progress_filename)

    try:
        # Write invalid JSON data
        with open(test_json_path, "w", encoding="utf-8") as f:
            f.write("INVALID JSON FORMAT")

        with open(test_progress_path, "w", encoding="utf-8") as f:
            f.write("INVALID JSON FORMAT")

        with pytest.raises(json.JSONDecodeError):
            get_restaurant_batch(
                test_json_filename, test_progress_filename, batch_size=5
            )

    finally:
        if os.path.exists(test_json_path):
            os.remove(test_json_path)
        if os.path.exists(test_progress_path):
            os.remove(test_progress_path)
