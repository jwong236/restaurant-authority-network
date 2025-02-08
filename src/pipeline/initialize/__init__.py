import json
import os
import datetime
import ijson

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def get_json_path(filename):
    """Returns the absolute path of a JSON file inside the data directory."""
    return os.path.join(DATA_DIR, filename)


def get_progress_tracker_path(filename):
    """Returns the absolute path of the progress tracker file inside the data directory."""
    return os.path.join(DATA_DIR, filename)


def load_json_file(file_path, batch_size=10, start_index=0):
    """Streams a JSON file and yields data in batches instead of loading everything into memory."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        batch = []
        with open(file_path, "rb") as f:
            parser = ijson.items(f, "item")

            for index, item in enumerate(parser):
                if index < start_index:
                    continue

                batch.append(item)

                if len(batch) == batch_size:
                    yield batch
                    batch = []

            if batch:
                yield batch

    except ijson.JSONError:
        raise json.JSONDecodeError(
            f"Error decoding JSON file: {file_path}", file_path, 0
        )


def load_progress(progress_filename):
    """Loads the progress tracker file and returns the last processed index.
    If the file doesn't exist, it creates one and returns 0.
    """
    progress_tracker_path = get_progress_tracker_path(progress_filename)

    if not os.path.exists(progress_tracker_path):
        print(f"Creating new progress tracker: {progress_filename}")
        save_progress(progress_filename, 0)
        return 0

    try:
        with open(progress_tracker_path, "r", encoding="utf-8") as f:
            progress_data = json.load(f)
            return progress_data.get("last_index", 0)
    except json.JSONDecodeError:
        print(f"Error loading progress tracker: {progress_filename}")
        save_progress(progress_filename, 0)
        return 0


def save_progress(progress_filename, last_index):
    """Updates the progress tracker with the last processed index."""
    progress_tracker_path = get_progress_tracker_path(progress_filename)

    try:
        os.makedirs(os.path.dirname(progress_tracker_path), exist_ok=True)
        with open(progress_tracker_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "last_index": last_index,
                    "date_time": datetime.datetime.now().isoformat(),
                },
                f,
                indent=4,
            )
    except Exception as e:
        print(f"⚠️ Error saving progress: {e}")


def get_restaurant_batch(json_filename, progress_filename, batch_size=10):
    """Reads a batch of restaurants from the specified JSON file and logs progress."""
    json_path = get_json_path(json_filename)
    last_index = load_progress(progress_filename)

    try:
        for batch in load_json_file(
            json_path, batch_size=batch_size, start_index=last_index
        ):
            new_index = last_index + len(batch)
            save_progress(progress_filename, new_index)
            return batch

        return []

    except FileNotFoundError as e:
        print(e)
        raise
    except json.decoder.JSONDecodeError:
        print(f"Error decoding JSON file: {json_filename}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
