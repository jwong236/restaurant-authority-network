import re


def clean_text(text):
    """Removes unwanted characters and excessive whitespace."""
    text = re.sub(r"\s+", " ", text)  # Remove extra spaces
    text = re.sub(r"[^\w\s.,!?]", "", text)  # Remove special characters
    return text.strip()
