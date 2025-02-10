from transformers import pipeline

# Use a zero-shot classification model
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")


def is_restaurant_review(text):
    """Returns True if text is related to restaurants, otherwise False."""
    labels = ["restaurant review", "technology news", "politics", "sports"]
    result = classifier(text, labels)
    return result["labels"][0] == "restaurant review"
