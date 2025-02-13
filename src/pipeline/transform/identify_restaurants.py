import spacy

nlp = spacy.load("en_core_web_trf")


def identify_restaurants(webpage_type, soup):
    restaurant_names = set()

    # Extract text from relevant sections
    if webpage_type == "simple_review":
        text = soup.title.get_text() if soup.title else ""
        text += " ".join([h.get_text() for h in soup.find_all("h1")])
    elif webpage_type in ["aggregated_review_list", "simple_review_list"]:
        text = " ".join([h.get_text() for h in soup.find_all(["h2", "h3", "li"])])

    # Run NER to extract potential restaurant names
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ in [
            "ORG",
            "GPE",
            "PRODUCT",
        ]:  # Organizations and locations (restaurants often fall here)
            restaurant_names.add(ent.text)

    return list(restaurant_names)
