# ./src/pipeline/transform/identify_restaurants.py
import spacy


def identify_restaurants(soup):
    nlp = spacy.load("en_core_web_trf")
    restaurant_names = set()
    doc = nlp(soup.get_text())
    for ent in doc.ents:
        if ent.label_ in ["ORG", "PRODUCT"]:
            restaurant_names.add(ent.text)
    return list(restaurant_names)
