import json
import requests
from bs4 import BeautifulSoup


def main():
    url = "https://sandiegomagazine.com/archive/rapid-review-hironori-craft-ramen/"

    # Use a real browser's User-Agent to avoid bot detection
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:  # Ensure request was successful
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract title
        page_title = soup.title.get_text(strip=True)

        # Extract review or main content
        review_section = soup.find(["article", "div"], class_="review")
        if not review_section:
            review_section = soup.find("main")  # Fallback to main content

        if review_section:
            extracted_text = review_section.get_text(separator="\n", strip=True)
        else:
            extracted_text = soup.body.get_text(separator="\n", strip=True)

        # Store extracted content in a dictionary
        data = {"title": page_title, "content": extracted_text}

        # Write to JSON file
        try:
            with open("content.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print("✅ Content successfully saved to content.json")
        except Exception as e:
            print(f"❌ Error writing to file: {e}")

    else:
        print(f"❌ Failed to fetch URL. Status code: {response.status_code}")


if __name__ == "__main__":
    main()
