import json
import requests
from bs4 import BeautifulSoup


def main():
    url = "https://cosmopolitanlasvegas.mgmresorts.com/en/restaurants.html?filter=property,The_Cosmopolitan_Of_Las_Vegas"

    # Use a real browser's User-Agent to avoid bot detection
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:  # Ensure request was successful
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract title of the page
        page_title = soup.title.get_text(strip=True)

        # Find all headers (h1, h2, h3, h4) that might contain restaurant names
        extracted_headers = {}
        for tag in ["h1", "h2", "h3", "h4"]:
            headers_list = [
                header.get_text(strip=True) for header in soup.find_all(tag)
            ]
            if headers_list:  # Only store if headers exist
                extracted_headers[tag] = headers_list

        # Store extracted content in a dictionary
        data = {
            "title": page_title,
            "headers": extracted_headers,  # Headers grouped by type
        }

        # Write to JSON file
        try:
            with open("headers.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print("✅ Headers successfully saved to headers.json")
        except Exception as e:
            print(f"❌ Error writing to file: {e}")

    else:
        print(f"❌ Failed to fetch URL. Status code: {response.status_code}")


if __name__ == "__main__":
    main()
