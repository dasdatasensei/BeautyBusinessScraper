import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Facebook API Token
FACEBOOK_ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN")

# Search Keywords (Modify as Needed)
SEARCH_KEYWORDS = [
    "beauty salon", "spa", "nail salon", "barber shop",
    "weight loss clinic", "massage therapist", "dentist", "counseling center"
]

# Cities in Macedonia to Search
CITIES = [
    "Skopje", "Bitola", "Kumanovo", "Prilep", "Tetovo", "Veles", "Ohrid", "Gostivar",
    "Štip", "Strumica", "Kavadarci", "Kočani", "Kičevo", "Struga", "Radoviš", "Gevgelija",
    "Debar", "Kriva Palanka", "Sveti Nikole", "Negotino", "Delčevo", "Vinica", "Resen",
    "Berovo", "Probištip", "Kratovo", "Bogdanci", "Kruševo", "Makedonski Brod",
    "Dojran", "Valandovo", "Demir Hisar", "Pehčevo", "Demir Kapija"
]

# Output File
OUTPUT_FILE = "data/output/facebook_macedonia_wellness.csv"

# Facebook API Endpoint
FACEBOOK_GRAPH_URL = "https://graph.facebook.com/v15.0/search"


def fetch_facebook_businesses(keyword, city):
    """Fetch business data from Facebook Graph API."""
    businesses = []
    params = {
        "type": "place",
        "q": f"{keyword} in {city}, Macedonia",
        "fields": "name,category_list,phone,emails,website,link,location",
        "access_token": FACEBOOK_ACCESS_TOKEN
    }

    response = requests.get(FACEBOOK_GRAPH_URL, params=params)

    if response.status_code == 200:
        data = response.json().get("data", [])
        for biz in data:
            businesses.append({
                "business_name": biz.get("name", "N/A"),
                "category": biz.get("category_list", [{}])[0].get("name", "N/A") if biz.get("category_list") else "N/A",
                "phone": biz.get("phone", "N/A"),
                "email": biz.get("emails", ["N/A"])[0] if biz.get("emails") else "N/A",
                "website": biz.get("website", "N/A"),
                "facebook_profile": biz.get("link", "N/A"),
                "address": biz.get("location", {}).get("street", "N/A") if biz.get("location") else "N/A"
            })
    else:
        print(f"⚠️ Error: {response.status_code} - {response.text}")

    return businesses


def scrape_facebook_businesses():
    """Loop through all search queries and collect business data."""
    all_businesses = []

    for city in CITIES:
        for keyword in SEARCH_KEYWORDS:
            print(f"🔍 Searching for {keyword} in {city}...")
            businesses = fetch_facebook_businesses(keyword, city)
            all_businesses.extend(businesses)

    # Save to CSV
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df = pd.DataFrame(all_businesses)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n🎉 Scraping complete! Data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    scrape_facebook_businesses()
