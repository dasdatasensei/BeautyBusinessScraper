import os
import time
import requests
import pandas as pd
import openai
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Google Places API Key
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
# OpenAI API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Google Places API Endpoint
GOOGLE_PLACES_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"

# OpenAI GPT-3/4 Model
MODEL = "gpt-4"  # or "gpt-3.5-turbo" based on your preference and plan

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
OUTPUT_FILE = "data/output/google_places_macedonia_wellness_enriched.csv"


def fetch_businesses_from_google_places(keyword, city):
    """Fetch business data from Google Places API."""
    businesses = []
    params = {
        "query": f"{keyword} in {city}, Macedonia",
        "key": GOOGLE_PLACES_API_KEY
    }

    response = requests.get(GOOGLE_PLACES_URL, params=params)

    if response.status_code == 200:
        data = response.json().get("results", [])
        for place in data:
            businesses.append({
                "business_name": place.get("name", "N/A"),
                "category": keyword,
                "address": place.get("formatted_address", "N/A"),
                "phone": place.get("formatted_phone_number", "N/A"),
                "website": place.get("website", "N/A"),
                "google_maps_url": f"https://www.google.com/maps/place/?q=place_id:{place.get('place_id')}"
            })
    else:
        print(f"⚠️ Error: {response.status_code} - {response.text}")

    return businesses


def enrich_business_data_with_openai(businesses):
    """Enrich and clean business data using OpenAI GPT."""
    enriched_businesses = []
    for business in businesses:
        # Prepare prompt for OpenAI to clean and enrich data
        prompt = f"""
        Clean and enrich the following business information:

        Business Name: {business['business_name']}
        Category: {business['category']}
        Address: {business['address']}
        Phone: {business['phone']}
        Website: {business['website']}
        Google Maps URL: {business['google_maps_url']}

        Provide a JSON object with the following:
        - Ensure valid website URLs (add if missing)
        - Provide a valid email if not available
        - Ensure phone numbers are in international format (if not already)
        - Include missing data if possible.

        Output in the following format:
        {{
            "business_name": "<name>",
            "category": "<category>",
            "address": "<address>",
            "phone": "<phone>",
            "website": "<website>",
            "email": "<email>",
            "google_maps_url": "<url>"
        }}
        """

        # Request OpenAI API to process the business information
        response = openai.Completion.create(
            model=MODEL,
            prompt=prompt,
            max_tokens=500,
            temperature=0.7
        )

        # Get the cleaned and enriched data from the response
        enriched_data = response.choices[0].text.strip()

        try:
            enriched_business = json.loads(enriched_data)
            enriched_businesses.append(enriched_business)
        except json.JSONDecodeError as e:
            print(f"Error parsing OpenAI response for {business['business_name']}: {e}")

    return enriched_businesses


def scrape_google_places_and_enrich():
    """Loop through all search queries and collect, enrich business data."""
    all_businesses = []

    for city in CITIES:
        for keyword in SEARCH_KEYWORDS:
            print(f"🔍 Searching for {keyword} in {city}...")
            businesses = fetch_businesses_from_google_places(keyword, city)
            if businesses:
                enriched_businesses = enrich_business_data_with_openai(businesses)
                all_businesses.extend(enriched_businesses)
            time.sleep(1)  # Avoid hitting rate limits

    # Save enriched data to CSV
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df = pd.DataFrame(all_businesses)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n🎉 Scraping and enrichment complete! Data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    scrape_google_places_and_enrich()
