import os
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# API Keys
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
FACEBOOK_ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

# Constants
OUTPUT_FILE = "data/output/macedonia_wellness_companies.csv"
CITIES = [
    "Skopje", "Bitola", "Kumanovo", "Prilep", "Tetovo", "Veles", "Ohrid", "Gostivar", "Štip",
    "Strumica", "Kavadarci", "Kočani", "Kičevo", "Struga", "Radoviš", "Gevgelija", "Debar", "Kriva Palanka",
    "Sveti Nikole", "Negotino", "Delčevo", "Vinica", "Resen", "Berovo", "Probištip", "Kratovo", "Bogdanci",
    "Kruševo", "Makedonski Brod", "Dojran", "Valandovo", "Demir Hisar", "Pehčevo", "Demir Kapija"
]
CATEGORIES = [
    "barber shop", "beauty salon", "nail salon", "spa", "weight loss clinic",
    "massage therapist", "dentist", "counseling center"
]

class BusinessScraper:
    """Scrapes business listings using SerpAPI, Facebook API, and Hunter.io."""

    SERPAPI_URL = "https://serpapi.com/search.json"
    FACEBOOK_URL = "https://graph.facebook.com/v15.0/search"
    HUNTER_URL = "https://api.hunter.io/v2/domain-search"

    def __init__(self):
        self.businesses = []

    def fetch_from_serpapi(self, city, category):
        """Fetch business data from SerpAPI Google Maps search."""
        try:
            params = {
                "engine": "google_maps",
                "q": f"{category} in {city}, Macedonia",
                "api_key": SERPAPI_KEY
            }
            response = requests.get(self.SERPAPI_URL, params=params)
            response.raise_for_status()
            return response.json().get("local_results", [])
        except requests.RequestException as e:
            logging.error(f"❌ Error fetching from SerpAPI: {e}")
            return []

    def fetch_from_facebook(self, business_name):
        """Fetch social media and contact details from Facebook API."""
        try:
            params = {
                "type": "place",
                "q": business_name,
                "fields": "name,phone,website,location",
                "access_token": FACEBOOK_ACCESS_TOKEN
            }
            response = requests.get(self.FACEBOOK_URL, params=params)
            response.raise_for_status()
            results = response.json().get("data", [])
            return results[0] if results else {}
        except requests.RequestException as e:
            logging.error(f"❌ Error fetching from Facebook: {e}")
            return {}

    def fetch_email_from_hunter(self, domain):
        """Fetch business email using Hunter.io API."""
        try:
            params = {
                "domain": domain,
                "api_key": HUNTER_API_KEY
            }
            response = requests.get(self.HUNTER_URL, params=params)
            response.raise_for_status()
            emails = response.json().get("data", {}).get("emails", [])
            return emails[0]["value"] if emails else "N/A"
        except requests.RequestException as e:
            logging.error(f"❌ Error fetching from Hunter.io: {e}")
            return "N/A"

    def scrape(self):
        """Main scraping function that iterates over cities and categories."""
        total_tasks = len(CITIES) * len(CATEGORIES)
        task_counter = 0

        for city in CITIES:
            for category in CATEGORIES:
                task_counter += 1
                print(f"🔍 Processing [{task_counter}/{total_tasks}]: {category} in {city}...")

                results = self.fetch_from_serpapi(city, category)
                logging.info(f"✅ Fetched {len(results)} results for {category} in {city}")

                if not results:
                    print(f"⚠️ No results found for {category} in {city}")

                for place in results:
                    business_name = place.get("title", "N/A")
                    website = place.get("website", "N/A")

                    # Fetch additional details
                    fb_data = self.fetch_from_facebook(business_name)
                    email = self.fetch_email_from_hunter(website.split("//")[-1]) if website != "N/A" else "N/A"

                    self.businesses.append({
                        "business_name": business_name,
                        "category": category,
                        "city": city,
                        "address": place.get("address", "N/A"),
                        "phone": place.get("phone", fb_data.get("phone", "N/A")),
                        "website": website if website != "N/A" else fb_data.get("website", "N/A"),
                        "email": email,
                        "facebook": fb_data.get("website", "N/A")
                    })
                time.sleep(1)  # Avoid rate limits

        self.save_to_csv()

    def save_to_csv(self):
        """Saves scraped data to CSV."""
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        df = pd.DataFrame(self.businesses)
        df.to_csv(OUTPUT_FILE, index=False)
        logging.info(f"✅ Data saved to {OUTPUT_FILE}")
        print(f"\n🎉 Scraping complete! Data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    scraper = BusinessScraper()
    scraper.scrape()
