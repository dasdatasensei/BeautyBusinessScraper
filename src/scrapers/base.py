import asyncio
import logging
import os
import json
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from dotenv import load_dotenv
import openai

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("beauty_wellness_scraper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# BrightData Proxy Credentials
BRIGHTDATA_USER = os.getenv("BRIGHTDATA_USER")
BRIGHTDATA_PASS = os.getenv("BRIGHTDATA_PASS")
BRIGHTDATA_HOST = os.getenv("BRIGHTDATA_HOST")
BRIGHTDATA_PORT = os.getenv("BRIGHTDATA_PORT")

# OpenAI API Key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Proxy URL
PROXY_URL = f"http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"

# Business Categories
CATEGORIES = [
    "Barbering", "Face Beauty", "Nails", "Hair Removal", "Massage",
    "Body Treatments", "Counseling & Mental Health", "Anti-Aging Treatments",
    "Weight Loss Treatments", "Dentists & Dental Clinics"
]

class BeautyWellnessScraper:
    def __init__(self, seed_urls, headless=True):
        self.seed_urls = seed_urls
        self.visited_urls = set()
        self.all_businesses = []
        self.headless = headless  # Allow configurable headless mode

    async def fetch_html_playwright(self, url):
        """Fetch HTML content using Playwright with BrightData proxy."""
        async with async_playwright() as p:
            for attempt in range(2):  # Retry once before failing
                browser = await p.chromium.launch(headless=self.headless)
                context = await browser.new_context(
                    proxy={
                        "server": BRIGHTDATA_HOST,
                        "username": BRIGHTDATA_USER,
                        "password": BRIGHTDATA_PASS
                    },
                    ignore_https_errors=True,  # ✅ Ignore SSL errors
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()

                # ✅ Apply stealth script to avoid detection
                await page.add_init_script(
                    "() => { Object.defineProperty(navigator, 'webdriver', {get: () => undefined}) }")

                try:
                    await page.goto(url, timeout=120000, wait_until="networkidle")
                    content = await page.content()  # Get fully rendered HTML
                    await browser.close()
                    return content
                except Exception as e:
                    logger.error(f"Error loading {url} (attempt {attempt + 1}): {e}")
                    await browser.close()
                    if attempt == 1:  # Fail only on the second attempt
                        return None

    def extract_social_media_links(self, soup):
        """Extract social media links."""
        social_links = {}
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "facebook.com" in href:
                social_links["Facebook"] = href
            elif "instagram.com" in href:
                social_links["Instagram"] = href
            elif "twitter.com" in href:
                social_links["Twitter"] = href
            elif "linkedin.com" in href:
                social_links["LinkedIn"] = href
        return social_links

    def extract_address(self, soup):
        """Extract address, including Google Maps links."""
        address = None
        for tag in soup.find_all(["p", "span", "div"]):
            if "address" in tag.get_text().lower():
                address = tag.get_text().strip()
                break
        for link in soup.find_all("a", href=True):
            if "google.com/maps" in link["href"]:
                address = f"Google Maps Link: {link['href']}"
        return address

    def extract_contact_info(self, soup):
        """Extract phone number and email."""
        phone = None
        email = None
        for tag in soup.find_all(["p", "span", "div", "a"]):
            text = tag.get_text().strip()
            if "@" in text and "." in text:
                email = text
            if text.startswith("+") or text.replace("-", "").isdigit():
                phone = text
        return phone, email

    def extract_business_name(self, soup):
        """Extract business name from HTML."""
        if soup.title:
            return soup.title.text.strip()
        h1 = soup.find("h1")
        if h1:
            return h1.text.strip()
        return None

    async def extract_business_data(self, html, url):
        """Extract business data using BeautifulSoup, with OpenAI fallback."""
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        business_name = self.extract_business_name(soup) or "Unknown"
        phone, email = self.extract_contact_info(soup)
        social_media_links = self.extract_social_media_links(soup)
        address = self.extract_address(soup)

        business_data = {
            "Business Name": business_name,
            "Category": None,
            "Website URL": url,
            "Social Media Links": social_media_links,
            "Phone Number": phone,
            "Email": email,
            "Address": address,
        }

        if not phone or not address or not business_data["Category"]:
            logger.info(f"Using OpenAI to fill missing fields for: {business_name}")

            prompt = f"""
            Extract business information from the following HTML. Return a JSON object:
            {{
                "Category": "One of {', '.join(CATEGORIES)}",
                "Email": "If available",
                "Description": "Short description of services"
            }}
            HTML: {html[:4000]}
            """

            try:
                response = openai.ChatCompletion.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": "Extract structured business data as JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2
                )
                gpt_response = response.choices[0].message.content.strip()

                # **Fix JSON Parsing Issues**
                try:
                    gpt_data = json.loads(gpt_response)
                    business_data.update(gpt_data)
                except json.JSONDecodeError:
                    logger.warning(f"OpenAI returned malformed JSON: {gpt_response}")
            except Exception as e:
                logger.error(f"OpenAI extraction error: {e}")

        return business_data

    async def scrape(self):
        """Scrape businesses, leveraging OpenAI only when needed."""
        urls_to_scrape = set(self.seed_urls)

        while urls_to_scrape:
            url = urls_to_scrape.pop()
            if url in self.visited_urls:
                continue
            self.visited_urls.add(url)
            logger.info(f"Processing: {url}")

            html = await self.fetch_html_playwright(url)
            if not html:
                continue

            business_data = await self.extract_business_data(html, url)
            if business_data:
                self.all_businesses.append(business_data)

            # Discover additional business pages (Avoid infinite loops)
            soup = BeautifulSoup(html, "html.parser")
            for link in soup.find_all("a", href=True):
                full_url = link["href"]
                if "wellness" in full_url and full_url not in self.visited_urls and full_url.startswith("http"):
                    urls_to_scrape.add(full_url)

        df = pd.DataFrame(self.all_businesses)
        df.to_csv("beauty_wellness_results.csv", index=False)
        logger.info(f"Saved {len(df)} businesses to beauty_wellness_results.csv")
        return df


async def main():
    seed_urls = [
        # Individual Business Websites
        "https://www.myguidemacedonia.com/wellness/beauty-centre-afrodita-s",
        "https://www.myguidemacedonia.com/wellness/silhouette-beauty-centre",
        "https://www.myguidemacedonia.com/wellness/endomak-dental-clinic",
        "https://www.myguidemacedonia.com/wellness/dior-wellness-spa-and-hair-studio",
        "https://www.myguidemacedonia.com/wellness/katlanovska-spa",
        "https://www.myguidemacedonia.com/wellness/tana-cosmetic",
        "https://www.myguidemacedonia.com/wellness/studio-sense",
        "https://www.myguidemacedonia.com/wellness/derma-beauty-aesthetic-cosmetology",
        "https://www.myguidemacedonia.com/wellness/elite-cosmetic-studio",
        "https://www.myguidemacedonia.com/wellness/hotel-sirius",
        "https://www.myguidemacedonia.com/wellness/negorski-banji-gevgelija",
        "https://www.myguidemacedonia.com/wellness/aura-spa-and-beauty-centre",

        # Beauty and Wellness Associations
        "https://swam.mk/about-swam/",  # Spa & Wellness Association of Macedonia (SWAM)

        # Business Directories and Databases
        "https://6sense.com/company/country-macedonia--industry-Beauty%2C-Health%2C-and-Wellness",
        "https://starngage.com/plus/en-us/brand/ranking/macao/macedonia/beauty",

        # Search Engine Queries
        "https://www.google.com/search?q=beauty+and+wellness+businesses+in+Macedonia",
        "https://www.bing.com/search?q=beauty+and+wellness+salons+in+Macedonia",
        "https://www.yellowpages.mk/search/beauty+salon",
        "https://www.tripadvisor.com/Attractions-g295110-Activities-c40-North_Macedonia.html",
        # Spa & Wellness options in Macedonia
        "https://www.yelp.com/search?cflt=beautyservices&find_loc=North+Macedonia"
    ]

    scraper = BeautyWellnessScraper(seed_urls, headless=False)  # Run with headless=False for debugging
    df = await scraper.scrape()
    logger.info(f"Total businesses extracted: {len(df)}")


if __name__ == "__main__":
    asyncio.run(main())
