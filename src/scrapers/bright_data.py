import os
import logging
from pathlib import Path
import asyncio
import aiohttp
import pandas as pd
from typing import List, Dict

from bs4 import BeautifulSoup
from openai import AsyncOpenAI
from dotenv import load_dotenv
import time
import tiktoken
import json

from ..utils.rate_limiter import RateLimiter

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PROXY_URL = "http://brd-customer-hl_402ac692-zone-residential_proxy:cxmobfmrh8qp@brd.superproxy.io:33335"


class BusinessFinder:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.businesses = []
        self.rate_limiter = RateLimiter()
        self.encoder = tiktoken.encoding_for_model("gpt-4")

        # Start with just a few test cities and categories
        self.cities = ["Skopje", "Bitola"]  # Limited initial set
        self.categories = ["beauty salon", "hair salon"]  # Limited initial set

    def count_tokens(self, text: str) -> int:
        """Count tokens in text"""
        return len(self.encoder.encode(text))

    async def generate_search_queries(self, city: str, category: str) -> List[str]:
        """Use GPT to generate effective search queries with rate limiting"""
        prompt = f"""
        Generate 2 effective search queries to find {category} businesses in {city}, Macedonia.
        Include one query in English and one in Macedonian.
        Return only the queries, one per line.
        Keep queries short and specific.
        """

        tokens_needed = self.count_tokens(prompt) + 100  # Buffer for response
        await self.rate_limiter.acquire(tokens_needed)

        try:
            response = await self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a local business search expert in Macedonia.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=100,  # Limit response size
            )

            queries = response.choices[0].message.content.strip().split("\n")
            logger.info(f"Generated queries for {category} in {city}: {queries}")
            return queries

        except Exception as e:
            logger.error(f"Error generating queries: {e}")
            return [f"{category} {city}", f"{category} {city} Macedonia"]

    async def extract_business_info(
        self, search_results: str, max_length=1000
    ) -> List[Dict]:
        """Extract business information with validation and structured JSON formatting."""

        # Truncate input to manage token usage
        truncated_results = search_results[:max_length]

        prompt = f"""
        Extract, validate, and clean business information from the provided listings.

        1. Ensure data accuracy by removing invalid, duplicate, or incomplete entries.
        2. Format the output as a well-structured JSON array with this structure:

        [
            {{
                "business_name": "Example Business",
                "category": "Beauty Salon",
                "address": "123 Main St, Skopje, Macedonia",
                "phone": "+389 70 123 456",
                "website": "https://example.com",
                "email": "contact@example.com",
                "social_media": {{
                    "facebook": "https://facebook.com/example",
                    "instagram": "https://instagram.com/example",
                    "linkedin": "https://linkedin.com/company/example"
                }}
            }}
        ]

        Requirements:
        - Remove entries missing key details (e.g., no business name or address).
        - Standardize data formatting (phone numbers in international format, full URLs).
        - Ensure uniqueness by eliminating duplicates.
        - Validate email and website fields to exclude invalid data.

        Deliver a clean, structured JSON array ensuring high data quality.

        Listings:
        {truncated_results}
        """

        # Calculate token needs and prevent exceeding model limits
        tokens_needed = self.count_tokens(prompt) + 200
        max_tokens = min(4096 - tokens_needed, 1500)  # Avoid exceeding limit

        await self.rate_limiter.acquire(tokens_needed)

        try:
            response = await self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a business data extraction expert.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
            )

            # Log raw response
            response_content = response.choices[0].message.content.strip()
            logger.info(f"GPT Response: {response_content}")

            # Ensure response is a valid JSON array
            if not response_content.startswith("["):
                logger.warning(
                    "GPT did not return a valid JSON array. Response:\n"
                    + response_content
                )
                return []

            businesses = json.loads(response_content)

            if isinstance(businesses, list) and businesses:
                logger.info(f"✅ Extracted {len(businesses)} businesses")
                for business in businesses:
                    logger.info(
                        f"📌 Found business: {business.get('business_name', 'No name')}"
                    )
            else:
                logger.warning("⚠️ No businesses extracted from the response")
                return []

            return businesses

        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON parsing error: {e}")
            logger.error(f"📝 Raw content: {response_content}")
            return []

        except Exception as e:
            logger.error(f"Error extracting business info: {e}")
            return []

        except Exception as e:
            logger.error(f"Error extracting business info: {e}")
            return []

    async def search_businesses(self, city: str, category: str) -> List[Dict]:
        """Search for businesses with rate limiting"""
        queries = await self.generate_search_queries(city, category)
        results = []

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "mk-MK,mk;q=0.9,en-US;q=0.8,en;q=0.7",
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            for query in queries[:2]:  # Limit to 2 queries per category
                # Format URL for Yellow Pages Macedonia
                search_url = f"https://zk.mk/en/search/{quote(query)}"

                try:
                    async with session.get(
                        search_url, proxy=PROXY_URL, ssl=False, timeout=30
                    ) as response:
                        if response.status == 200:
                            text = await response.text()

                            # Parse HTML with BeautifulSoup first
                            soup = BeautifulSoup(text, "html.parser")

                            # Extract business listings
                            business_listings = []
                            for listing in soup.select(
                                ".company-list-item, .business-card"
                            ):
                                try:
                                    business = {
                                        "name": self._extract_text(
                                            listing, ".company-name, .business-title"
                                        ),
                                        "address": self._extract_text(
                                            listing, ".company-address, .address"
                                        ),
                                        "phone": self._extract_text(
                                            listing, ".company-phone, .phone"
                                        ),
                                        "category": category,
                                        "city": city,
                                    }
                                    if business["name"]:  # Only add if we found a name
                                        business_listings.append(business)
                                except Exception as e:
                                    logger.error(f"Error parsing listing: {e}")

                            if business_listings:
                                # Convert to formatted text for GPT
                                formatted_listings = "\n\n".join(
                                    [
                                        f"Business Name: {b['name']}\n"
                                        f"Address: {b['address']}\n"
                                        f"Phone: {b['phone']}"
                                        for b in business_listings
                                    ]
                                )

                                logger.info(
                                    f"Found {len(business_listings)} potential businesses"
                                )
                                cleaned_results = await self.extract_business_info(
                                    formatted_listings
                                )
                                results.extend(cleaned_results)

                        await asyncio.sleep(3)  # Rate limiting
                except Exception as e:
                    logger.error(f"Error searching {search_url}: {e}")

        return results

    def _extract_text(self, element, selector):
        """Helper method to extract text from HTML elements"""
        found = element.select_one(selector)
        return found.get_text(strip=True) if found else ""

    async def process_all_locations(self):
        """Process all cities and categories with delays"""
        for city in self.cities:
            for category in self.categories:
                logger.info(f"Processing {category} in {city}")
                try:
                    businesses = await self.search_businesses(city, category)
                    self.businesses.extend(businesses)

                    # Save progress after each category
                    self.save_results()

                    # Add delay between categories
                    await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"Error processing {category} in {city}: {e}")
                    continue

    def save_results(self):
        """Save results to CSV"""
        if self.businesses:
            df = pd.DataFrame(self.businesses)
            Path("data/output").mkdir(parents=True, exist_ok=True)
            output_file = "data/output/macedonia_businesses.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(self.businesses)} businesses to {output_file}")
            logger.info(f"Sample of saved data:\n{df.head()}")
        else:
            logger.warning("No businesses to save")


async def main():
    finder = BusinessFinder()
    await finder.process_all_locations()


if __name__ == "__main__":
    asyncio.run(main())
