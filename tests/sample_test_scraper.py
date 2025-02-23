import logging
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from urllib.parse import quote
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
BRIGHTDATA_PROXY_URL = os.getenv("BRIGHTDATA_PROXY_URL")
BASE_URL = os.getenv("ZK_BASE_URL", "https://zk.mk")

if not BRIGHTDATA_PROXY_URL:
    logger.error("BRIGHTDATA_PROXY_URL environment variable is not set")
    raise ValueError("BRIGHTDATA_PROXY_URL environment variable is required")

# Main search URL
BASE_URL = "https://zk.mk"

# Test with one city and category first
city = "Skopje"
category = "beauty salon"


async def test_local_directory():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "mk-MK,mk;q=0.9,en-US;q=0.8,en;q=0.7",  # Added Macedonian language
        "Origin": "https://zk.mk",
        "Referer": "https://zk.mk/",
    }

    search_url = f"{BASE_URL}/en/search?q={quote(category)}+{quote(city)}"

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            logger.info(f"Attempting to fetch: {search_url}")

            async with session.get(
                search_url, proxy=BRIGHTDATA_PROXY_URL, ssl=False, timeout=30
            ) as response:
                logger.info(f"Status: {response.status}")
                text = await response.text()

                # Save raw response for analysis
                with open("response_debug.html", "w", encoding="utf-8") as f:
                    f.write(text)
                logger.info("Saved raw response to response_debug.html")

                # Parse content
                soup = BeautifulSoup(text, "html.parser")

                # Try several potential selectors
                logger.info("\nTrying different selectors:")

                # Business cards
                cards = soup.select(".business-card, .listing-item, .company-item")
                logger.info(f"Business cards found: {len(cards)}")

                # Business names
                names = soup.select("h2.business-name, .company-title, .listing-title")
                logger.info(f"Business names found: {len(names)}")

                # Addresses
                addresses = soup.select(".address, .location, .company-address")
                logger.info(f"Addresses found: {len(addresses)}")

                # Print first few elements of each type if found
                if cards:
                    logger.info("\nSample card content:")
                    logger.info(cards[0].text.strip()[:200])

                if names:
                    logger.info("\nSample names:")
                    for name in names[:3]:
                        logger.info(name.text.strip())

                if addresses:
                    logger.info("\nSample addresses:")
                    for addr in addresses[:3]:
                        logger.info(addr.text.strip())

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(test_local_directory())
