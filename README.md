# Beauty & Wellness Business Scraper

A comprehensive web scraping solution for collecting information about beauty and wellness businesses. This project includes specialized scrapers for various data sources including Google Places, Facebook, and custom web scraping implementations.

## Features

- Multi-source data collection from:
  - Google Places API
  - Facebook Business Pages
  - Custom web scraping with BrightData proxy support
  - Specialized dental practice scraper
- Asynchronous scraping capabilities
- Intelligent rate limiting
- Proxy support for avoiding IP blocks
- OpenAI integration for enhanced data extraction
- Comprehensive logging system

## Technologies

- Python 3.10+
- Playwright for web automation
- OpenAI API for intelligent data processing
- Pandas for data handling
- Beautiful Soup 4 for HTML parsing
- Async HTTP clients (aiohttp, httpx)
- Poetry for dependency management

## Project Structure

```
BeautyBusinessScraper/
├── src/               # Source code
│   ├── scrapers/     # Different scraper implementations
│   └── utils/        # Utility functions and classes
├── data/             # Data storage
│   └── raw/         # Raw scraped data
├── logs/             # Log files
```

## Prerequisites

- Python 3.10 or higher
- Poetry for dependency management
- Required API keys:
  - OpenAI API key
  - BrightData proxy credentials
  - Google Places API key (optional)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/BeautyBusinessScraper.git
cd BeautyBusinessScraper
```

2. Install dependencies using Poetry:

```bash
poetry install
```

3. Set up environment variables:
   Create a `.env` file in the root directory with the following variables:

```
OPENAI_API_KEY=your_openai_api_key
BRIGHTDATA_USER=your_brightdata_username
BRIGHTDATA_PASS=your_brightdata_password
BRIGHTDATA_HOST=your_brightdata_host
BRIGHTDATA_PORT=your_brightdata_port
```

## Usage

The project contains multiple scrapers for different data sources. Each scraper can be run independently:

1. Beauty and Wellness Businesses:

```bash
poetry run python src/scrapers/beauty_wellness.py
```

2. Dental Practices:

```bash
poetry run python src/scrapers/dentists.py
```

3. Facebook Business Pages:

```bash
poetry run python src/scrapers/facebook.py
```

4. Google Places:

```bash
poetry run python src/scrapers/google_places.py
```

## Data Output

Scraped data is saved in CSV format in the `data/raw/` directory. Each scraper produces its own CSV file with relevant business information.

## Logging

Logs are stored in the `logs/` directory. Each scraper maintains its own log file with detailed information about the scraping process, including:

- Successful scrapes
- Failed attempts
- Rate limiting information
- Error messages

## License

-

## Author

Dr. Jody-Ann S. Jones (Founder, The Data Sensei)

## Acknowledgments

- BrightData for proxy services
- OpenAI for API services
