import time
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup Selenium WebDriver
options = Options()
options.add_argument("--headless")  # Run browser in background
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("start-maximized")
options.add_argument("disable-infobars")
options.add_argument("--disable-blink-features=AutomationControlled")  # Prevent bot detection

# Initialize WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 10)  # Explicit wait time for elements

# Target URL
url = "https://zk.mk/dentists/gevgelija?lang=en"

# Open the page
driver.get(url)
time.sleep(5)  # Allow page to fully load

# Store extracted data
dentists_data = []

# Find all dentist listings
listings = driver.find_elements(By.CSS_SELECTOR, "div.result")

for listing in listings:
    try:
        name_element = listing.find_element(By.CSS_SELECTOR, "h2.fontot a.companyname")
        name = name_element.text.strip()
        profile_link = name_element.get_attribute("href")
    except:
        name = "N/A"
        profile_link = None

    phone = "N/A"  # Default value

    if profile_link:
        # Click on the profile link
        driver.execute_script("arguments[0].click();", name_element)
        time.sleep(3)  # Allow profile page to load

        try:
            # Wait for the phone number to appear
            phone_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[itemprop='telephone']")))
            phone = phone_element.text.strip()
            phone = phone.replace("\u00a0", " ")  # Fix non-breaking spaces
        except:
            phone = "N/A"

        # Navigate back to the main listing page
        driver.back()
        time.sleep(3)  # Allow main page to reload

    try:
        address = listing.find_element(By.CSS_SELECTOR, "ul.details li[origcaption='Адреса']").text.replace("Address :", "").strip()
    except:
        address = "N/A"

    try:
        place = listing.find_element(By.CSS_SELECTOR, "ul.details li[origcaption='Место']").text.replace("Place:", "").strip()
    except:
        place = "N/A"

    try:
        working_hours = listing.find_element(By.CSS_SELECTOR, "li[origcaption='Работно време'] span.workingtime").text.strip()
    except:
        working_hours = "N/A"

    try:
        google_maps_link = listing.find_element(By.CSS_SELECTOR, "li[origcaption='Оддалеченст'] a").get_attribute("href")
    except:
        google_maps_link = "N/A"

    try:
        website = listing.find_element(By.CSS_SELECTOR, "li a.website").get_attribute("href")
    except:
        website = "N/A"

    # Append data to list
    dentists_data.append({
        "Business Name": name,
        "Phone": phone,
        "Address": address,
        "City": place,
        "Working Hours": working_hours,
        "Google Maps": google_maps_link,
        "Website": website
    })

# Close browser
driver.quit()

# Ensure output directory exists
output_dir = "data/output"
os.makedirs(output_dir, exist_ok=True)

# Convert to DataFrame and save as CSV
output_path = os.path.join(output_dir, "macedonia_dentists_gevgelija_3.csv")
df = pd.DataFrame(dentists_data)
df.to_csv(output_path, index=False)

print(f"✅ Data extracted and saved to: {output_path}")
