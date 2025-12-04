"""
Simple Google Maps Scraper - Single query, maximum leads
Usage: python maps_scraper.py "restaurants in New York"
"""
import time
import random
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re


def setup_driver():
    """Setup Chrome driver"""
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def scroll_results(driver, scrollable_div, callback=None):
    """Scroll until we reach the absolute end - no limits"""
    print("Scrolling to load ALL results (no limit)...")
    last_height = 0
    scroll_count = 0
    no_change_count = 0
    
    while True:
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_div)
        time.sleep(random.uniform(1.5, 2.5))
        
        new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
        
        # Check if height changed
        if new_height == last_height:
            no_change_count += 1
            if no_change_count >= 5:
                print(f"Reached absolute end after {scroll_count} scrolls")
                if callback:
                    callback("scroll_complete", {"count": scroll_count})
                break
            time.sleep(1)
        else:
            no_change_count = 0
        
        last_height = new_height
        scroll_count += 1
        
        if scroll_count % 5 == 0:
            print(f"Scrolled {scroll_count} times, still loading...")
            if callback:
                callback("scrolling", {"count": scroll_count})
    
    return scroll_count


def extract_business_info(driver):
    """Extract business info from the details panel"""
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Get current Google Maps URL for this business
        google_maps_link = driver.current_url
        
        # Business name
        name_elem = soup.find('h1', class_='DUwDvf')
        name = name_elem.text.strip() if name_elem else ""
        
        if not name:
            return None
        
        # Rating
        rating = ""
        rating_elem = soup.find('span', class_='ceNzKf')
        if rating_elem:
            rating = rating_elem.get('aria-label', '')
        
        # Reviews count
        reviews = ""
        reviews_elem = soup.find('span', class_='F7nice')
        if reviews_elem:
            reviews = reviews_elem.text.strip()
        
        # Address
        address = ""
        address_button = soup.find('button', {'data-item-id': 'address'})
        if address_button:
            address = address_button.text.strip()
        
        # Phone
        phone = ""
        phone_button = soup.find('button', {'data-item-id': re.compile(r'phone:tel:')})
        if phone_button:
            phone = phone_button.text.strip()
        
        # Website
        website = ""
        website_link = soup.find('a', {'data-item-id': 'authority'})
        if website_link:
            website = website_link.get('href', '')
        
        # Category
        category = ""
        category_elem = soup.find('button', class_='DkEaL')
        if category_elem:
            category = category_elem.text.strip()
        
        return {
            'business_name': name,
            'category': category,
            'address': address,
            'phone': phone,
            'website': website,
            'has_website': 'Yes' if website else 'No',
            'rating': rating,
            'reviews': reviews,
            'google_maps_link': google_maps_link
        }
        
    except Exception as e:
        return None


def scrape_maps(query):
    """Main scraping function (without progress callback)"""
    return scrape_maps_with_progress(query, None)


def scrape_maps_with_progress(query, callback=None):
    """Main scraping function with progress callback"""
    print(f"\n{'='*60}")
    print(f"Google Maps Scraper")
    print(f"Query: {query}")
    print(f"{'='*60}\n")
    
    if callback:
        callback("browser_starting", {"message": "Starting Chrome browser..."})
    
    driver = setup_driver()
    results = []
    
    try:
        # Navigate to Google Maps search
        maps_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        print(f"Opening: {maps_url}")
        if callback:
            callback("navigating", {"message": f"Opening Google Maps: {query}"})
        driver.get(maps_url)
        time.sleep(4)
        
        # Wait for results panel
        try:
            if callback:
                callback("waiting", {"message": "Waiting for results to load..."})
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
            )
        except:
            print("Could not find results panel. Try a different query.")
            if callback:
                callback("error", {"message": "Could not find results panel"})
            return []
        
        # Get scrollable div and scroll to load ALL results (no limit)
        if callback:
            callback("scrolling", {"count": 0, "message": "Scrolling to load all results..."})
        scrollable_div = driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
        scroll_results(driver, scrollable_div, callback)
        
        # Find all listings
        listings = driver.find_elements(By.CSS_SELECTOR, "div[role='feed'] > div > div > a")
        total_listings = len(listings)
        print(f"\nFound {total_listings} listings to process - will extract ALL of them")
        
        if callback:
            callback("listings_found", {"total": total_listings, "message": f"Found {total_listings} businesses to extract"})
        
        # Process each listing - ALL of them, no deduplication
        for i, listing in enumerate(listings):
            try:
                # Scroll listing into view and click
                driver.execute_script("arguments[0].scrollIntoView(true);", listing)
                time.sleep(0.3)
                listing.click()
                time.sleep(random.uniform(1.2, 2))
                
                # Extract info
                info = extract_business_info(driver)
                
                if info:
                    results.append(info)
                    
                    if callback:
                        callback("extracting", {
                            "current": i + 1,
                            "total": total_listings,
                            "collected": len(results),
                            "business": info.get("business_name", "Unknown")
                        })
                    
                    if len(results) % 10 == 0:
                        print(f"Collected {len(results)}/{total_listings} leads...")
                        
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"Error: {e}")
        if callback:
            callback("error", {"message": str(e)})
    finally:
        driver.quit()
        if callback:
            callback("browser_closed", {"message": "Browser closed"})
    
    return results


def save_results(results, query):
    """Save results to CSV"""
    if not results:
        print("No results to save")
        return
    
    df = pd.DataFrame(results)
    
    # Create filename from query
    safe_query = re.sub(r'[^\w\s-]', '', query).replace(' ', '_')[:30]
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"leads_{safe_query}_{date_str}.csv"
    
    df.to_csv(filename, index=False)
    print(f"\n{'='*60}")
    print(f"DONE!")
    print(f"{'='*60}")
    print(f"Total leads: {len(df)}")
    print(f"With website: {len(df[df['has_website'] == 'Yes'])}")
    print(f"WITHOUT website (hot leads!): {len(df[df['has_website'] == 'No'])}")
    print(f"\nSaved to: {filename}")
    
    return filename


def main():
    # Get query from command line or prompt
    if len(sys.argv) > 1:
        query = ' '.join(sys.argv[1:])
    else:
        print("Google Maps Lead Scraper")
        print("-" * 40)
        query = input("Enter search query (e.g., 'plumbers in Chicago'): ").strip()
    
    if not query:
        print("No query provided. Exiting.")
        return
    
    # Run scraper
    results = scrape_maps(query)
    
    # Save results
    if results:
        save_results(results, query)
    else:
        print("No results found.")


if __name__ == "__main__":
    main()
