"""
Simple Google Maps Scraper - Single query, maximum leads
Usage: python maps_scraper.py "restaurants in New York"
"""
import time
import random
import sys
import os
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
    options.add_argument("--headless=new")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--single-process")
    options.add_argument("--lang=en-US")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    if os.environ.get('RENDER'):
        options.binary_location = "/usr/bin/google-chrome-stable"
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def handle_consent(driver):
    """Handle Google consent popup if present"""
    try:
        # Try to find and click "Accept all" or "Reject all" button
        consent_buttons = [
            "//button[contains(text(), 'Accept all')]",
            "//button[contains(text(), 'Reject all')]",
            "//button[contains(text(), 'Accept')]",
            "//button[@aria-label='Accept all']",
            "//form//button",
        ]
        for xpath in consent_buttons:
            try:
                btn = driver.find_element(By.XPATH, xpath)
                if btn.is_displayed():
                    btn.click()
                    print("Clicked consent button")
                    time.sleep(2)
                    return True
            except:
                continue
    except Exception as e:
        print(f"No consent popup or error: {e}")
    return False


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


def extract_from_list_view(driver, callback=None):
    """Extract business info directly from list view - FAST method for cloud"""
    results = []
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find all business cards in the feed - try multiple selectors
        listings = soup.find_all('div', class_='Nv2PK')
        
        # Fallback selectors if primary doesn't work
        if not listings:
            listings = soup.find_all('div', class_='lI9IFe')
        if not listings:
            # Try finding by the link pattern
            feed = soup.find('div', {'role': 'feed'})
            if feed:
                listings = feed.find_all('div', recursive=False)
        
        if callback:
            callback("extracting_list", {"total": len(listings), "message": f"Extracting {len(listings)} businesses from list..."})
        
        for i, listing in enumerate(listings):
            try:
                # Business name
                name_elem = listing.find('div', class_='qBF1Pd')
                if not name_elem:
                    name_elem = listing.find('span', class_='OSrXXb')
                name = name_elem.text.strip() if name_elem else ""
                
                if not name:
                    continue
                
                # Rating and reviews
                rating = ""
                reviews = ""
                rating_elem = listing.find('span', class_='MW4etd')
                if rating_elem:
                    rating = rating_elem.text.strip()
                reviews_elem = listing.find('span', class_='UY7F9')
                if reviews_elem:
                    reviews = reviews_elem.text.strip().replace('(', '').replace(')', '')
                
                # Category/type
                category = ""
                category_spans = listing.find_all('span')
                for span in category_spans:
                    text = span.text.strip()
                    if text and '·' in text:
                        parts = text.split('·')
                        if len(parts) > 0:
                            category = parts[0].strip()
                            break
                
                # Address - usually in the W4Efsd class
                address = ""
                info_divs = listing.find_all('div', class_='W4Efsd')
                for div in info_divs:
                    text = div.text.strip()
                    # Look for address patterns (contains numbers or common address words)
                    if any(x in text.lower() for x in ['street', 'road', 'ave', 'blvd', 'dr', 'lane', 'st,', 'rd,']) or re.search(r'\d{2,}', text):
                        address = text
                        break
                
                # Google Maps link
                link_elem = listing.find('a', class_='hfpxzc')
                maps_link = ""
                if link_elem:
                    maps_link = link_elem.get('href', '')
                
                # Website - check for website indicator
                has_website = "Unknown"
                website_elem = listing.find('a', {'data-value': 'Website'})
                if website_elem:
                    has_website = "Yes"
                
                results.append({
                    'business_name': name,
                    'category': category,
                    'address': address,
                    'phone': '',  # Not available in list view
                    'website': '',  # Need to click for this
                    'has_website': has_website,
                    'rating': rating,
                    'reviews': reviews,
                    'google_maps_link': maps_link
                })
                
                if callback and (i + 1) % 10 == 0:
                    callback("extracting", {
                        "current": i + 1,
                        "total": len(listings),
                        "collected": len(results),
                        "business": name
                    })
                    
            except Exception as e:
                continue
        
        print(f"Extracted {len(results)} businesses from list view")
        
        # FALLBACK: If no results, try extracting from aria-label on links
        if not results:
            print("Primary extraction failed, trying fallback method...")
            links = soup.find_all('a', class_='hfpxzc')
            for link in links:
                try:
                    aria_label = link.get('aria-label', '')
                    href = link.get('href', '')
                    if aria_label:
                        results.append({
                            'business_name': aria_label,
                            'category': '',
                            'address': '',
                            'phone': '',
                            'website': '',
                            'has_website': 'Unknown',
                            'rating': '',
                            'reviews': '',
                            'google_maps_link': href
                        })
                except:
                    continue
            print(f"Fallback extracted {len(results)} businesses")
        
    except Exception as e:
        print(f"List extraction error: {e}")
    
    return results


def extract_business_info(driver):
    """Extract business info from the details panel"""
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        google_maps_link = driver.current_url
        
        name_elem = soup.find('h1', class_='DUwDvf')
        name = name_elem.text.strip() if name_elem else ""
        
        if not name:
            return None
        
        rating = ""
        rating_elem = soup.find('span', class_='ceNzKf')
        if rating_elem:
            rating = rating_elem.get('aria-label', '')
        
        reviews = ""
        reviews_elem = soup.find('span', class_='F7nice')
        if reviews_elem:
            reviews = reviews_elem.text.strip()
        
        address = ""
        address_button = soup.find('button', {'data-item-id': 'address'})
        if address_button:
            address = address_button.text.strip()
        
        phone = ""
        phone_button = soup.find('button', {'data-item-id': re.compile(r'phone:tel:')})
        if phone_button:
            phone = phone_button.text.strip()
        
        website = ""
        website_link = soup.find('a', {'data-item-id': 'authority'})
        if website_link:
            website = website_link.get('href', '')
        
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


def scrape_maps_with_progress(query, callback=None, fast_mode=True, detail_limit=10):
    """Main scraping function with progress callback
    
    Args:
        query: Search query
        callback: Progress callback function
        fast_mode: If True, extract from list view (fast, works on cloud)
                   If False, click each business for full details (slow, may fail on cloud)
        detail_limit: When fast_mode=False, limit how many businesses to extract details from
    """
    print(f"\n{'='*60}")
    print(f"Google Maps Scraper {'(FAST MODE)' if fast_mode else '(DETAIL MODE)'}")
    print(f"Query: {query}")
    print(f"{'='*60}\n")
    
    if callback:
        callback("browser_starting", {"message": "Starting Chrome browser..."})
    
    driver = setup_driver()
    results = []
    
    try:
        maps_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        print(f"Opening: {maps_url}")
        if callback:
            callback("navigating", {"message": f"Opening Google Maps: {query}"})
        driver.get(maps_url)
        time.sleep(3)
        
        # Handle consent popup
        handle_consent(driver)
        time.sleep(2)
        
        try:
            if callback:
                callback("waiting", {"message": "Waiting for results to load..."})
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
            )
        except:
            # Try to save screenshot for debugging
            try:
                driver.save_screenshot("/app/leads/debug_screenshot.png")
                print(f"Page title: {driver.title}")
                print(f"Current URL: {driver.current_url}")
            except:
                pass
            print("Could not find results panel. Try a different query.")
            if callback:
                callback("error", {"message": "Could not find results panel - Google may be blocking or showing consent page"})
            return []
        
        if callback:
            callback("scrolling", {"count": 0, "message": "Scrolling to load all results..."})
        scrollable_div = driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
        scroll_results(driver, scrollable_div, callback)
        
        # Count total listings
        listings = driver.find_elements(By.CSS_SELECTOR, "div[role='feed'] > div > div > a")
        total_listings = len(listings)
        print(f"\nFound {total_listings} listings to process")
        
        if callback:
            callback("listings_found", {"total": total_listings, "message": f"Found {total_listings} businesses"})
        
        if fast_mode:
            # FAST MODE: Extract directly from list view (works on cloud!)
            if callback:
                callback("extracting_fast", {"message": "Using fast extraction mode..."})
            results = extract_from_list_view(driver, callback)
            
            # Optionally get details for first few businesses
            if detail_limit > 0 and len(listings) > 0:
                if callback:
                    callback("enriching", {"message": f"Getting phone/website for first {min(detail_limit, len(listings))} businesses..."})
                
                for i, listing in enumerate(listings[:detail_limit]):
                    try:
                        driver.execute_script("arguments[0].scrollIntoView(true);", listing)
                        time.sleep(0.3)
                        listing.click()
                        time.sleep(random.uniform(1.0, 1.5))
                        
                        info = extract_business_info(driver)
                        if info:
                            # Update the matching result with phone/website
                            for r in results:
                                if r['business_name'] == info['business_name']:
                                    r['phone'] = info['phone']
                                    r['website'] = info['website']
                                    r['has_website'] = info['has_website']
                                    r['address'] = info['address'] or r['address']
                                    break
                        
                        if callback:
                            callback("enriching_progress", {
                                "current": i + 1,
                                "total": min(detail_limit, len(listings)),
                                "business": info.get("business_name", "Unknown") if info else "Unknown"
                            })
                    except Exception as e:
                        print(f"Detail extraction error: {e}")
                        continue
        else:
            # SLOW MODE: Click each business (may fail on cloud IPs)
            for i, listing in enumerate(listings):
                if detail_limit > 0 and i >= detail_limit:
                    print(f"Reached detail limit of {detail_limit}")
                    break
                    
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", listing)
                    time.sleep(0.3)
                    listing.click()
                    time.sleep(random.uniform(1.2, 2))
                    
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
    
    print(f"\nTotal results: {len(results)}")
    return results


def save_results(results, query):
    """Save results to CSV"""
    if not results:
        print("No results to save")
        return
    
    df = pd.DataFrame(results)
    safe_query = re.sub(r'[^\w\s-]', '', query).replace(' ', '_')[:30]
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"leads_{safe_query}_{date_str}.csv"
    
    df.to_csv(filename, index=False)
    print(f"\nSaved {len(df)} leads to {filename}")
    return filename


def main():
    if len(sys.argv) > 1:
        query = ' '.join(sys.argv[1:])
    else:
        print("Google Maps Lead Scraper")
        query = input("Enter search query: ").strip()
    
    if not query:
        print("No query provided.")
        return
    
    results = scrape_maps(query)
    if results:
        save_results(results, query)


if __name__ == "__main__":
    main()
