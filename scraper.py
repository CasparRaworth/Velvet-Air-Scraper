import asyncio
import re
from playwright.async_api import async_playwright
from supabase import create_client, Client
from datetime import datetime
import os
from dateutil import parser
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not found. Check your .env or GitHub Secrets.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_price(price_str):
    if not price_str: return None
    clean = re.sub(r'[^\d.]', '', price_str)
    try:
        return float(clean)
    except:
        return None

def clean_seats(seats_str):
    if not seats_str: return 0
    numbers = re.findall(r'\d+', seats_str)
    if numbers:
        return int(numbers[0])
    return 0

async def handle_cookie_banner(page):
    """Checks for and closes the K9 cookie banner if it exists"""
    try:
        # Try multiple common selectors for the "Accept" button
        # The logs suggest a 'cmplz' (Complianz) banner
        banner_btn = page.locator(".cmplz-accept, .cmplz-btn.cmplz-accept, #ucc-c-btn")
        if await banner_btn.count() > 0 and await banner_btn.is_visible():
            print("   🍪 Cookie banner detected. Smashing it...")
            await banner_btn.first.click()
            await page.wait_for_timeout(1000) # Wait for animation to clear
    except Exception as e:
        # It's okay if we don't find it, maybe it's already gone
        pass

async def get_dropdown_options(page, selector):
    try:
        await page.wait_for_selector(f"{selector} option", timeout=5000)
    except:
        pass 
    options = await page.locator(f"{selector} option").all()
    results = []
    for option in options:
        val = await option.get_attribute("value")
        label = await option.inner_text()
        if val: 
            results.append({"value": val, "label": label.strip()})
    return results

async def scrape_bark_air(page):
    print("🐶 Scraping Bark Air (Direct URL Mode)...")
    
    cities = [
        "London", "New York", "Los Angeles", "Paris", 
        "San Francisco", "Madrid", "Seattle", "Honolulu", 
        "Lisbon", "Kailua-Kona"
    ]
    
    all_flights = []
    
    for origin in cities:
        for dest in cities:
            if origin == dest: continue 
            
            route_slug = f"{origin.replace(' ', '+')}+To+{dest.replace(' ', '+')}"
            url = f"https://air.bark.co/collections/bookings?filter.v.option.location={route_slug}&sort_by=created-ascending"
            
            print(f"   🔎 Checking Route: {origin} -> {dest}...")
            
            try:
                await page.goto(url, timeout=30000)
                await page.wait_for_timeout(1500)
                
                cards = await page.locator(".flight_box").all()
                if len(cards) == 0: continue
                
                print(f"      ✅ Found {len(cards)} flights!")

                for card in cards:
                    try:
                        header = card.locator(".flight_details").first 
                        raw_date = await header.get_attribute("data-flight-date")
                        if not raw_date: continue

                        price_el = card.locator(".price-item--regular").first
                        price_text = await price_el.inner_text() if await price_el.count() > 0 else "0"
                        
                        seats_el = card.locator(".flight-availability-info").first
                        seats_text = await seats_el.inner_text() if await seats_el.count() > 0 else "0"
                        
                        is_sold_out = await card.locator(".sold-out-tag").count() > 0
                        status = "Sold Out" if is_sold_out else "Available"
                        
                        all_flights.append({
                            "competitor": "Bark Air",
                            "date": raw_date,
                            "route": f"{origin} -> {dest}", 
                            "price": clean_price(price_text),
                            "seats": clean_seats(seats_text),
                            "status": status,
                            "operator": "Gulfstream G5"
                        })
                    except Exception:
                        continue
            except Exception as e:
                continue

    print(f"\nFound {len(all_flights)} TOTAL Bark flights.")
    return all_flights

async def scrape_k9_jets(page):
    """
    K9 Jets scraper using a hybrid approach:
    1. Try direct URL construction with filter parameters
    2. Fallback to scraping all visible flights and parsing route from text
    """
    print("✈️ Scraping K9 Jets (Direct URL + All Flights Approach)...")
    
    all_flights = []
    
    # STRATEGY 1: Get all dropdown combinations and try direct URLs
    print("   📋 Strategy 1: Attempting filter combinations via URL...")
    
    await page.goto("https://www.k9jets.com/routes/", timeout=60000)
    await handle_cookie_banner(page)
    await page.wait_for_timeout(2000)
    
    # Get all possible filter values from the page
    origins = await get_dropdown_options(page, 'select[name="pa_departure-location"]')
    destinations = await get_dropdown_options(page, 'select[name="pa_arrival-location"]')
    months = await get_dropdown_options(page, 'select[name="pa_flight-month"]')
    
    print(f"   Found {len(origins)} origins, {len(destinations)} destinations, {len(months)} months")
    
    # Try each combination via URL parameters
    tested_combinations = 0
    for origin in origins:
        for dest in destinations:
            if origin['value'] == "" or dest['value'] == "": continue
            if "flying from" in origin['label'].lower() or "flying to" in dest['label'].lower(): continue
            
            # K9 Jets uses WooCommerce variation attributes in URLs
            # Format: ?filter_departure-location=london-uk&filter_arrival-location=new-jersey-us
            url = f"https://www.k9jets.com/routes/?filter_departure-location={origin['value']}&filter_arrival-location={dest['value']}"
            
            try:
                await page.goto(url, timeout=15000)
                await page.wait_for_timeout(1500)
                
                cards = await page.locator("article.elementor-post").all()
                if len(cards) > 0:
                    tested_combinations += 1
                    print(f"   ✅ {origin['label']} → {dest['label']}: {len(cards)} flights")
                    
                    for card in cards:
                        try:
                            # Extract date
                            date_el = card.locator(".elementor-icon-box-title")
                            if await date_el.count() == 0: continue
                            raw_date = await date_el.inner_text()
                            
                            # Extract route from card description
                            route_el = card.locator(".elementor-icon-box-description")
                            route_text = await route_el.inner_text() if await route_el.count() > 0 else ""
                            
                            # Extract price
                            price_el = card.locator(".woocommerce-Price-amount").first
                            price_text = await price_el.inner_text() if await price_el.count() > 0 else "0"
                            
                            # Extract seats
                            seats_el = card.locator(".stock").first
                            seats_text = await seats_el.inner_text() if await seats_el.count() > 0 else "0"
                            
                            # Extract operator (if available)
                            operator_el = card.locator("p.elementor-heading-title")
                            operator_text = "Unknown"
                            for i in range(await operator_el.count()):
                                text = await operator_el.nth(i).inner_text()
                                if "Operator:" in text:
                                    operator_text = text.replace("Operator:", "").strip()
                                    break
                            
                            all_flights.append({
                                "competitor": "K9 Jets",
                                "date": raw_date.strip(),
                                "route": route_text.strip() if route_text else f"{origin['label']} -> {dest['label']}", 
                                "operator": operator_text,
                                "price": clean_price(price_text),
                                "seats": clean_seats(seats_text),
                                "status": "Available" if clean_seats(seats_text) > 0 else "Sold Out"
                            })
                        except:
                            continue
            except:
                continue
            
            # Rate limiting
            if tested_combinations % 10 == 0:
                await page.wait_for_timeout(1000)
    
    # STRATEGY 2: If Strategy 1 found nothing, scrape everything visible on main page
    if len(all_flights) == 0:
        print("   📋 Strategy 2: Scraping all visible flights from main page...")
        
        await page.goto("https://www.k9jets.com/routes/", timeout=60000)
        await handle_cookie_banner(page)
        await page.wait_for_timeout(2000)
        
        # Scroll to load lazy-loaded content
        for _ in range(5):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await page.wait_for_timeout(500)
        
        cards = await page.locator("article.elementor-post").all()
        print(f"   Found {len(cards)} total visible flight cards")
        
        for card in cards:
            try:
                # Extract date
                date_el = card.locator(".elementor-icon-box-title")
                if await date_el.count() == 0: continue
                raw_date = await date_el.inner_text()
                
                # Extract route
                route_el = card.locator(".elementor-icon-box-description")
                route_text = await route_el.inner_text() if await route_el.count() > 0 else "Unknown Route"
                
                # Extract price
                price_el = card.locator(".woocommerce-Price-amount").first
                price_text = await price_el.inner_text() if await price_el.count() > 0 else "0"
                
                # Extract seats
                seats_el = card.locator(".stock").first
                seats_text = await seats_el.inner_text() if await seats_el.count() > 0 else "0"
                
                # Extract operator
                operator_el = card.locator("p.elementor-heading-title")
                operator_text = "Unknown"
                for i in range(await operator_el.count()):
                    text = await operator_el.nth(i).inner_text()
                    if "Operator:" in text:
                        operator_text = text.replace("Operator:", "").strip()
                        break
                
                all_flights.append({
                    "competitor": "K9 Jets",
                    "date": raw_date.strip(),
                    "route": route_text.strip(),
                    "operator": operator_text,
                    "price": clean_price(price_text),
                    "seats": clean_seats(seats_text),
                    "status": "Available" if clean_seats(seats_text) > 0 else "Sold Out"
                })
            except Exception as e:
                continue

    print(f"Found {len(all_flights)} TOTAL K9 flights.")        
    return all_flights

async def save_to_supabase(data):
    print(f"💾 Processing {len(data)} scraped rows...")
    
    unique_data = {}
    for item in data:
        signature = f"{item['competitor']}_{item['route']}_{item['date']}"
        unique_data[signature] = item
    
    clean_data = list(unique_data.values())
    print(f"   📉 Deduplicated: Removed {len(data) - len(clean_data)} duplicate entries.")
    print(f"   🚀 Uploading {len(clean_data)} unique snapshots to Supabase...")

    for item in clean_data:
        try:
            dt_obj = parser.parse(item['date'])
            clean_date = dt_obj.strftime("%Y-%m-%d")
        except:
            continue

        flight_payload = {
            "competitor": item['competitor'],
            "origin": item['route'].split("->")[0].strip(),
            "destination": item['route'].split("->")[-1].strip(),
            "departure_date": clean_date,
            "operator": item.get('operator')
        }
        
        res = supabase.table("flights").upsert(
            flight_payload, on_conflict="competitor,origin,destination,departure_date"
        ).execute()
        
        if res.data:
            flight_id = res.data[0]['id']
            snapshot_payload = {
                "flight_id": flight_id,
                "price": item.get('price'),
                "seats_available": item.get('seats'),
                "status": item.get('status', 'Available')
            }
            supabase.table("flight_snapshots").insert(snapshot_payload).execute()

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        bark_data = await scrape_bark_air(page)
        k9_data = await scrape_k9_jets(page)
        
        await save_to_supabase(bark_data + k9_data)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())