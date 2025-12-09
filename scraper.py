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
    """Removes currency symbols, slashes, and returns float"""
    if not price_str: return None
    clean = re.sub(r'[^\d.]', '', price_str)
    try:
        return float(clean)
    except:
        return None

def clean_seats(seats_str):
    """Extracts the first number from a string like '6 seats left'"""
    if not seats_str: return 0
    numbers = re.findall(r'\d+', seats_str)
    if numbers:
        return int(numbers[0])
    return 0

async def get_dropdown_options(page, selector):
    """Helper to get value/label pairs from a dropdown"""
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
        "London", "New York", "Los Angeles", "Paris", "San Francisco", "Madrid", "Seattle", "Honolulu", "Lisbon", "Kailua-Kona"
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
    print("✈️ Scraping K9 Jets (Reset & Retry Strategy)...")
    
    await page.goto("https://www.k9jets.com/routes/", timeout=60000) 
    await page.wait_for_timeout(5000)
    
    origins = await get_dropdown_options(page, 'select[name="pa_departure-location"]')
    print(f"   Found {len(origins)} Origins to scan.")

    all_flights = []

    for origin in origins:
        print(f"   📍 Scanning Origin: {origin['label']}...")
        
        try:
            # FIX 1: Click the "Reset" button (Refresh Icon) to clear state
            reset_btn = page.locator('.jet-remove-all-filters__button')
            if await reset_btn.is_visible():
                await reset_btn.click()
                await page.wait_for_timeout(2000) # Wait for clear
            
            # Select Origin
            await page.select_option('select[name="pa_departure-location"]', origin['value'])
            
            # FIX 2: RETRY LOOP for Destinations
            # We check 5 times (total 5 seconds) to see if destinations appear
            dests = []
            for _ in range(5):
                dests = await get_dropdown_options(page, 'select[name="pa_arrival-location"]')
                # If we found valid destinations (more than just placeholder), break loop
                if len(dests) > 0 and dests[0]['value'] != "":
                    break
                await page.wait_for_timeout(1000)
            
            if not dests:
                print(f"      ⚠️ No destinations found for {origin['label']} (Skipping)")
                continue
            
            for dest in dests:
                if dest['value'] == "" or "flying to" in dest['label'].lower(): continue
                
                print(f"      ↳ Dest: {dest['label']}")
                
                try:
                    # Re-assert selections to keep session alive
                    await page.select_option('select[name="pa_departure-location"]', origin['value'])
                    await page.wait_for_timeout(200)
                    await page.select_option('select[name="pa_arrival-location"]', dest['value'])
                    await page.wait_for_timeout(1000)
                    
                    # Get Months (Retry loop here too)
                    months = []
                    for _ in range(3):
                        months = await get_dropdown_options(page, 'select[name="pa_flight-month"]')
                        if len(months) > 0: break
                        await page.wait_for_timeout(500)
                        
                    if not months: continue

                    for month in months:
                        await page.select_option('select[name="pa_flight-month"]', month['value'])
                        
                        search_btn = page.locator('.apply-filters__button')
                        if await search_btn.is_visible():
                            await search_btn.click()
                            await page.wait_for_timeout(3000)
                        
                        cards = await page.locator("article.elementor-post").all()
                        
                        if len(cards) > 0:
                            print(f"         📅 {month['label']}: Found {len(cards)} flights")
                        
                        for card in cards:
                            try:
                                date_el = card.locator(".elementor-icon-box-title")
                                if await date_el.count() == 0: continue
                                raw_date = await date_el.inner_text()
                                
                                price_el = card.locator(".woocommerce-Price-amount").first
                                price_text = await price_el.inner_text() if await price_el.count() > 0 else "0"
                                
                                seats_el = card.locator(".stock").first
                                seats_text = await seats_el.inner_text() if await seats_el.count() > 0 else "0"
                                
                                all_flights.append({
                                    "competitor": "K9 Jets",
                                    "date": raw_date.strip(),
                                    "route": f"{origin['label']} -> {dest['label']}", 
                                    "operator": "Pegasus/AirX", 
                                    "price": clean_price(price_text),
                                    "seats": clean_seats(seats_text),
                                    "status": "Available" if clean_seats(seats_text) > 0 else "Sold Out"
                                })
                            except:
                                continue
                except:
                    continue
        except Exception as e:
            print(f"      ⚠️ Error scanning origin {origin['label']}: {e}")
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