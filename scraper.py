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
# (Ideally, keep these in your .env file, but this works for now)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://jletfmpkwuoouspqwvqi.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "your_key_here") 
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_price(price_str):
    """Removes currency symbols, slashes, and returns float"""
    if not price_str: return None
    # Remove everything that isn't a digit or decimal
    clean = re.sub(r'[^\d.]', '', price_str)
    try:
        return float(clean)
    except:
        return None

def clean_seats(seats_str):
    """Extracts the first number from a string like '6 seats left'"""
    if not seats_str: return 0
    # Find all numbers in the string
    numbers = re.findall(r'\d+', seats_str)
    if numbers:
        return int(numbers[0])
    return 0

async def scrape_bark_air(page):
    print("🐶 Scraping Bark Air (Direct URL Mode)...")
    
    # Master list of cities from Bark Air's dropdowns
    cities = [
        "London", "New York", "Los Angeles", "Paris", 
        "San Francisco", "Madrid", "Seattle", "Honolulu", 
        "Lisbon"
    ]
    
    all_flights = []
    
    # Iterate through every possible "From -> To" combination
    for origin in cities:
        for dest in cities:
            if origin == dest: continue # Skip same-city flights
            
            # Construct the URL parameter (e.g., "London+To+New+York")
            route_slug = f"{origin.replace(' ', '+')}+To+{dest.replace(' ', '+')}"
            
            # The sorting parameter ensures we see the list clearly
            url = f"https://air.bark.co/collections/bookings?filter.v.option.location={route_slug}&sort_by=created-ascending"
            
            print(f"   🔎 Checking Route: {origin} -> {dest}...")
            
            try:
                await page.goto(url, timeout=30000)
                # Wait a moment for flights to render
                await page.wait_for_timeout(2000)
                
                # Check if any flights exist
                cards = await page.locator(".flight_box").all()
                
                if len(cards) == 0:
                    continue
                
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
                        
                        cleaned_price = clean_price(price_text)
                        cleaned_seats = clean_seats(seats_text)

                        all_flights.append({
                            "competitor": "Bark Air",
                            "date": raw_date,
                            # We use the loop variables so the route name is always clean and consistent
                            "route": f"{origin} -> {dest}",
                            "price": cleaned_price,
                            "seats": cleaned_seats,
                            "status": status,
                            "operator": "Gulfstream G5"
                        })
                    except Exception:
                        continue
                        
            except Exception as e:
                print(f"      ⚠️ Error checking URL: {e}")
                continue

    print(f"\nFound {len(all_flights)} TOTAL Bark flights.")
    return all_flights

async def get_dropdown_options(page, selector):
    """Helper to get value/label pairs from a dropdown"""
    options = await page.locator(f"{selector} option").all()
    results = []
    for option in options:
        val = await option.get_attribute("value")
        label = await option.inner_text()
        if val: # Skip empty placeholders like "flying from..."
            results.append({"value": val, "label": label.strip()})
    return results

async def scrape_k9_jets(page):
    print("✈️ Scraping K9 Jets (Deep Search)...")
    await page.goto("https://www.k9jets.com/routes/", timeout=60000) 
    await page.wait_for_timeout(5000)

    all_flights = []

    # 1. Get all Origins
    origins = await get_dropdown_options(page, 'select[name="pa_departure-location"]')
    print(f"   Found {len(origins)} Origins.")

    for origin in origins:
        print(f"   📍 Origin: {origin['label']}")
        
        # Capture the current "Destination" list before we change anything
        # We will use this to verify if the list actually changes later
        old_dests_text = await page.locator('select[name="pa_arrival-location"]').inner_text()
        
        # Select Origin
        await page.select_option('select[name="pa_departure-location"]', origin['value'])
        
        # --- FIX: SMART WAIT ---
        # Wait up to 5 seconds for the Destination list to update
        # We check if the text inside the dropdown is different from before
        try:
             await page.wait_for_function(
                f"document.querySelector('select[name=\"pa_arrival-location\"]').innerText !== `{old_dests_text}`",
                timeout=5000
            )
        except:
            # If it didn't change (rare, or if destinations are identical), wait a safe 3 seconds
            await page.wait_for_timeout(3000)

        # 2. Get FRESH Destinations
        dests = await get_dropdown_options(page, 'select[name="pa_arrival-location"]')
        
        for dest in dests:
            # Skip invalid/placeholder destinations
            if dest['value'] == "" or "flying to" in dest['label'].lower():
                continue
                
            print(f"      ↳ Dest: {dest['label']}")
            
            # Error handling for specific routes
            try:
                # RE-ASSERT ORIGIN (To keep session alive)
                await page.select_option('select[name="pa_departure-location"]', origin['value'])
                await page.wait_for_timeout(1000) 

                # Select Destination
                await page.select_option('select[name="pa_arrival-location"]', dest['value'])
                await page.wait_for_timeout(1000) 

                # 3. Get Months
                months = await get_dropdown_options(page, 'select[name="pa_flight-month"]')
                
                if not months:
                    # If valid route but no dates (e.g. sold out year), just skip
                    continue

                for month in months:
                    # Re-Select All (Paranoid Mode)
                    await page.select_option('select[name="pa_departure-location"]', origin['value'])
                    await page.select_option('select[name="pa_arrival-location"]', dest['value'])
                    await page.select_option('select[name="pa_flight-month"]', month['value'])
                    
                    # Click Search
                    search_btn = page.locator('.apply-filters__button')
                    if await search_btn.is_visible():
                        await search_btn.click()
                        await page.wait_for_timeout(3000)
                    
                    # Scrape Cards
                    cards = await page.locator("article.elementor-post").all()
                    
                    # Log finding
                    if len(cards) > 0:
                         print(f"         📅 {month['label']}: Found {len(cards)} flights")
                    
                    for card in cards:
                        try:
                            date_el = card.locator(".elementor-icon-box-title")
                            if await date_el.count() == 0: continue
                            raw_date = await date_el.inner_text()
                            
                            route_el = card.locator(".elementor-icon-box-description").first
                            if await route_el.count() > 0:
                                raw_text = await route_el.inner_text()
                                clean_route = raw_text.replace(" - ", " -> ").strip()
                            else:
                                clean_route = f"{origin['label']} -> {dest['label']}"

                            price_el = card.locator(".woocommerce-Price-amount").first
                            price_text = await price_el.inner_text() if await price_el.count() > 0 else "0"
                            
                            seats_el = card.locator(".stock").first
                            seats_text = await seats_el.inner_text() if await seats_el.count() > 0 else "0"
                            
                            cleaned_price = clean_price(price_text)
                            cleaned_seats = clean_seats(seats_text)
                            
                            all_flights.append({
                                "competitor": "K9 Jets",
                                "date": raw_date.strip(),
                                "route": clean_route,
                                "operator": "Pegasus/AirX", 
                                "price": cleaned_price,
                                "seats": cleaned_seats,
                                "status": "Available" if cleaned_seats > 0 else "Sold Out"
                            })
                        except Exception:
                            continue
            
            except Exception as e:
                # If a specific route fails, log it and move to the next one
                # print(f"      ⚠️ Skipped {dest['label']} (Page reset or stale)")
                continue

    print(f"Found {len(all_flights)} TOTAL K9 flights.")        
    return all_flights

async def save_to_supabase(data):
    print(f"💾 Saving {len(data)} rows to Supabase...")
    
    for item in data:
        try:
            # Parse Date
            dt_obj = parser.parse(item['date'])
            clean_date = dt_obj.strftime("%Y-%m-%d")
        except:
            print(f"⚠️ Could not parse date: {item['date']}")
            continue

        flight_payload = {
            "competitor": item['competitor'],
            "origin": item['route'].split("->")[0].strip(),
            "destination": item['route'].split("->")[-1].strip(),
            "departure_date": clean_date,
            "operator": item.get('operator')
        }
        
        # Upsert Flight
        res = supabase.table("flights").upsert(
            flight_payload, on_conflict="competitor,origin,destination,departure_date"
        ).execute()
        
        if res.data:
            flight_id = res.data[0]['id']
            
            # Insert Snapshot
            snapshot_payload = {
                "flight_id": flight_id,
                "price": item.get('price'), # This is now a float
                "seats_available": item.get('seats'), # This is now an int
                "status": item.get('status', 'Available')
            }
            supabase.table("flight_snapshots").insert(snapshot_payload).execute()
            print(f"✅ Saved snapshot for Flight {flight_id}")

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