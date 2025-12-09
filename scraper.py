import asyncio
import re
from datetime import datetime
import os

import httpx
from dateutil import parser
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from supabase import Client, create_client

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


def _strip_html(text: str) -> str:
    """Very small helper to remove HTML tags."""
    return re.sub(r"<.*?>", "", text, flags=re.S).strip()


def _extract_select_options(html: str, select_name: str) -> list[dict]:
    """Parse <select name="..."> options from raw HTML without extra deps."""
    select_regex = re.compile(
        rf'<select[^>]*name=["\']{re.escape(select_name)}["\'][^>]*>(.*?)</select>',
        re.I | re.S,
    )
    match = select_regex.search(html)
    if not match:
        return []

    inner = match.group(1)
    option_regex = re.compile(
        r'<option[^>]*value=["\']([^"\']+)["\'][^>]*>(.*?)</option>', re.I | re.S
    )

    options: list[dict] = []
    for value, label in option_regex.findall(inner):
        label_clean = _strip_html(label)
        value_clean = value.strip()
        if value_clean:
            options.append({"value": value_clean, "label": label_clean})
    return options


def _extract_k9_flights_from_html(html: str) -> list[dict]:
    """Parse K9 flight cards from routes HTML using regex-based extraction."""
    flights: list[dict] = []

    # Grab each <article ... elementor-post ...>...</article>
    article_regex = re.compile(
        r'<article[^>]*class="[^"]*elementor-post[^"]*"[^>]*>(.*?)</article>',
        re.I | re.S,
    )

    title_regex = re.compile(
        r'class="[^"]*elementor-icon-box-title[^"]*"[^>]*>(.*?)</', re.I | re.S
    )
    route_regex = re.compile(
        r'class="[^"]*elementor-icon-box-description[^"]*"[^>]*>(.*?)</',
        re.I | re.S,
    )
    price_regex = re.compile(
        r'class="[^"]*woocommerce-Price-amount[^"]*"[^>]*>(.*?)</', re.I | re.S
    )
    seats_regex = re.compile(
        r'class="[^"]*stock[^"]*"[^>]*>(.*?)</', re.I | re.S
    )
    operator_regex = re.compile(
        r'<p[^>]*class="[^"]*elementor-heading-title[^"]*"[^>]*>(.*?)</p>',
        re.I | re.S,
    )

    for article_html in article_regex.findall(html):
        date_match = title_regex.search(article_html)
        if not date_match:
            continue
        raw_date = _strip_html(date_match.group(1))
        if not raw_date:
            continue

        route_match = route_regex.search(article_html)
        raw_route = (
            _strip_html(route_match.group(1)) if route_match else "Unknown Route"
        )

        price_match = price_regex.search(article_html)
        raw_price = _strip_html(price_match.group(1)) if price_match else "0"

        seats_match = seats_regex.search(article_html)
        raw_seats = _strip_html(seats_match.group(1)) if seats_match else "0"

        operator_match = operator_regex.search(article_html)
        operator_text = "Unknown"
        if operator_match:
            op_raw = _strip_html(operator_match.group(1))
            if "Operator:" in op_raw:
                operator_text = op_raw.replace("Operator:", "").strip()
            elif op_raw:
                operator_text = op_raw

        flights.append(
            {
                "competitor": "K9 Jets",
                "date": raw_date,
                "route": raw_route,
                "operator": operator_text,
                "price": clean_price(raw_price),
                "seats": clean_seats(raw_seats),
                "status": "Available" if clean_seats(raw_seats) > 0 else "Sold Out",
            }
        )

    return flights

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

async def scrape_k9_jets_ajax(page):
    """
    AJAX-driven approach: Interact with dropdowns to get all origin/destination combinations.
    Returns list of flights if successful, empty list if AJAX fails.
    """
    print("   🔄 Strategy 1: AJAX Filter Approach...")
    
    all_flights = []
    seen_flights = set()
    
    try:
        await page.goto("https://www.k9jets.com/routes/", timeout=60000)
        await handle_cookie_banner(page)
        await page.wait_for_timeout(3000)  # Give AJAX time to initialize
        
        # Get initial origin options
        origins = await get_dropdown_options(page, 'select[name="pa_departure-location"]')
        origins = [o for o in origins if o['value'] and "flying from" not in o['label'].lower()]
        
        if len(origins) == 0:
            print("      ⚠️  No origin options found")
            return []
        
        print(f"      → Found {len(origins)} origins to test")
        
        # Try first origin to see if destinations populate
        test_origin = origins[0]
        await page.select_option('select[name="pa_departure-location"]', test_origin['value'])
        await page.wait_for_timeout(3000)  # Wait for AJAX
        
        test_dests = await get_dropdown_options(page, 'select[name="pa_arrival-location"]')
        test_dests = [d for d in test_dests if d['value'] and "flying to" not in d['label'].lower()]
        
        if len(test_dests) == 0:
            print("      ⚠️  AJAX not populating destinations (headless issue)")
            return []
        
        print(f"      ✅ AJAX working! Found {len(test_dests)} destinations for test origin")
        
        # Full AJAX scrape
        for idx, origin in enumerate(origins):
            try:
                await page.goto("https://www.k9jets.com/routes/", timeout=60000)
                await handle_cookie_banner(page)
                await page.wait_for_timeout(2000)
                
                await page.select_option('select[name="pa_departure-location"]', origin['value'])
                await page.wait_for_timeout(2500)
                
                destinations = await get_dropdown_options(page, 'select[name="pa_arrival-location"]')
                destinations = [d for d in destinations if d['value'] and "flying to" not in d['label'].lower()]
                
                for dest in destinations:
                    await page.select_option('select[name="pa_arrival-location"]', dest['value'])
                    await page.wait_for_timeout(500)
                    
                    search_btn = page.locator('.apply-filters__button')
                    if await search_btn.count() > 0:
                        await search_btn.click()
                        await page.wait_for_timeout(3000)
                        
                        cards = await page.locator("article.elementor-post").all()
                        if len(cards) > 0:
                            print(f"      [{idx+1}/{len(origins)}] {origin['label']} → {dest['label']}: {len(cards)} flights")
                        
                        for card in cards:
                            try:
                                date_el = card.locator(".elementor-icon-box-title")
                                if await date_el.count() == 0: continue
                                raw_date = await date_el.inner_text()
                                
                                route_el = card.locator(".elementor-icon-box-description")
                                route_text = await route_el.inner_text() if await route_el.count() > 0 else f"{origin['label']} -> {dest['label']}"
                                
                                flight_key = f"{raw_date}|{route_text}"
                                if flight_key in seen_flights:
                                    continue
                                seen_flights.add(flight_key)
                                
                                price_el = card.locator(".woocommerce-Price-amount").first
                                price_text = await price_el.inner_text() if await price_el.count() > 0 else "0"
                                
                                seats_el = card.locator(".stock").first
                                seats_text = await seats_el.inner_text() if await seats_el.count() > 0 else "0"
                                
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
                            except:
                                continue
                        
                        await page.goto("https://www.k9jets.com/routes/", timeout=60000)
                        await page.wait_for_timeout(1000)
            except:
                continue
        
        return all_flights
        
    except Exception as e:
        print(f"      ⚠️  AJAX approach failed: {str(e)[:100]}")
        return []

async def scrape_k9_jets_fallback(page):
    """
    Fallback: Scrape all visible flights with enhanced scrolling.
    """
    print("   📜 Strategy 2: Enhanced Scrolling (Fallback)...")
    
    all_flights = []
    seen_flights = set()
    
    await page.goto("https://www.k9jets.com/routes/", timeout=60000)
    await handle_cookie_banner(page)
    await page.wait_for_timeout(2000)
    
    # Aggressive scrolling
    previous_count = 0
    no_change_count = 0
    max_scrolls = 50
    
    for _ in range(max_scrolls):
        current_count = await page.locator("article.elementor-post").count()
        
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1500)
        await page.evaluate("window.scrollBy(0, -500)")
        await page.wait_for_timeout(500)
        
        if current_count == previous_count:
            no_change_count += 1
            if no_change_count >= 3:
                print(f"      → Loaded {current_count} flights total")
                break
        else:
            no_change_count = 0
        
        previous_count = current_count
    
    cards = await page.locator("article.elementor-post").all()
    print(f"      → Scraping {len(cards)} visible cards...")
    
    for card in cards:
        try:
            date_el = card.locator(".elementor-icon-box-title")
            if await date_el.count() == 0:
                continue
            raw_date = await date_el.inner_text()

            route_el = card.locator(".elementor-icon-box-description")
            route_text = (
                await route_el.inner_text()
                if await route_el.count() > 0
                else "Unknown Route"
            )

            flight_key = f"{raw_date}|{route_text}"
            if flight_key in seen_flights:
                continue
            seen_flights.add(flight_key)

            price_el = card.locator(".woocommerce-Price-amount").first
            price_text = (
                await price_el.inner_text() if await price_el.count() > 0 else "0"
            )

            seats_el = card.locator(".stock").first
            seats_text = (
                await seats_el.inner_text() if await seats_el.count() > 0 else "0"
            )

            operator_el = card.locator("p.elementor-heading-title")
            operator_text = "Unknown"
            for i in range(await operator_el.count()):
                text = await operator_el.nth(i).inner_text()
                if "Operator:" in text:
                    operator_text = text.replace("Operator:", "").strip()
                    break

            all_flights.append(
                {
            "competitor": "K9 Jets",
            "date": raw_date.strip(),
                    "route": route_text.strip(),
                    "operator": operator_text,
            "price": clean_price(price_text),
                    "seats": clean_seats(seats_text),
                    "status": "Available"
                    if clean_seats(seats_text) > 0
                    else "Sold Out",
                }
            )
        except:
            continue
    
    return all_flights

async def scrape_k9_jets(page):
    """
    Hybrid K9 Jets scraper: Try AJAX approach first, fall back to scrolling if needed.
    """
    print("✈️ Scraping K9 Jets (Hybrid: AJAX + Fallback)...")
    
    # Try AJAX approach first
    ajax_flights = await scrape_k9_jets_ajax(page)
    
    if len(ajax_flights) > 100:  # AJAX succeeded and got more than the 100-card limit
        print(f"   ✅ AJAX approach successful!")
        print(f"Found {len(ajax_flights)} TOTAL K9 flights.")
        return ajax_flights
    
    # Fall back to scrolling
    print("   → AJAX failed or returned limited results, using fallback...")
    fallback_flights = await scrape_k9_jets_fallback(page)
    print(f"Found {len(fallback_flights)} TOTAL K9 flights.")
    return fallback_flights


async def scrape_k9_jets_http() -> list[dict]:
    """
    HTTP-only K9 Jets scraper that calls the same endpoints as the site,
    avoiding headless / AJAX timing issues.

    Strategy:
      1. GET /routes/ and parse all departure options (pa_departure-location).
      2. For each origin ID, POST back to /routes/ with:
            jsf = epro-posts/default
            _tax_query_pa_departure-location = <origin_id>
            jet-smart-filters-redirect = 1
         and parse all resulting flight cards.
    """
    print("✈️ Scraping K9 Jets via direct HTTP (no headless limitations)...")

    base_url = "https://www.k9jets.com"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": f"{base_url}/routes/",
    }

    all_flights: list[dict] = []
    seen_keys: set[str] = set()

    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        try:
            resp = await client.get(f"{base_url}/routes/")
            resp.raise_for_status()
        except Exception as e:
            print(f"   ⚠️ HTTP error loading routes page: {e}")
            return []

        html = resp.text
        origins = _extract_select_options(html, "pa_departure-location")
        # Filter out placeholders like "Flying from..."
        origins = [
            o
            for o in origins
            if o["value"] and "flying from" not in o["label"].lower()
        ]

        if not origins:
            print("   ⚠️ No origin options found in HTML.")
            return []

        print(f"   → Found {len(origins)} origin options from HTML.")

        for idx, origin in enumerate(origins, start=1):
            origin_id = origin["value"]
            origin_label = origin["label"]
            print(f"   📍 [{idx}/{len(origins)}] Origin: {origin_label} (id={origin_id})")

            data = {
                "jsf": "epro-posts/default",
                "_tax_query_pa_departure-location": origin_id,
                "jet-smart-filters-redirect": "1",
            }

            try:
                resp = await client.post(f"{base_url}/routes/", data=data)
                resp.raise_for_status()
            except Exception as e:
                print(f"      ⚠️ HTTP error for origin {origin_label}: {e}")
                continue

            flights = _extract_k9_flights_from_html(resp.text)
            print(f"      → Found {len(flights)} flights for origin {origin_label}")

            for f in flights:
                # Ensure route has a sensible format; if not, prefix origin.
                route = f.get("route") or "Unknown Route"
                if "->" not in route and " to " not in route.lower():
                    route = f"{origin_label} -> {route}"
                    f["route"] = route

                key = f"{f['date']}|{f['route']}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                all_flights.append(f)

    print(f"   ✅ HTTP K9 scraper collected {len(all_flights)} unique flights.")
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

        # Parse origin / destination from the route string
        route_str = item.get("route", "").strip()
        if "->" in route_str:
            left, right = route_str.split("->", 1)
            origin = left.strip()
            dest_raw = right.strip()
            # K9 routes often look like "London, UK - Van Nuys, California"
            # In that case we only want the final destination after the last " - "
            if " - " in dest_raw:
                destination = dest_raw.split(" - ")[-1].strip()
            else:
                destination = dest_raw
        else:
            # Fallback: treat the whole string as both origin and destination
            origin = route_str
            destination = route_str

        flight_payload = {
            "competitor": item['competitor'],
            "origin": origin,
            "destination": destination,
            "departure_date": clean_date,
            "operator": item.get('operator')
        }
        
        res = supabase.table("flights").upsert(
            flight_payload, on_conflict="competitor,origin,destination,departure_date"
        ).execute()
        
        if res.data:
            flight_id = res.data[0]["id"]
        snapshot_payload = {
            "flight_id": flight_id,
                "price": item.get("price"),
                "seats_available": item.get("seats"),
                "status": item.get("status", "Available"),
        }
        supabase.table("flight_snapshots").insert(snapshot_payload).execute()

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        bark_data = await scrape_bark_air(page)
        # Prefer HTTP-based K9 scraper; fall back to Playwright hybrid if needed.
        k9_http_data = await scrape_k9_jets_http()
        k9_data = k9_http_data or await scrape_k9_jets(page)
        
        await save_to_supabase(bark_data + k9_data)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())