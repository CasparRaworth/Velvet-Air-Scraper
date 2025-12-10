#!/usr/bin/env python3
"""
Diagnostic script to debug K9 Jets price extraction.
Fetches a single flight detail page and shows exactly what's being matched.
"""

import re
import httpx

def _strip_html(text: str) -> str:
    """Remove HTML tags."""
    return re.sub(r"<.*?>", "", text, flags=re.S).strip()

def clean_price(price_str):
    if not price_str:
        return None
    clean = re.sub(r'[^\d.]', '', price_str)
    try:
        return float(clean)
    except:
        return None

def debug_k9_page(url: str):
    print(f"\n{'='*60}")
    print(f"Debugging: {url}")
    print(f"{'='*60}\n")
    
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    
    resp = httpx.get(url, headers=headers, timeout=30)
    html = resp.text
    
    # Save raw HTML for inspection
    with open("debug_k9_page.html", "w") as f:
        f.write(html)
    print("📄 Saved full HTML to debug_k9_page.html\n")
    
    # ========== METHOD 1: BDI regex (current approach) ==========
    print("=" * 40)
    print("METHOD 1: BDI regex matches")
    print("=" * 40)
    
    bdi_regex = re.compile(
        r'class="[^"]*woocommerce-Price-amount[^"]*"[^>]*>\s*<bdi[^>]*>(.*?)</bdi>',
        re.I | re.S,
    )
    
    bdi_matches = bdi_regex.findall(html)
    print(f"Found {len(bdi_matches)} matches:\n")
    
    import html as html_module
    for i, match in enumerate(bdi_matches[:20]):  # Limit to first 20
        stripped = _strip_html(match)
        unescaped = html_module.unescape(stripped)  # THE FIX!
        price_wrong = clean_price(stripped)  # Old (buggy) way
        price_fixed = clean_price(unescaped)  # New (correct) way
        print(f"  [{i+1}] Raw: {match[:80]!r}...")
        print(f"       Stripped: {stripped!r}")
        print(f"       Unescaped: {unescaped!r}")
        print(f"       Price (OLD/buggy): {price_wrong}")
        print(f"       Price (NEW/fixed): {price_fixed}")
        print()
    
    # ========== METHOD 2: Dollar regex (fallback) ==========
    print("\n" + "=" * 40)
    print("METHOD 2: Dollar regex matches")
    print("=" * 40)
    
    dollar_regex = re.compile(r'\$[\d,]+\.?\d*')
    
    dollar_matches = dollar_regex.findall(html)
    print(f"Found {len(dollar_matches)} matches:\n")
    
    # Show unique values and their counts
    from collections import Counter
    counts = Counter(dollar_matches)
    for match, count in counts.most_common(20):
        price = clean_price(match)
        print(f"  {match!r} (x{count}) -> {price}")
    
    # ========== METHOD 3: Find the specific price container ==========
    print("\n" + "=" * 40)
    print("METHOD 3: Looking for 'Price:' text context")
    print("=" * 40)
    
    # Find context around "Price:" text
    price_context_regex = re.compile(
        r'Price:</span>\s*(.*?)</p>',
        re.I | re.S,
    )
    
    context_matches = price_context_regex.findall(html)
    print(f"Found {len(context_matches)} 'Price:' context matches:\n")
    
    for i, match in enumerate(context_matches[:10]):
        stripped = _strip_html(match)
        print(f"  [{i+1}] Raw: {match[:100]!r}...")
        print(f"       Stripped: {stripped!r}")
        print()
    
    # ========== METHOD 4: Stock/seats ==========
    print("\n" + "=" * 40)
    print("METHOD 4: Seats/stock info")
    print("=" * 40)
    
    stock_regex = re.compile(
        r'<p[^>]*class="[^"]*stock[^"]*"[^>]*>(.*?)</p>',
        re.I | re.S,
    )
    
    stock_matches = stock_regex.findall(html)
    print(f"Found {len(stock_matches)} stock matches:\n")
    
    for i, match in enumerate(stock_matches[:10]):
        stripped = _strip_html(match)
        print(f"  [{i+1}] {stripped!r}")

if __name__ == "__main__":
    # Grab a sample K9 flight detail URL
    # First, let's get the routes page and find a detail link
    
    print("Fetching K9 routes page to find a flight detail URL...")
    
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    
    resp = httpx.get("https://www.k9jets.com/routes/", headers=headers, timeout=30)
    html = resp.text
    
    # Find a detail URL
    url_regex = re.compile(
        r'href="(https://www\.k9jets\.com/flight/[^"]+)"',
        re.I,
    )
    
    urls = url_regex.findall(html)
    unique_urls = list(dict.fromkeys(urls))[:5]  # First 5 unique
    
    print(f"Found {len(unique_urls)} flight detail URLs")
    
    if unique_urls:
        print(f"\nAnalyzing first URL: {unique_urls[0]}\n")
        debug_k9_page(unique_urls[0])
    else:
        print("No flight URLs found!")
        
    print("\n" + "=" * 60)
    print("DONE! Check debug_k9_page.html for the full page source.")
    print("=" * 60)

