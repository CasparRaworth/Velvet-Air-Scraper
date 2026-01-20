"""
Export Supabase flight data to Excel/CSV for analysis.
Run: python export_to_excel.py
"""
import os
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def export_data():
    print("📊 Fetching flight data from Supabase...")
    
    # Fetch all snapshots joined with flight details
    response = supabase.table("flight_snapshots").select(
        "*, flights(competitor, origin, destination, departure_date, departure_time, operator)"
    ).order("scraped_at", desc=True).execute()
    
    if not response.data:
        print("❌ No data found!")
        return
    
    # Flatten the nested structure for Excel
    rows = []
    for snapshot in response.data:
        flight = snapshot.get("flights", {})
        rows.append({
            "competitor": flight.get("competitor"),
            "origin": flight.get("origin"),
            "destination": flight.get("destination"),
            "departure_date": flight.get("departure_date"),
            "departure_time": flight.get("departure_time"),
            "operator": flight.get("operator"),
            "price": snapshot.get("price"),
            "seats_available": snapshot.get("seats_available"),
            "status": snapshot.get("status"),
            "scraped_at": snapshot.get("scraped_at"),
        })
    
    # Try to use pandas for Excel, fall back to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    try:
        import pandas as pd
        df = pd.DataFrame(rows)
        
        # Export to Excel
        excel_path = f"flight_data_{timestamp}.xlsx"
        df.to_excel(excel_path, index=False, sheet_name="Flight Data")
        print(f"✅ Exported {len(rows)} rows to {excel_path}")
        
    except ImportError:
        # Fallback to CSV if pandas not installed
        import csv
        csv_path = f"flight_data_{timestamp}.csv"
        
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"✅ Exported {len(rows)} rows to {csv_path}")
        print("💡 Tip: Install pandas + openpyxl for Excel export: pip install pandas openpyxl")


if __name__ == "__main__":
    export_data()

