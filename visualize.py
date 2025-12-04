import os
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# This looks for the KEY named "SUPABASE_URL" inside your .env file
SUPABASE_URL = os.getenv("SUPABASE_URL") 
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_data():
    """Fetches the latest snapshot for every future flight"""
    print("Fetching data from Supabase...")
    
    # 1. Get Flights and Snapshots
    # Note: For large datasets, you'd join in SQL. For now, Python is fine.
    flights = supabase.table("flights").select("*").execute().data
    snapshots = supabase.table("flight_snapshots").select("*").execute().data
    
    # Convert to DataFrames
    df_f = pd.DataFrame(flights)
    df_s = pd.DataFrame(snapshots)
    
    # Merge on flight_id
    df = pd.merge(df_s, df_f, left_on="flight_id", right_on="id")
    
    # Keep only the latest snapshot for each flight
    df = df.sort_values("scraped_at").groupby("flight_id").tail(1)
    
    return df

def plot_booking_curve(df):
    """Plots Days Out vs. Seats Available"""
    
    # Calculate "Days Out"
    df['departure_date'] = pd.to_datetime(df['departure_date'])
    df['today'] = pd.to_datetime("today")
    df['days_out'] = (df['departure_date'] - df['today']).dt.days
    
    # Filter for future flights only
    df = df[df['days_out'] >= 0]

    plt.figure(figsize=(12, 6))
    
    # Plot Bark Air
    bark = df[df['competitor'] == 'Bark Air']
    plt.scatter(bark['days_out'], bark['seats_available'], 
                color='#d93025', label='Bark Air', alpha=0.6, s=50)
    
    # Plot K9 Jets
    k9 = df[df['competitor'] == 'K9 Jets']
    plt.scatter(k9['days_out'], k9['seats_available'], 
                color='#1a73e8', label='K9 Jets', alpha=0.6, s=50, marker='^')
    
    plt.title("Forward Availability Curve: How Early Do They Sell Out?", fontsize=14)
    plt.xlabel("Days Until Departure", fontsize=12)
    plt.ylabel("Seats Available", fontsize=12)
    plt.axvline(x=30, color='gray', linestyle='--', alpha=0.5, label='30-Day Warning')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Invert Y axis so "0 seats" (Sold Out) is at the bottom? 
    # Actually, standard is 0 at bottom. Low dots = High Sales.
    plt.ylim(-0.5, 15) 
    
    print("Graph generated! Check 'booking_curve.png'")
    plt.savefig("booking_curve.png")
    plt.show()

if __name__ == "__main__":
    data = fetch_data()
    if not data.empty:
        plot_booking_curve(data)
    else:
        print("No data found!")