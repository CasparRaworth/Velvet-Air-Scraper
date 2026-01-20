import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") 
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_data():
    """Fetches ALL snapshots for every future flight to track sales over time"""
    print("Fetching full history from Supabase...")
    
    flights = supabase.table("flights").select("*").execute().data
    snapshots = supabase.table("flight_snapshots").select("*").execute().data
    
    if not flights or not snapshots:
        return pd.DataFrame()

    df_f = pd.DataFrame(flights)
    df_s = pd.DataFrame(snapshots)
    
    # Merge on flight_id
    df = pd.merge(df_s, df_f, left_on="flight_id", right_on="id")
    
    return df

def plot_booking_curve(df):
    """
    Plots Days Out (X) vs. Seats Available (Y)
    Includes: Sell-Out Risk and Confidence Intervals
    """
    
    # Calculate "Days Out"
    df['departure_date'] = pd.to_datetime(df['departure_date']).dt.tz_localize(None)
    df['scraped_at'] = pd.to_datetime(df['scraped_at']).dt.tz_localize(None)
    df['days_out'] = (df['departure_date'] - df['scraped_at']).dt.days
    
    # Filter 0-100 days
    df = df[(df['days_out'] >= 0) & (df['days_out'] <= 100)].copy()

    if df.empty:
        print("No flight data to plot!")
        return

    # Create Subplots
    fig, axes = plt.subplots(2, 1, figsize=(12, 14), sharex=True)
    
    competitors = [
        ('Bark Air', '#d93025'),
        ('K9 Jets', '#1a73e8')
    ]

    for i, (name, color) in enumerate(competitors):
        ax = axes[i]
        comp_df = df[df['competitor'] == name]
        
        if comp_df.empty:
            ax.text(0.5, 0.5, f"No data for {name}", transform=ax.transAxes, ha='center')
            continue

        grouped = comp_df.groupby('days_out')['seats_available']
        
        # 1. Average Seats
        daily_avg = grouped.mean().reindex(np.arange(0, 101))
        trend_curve = daily_avg.rolling(window=14, min_periods=1, center=True).mean()
        
        # 2. Sell-Out Percentage
        sell_out_series = grouped.apply(lambda x: (x == 0).mean() * 100).reindex(np.arange(0, 101))
        sell_out_trend = sell_out_series.rolling(window=14, min_periods=1, center=True).mean()

        # 3. Variability
        p25 = grouped.quantile(0.25).reindex(np.arange(0, 101)).rolling(window=14, min_periods=1).mean()
        p75 = grouped.quantile(0.75).reindex(np.arange(0, 101)).rolling(window=14, min_periods=1).mean()

        # Plotting
        ax.fill_between(trend_curve.index, p25, p75, color=color, alpha=0.1, label='Typical Range')
        ax.plot(trend_curve.index, trend_curve.values, color=color, linewidth=3, label=f'Avg Seats Available', zorder=5)

        ax2 = ax.twinx()
        ax2.plot(sell_out_trend.index, sell_out_trend.values, color='gray', linestyle='--', linewidth=2, alpha=0.7, label='Sell-Out Probability')
        ax2.fill_between(sell_out_trend.index, 0, sell_out_trend.values, color='gray', alpha=0.05)
        
        ax.set_title(f"{name}: Booking Curve & Sell-Out Risk", fontsize=14, fontweight='bold')
        ax.set_ylabel("Avg Seats Available", fontsize=12, color=color)
        ax.tick_params(axis='y', labelcolor=color)
        ax.set_ylim(-0.5, 15)
        ax.grid(True, alpha=0.2)
        ax2.set_ylabel("Probability of Sell-Out (%)", fontsize=12, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
        ax2.set_ylim(0, 100)
        
        lines_1, labels_1 = ax.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')
        ax.set_xlim(100, -2)

    axes[1].set_xlabel("Days Until Departure", fontsize=12)
    plt.tight_layout()
    plt.savefig("booking_curve_risk.png", dpi=300)
    print(f"Graph generated! Check 'booking_curve_risk.png'")
    plt.show()

def plot_pricing_index(df):
    """
    Plots Price Trend normalized to each flight's own average.
    Eliminates "Mix Shift" errors (e.g. expensive flights selling out first).
    """
    price_col = 'price' 

    if 'days_out' not in df.columns:
        df['departure_date'] = pd.to_datetime(df['departure_date']).dt.tz_localize(None)
        df['scraped_at'] = pd.to_datetime(df['scraped_at']).dt.tz_localize(None)
        df['days_out'] = (df['departure_date'] - df['scraped_at']).dt.days
    
    df = df[(df['days_out'] >= 0) & (df['days_out'] <= 100)].copy()

    if price_col not in df.columns:
        print(f"Warning: Price column '{price_col}' not found.")
        return

    # --- THE FIX: NORMALIZE PER FLIGHT FIRST ---
    # 1. Calculate the mean price for EACH individual flight
    df['flight_mean_price'] = df.groupby('flight_id')[price_col].transform('mean')
    
    # 2. Create an index: 100 = The average price for that specific flight
    # If index > 100, price is higher than usual. If < 100, it's discounted.
    df['price_index'] = (df[price_col] / df['flight_mean_price']) * 100

    fig, axes = plt.subplots(2, 1, figsize=(12, 14), sharex=True)
    
    competitors = [
        ('Bark Air', '#d93025'),
        ('K9 Jets', '#1a73e8')
    ]

    for i, (name, color) in enumerate(competitors):
        ax = axes[i]
        comp_df = df[df['competitor'] == name]
        
        if comp_df.empty:
            ax.text(0.5, 0.5, f"No data for {name}", transform=ax.transAxes, ha='center')
            continue

        # --- PREPARE DATA ---
        grouped = comp_df.groupby('days_out')
        
        # Average the INDEX, not the raw price
        daily_index = grouped['price_index'].mean().reindex(np.arange(0, 101))
        index_trend = daily_index.rolling(window=14, min_periods=1, center=True).mean()

        # Seat context (for reference)
        daily_seats = grouped['seats_available'].mean().reindex(np.arange(0, 101))
        seat_trend = daily_seats.rolling(window=14, min_periods=1, center=True).mean()

        # --- PLOTTING ---
        # Draw the "100" baseline (Neutral Price)
        ax.axhline(100, color='black', linewidth=1, linestyle='-', alpha=0.3, label='Baseline Price (100)')

        # Plot Price Strength
        ax.plot(index_trend.index, index_trend.values, 
                color='green', linewidth=4, label='Relative Price Strength', zorder=5)
        
        # Annotate Movement
        try:
            valid = index_trend.dropna()
            if not valid.empty:
                start_val = valid.iloc[-1]
                end_val = valid.iloc[0]
                pct_diff = end_val - start_val # Change in index points
                
                msg = "Prices Rising" if pct_diff > 0 else "Prices Falling"
                ax.text(50, 105, 
                        f"Trend: {msg} ({pct_diff:+.1f} pts)", 
                        fontsize=12, fontweight='bold', color='green', 
                        bbox=dict(facecolor='white', alpha=0.9, edgecolor='green', boxstyle='round'),
                        ha='center')
        except:
            pass

        # Secondary Axis: Seats
        ax2 = ax.twinx()
        ax2.plot(seat_trend.index, seat_trend.values, 
                 color=color, linestyle='--', linewidth=2, alpha=0.5, label='Avg Seats Available')
        
        ax.set_title(f"{name}: True Pricing Strategy (Normalized)", fontsize=14, fontweight='bold')
        ax.set_ylabel("Price Index (100 = Flight Average)", fontsize=12, color='green')
        ax.tick_params(axis='y', labelcolor='green')
        ax.grid(True, alpha=0.2)
        ax2.set_ylabel("Avg Seats Available", fontsize=12, color=color)
        ax2.tick_params(axis='y', labelcolor=color)
        ax2.set_ylim(0, 15)
        
        lines_1, labels_1 = ax.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')
        ax.set_xlim(100, -2)

    axes[1].set_xlabel("Days Until Departure", fontsize=12)
    plt.tight_layout()
    
    filename = "pricing_strategy_normalized.png"
    plt.savefig(filename, dpi=300)
    print(f"Graph generated! Check '{filename}'")
    plt.show()

if __name__ == "__main__":
    data = fetch_data()
    if not data.empty:
        print("--- Generating Booking Curves ---")
        plot_booking_curve(data)
        
        print("\n--- Generating Pricing Strategy ---")
        plot_pricing_index(data)
    else:
        print("No data found!")