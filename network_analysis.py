import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") 
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- CONFIG: Airport Normalization ---
# COMPREHENSIVE MAPPING to fix K9's bad data
AIRPORT_MAPPING = {
    # --- UK / Europe Fixes ---
    "London, UK": "London",
    "London/, UK": "London",
    "Farnborough, UK": "London (Farnborough)",
    "Biggin Hill, UK": "London (Biggin Hill)",
    "Birmingham, UK": "Birmingham",
    "Milan, UK": "Milan",
    "Milan, Italy": "Milan",
    "Madrid, UK": "Madrid",
    "Madrid, Spain": "Madrid",
    "Paris, France": "Paris",
    "Le Bourget Paris, France": "Paris (Le Bourget)",
    "Paris (Le Bourget)": "Paris (Le Bourget)",
    "Nice, France": "Nice",
    "Geneva, Switzerland": "Geneva",
    "Zurich, Switzerland": "Zurich",
    "Frankfurt, Germany": "Frankfurt",
    "Lisbon, Portugal": "Lisbon",
    "Dublin, Ireland": "Dublin",
    
    # --- North America Fixes ---
    "Toronto, UK": "Toronto",
    "Toronto, Canada": "Toronto",
    "Vancouver, Canada": "Vancouver",
    "New Jersey, US": "New York (Teterboro)",
    "Teterboro, NJ": "New York (Teterboro)", 
    "Teterboro, New Jersey": "New York (Teterboro)",
    "New York (Teterboro)": "New York (Teterboro)",
    "White Plains, NY": "New York (White Plains)",
    
    # --- CA Variations (THE FIX) ---
    "Los Angeles, US": "Los Angeles (Van Nuys)", # Merged
    "Los Angeles": "Los Angeles (Van Nuys)",     # Merged generic LA into Van Nuys
    "Van Nuys, California": "Los Angeles (Van Nuys)",
    "Van Nuys, CA": "Los Angeles (Van Nuys)",
    
    # FL Variations
    "Miami, US": "Miami",
    "Miami, Florida": "Miami",
    "West Palm Beach, Florida": "West Palm Beach",
    
    # --- Middle East ---
    "Dubai, UAE": "Dubai"
}

def normalize_airport(name):
    """Cleans up airport names using the mapping dictionary"""
    if not name or pd.isna(name):
        return None
    
    # Clean string
    clean_name = str(name).strip()
    
    # Reject explicit "0" strings
    if clean_name == '0':
        return None
        
    # Return mapped name, or original if not in map
    return AIRPORT_MAPPING.get(clean_name, clean_name)

def fetch_flights():
    """Fetches unique flight legs"""
    print("Fetching flight routes...")
    response = supabase.table("flights").select("*").execute()
    df = pd.DataFrame(response.data)
    return df

def analyze_network_balance(df):
    unique_flights = df.drop_duplicates(subset=['id']).copy()
    
    # 1. CLEAN DATA (Normalize names FIRST)
    unique_flights['origin'] = unique_flights['origin'].apply(normalize_airport)
    unique_flights['destination'] = unique_flights['destination'].apply(normalize_airport)
    
    unique_flights = unique_flights.dropna(subset=['origin', 'destination'])
    
    # 2. CALCULATE FLOWS
    outbound = unique_flights.groupby(['competitor', 'origin']).size().reset_index(name='departures')
    inbound = unique_flights.groupby(['competitor', 'destination']).size().reset_index(name='arrivals')
    
    # Merge
    balance = pd.merge(outbound, inbound, 
                       left_on=['competitor', 'origin'], 
                       right_on=['competitor', 'destination'], 
                       how='outer')
    
    # Coalesce Airport Name
    balance['airport'] = balance['origin'].combine_first(balance['destination'])
    
    # Fill NaNs with 0 temporarily
    balance['arrivals'] = balance['arrivals'].fillna(0)
    balance['departures'] = balance['departures'].fillna(0)
    
    # 3. FILTER OUT SPECIFIC ARTIFACTS
    invalid_entries = ["0", 0, "Los Angeles, California -> Van Nuys, California"]
    balance = balance[~balance['airport'].isin(invalid_entries)]
    
    # 4. AGGREGATE AGAIN (Crucial Step)
    balance = balance[['competitor', 'airport', 'arrivals', 'departures']]
    balance = balance.groupby(['competitor', 'airport']).sum().reset_index()
    
    balance['net_flow'] = balance['arrivals'] - balance['departures']
    
    return balance

def plot_balance_sheet(balance_df):
    competitors = balance_df['competitor'].unique()
    
    for comp in competitors:
        comp_data = balance_df[balance_df['competitor'] == comp].copy()
        
        # Sort
        comp_data['total'] = comp_data['arrivals'] + comp_data['departures']
        comp_data = comp_data.sort_values('total', ascending=False)
        
        melted = comp_data.melt(id_vars=['airport'], value_vars=['arrivals', 'departures'], 
                                var_name='Type', value_name='Count')
        
        plt.figure(figsize=(14, 8)) 
        
        sns.barplot(data=melted, x='airport', y='Count', hue='Type', 
                    palette={'arrivals': '#2ecc71', 'departures': '#e74c3c'})
        
        plt.xticks(rotation=45, ha='right')

        # Annotate
        for i, row in enumerate(comp_data.itertuples()):
            if row.net_flow != 0:
                label = f"{int(row.net_flow)} Stuck" if row.net_flow > 0 else f"{abs(int(row.net_flow))} Appear"
                color = 'red' if row.net_flow != 0 else 'gray'
                max_val = max(row.arrivals, row.departures)
                plt.text(i, max_val + 0.5, label, ha='center', fontsize=9, fontweight='bold', color=color)

        plt.title(f"{comp}: Network Balance Sheet (In vs. Out)", fontsize=16, fontweight='bold')
        plt.xlabel("Airport", fontsize=12)
        plt.ylabel("Number of Flights", fontsize=12)
        plt.grid(axis='y', alpha=0.1)
        plt.legend(title=None)
        
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.25)
        
        filename = f"{comp.replace(' ', '_')}_network_balance.png"
        plt.savefig(filename, dpi=300)
        print(f"Generated {filename}")
        plt.show()

if __name__ == "__main__":
    df = fetch_flights()
    if not df.empty:
        balance = analyze_network_balance(df)
        print("\n--- NETWORK IMBALANCE REPORT ---")
        # Print non-zero flows for debugging
        print(balance[balance['net_flow'] != 0])
        plot_balance_sheet(balance)
    else:
        print("No flight data found.")