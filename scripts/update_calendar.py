import requests
import pandas as pd
from datetime import datetime, timedelta
import json

def fetch_economic_calendar():
    """Pobiera dane kalendarza ekonomicznego"""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.csv"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        df = pd.read_csv(pd.compat.StringIO(response.text))
        
        # Konwersja daty i czasu
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%m-%d-%Y %I:%M%p')
        df['Timestamp'] = df['DateTime'].astype(int) // 10**9
        
        # Przygotowanie danych dla TradingView
        tv_data = []
        for _, row in df.iterrows():
            impact_value = {
                'High': 3,
                'Medium': 2, 
                'Low': 1,
                'Holiday': 0
            }.get(row['Impact'], 1)
            
            tv_data.append({
                'time': int(row['Timestamp']),
                'open': impact_value,
                'high': impact_value,
                'low': impact_value,
                'close': impact_value,
                'volume': 0
            })
        
        return tv_data
        
    except Exception as e:
        print(f"Error: {e}")
        return []

def save_to_csv(data, filename="data/ECONOMIC_CALENDAR.csv"):
    """Zapisuje dane do CSV"""
    df = pd.DataFrame(data)
    df = df.sort_values('time')
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} events to {filename}")
    return df

def update_repository():
    """Główna funkcja aktualizująca"""
    print("Updating economic calendar...")
    
    calendar_data = fetch_economic_calendar()
    
    if calendar_data:
        df = save_to_csv(calendar_data)
        
        # Metadane (opcjonalnie)
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'events_count': len(df)
        }
        
        with open('data/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print("Update completed!")
    else:
        print("No data!")

if __name__ == "__main__":
    update_repository()
