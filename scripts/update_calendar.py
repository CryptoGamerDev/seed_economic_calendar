import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json

def fetch_economic_calendar():
    """Pobiera dane kalendarza ekonomicznego"""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.csv"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Konwersja CSV na DataFrame
        df = pd.read_csv(pd.compat.StringIO(response.text))
        
        # Konwersja daty i czasu
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%m-%d-%Y %I:%M%p')
        df['Timestamp'] = df['DateTime'].astype(int) // 10**9  # Unix timestamp
        
        # Przygotowanie danych dla TradingView
        # TradingView wymaga kolumn: time, open, high, low, close, volume
        tv_data = []
        for _, row in df.iterrows():
            # Używamy 'close' do przechowywania ważności wydarzenia (1-3)
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
        print(f"Error fetching data: {e}")
        return []

def save_to_csv(data, filename="data/ECONOMIC_CALENDAR.csv"):
    """Zapisuje dane do formatu CSV dla TradingView"""
    df = pd.DataFrame(data)
    
    # Sortowanie według czasu
    df = df.sort_values('time')
    
    # Zapis do CSV
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    
    return df

def update_repository():
    """Główna funkcja aktualizująca repozytorium"""
    print("Starting economic calendar update...")
    
    # Pobierz dane
    calendar_data = fetch_economic_calendar()
    
    if calendar_data:
        # Zapisz dane
        df = save_to_csv(calendar_data)
        print(f"Updated {len(df)} economic events")
        
        # Dodatkowo zapisz metadane
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'events_count': len(df),
            'next_update': (datetime.now() + timedelta(hours=6)).isoformat()
        }
        
        with open('data/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print("Update completed successfully!")
    else:
        print("No data fetched!")

if __name__ == "__main__":
    update_repository()
