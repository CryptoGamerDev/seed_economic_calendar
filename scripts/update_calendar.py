import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import io

def fetch_economic_calendar():
    """Pobiera dane kalendarza ekonomicznego"""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.csv"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # POPRAWKA: Użyj io.StringIO zamiast pd.compat.StringIO
        df = pd.read_csv(io.StringIO(response.text))
        
        # Konwersja daty i czasu
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%m-%d-%Y %I:%M%p')
        df['Timestamp'] = df['DateTime'].astype(int) // 10**9  # Unix timestamp
        
        # Przygotowanie danych dla TradingView
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

def next_scheduled_update():
    """Oblicza następną zaplanowaną aktualizację"""
    now = datetime.now()
    
    # Jeśli dziś jest wtorek (1) lub czwartek (3) i przed 06:00
    if now.weekday() in [1, 3] and now.hour < 6:
        return now.replace(hour=6, minute=0, second=0, microsecond=0)
    
    # Znajdź następny wtorek lub czwartek
    days_ahead = 1
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in [1, 3]:  # Wtorek=1, Czwartek=3
            return next_day.replace(hour=6, minute=0, second=0, microsecond=0)
        days_ahead += 1

def update_repository():
    """Główna funkcja aktualizująca repozytorium"""
    print(f"Starting economic calendar update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Update schedule: Every Tuesday and Thursday at 06:00 UTC")
    
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
            'update_schedule': 'Every Tuesday and Thursday at 06:00 UTC',
            'next_scheduled_update': next_scheduled_update().isoformat()
        }
        
        with open('data/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print("Update completed successfully!")
    else:
        print("No data fetched!")

if __name__ == "__main__":
    update_repository()
