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
        
        # Użyj io.StringIO do wczytania CSV
        df = pd.read_csv(io.StringIO(response.text))
        
        print(f"Pobrano {len(df)} wydarzeń")
        print("Przykładowe dane:")
        print(df.head(3))
        
        # POPRAWKA: Konwersja daty i czasu z właściwego formatu
        # Format: "09-29-2025" (Date) i "7:00am" (Time)
        def parse_datetime(date_str, time_str):
            try:
                # Połącz datę i czas
                datetime_str = f"{date_str} {time_str}"
                # Parsuj z właściwym formatem
                return pd.to_datetime(datetime_str, format='%m-%d-%Y %I:%M%p')
            except Exception as e:
                print(f"Błąd parsowania: {date_str} {time_str} - {e}")
                return None
        
        df['DateTime'] = df.apply(lambda row: parse_datetime(row['Date'], row['Time']), axis=1)
        
        # Usuń wiersze z błędnymi datami
        df = df.dropna(subset=['DateTime'])
        
        # Konwersja na timestamp
        df['Timestamp'] = df['DateTime'].astype(int) // 10**9
        
        print(f"Poprawnie sparsowano {len(df)} wydarzeń")
        
        # Przygotowanie danych dla TradingView
        tv_data = []
        for _, row in df.iterrows():
            # Mapowanie ważności na wartości numeryczne
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
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return []

def save_to_csv(data, filename="data/ECONOMIC_CALENDAR.csv"):
    """Zapisuje dane do formatu CSV dla TradingView"""
    if not data:
        print("Brak danych do zapisania!")
        return None
        
    df = pd.DataFrame(data)
    
    # Sortowanie według czasu
    df = df.sort_values('time')
    
    # Zapis do CSV
    df.to_csv(filename, index=False)
    print(f"Zapisano {len(df)} wydarzeń do {filename}")
    
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
    print(f"Rozpoczynanie aktualizacji kalendarza - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Harmonogram aktualizacji: Wtorek i Czwartek o 06:00 UTC")
    
    # Pobierz dane
    calendar_data = fetch_economic_calendar()
    
    if calendar_data:
        # Zapisz dane
        df = save_to_csv(calendar_data)
        
        # Dodatkowo zapisz metadane
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'events_count': len(df),
            'update_schedule': 'Every Tuesday and Thursday at 06:00 UTC',
            'next_scheduled_update': next_scheduled_update().isoformat()
        }
        
        with open('data/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print("Aktualizacja zakończona pomyślnie!")
    else:
        print("Nie udało się pobrać danych!")

if __name__ == "__main__":
    update_repository()
