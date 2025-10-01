import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import io
import os

def fetch_economic_calendar():
    """Pobiera dane kalendarza ekonomicznego"""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.csv"
    
    try:
        print("Pobieranie danych z:", url)
        response = requests.get(url)
        response.raise_for_status()
        
        # Użyj io.StringIO do wczytania CSV
        df = pd.read_csv(io.StringIO(response.text))
        
        print(f"Pobrano {len(df)} wydarzeń")
        print("Przykładowe dane:")
        print(df[['Title', 'Date', 'Time', 'Impact']].head(3))
        
        # Konwersja daty i czasu z właściwego formatu
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
        initial_count = len(df)
        df = df.dropna(subset=['DateTime'])
        dropped_count = initial_count - len(df)
        if dropped_count > 0:
            print(f"Usunięto {dropped_count} wierszy z błędnymi datami")
        
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
    
    # Sprawdź czy folder data istnieje
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Zapis do CSV
    df.to_csv(filename, index=False)
    print(f"Zapisano {len(df)} wydarzeń do {filename}")
    
    # Sprawdź czy plik został utworzony
    if os.path.exists(filename):
        file_size = os.path.getsize(filename)
        print(f"Plik {filename} utworzony pomyślnie, rozmiar: {file_size} bajtów")
        
        # Wyświetl przykładowe dane z zapisanego pliku
        try:
            saved_df = pd.read_csv(filename)
            print("Przykładowe dane z zapisanego pliku:")
            print(saved_df.head(3))
        except Exception as e:
            print(f"Błąd przy odczycie zapisanego pliku: {e}")
    else:
        print(f"BŁĄD: Plik {filename} nie został utworzony!")
    
    return df

def next_scheduled_update():
    """Oblicza następną zaplanowaną aktualizację"""
    now = datetime.now()
    
    # Jeśli dziś jest poniedziałek (0) lub środa (2) i przed 00:20
    if now.weekday() in [0, 2] and (now.hour < 0 or (now.hour == 0 and now.minute < 20)):
        return now.replace(hour=0, minute=20, second=0, microsecond=0)
    
    # Znajdź następny poniedziałek lub środę
    days_ahead = 1
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() in [0, 2]:  # Poniedziałek=0, Środa=2
            return next_day.replace(hour=0, minute=20, second=0, microsecond=0)
        days_ahead += 1

def update_repository():
    """Główna funkcja aktualizująca repozytorium"""
    print(f"Rozpoczynanie aktualizacji kalendarza - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Harmonogram aktualizacji: Poniedziałek i Środa o 00:20 UTC")
    
    # Sprawdź obecny stan folderu data
    if os.path.exists("data/ECONOMIC_CALENDAR.csv"):
        print("Znaleziono istniejący plik ECONOMIC_CALENDAR.csv")
        try:
            existing_df = pd.read_csv("data/ECONOMIC_CALENDAR.csv")
            print(f"Obecny plik zawiera {len(existing_df)} wydarzeń")
        except Exception as e:
            print(f"Błąd przy odczycie istniejącego pliku: {e}")
    
    # Pobierz dane
    calendar_data = fetch_economic_calendar()
    
    if calendar_data:
        # Zapisz dane
        df = save_to_csv(calendar_data)
        
        # Dodatkowo zapisz metadane
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'events_count': len(df),
            'update_schedule': 'Every Monday and Wednesday at 00:20 UTC',
            'next_scheduled_update': next_scheduled_update().isoformat()
        }
        
        with open('data/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print("Aktualizacja zakończona pomyślnie!")
        return True
    else:
        print("Nie udało się pobrać danych!")
        return False

if __name__ == "__main__":
    update_repository()
