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
        response = requests.get(url)
        response.raise_for_status()
        
        df = pd.read_csv(io.StringIO(response.text))
        
        print(f"Pobrano {len(df)} wydarzeń")
        
        # Konwersja daty i czasu
        def parse_datetime(date_str, time_str):
            try:
                datetime_str = f"{date_str} {time_str}"
                return pd.to_datetime(datetime_str, format='%m-%d-%Y %I:%M%p')
            except Exception as e:
                print(f"Błąd parsowania: {date_str} {time_str} - {e}")
                return None
        
        df['DateTime'] = df.apply(lambda row: parse_datetime(row['Date'], row['Time']), axis=1)
        df = df.dropna(subset=['DateTime'])
        
        print(f"Poprawnie sparsowano {len(df)} wydarzeń")
        
        return df
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()

def prepare_daily_data(df):
    """Przygotowuje dane dzienne w formacie OHLCV dla TradingView"""
    if df.empty:
        return pd.DataFrame()
    
    # Mapowanie ważności na wartości numeryczne
    impact_mapping = {
        'High': 3,
        'Medium': 2, 
        'Low': 1,
        'Holiday': 0
    }
    
    df['ImpactValue'] = df['Impact'].map(impact_mapping).fillna(1)
    
    # Grupowanie według daty (bez czasu)
    df['DateOnly'] = df['DateTime'].dt.date
    
    # Dla każdego dnia znajdujemy maksymalną ważność wydarzenia
    daily_data = df.groupby('DateOnly').agg({
        'ImpactValue': 'max'
    }).reset_index()
    
    # Tworzymy dane OHLCV - wszystkie wartości takie same (maksymalna ważność dnia)
    daily_data['open'] = daily_data['ImpactValue']
    daily_data['high'] = daily_data['ImpactValue']
    daily_data['low'] = daily_data['ImpactValue']
    daily_data['close'] = daily_data['ImpactValue']
    daily_data['volume'] = 0
    
    # Format daty do YYYYMMDDT
    daily_data['date'] = daily_data['DateOnly'].apply(lambda x: x.strftime('%Y%m%dT'))
    
    # Sortowanie według daty
    daily_data = daily_data.sort_values('date')
    
    return daily_data[['date', 'open', 'high', 'low', 'close', 'volume']]

def save_symbol_info():
    """Tworzy plik symbol_info.json"""
    symbol_info = {
        "symbol": ["ECONOMIC_CALENDAR"],
        "description": ["Economic Calendar Impact Events"],
        "pricescale": 1
    }
    
    os.makedirs('symbol_info', exist_ok=True)
    
    with open('symbol_info/seed_economic_calendar.json', 'w') as f:
        json.dump(symbol_info, f, indent=2)
    
    print("Utworzono plik symbol_info.json")

def save_to_csv(data, filename="data/ECONOMIC_CALENDAR.csv"):
    """Zapisuje dane do formatu CSV dla TradingView"""
    if data.empty:
        print("Brak danych do zapisania!")
        return None
        
    # Zapis do CSV bez nagłówka i indeksu
    data.to_csv(filename, index=False, header=False)
    print(f"Zapisano {len(data)} dni danych do {filename}")
    
    # Sprawdź czy plik został utworzony
    if os.path.exists(filename):
        file_size = os.path.getsize(filename)
        print(f"Plik {filename} utworzony pomyślnie, rozmiar: {file_size} bajtów")
        
        # Wyświetl przykładowe dane
        print("Przykładowe dane:")
        print(data.head())
    else:
        print(f"BŁĄD: Plik {filename} nie został utworzony!")
    
    return data

def update_repository():
    """Główna funkcja aktualizująca repozytorium"""
    print(f"Rozpoczynanie aktualizacji kalendarza - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Utwórz strukturę katalogów
    os.makedirs('symbol_info', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    
    # Utwórz plik symbol_info
    save_symbol_info()
    
    # Pobierz dane
    raw_data = fetch_economic_calendar()
    
    if not raw_data.empty:
        # Przygotuj dane dzienne
        daily_data = prepare_daily_data(raw_data)
        
        # Zapisz dane
        if not daily_data.empty:
            df = save_to_csv(daily_data)
            
            # Dodatkowo zapisz metadane
            metadata = {
                'last_updated': datetime.now().isoformat(),
                'days_count': len(daily_data),
                'events_count': len(raw_data),
                'data_format': 'OHLCV',
                'impact_scale': '0=Holiday, 1=Low, 2=Medium, 3=High'
            }
            
            with open('data/metadata.json', 'w') as f:
                json.dump(metadata, f, indent=2)
                
            print("Aktualizacja zakończona pomyślnie!")
            return True
        else:
            print("Brak danych do zapisania po przetworzeniu!")
    else:
        print("Nie udało się pobrać danych!")
    
    return False

if __name__ == "__main__":
    update_repository()
