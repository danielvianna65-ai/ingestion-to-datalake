import requests
import pandas as pd
from datetime import datetime
import os

def fetch_weather_data(latitude=-23.55, longitude=-46.63):
    """Busca dados climáticos horários da API Open-Meteo."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def transform_to_dataframe(json_data):
    """Transforma o JSON em DataFrame pandas com colunas limpas."""
    hourly = json_data.get("hourly", {})
    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature": hourly.get("temperature_2m", []),
        "humidity": hourly.get("relative_humidity_2m", []),
        "wind_speed": hourly.get("wind_speed_10m", [])
    })
    # converte coluna time para datetime
    if not df.empty:
        df["time"] = pd.to_datetime(df["time"])
    return df

def save_csv(df, output_path="data/raw"):
    os.makedirs(output_path, exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_path, f"weather_{now}.csv")
    df.to_csv(file_path, index=False)
    print(f"✔ Arquivo salvo em: {file_path}")

def run_pipeline(latitude=-23.55, longitude=-46.63):
    json_data = fetch_weather_data(latitude=latitude, longitude=longitude)
    df = transform_to_dataframe(json_data)
    if df.empty:
        print("Aviso: DataFrame vazio — verifique a resposta da API.")
    else:
        save_csv(df)

if __name__ == "__main__":
    # valores padrão: São Paulo (latitude -23.55, longitude -46.63)
    run_pipeline()
