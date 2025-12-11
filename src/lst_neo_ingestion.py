import os
import requests

RAW_DIR = "data/raw/lst"

def download_lst_csv(year: int, month: int, raw_dir=RAW_DIR):
    os.makedirs(raw_dir, exist_ok=True)
    file_name = f"MOD_LSTD_M_{year}-{month:02d}.CSV.gz"
    url = f"https://neo.gsfc.nasa.gov/archive/csv/MOD_LSTD_M/{file_name}"
    local_path = os.path.join(raw_dir, file_name)

    if os.path.exists(local_path):
        print(f"Arquivo já existe: {local_path}")
        return local_path

    print(f"→ Baixando: {url}")
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        raise FileNotFoundError(f"Arquivo não existe na NASA NEO: {file_name}")

    with open(local_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

    print(f"✓ Salvo no RAW: {local_path}")
    return local_path

if __name__ == "__main__":
    # Exemplo: baixar maio de 2025
    download_lst_csv(2025, 5)

