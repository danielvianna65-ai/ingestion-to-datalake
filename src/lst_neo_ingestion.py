from pyspark.sql import SparkSession
import requests
import os

def download_lst_csv(year: int, month: int, download_dir: str = "data/raw"):
    """
    Faz download do arquivo CSV.gz da NASA NEO para LST (Land Surface Temperature)
    usando o padrão real de nome encontrado no catálogo.

    Exemplo de arquivo real:
    MOD_LSTD_M_2025-05.CSV.gz
    """

    os.makedirs(download_dir, exist_ok=True)

    file_name = f"MOD_LSTD_M_{year}-{month:02d}.CSV.gz"
    url = f"https://neo.gsfc.nasa.gov/archive/csv/MOD_LSTD_M/{file_name}"

    local_path = os.path.join(download_dir, file_name)

    print(f"\n→ Baixando: {url}")

    response = requests.get(url, stream=True)

    if response.status_code != 200:
        raise ValueError(f"Arquivo NÃO encontrado no NASA NEO para {year}-{month:02d}")

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print(f"✓ Download concluído: {local_path}")
    return local_path


def run_spark_ingestion(year: int, month: int):
    """
    Cria sessão Spark, lê o CSV.gz e salva no datalake.
    """
    spark = (
        SparkSession.builder
        .appName("NASA LST NEO Ingestion")
        .getOrCreate()
    )

    # 1 — Baixar arquivo
    csv_path = download_lst_csv(year, month)

    print(f"\n→ Lendo CSV.gz com Spark: {csv_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)  # Spark lê .gz automaticamente
    )

    print("✓ Dados carregados, mostrando esquema:")
    df.printSchema()

    print("\nPrimeiras linhas:")
    df.show(5)

    # 2 — Salvar no Datalake (bronze)
    output_path = f"data/raw/lst/year={year}/month={month:02d}"

    print(f"\n→ Salvando no datalake: {output_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"✓ Arquivo salvo em {output_path}")

    spark.stop()


if __name__ == "__main__":
    # Exemplo: baixa e processa maio/2025
    run_spark_ingestion(2025, 5)

