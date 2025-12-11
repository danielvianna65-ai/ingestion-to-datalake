import os
import gzip
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_DIR = "data/raw/lst"
SILVER_DIR = "data/silver/lst"

def decompress_if_needed(filepath_gz: str) -> str:
    if not filepath_gz.endswith(".gz"):
        return filepath_gz
    decompressed = filepath_gz.replace(".gz", "")
    if not os.path.exists(decompressed):
        print(f"Descompactando {filepath_gz} → {decompressed}")
        with gzip.open(filepath_gz, "rb") as f_in:
            with open(decompressed, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    return decompressed

def extract_year_month(file_name: str):
    base = os.path.basename(file_name)
    base = base.replace(".CSV.gz", "").replace(".CSV", "").replace(".csv.gz","").replace(".csv","")
    parts = base.split("_")[-1]
    year, month = parts.split("-")
    return int(year), int(month)

def process_silver():
    spark = (
        SparkSession.builder
        .appName("LST-NEO-Silver")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    files = [f for f in os.listdir(RAW_DIR) if f.lower().endswith(".csv") or f.lower().endswith(".csv.gz")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo RAW encontrado")

    raw_file_gz = os.path.join(RAW_DIR, files[0])
    raw_file = decompress_if_needed(raw_file_gz)

    print(f"Lendo RAW: {raw_file}")

    df = spark.read.option("header", True).option("inferSchema", False).csv(raw_file)
    new_columns = [f"pixel_{i:05d}" for i in range(len(df.columns))]
    df = df.toDF(*new_columns)
    df = df.select(*[F.col(c).cast("double").alias(c) for c in df.columns])

    year, month = extract_year_month(raw_file_gz)
    silver_path = f"{SILVER_DIR}/year={year}/month={month:02d}"
    os.makedirs(silver_path, exist_ok=True)

    df.repartition(8).write.mode("overwrite").parquet(silver_path)
    print(f"Silver finalizado: {silver_path}")
    spark.stop()

if __name__ == "__main__":
    process_silver()

