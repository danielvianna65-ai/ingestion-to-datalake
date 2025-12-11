import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_DIR = "data/silver/lst"
GOLD_DIR = "data/gold/lst"

def process_gold():
    spark = SparkSession.builder.appName("LST-NEO-Gold").getOrCreate()

    for year_folder in os.listdir(SILVER_DIR):
        year_path = os.path.join(SILVER_DIR, year_folder)
        if not os.path.isdir(year_path):
            continue
        for month_folder in os.listdir(year_path):
            silver_path = os.path.join(year_path, month_folder)
            df = spark.read.parquet(silver_path)

            df_long = df.select(F.posexplode(F.array(*df.columns)).alias("pixel_index", "lst_kelvin"))
            df_long = df_long.withColumn("year", F.lit(int(year_folder.split("=")[-1])))
            df_long = df_long.withColumn("month", F.lit(int(month_folder.split("=")[-1])))

            gold_path = os.path.join(GOLD_DIR, f"year={year_folder.split('=')[-1]}", f"month={month_folder.split('=')[-1]}")
            os.makedirs(gold_path, exist_ok=True)
            df_long.write.mode("overwrite").parquet(gold_path)
            print(f"Gold finalizado: {gold_path}")

    spark.stop()

if __name__ == "__main__":
    process_gold()

