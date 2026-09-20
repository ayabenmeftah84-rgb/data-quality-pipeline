"""Nettoyage des donnees brutes Inside Airbnb (lecture, appel de cleaning.py, ecriture).

Entree  : data/raw/listings.csv.gz
Sorties : data/clean/listings_clean.parquet         (lignes valides)
          data/quarantine/listings_rejected.parquet (lignes rejetees + motif)
          reports/cleaning_log.json                 (chiffres du nettoyage)
"""

import json
from pathlib import Path

from pyspark.sql import SparkSession

from cleaning import clean_dataframe

INPUT_PATH = "data/raw/listings.csv.gz"
CLEAN_PATH = "data/clean/listings_clean.parquet"
QUARANTINE_PATH = "data/quarantine/listings_rejected.parquet"
LOG_PATH = Path("reports/cleaning_log.json")


def main():
    spark = SparkSession.builder.master("local[*]").appName("cleaning").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True).option("multiLine", True)
        .option("quote", '"').option("escape", '"').csv(INPUT_PATH)
    )
    clean, rejected, log = clean_dataframe(raw)

    clean.write.mode("overwrite").parquet(CLEAN_PATH)
    rejected.write.mode("overwrite").parquet(QUARANTINE_PATH)
    LOG_PATH.parent.mkdir(exist_ok=True)
    LOG_PATH.write_text(json.dumps(log, indent=2, ensure_ascii=False))

    print(json.dumps(log, indent=2, ensure_ascii=False))
    print(f"\nDonnees propres : {CLEAN_PATH}\nQuarantaine     : {QUARANTINE_PATH}\nJournal         : {LOG_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
