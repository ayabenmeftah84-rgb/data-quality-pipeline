"""Experience : injecter des erreurs connues dans les donnees brutes et mesurer ce qui est detecte.

Prerequis : avoir lance `python src/run_pipeline.py` (on reutilise data/clean, data/quarantine
et reports/cleaning_log.json comme reference).

Sorties : data/injected/*.parquet (ignore par Git)
          reports/error_injection_report.json et reports/error_injection_report.md

Usage : python src/evaluate_detection.py [--n 500] [--seed 42]
"""

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cleaning import clean_dataframe
from error_injection import count_collateral, evaluate_detection, inject_errors

ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = "data/raw/listings.csv.gz"
CLEAN_PATH = "data/clean/listings_clean.parquet"
QUARANTINE_PATH = "data/quarantine/listings_rejected.parquet"
BASELINE_LOG = Path("reports/cleaning_log.json")
RAW_PARQUET = "data/injected/raw.parquet"
CORRUPTED_PARQUET = "data/injected/raw_with_errors.parquet"
REPORT_JSON = Path("reports/error_injection_report.json")
REPORT_MD = Path("reports/error_injection_report.md")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=500, help="erreurs injectees par type")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    os.chdir(ROOT)  # les chemins ci-dessous sont relatifs a la racine du projet

    for path in (RAW_PATH, CLEAN_PATH, QUARANTINE_PATH, str(BASELINE_LOG)):
        if not Path(path).exists():
            print(f"Fichier introuvable : {path}. Lancez d'abord : python src/run_pipeline.py")
            return 2

    spark = SparkSession.builder.master("local[*]").appName("error-injection").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Lecture du CSV une seule fois, puis copie en Parquet (beaucoup plus rapide a relire)
    (
        spark.read.option("header", True).option("multiLine", True)
        .option("quote", '"').option("escape", '"').csv(RAW_PATH)
        .write.mode("overwrite").parquet(RAW_PARQUET)
    )
    raw = spark.read.parquet(RAW_PARQUET)

    # 2. Reference : lignes valides et lignes deja en quarantaine avant l'experience
    valid_ids = spark.read.parquet(CLEAN_PATH).select(F.col("id").cast("string").alias("id"))
    baseline_rejected = {r["id"] for r in spark.read.parquet(QUARANTINE_PATH).select("id").collect()}
    baseline_dups = json.loads(BASELINE_LOG.read_text()).get("duplicate_ids_removed", 0)

    # 3. Injection, puis nettoyage du fichier corrompu
    corrupted, types, injected = inject_errors(raw, valid_ids, n=args.n, seed=args.seed)
    corrupted.write.mode("overwrite").parquet(CORRUPTED_PARQUET)
    clean, rejected, log = clean_dataframe(spark.read.parquet(CORRUPTED_PARQUET))

    # 4. Mesure
    results = evaluate_detection(clean, rejected, types, injected, log["duplicate_ids_removed"] - baseline_dups)
    collateral = count_collateral(rejected, injected, baseline_rejected)
    total_injected = sum(r["injected"] for r in results)
    total_detected = sum(r["detected"] for r in results)
    report = {
        "seed": args.seed,
        "errors_per_type": args.n,
        "results": results,
        "total_injected": total_injected,
        "total_detected": total_detected,
        "overall_recall_pct": round(100 * total_detected / total_injected, 1),
        "collateral_quarantined_rows": collateral,
    }
    REPORT_JSON.parent.mkdir(exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    lines = [
        "| Injected error | Rows injected | Detected | Detection rate |",
        "|---|---|---|---|",
    ] + [f"| {r['description']} | {r['injected']} | {r['detected']} | {r['recall_pct']} % |" for r in results]
    lines += [
        "",
        f"Overall: {total_detected} of {total_injected} injected errors detected ({report['overall_recall_pct']} %).",
        f"Rows quarantined that were neither injected nor already quarantined before the experiment: {collateral}.",
        f"Seed: {args.seed}. {args.n} errors per type, on distinct rows that were valid in the original data.",
    ]
    REPORT_MD.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nRapports : {REPORT_JSON} et {REPORT_MD}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
