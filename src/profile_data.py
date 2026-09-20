"""Profilage des donnees brutes : pourcentage de valeurs manquantes,
valeurs distinctes, min/max numeriques et un exemple de valeur par colonne."""

import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = "data/raw/listings.csv.gz"
OUTPUT_PATH = Path("reports/profile_before.csv")


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("profiling")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Tout est lu en texte (pas d'inferSchema) : on veut voir les problemes de type
    # tels quels, par exemple un prix ecrit "$85.00".
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)  # certaines descriptions contiennent des retours a la ligne
        .option("quote", '"')
        .option("escape", '"')
        .csv(INPUT_PATH)
    )

    n_rows = df.count()
    n_cols = len(df.columns)
    print(f"\nLignes : {n_rows}  |  Colonnes : {n_cols}")

    # Doublons
    n_distinct_rows = df.distinct().count()
    print(f"Lignes entierement dupliquees : {n_rows - n_distinct_rows}")
    if "id" in df.columns:
        n_distinct_ids = df.select("id").distinct().count()
        print(f"Identifiants (id) en double : {n_rows - n_distinct_ids}")

    # Une valeur est "manquante" si elle est nulle ou faite uniquement d'espaces.
    # try_cast renvoie null quand le texte n'est pas un nombre (au lieu de planter).
    aggs = []
    for c in df.columns:
        col = F.col(f"`{c}`")
        is_missing = col.isNull() | (F.trim(col) == "")
        numeric = F.expr(f"try_cast(`{c}` AS DOUBLE)")
        aggs += [
            F.sum(F.when(is_missing, 1).otherwise(0)).alias(f"{c}__missing"),
            F.approx_count_distinct(col).alias(f"{c}__distinct"),
            F.min(numeric).alias(f"{c}__min"),
            F.max(numeric).alias(f"{c}__max"),
            F.first(col, ignorenulls=True).alias(f"{c}__sample"),
        ]
    row = df.agg(*aggs).collect()[0].asDict()

    report = []
    for c in df.columns:
        missing = row[f"{c}__missing"] or 0
        sample = row[f"{c}__sample"]
        sample_text = str(sample).replace("\n", " ")[:30] if sample is not None else ""
        report.append(
            {
                "column": c,
                "missing_count": missing,
                "missing_pct": round(100 * missing / n_rows, 2),
                "distinct_approx": row[f"{c}__distinct"],
                "numeric_min": row[f"{c}__min"],
                "numeric_max": row[f"{c}__max"],
                "sample_value": sample_text,
            }
        )
    report.sort(key=lambda r: r["missing_pct"], reverse=True)

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(report[0].keys()))
        writer.writeheader()
        writer.writerows(report)

    print("\nColonnes classees par pourcentage de valeurs manquantes :\n")
    print(f"{'column':<40}{'missing %':>10}{'distinct':>10}{'min':>12}{'max':>12}  sample")
    for r in report:
        mn = "" if r["numeric_min"] is None else f"{r['numeric_min']:.6g}"
        mx = "" if r["numeric_max"] is None else f"{r['numeric_max']:.6g}"
        print(
            f"{r['column'][:39]:<40}{r['missing_pct']:>10}{r['distinct_approx']:>10}"
            f"{mn:>12}{mx:>12}  {r['sample_value']}"
        )
    print(f"\nRapport enregistre dans {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()