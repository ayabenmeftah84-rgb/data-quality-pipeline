"""Validation de la qualite avec Great Expectations, avant et apres nettoyage.

AVANT : donnees brutes, avec seulement un typage (aucune correction de valeur)
APRES : donnees nettoyees (data/clean/listings_clean.parquet)
Sorties : reports/validation_report.json et reports/validation_report.md
"""

import json
import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")   # pas d'envoi de statistiques d'usage

from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context.types.base import ProgressBarsConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "data/raw/listings.csv.gz"
CLEAN_PATH = "data/clean/listings_clean.parquet"
REPORT_JSON = Path("reports/validation_report.json")
REPORT_MD = Path("reports/validation_report.md")

NUMERIC_COLS = [
    "accommodates", "bedrooms", "beds", "bathrooms", "minimum_nights", "maximum_nights",
    "number_of_reviews", "latitude", "longitude", "price_quote_total_price",
    "price_quote_price_per_night", "estimated_revenue_l365d",
]
DATE_COLS = ["first_review", "last_review"]


def load_typed_raw(spark):
    """Donnees brutes avec typage minimal : aucune valeur n'est corrigee ni supprimee."""
    df = (
        spark.read.option("header", True).option("multiLine", True)
        .option("quote", '"').option("escape", '"').csv(RAW_PATH)
    )
    for c in df.columns:
        col = F.col(f"`{c}`")
        df = df.withColumn(c, F.when(col.isNull() | (F.trim(col) == ""), None).otherwise(F.trim(col)))
    cols = set(df.columns)
    if "id" in cols:
        df = df.withColumn("id", F.expr("try_cast(id AS BIGINT)"))
    if "price" in cols:
        df = df.withColumn("price", F.expr("try_cast(regexp_replace(price, '[$,]', '') AS DOUBLE)"))
    for c in NUMERIC_COLS + [c for c in df.columns if c.startswith("review_scores_")]:
        if c in cols:
            df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS DOUBLE)"))
    for c in DATE_COLS:
        if c in cols:
            df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS DATE)"))
    return df.cache()


def build_rules(columns):
    """Liste de (libelle, expectation). Une regle n'est creee que si la colonne existe."""
    cols = set(columns)
    rules = []

    def add(label, needed, expectation):
        if set(needed) <= cols:
            rules.append((label, expectation))

    add("id non nul", ["id"], gxe.ExpectColumnValuesToNotBeNull(column="id"))
    add("id unique", ["id"], gxe.ExpectColumnValuesToBeUnique(column="id"))
    add("price > 0 et <= 10 000", ["price"],
        gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0, strict_min=True, max_value=10_000))
    add("price renseigne dans au moins 95 % des lignes", ["price"],
        gxe.ExpectColumnValuesToNotBeNull(column="price", mostly=0.95))
    add("beds <= 20", ["beds"], gxe.ExpectColumnValuesToBeBetween(column="beds", min_value=0, max_value=20))
    add("bedrooms <= 15", ["bedrooms"],
        gxe.ExpectColumnValuesToBeBetween(column="bedrooms", min_value=0, max_value=15))
    add("bathrooms <= 10", ["bathrooms"],
        gxe.ExpectColumnValuesToBeBetween(column="bathrooms", min_value=0, max_value=10))
    for c in sorted(c for c in cols if c.startswith("review_scores_")):
        add(f"{c} dans ]0, 5]", [c],
            gxe.ExpectColumnValuesToBeBetween(column=c, min_value=0, strict_min=True, max_value=5))
    add("last_review >= first_review", ["first_review", "last_review"],
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="last_review", column_B="first_review", or_equal=True))
    add("latitude dans la boite de Paris", ["latitude"],
        gxe.ExpectColumnValuesToBeBetween(column="latitude", min_value=48.7, max_value=49.0))
    add("longitude dans la boite de Paris", ["longitude"],
        gxe.ExpectColumnValuesToBeBetween(column="longitude", min_value=2.1, max_value=2.6))
    add("host_about pas uniquement numerique", ["host_about"],
        gxe.ExpectColumnValuesToNotMatchRegex(column="host_about", regex=r"^[0-9\s.,+-]+$"))
    return rules


def validate(context, df, name, rules):
    """Valide toutes les regles sur df ; renvoie {libelle: resultat}."""
    ds = context.data_sources.add_spark(name=f"src_{name}")
    asset = ds.add_dataframe_asset(name=f"listings_{name}")
    batch = asset.add_batch_definition_whole_dataframe("whole").get_batch(batch_parameters={"dataframe": df})
    out = {}
    for label, expectation in rules:
        res = batch.validate(expectation)
        r = res.result
        out[label] = {
            "success": bool(res.success),
            "unexpected_count": r.get("unexpected_count"),
            "unexpected_percent": round(r["unexpected_percent"], 3) if r.get("unexpected_percent") is not None else None,
            "checked_rows": r.get("element_count"),
        }
    return out


def count_empty_columns(df):
    row = df.agg(*[F.max(F.when(F.col(f"`{c}`").isNotNull(), 1).otherwise(0)).alias(c) for c in df.columns]).collect()[0]
    return sum(1 for c in df.columns if not row[c])


def main():
    spark = SparkSession.builder.master("local[*]").appName("validation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)

    before_df = load_typed_raw(spark)
    after_df = spark.read.parquet(CLEAN_PATH).cache()

    summary = {
        "before": {"rows": before_df.count(), "columns": len(before_df.columns),
                   "fully_empty_columns": count_empty_columns(before_df)},
        "after": {"rows": after_df.count(), "columns": len(after_df.columns),
                  "fully_empty_columns": count_empty_columns(after_df)},
    }
    # Les regles sont definies sur les colonnes communes, pour comparer avant/apres
    rules = build_rules(set(before_df.columns) & set(after_df.columns))
    print(f"{len(rules)} regles a verifier...")
    before = validate(context, before_df, "before", rules)
    after = validate(context, after_df, "after", rules)

    report = {"summary": summary, "rules": {label: {"before": before[label], "after": after[label]} for label, _ in rules}}
    summary["before"]["rules_passed"] = sum(v["success"] for v in before.values())
    summary["after"]["rules_passed"] = sum(v["success"] for v in after.values())
    summary["rules_total"] = len(rules)

    REPORT_JSON.parent.mkdir(exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    def cell(v):
        status = "PASS" if v["success"] else "FAIL"
        if v["unexpected_count"]:
            return f"{status} ({v['unexpected_count']} lignes, {v['unexpected_percent']} %)"
        return status

    lines = [
        "| Regle | Avant nettoyage | Apres nettoyage |",
        "|---|---|---|",
    ] + [f"| {label} | {cell(before[label])} | {cell(after[label])} |" for label, _ in rules]
    lines += [
        "",
        f"Regles respectees : {summary['before']['rules_passed']}/{len(rules)} avant, "
        f"{summary['after']['rules_passed']}/{len(rules)} apres.",
        f"Lignes : {summary['before']['rows']} avant, {summary['after']['rows']} apres.",
        f"Colonnes : {summary['before']['columns']} avant, {summary['after']['columns']} apres "
        f"(colonnes entierement vides : {summary['before']['fully_empty_columns']} -> {summary['after']['fully_empty_columns']}).",
    ]
    REPORT_MD.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nRapports : {REPORT_JSON} et {REPORT_MD}")
    spark.stop()


if __name__ == "__main__":
    main()