"""Injection d'erreurs connues dans les donnees brutes, pour mesurer ce que le nettoyage detecte.

Principe : on choisit au hasard (mais de facon reproductible) des lignes valides, on y glisse
une erreur d'un type donne, et on retient leurs identifiants. Apres nettoyage, on compte combien
de ces lignes ont ete detectees. Les erreurs sont SYNTHETIQUES : elles ne viennent pas du fichier d'origine.
"""

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import functions as F


def _num(name):
    return F.expr(f"try_cast(`{name}` AS DOUBLE)")


def _price():
    return F.expr("try_cast(regexp_replace(price, '[$,]', '') AS DOUBLE)")


def _date(name):
    return F.expr(f"try_cast(`{name}` AS DATE)")


def _replace(df, column, target, new_value):
    """Remplace `column` par `new_value` uniquement sur les lignes ciblees."""
    return df.withColumn(column, F.when(target, new_value).otherwise(F.col(f"`{column}`")))


# --- Fonctions d'injection : (DataFrame, condition sur les lignes ciblees) -> DataFrame -------

def _inject_price_x100(df, target):
    return _replace(df, "price", target, F.concat(F.lit("$"), F.format_number(_price() * 100, 2)))


def _inject_price_negative(df, target):
    return _replace(df, "price", target, F.concat(F.lit("$-"), F.regexp_replace("price", r"^\$", "")))


def _inject_beds_extra_digit(df, target):
    # faute de frappe : un chiffre en trop (2 -> 23)
    digits = F.expr("cast(try_cast(try_cast(beds AS DOUBLE) AS INT) AS STRING)")
    return _replace(df, "beds", target, F.concat(digits, F.lit("3")))


def _inject_rating_above_5(df, target):
    return _replace(df, "review_scores_rating", target, F.lit("7.5"))


def _inject_dates_swapped(df, target):
    first, last = F.col("first_review"), F.col("last_review")
    return (
        df.withColumn("_first", F.when(target, last).otherwise(first))
        .withColumn("_last", F.when(target, first).otherwise(last))
        .drop("first_review", "last_review")
        .withColumnRenamed("_first", "first_review")
        .withColumnRenamed("_last", "last_review")
    )


def _gps_shift(degrees):
    def inject(df, target):
        return _replace(df, "latitude", target, (_num("latitude") + degrees).cast("string"))

    return inject


def _inject_host_about_phone(df, target):
    return _replace(df, "host_about", target, F.lit("0612345678"))


@dataclass(frozen=True)
class ErrorType:
    name: str
    description: str
    requires: tuple            # colonnes necessaires
    pool: Callable             # () -> condition : lignes eligibles a cette erreur
    inject: Callable           # (df, target) -> df ; None pour les doublons
    detection: str             # "quarantine" | "nullified" | "duplicates"
    reason: str = ""           # motif de quarantaine attendu
    column: str = ""           # colonne qui doit devenir vide ("nullified")


def error_types():
    return [
        ErrorType("price_x100", "Price multiplied by 100", ("price",),
                  lambda: _price() > 0, _inject_price_x100, "quarantine", reason="price_too_high"),
        ErrorType("price_negative", "Price made negative", ("price",),
                  lambda: _price() > 0, _inject_price_negative, "quarantine", reason="price_not_positive"),
        ErrorType("beds_extra_digit", "Extra digit typed in beds (2 becomes 23)", ("beds",),
                  lambda: _num("beds") >= 1, _inject_beds_extra_digit, "quarantine", reason="beds_too_high"),
        ErrorType("rating_above_5", "Review rating set to 7.5", ("review_scores_rating",),
                  lambda: _num("review_scores_rating").between(1, 5), _inject_rating_above_5,
                  "quarantine", reason="review_scores_rating_out_of_range"),
        ErrorType("dates_swapped", "first_review and last_review swapped", ("first_review", "last_review"),
                  lambda: _date("first_review") < _date("last_review"), _inject_dates_swapped,
                  "quarantine", reason="first_review_after_last_review"),
        ErrorType("gps_far_shift", "Latitude shifted by 5 degrees (about 550 km)", ("latitude", "longitude"),
                  lambda: _num("latitude").isNotNull(), _gps_shift(5.0),
                  "quarantine", reason="coordinates_outside_paris"),
        ErrorType("gps_small_shift", "Latitude shifted by 0.05 degrees (about 5 km)", ("latitude", "longitude"),
                  lambda: _num("latitude").isNotNull(), _gps_shift(0.05),
                  "quarantine", reason="coordinates_outside_paris"),
        ErrorType("host_about_phone", "Free text replaced by a phone number", ("host_about",),
                  lambda: F.col("host_about").isNotNull() & (F.trim(F.col("host_about")) != "")
                  & ~F.col("host_about").rlike(r"^[0-9\s.,+-]+$"),
                  _inject_host_about_phone, "nullified", column="host_about"),
        ErrorType("duplicate_rows", "Row duplicated (same id)", ("id",),
                  lambda: F.lit(True), None, "duplicates"),
    ]


def inject_errors(df, valid_ids, n=500, seed=42):
    """Injecte `n` erreurs de chaque type dans des lignes distinctes.

    df        : DataFrame brut (colonnes en texte)
    valid_ids : DataFrame avec une colonne `id` (texte) : les lignes valides d'origine.
                Seules ces lignes peuvent etre corrompues, pour ne pas melanger avec les
                defauts deja presents dans les donnees.
    Renvoie (df_corrompu, types_utilises, injected) ou injected = {nom_du_type: [ids]}.
    """
    base = df.join(valid_ids.select("id").distinct(), "id", "left_semi")
    used, injected, types_used = set(), {}, []
    for t in error_types():
        if not set(t.requires) <= set(df.columns):
            continue
        candidates = base.filter(t.pool())
        if used:
            candidates = candidates.filter(~F.col("id").isin(list(used)))
        rows = candidates.select("id").orderBy(F.xxhash64("id", F.lit(seed))).limit(n).collect()
        ids = [r["id"] for r in rows]
        if not ids:
            continue
        used.update(ids)
        injected[t.name] = ids
        types_used.append(t)
        target = F.col("id").isin(ids)
        if t.detection == "duplicates":
            df = df.unionByName(df.filter(target))
        else:
            df = t.inject(df, target)
    return df, types_used, injected


def evaluate_detection(clean, rejected, types, injected, duplicates_removed):
    """Pour chaque type d'erreur : combien de lignes injectees ont ete detectees ?"""
    results = []
    for t in types:
        ids = injected.get(t.name)
        if not ids:
            continue
        int_ids = [int(i) for i in ids]
        if t.detection == "quarantine":
            detected = rejected.filter(
                F.col("id").isin(int_ids) & F.col("reject_reason").contains(t.reason)
            ).count()
        elif t.detection == "nullified":
            detected = clean.filter(F.col("id").isin(int_ids) & F.col(t.column).isNull()).count()
        else:  # doublons : une ligne par doublon supprime
            detected = min(len(ids), duplicates_removed)
        results.append(
            {
                "error_type": t.name,
                "description": t.description,
                "injected": len(ids),
                "detected": detected,
                "recall_pct": round(100 * detected / len(ids), 1),
            }
        )
    return results


def count_collateral(rejected, injected, baseline_rejected_ids):
    """Lignes en quarantaine qui ne sont ni injectees ni deja rejetees avant l'experience."""
    injected_ids = {int(i) for ids in injected.values() for i in ids}
    rejected_ids = {r["id"] for r in rejected.select("id").distinct().collect()}
    return len(rejected_ids - injected_ids - set(baseline_rejected_ids))
