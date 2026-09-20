"""Logique de nettoyage : fonctions sur DataFrame Spark, sans lecture ni ecriture de fichiers.

Ce module est separe de clean_data.py pour pouvoir etre teste sur de petits DataFrames.
"""

from pyspark.sql import functions as F

# Seuils provisoires : a justifier dans le README (centiles observes au profilage)
MAX_PRICE = 10_000
MAX_BEDS = 20
MAX_BEDROOMS = 15
MAX_BATHROOMS = 10
PARIS_LAT = (48.7, 49.0)
PARIS_LON = (2.1, 2.6)

NUMERIC_COLS = [
    "accommodates", "bedrooms", "beds", "bathrooms", "minimum_nights", "maximum_nights",
    "number_of_reviews", "latitude", "longitude", "price_quote_total_price",
    "price_quote_price_per_night", "estimated_revenue_l365d",
]
DATE_COLS = ["first_review", "last_review"]
BOOL_COLS = ["host_is_superhost", "host_has_profile_pic", "host_identity_verified", "has_availability"]


def is_blank(name):
    return F.col(f"`{name}`").isNull() | (F.trim(F.col(f"`{name}`")) == "")


def clean_dataframe(df):
    """Nettoie un DataFrame brut dont toutes les colonnes sont du texte.

    Renvoie (lignes_propres, lignes_rejetees, journal).
    Les lignes rejetees gardent une colonne `reject_reason` avec la liste des regles violees.
    """
    log = {"rows_in": df.count()}

    # 1. Textes : espaces retires, chaines vides -> null
    for c in df.columns:
        df = df.withColumn(c, F.when(is_blank(c), None).otherwise(F.trim(F.col(f"`{c}`"))))

    # 2. Suppression des colonnes entierement vides
    non_empty = df.agg(*[F.max(F.when(F.col(f"`{c}`").isNotNull(), 1).otherwise(0)).alias(c)
                         for c in df.columns]).collect()[0].asDict()
    empty_cols = [c for c, v in non_empty.items() if not v]
    df = df.drop(*empty_cols)
    log["dropped_empty_columns"] = empty_cols
    cols = set(df.columns)

    # 3. Conversion des types (try_cast : une valeur invalide devient null au lieu de planter)
    casts = {}
    if "id" in cols:
        casts["id"] = "BIGINT"          # ids a 19 chiffres : jamais en double
    if "price" in cols:
        df = df.withColumn("price", F.regexp_replace("price", r"[$,]", ""))
        casts["price"] = "DOUBLE"
    for c in NUMERIC_COLS:
        if c in cols:
            casts[c] = "DOUBLE"
    for c in DATE_COLS:
        if c in cols:
            casts[c] = "DATE"
    for c in [c for c in df.columns if c.startswith("review_scores_")]:
        casts[c] = "DOUBLE"
    before = df.agg(*[F.sum(F.col(f"`{c}`").isNotNull().cast("int")).alias(c) for c in casts]).collect()[0].asDict()
    for c, t in casts.items():
        df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS {t})"))
    after = df.agg(*[F.sum(F.col(f"`{c}`").isNotNull().cast("int")).alias(c) for c in casts]).collect()[0].asDict()
    log["values_lost_in_conversion"] = {c: (before[c] or 0) - (after[c] or 0) for c in casts
                                        if (before[c] or 0) != (after[c] or 0)}
    for c in BOOL_COLS:
        if c in cols:
            df = df.withColumn(c, F.when(F.col(c) == "t", True).when(F.col(c) == "f", False))

    # 4. Corrections de valeurs
    if "price" in cols:
        df = df.withColumn("price_missing", F.col("price").isNull())   # on garde la ligne, avec un indicateur
    score_cols = [c for c in df.columns if c.startswith("review_scores_")]
    zeros = 0
    for c in score_cols:                                                # hypothese : 0 = "pas de note"
        zeros += df.filter(F.col(c) == 0).count()
        df = df.withColumn(c, F.when(F.col(c) == 0, None).otherwise(F.col(c)))
    log["review_score_zeros_set_to_null"] = zeros
    if "host_about" in cols:                                            # champ texte contenant seulement des chiffres
        numeric_only = F.col("host_about").rlike(r"^[0-9\s.,+-]+$")
        log["host_about_numeric_set_to_null"] = df.filter(numeric_only).count()
        df = df.withColumn("host_about", F.when(numeric_only, None).otherwise(F.col("host_about")))
    if "minimum_nights" in cols:
        df = df.withColumn("flag_minimum_nights_gt_365", F.col("minimum_nights") > 365)

    # 5. Doublons sur l'identifiant (les id invalides ne sont pas fusionnes : ils vont en quarantaine)
    if "id" in cols:
        with_id = df.filter(F.col("id").isNotNull())
        without_id = df.filter(F.col("id").isNull())
        n = with_id.count()
        with_id = with_id.dropDuplicates(["id"])
        log["duplicate_ids_removed"] = n - with_id.count()
        df = with_id.unionByName(without_id)

    # 6. Quarantaine : chaque ligne recoit la liste des regles qu'elle viole
    reasons = []
    if "id" in cols:
        reasons.append(F.when(F.col("id").isNull(), F.lit("id_null_or_invalid")))
    if "price" in cols:
        reasons.append(F.when(F.col("price") <= 0, F.lit("price_not_positive")))
        reasons.append(F.when(F.col("price") > MAX_PRICE, F.lit("price_too_high")))
    for name, limit in [("beds", MAX_BEDS), ("bedrooms", MAX_BEDROOMS), ("bathrooms", MAX_BATHROOMS)]:
        if name in cols:
            reasons.append(F.when(F.col(name) > limit, F.lit(f"{name}_too_high")))
    for c in score_cols:
        reasons.append(F.when((F.col(c) < 0) | (F.col(c) > 5), F.lit(f"{c}_out_of_range")))
    if {"first_review", "last_review"} <= cols:
        reasons.append(F.when(F.col("first_review") > F.col("last_review"), F.lit("first_review_after_last_review")))
    if {"latitude", "longitude"} <= cols:
        outside = ~(F.col("latitude").between(*PARIS_LAT) & F.col("longitude").between(*PARIS_LON))
        reasons.append(F.when(outside, F.lit("coordinates_outside_paris")))

    df = df.withColumn("reject_reason", F.concat_ws(",", *reasons)).cache()
    clean = df.filter(F.col("reject_reason") == "").drop("reject_reason")
    rejected = df.filter(F.col("reject_reason") != "")
    log["rows_clean"] = clean.count()
    log["rows_quarantined"] = rejected.count()
    log["quarantine_reasons"] = {
        r["reason"]: r["n"]
        for r in rejected.select(F.explode(F.split("reject_reason", ",")).alias("reason"))
        .groupBy("reason").agg(F.count("*").alias("n")).collect()
    }
    return clean, rejected, log
