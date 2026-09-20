"""Exploration ciblee des defauts suspectes, pour choisir les regles de validation."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = "data/raw/listings.csv.gz"


def read_raw(spark):
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(INPUT_PATH)
    )


def title(text):
    print(f"\n=== {text} ===")


def main():
    spark = SparkSession.builder.master("local[*]").appName("explore").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = read_raw(spark)
    cols = set(df.columns)
    total = df.count()
    print(f"Lignes : {total}")

    def num(name):
        return F.expr(f"try_cast(`{name}` AS DOUBLE)")

    def missing(name):
        return F.col(name).isNull() | (F.trim(F.col(name)) == "")

    # 1. Format du prix
    if "price" in cols:
        title("1. Format de price")
        price_ok = F.col("price").rlike(r"^\$[0-9,]+(\.[0-9]+)?$")
        bad = df.filter(~missing("price") & ~price_ok)
        print(f"Prix non vides qui ne respectent pas le format $123.45 : {bad.count()}")
        bad.select("price").distinct().limit(5).show(truncate=False)

        df = df.withColumn(
            "price_num", F.expr("try_cast(regexp_replace(price, '[$,]', '') AS DOUBLE)")
        )
        print(f"Prix <= 0 apres conversion : {df.filter(F.col('price_num') <= 0).count()}")
        q = df.approxQuantile("price_num", [0.01, 0.5, 0.99, 0.999], 0.001)
        print(f"Prix (apres conversion) : 1% = {q[0]}, mediane = {q[1]}, 99% = {q[2]}, 99.9% = {q[3]}")
        print("Les 5 prix les plus eleves :")
        df.select("id", "price_num").orderBy(F.col("price_num").desc()).limit(5).show()

    # 2. Coherence entre price et price_quote_price_per_night
    if {"price", "price_quote_price_per_night"} <= cols:
        title("2. Coherence price / price_quote_price_per_night")
        a = missing("price")
        b = missing("price_quote_price_per_night")
        print(f"price vide mais devis present : {df.filter(a & ~b).count()}")
        print(f"price present mais devis vide : {df.filter(~a & b).count()}")

    # 3. Colonnes de comptage : beds, bathrooms, bedrooms, accommodates
    for name in ["beds", "bathrooms", "bedrooms", "accommodates", "minimum_nights"]:
        if name in cols:
            title(f"3. Valeurs extremes de {name}")
            d = df.withColumn("v", num(name))
            q = d.approxQuantile("v", [0.5, 0.99, 0.999], 0.001)
            print(f"mediane = {q[0]}, 99% = {q[1]}, 99.9% = {q[2]}")
            d.filter(F.col("v").isNotNull()).select("id", "v").orderBy(F.col("v").desc()).limit(5).show()

    # 4. Notes d'avis
    score_cols = [c for c in df.columns if c.startswith("review_scores_")]
    if score_cols:
        title("4. Notes d'avis (attendu : entre 0 et 5)")
        for c in score_cols:
            v = num(c)
            out = df.filter(v.isNotNull() & ((v < 0) | (v > 5))).count()
            zeros = df.filter(v == 0).count()
            print(f"{c}: hors [0,5] = {out}, egales a 0 = {zeros}")

    # 5. Dates des avis
    if {"first_review", "last_review"} <= cols:
        title("5. first_review <= last_review")
        fr = F.expr("try_cast(first_review AS DATE)")
        lr = F.expr("try_cast(last_review AS DATE)")
        print(f"first_review apres last_review : {df.filter(fr > lr).count()}")
        print(f"first_review non vide mais non convertible en date : "
              f"{df.filter(~missing('first_review') & fr.isNull()).count()}")

    # 6. Coordonnees GPS (boite englobante approximative de Paris)
    if {"latitude", "longitude"} <= cols:
        title("6. Coordonnees hors de Paris (lat 48.7-49.0, lon 2.1-2.6)")
        lat, lon = num("latitude"), num("longitude")
        out = df.filter(lat.isNotNull() & lon.isNotNull() & ~(lat.between(48.7, 49.0) & lon.between(2.1, 2.6)))
        print(f"Annonces hors de la boite : {out.count()}")

    # 7. host_about numerique
    if "host_about" in cols:
        title("7. host_about contenant seulement des chiffres")
        n = df.filter(F.col("host_about").rlike(r"^[0-9\s.,+-]+$")).count()
        print(f"Valeurs numeriques dans un champ texte : {n}")

    spark.stop()


if __name__ == "__main__":
    main()