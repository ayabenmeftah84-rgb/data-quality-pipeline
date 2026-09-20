import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    """Une session Spark locale, creee une seule fois pour tous les tests."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# Une ligne "parfaite" : aucune regle de qualite n'est violee.
VALID_ROW = {
    "id": "1",
    "price": "$100.00",
    "beds": "2",
    "bedrooms": "1",
    "bathrooms": "1.0",
    "minimum_nights": "2",
    "review_scores_rating": "4.8",
    "first_review": "2020-01-01",
    "last_review": "2024-01-01",
    "latitude": "48.85",
    "longitude": "2.35",
    "host_about": "Hello, I love Paris",
}


@pytest.fixture
def make_df(spark):
    """Construit un DataFrame brut (tout en texte) a partir de lignes qui modifient VALID_ROW.

    Chaque ligne recoit un id different (1, 2, 3...) sauf si le test en impose un.
    """

    def _make(*overrides):
        rows = []
        for i, override in enumerate(overrides, start=1):
            rows.append({**VALID_ROW, "id": str(i), **override})
        columns = list(VALID_ROW)
        for row in rows:
            for key in row:
                if key not in columns:
                    columns.append(key)
        schema = StructType([StructField(c, StringType(), True) for c in columns])
        return spark.createDataFrame([tuple(r.get(c) for c in columns) for r in rows], schema)

    return _make
