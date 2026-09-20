"""Tests de la logique de nettoyage (src/cleaning.py) sur de petits DataFrames."""

from cleaning import MAX_BEDS, MAX_PRICE, clean_dataframe


def run(df):
    """Nettoie df et renvoie (lignes propres, lignes rejetees, journal) indexees par id."""
    clean, rejected, log = clean_dataframe(df)
    return (
        {r["id"]: r for r in clean.collect()},
        {r["id"]: r for r in rejected.collect()},
        log,
    )


# --- Conversions de types ---------------------------------------------------------------

def test_price_with_dollar_sign_and_comma_becomes_a_number(make_df):
    clean, _, _ = run(make_df({"price": "$1,200.50"}))
    assert clean[1]["price"] == 1200.5


def test_missing_price_keeps_the_row_and_is_flagged(make_df):
    # Deux lignes : si toutes les valeurs etaient vides, la colonne serait supprimee.
    clean, _, _ = run(make_df({"price": ""}, {"price": "$50.00"}))
    assert clean[1]["price"] is None
    assert clean[1]["price_missing"] is True


def test_present_price_is_not_flagged_as_missing(make_df):
    clean, _, _ = run(make_df({}))
    assert clean[1]["price_missing"] is False


def test_19_digit_id_keeps_all_its_digits(make_df):
    big_id = "1073735761485091887"
    clean, _, _ = run(make_df({"id": big_id}))
    assert int(big_id) in clean


def test_unparseable_number_becomes_missing_and_is_counted(make_df):
    clean, _, log = run(make_df({"beds": "abc"}))
    assert clean[1]["beds"] is None
    assert log["values_lost_in_conversion"]["beds"] == 1


# --- Colonnes vides ----------------------------------------------------------------------

def test_fully_empty_columns_are_dropped(make_df):
    df = make_df({"host_since": None}, {"host_since": ""})
    clean, _, log = clean_dataframe(df)
    assert "host_since" not in clean.columns
    assert log["dropped_empty_columns"] == ["host_since"]


def test_partially_filled_columns_are_kept(make_df):
    df = make_df({"host_since": "2015-01-01"}, {"host_since": None})
    clean, _, log = clean_dataframe(df)
    assert "host_since" in clean.columns
    assert log["dropped_empty_columns"] == []


# --- Quarantaine : seuils (valeur limite acceptee, valeur limite + 1 rejetee) -----------

def test_price_at_the_limit_is_kept(make_df):
    clean, rejected, _ = run(make_df({"price": f"${MAX_PRICE}.00"}))
    assert 1 in clean and not rejected


def test_price_above_the_limit_is_quarantined(make_df):
    clean, rejected, _ = run(make_df({"price": f"${MAX_PRICE + 1}.00"}))
    assert not clean
    assert rejected[1]["reject_reason"] == "price_too_high"


def test_zero_price_is_quarantined(make_df):
    _, rejected, _ = run(make_df({"price": "$0.00"}))
    assert rejected[1]["reject_reason"] == "price_not_positive"


def test_beds_at_the_limit_is_kept_and_above_is_quarantined(make_df):
    clean, rejected, _ = run(make_df({"beds": str(MAX_BEDS)}, {"beds": str(MAX_BEDS + 1)}))
    assert 1 in clean
    assert rejected[2]["reject_reason"] == "beds_too_high"


def test_coordinates_outside_paris_are_quarantined(make_df):
    _, rejected, _ = run(make_df({"latitude": "40.71", "longitude": "-74.0"}))
    assert rejected[1]["reject_reason"] == "coordinates_outside_paris"


def test_first_review_after_last_review_is_quarantined(make_df):
    _, rejected, _ = run(make_df({"first_review": "2025-01-01", "last_review": "2020-01-01"}))
    assert rejected[1]["reject_reason"] == "first_review_after_last_review"


def test_all_violated_rules_are_reported(make_df):
    _, rejected, log = run(make_df({"price": f"${MAX_PRICE + 1}.00", "beds": str(MAX_BEDS + 1)}))
    assert set(rejected[1]["reject_reason"].split(",")) == {"price_too_high", "beds_too_high"}
    assert log["rows_quarantined"] == 1  # une ligne, meme si deux regles sont violees


def test_invalid_id_is_quarantined(make_df):
    _, rejected, _ = run(make_df({"id": "abc"}))
    assert list(rejected.values())[0]["reject_reason"] == "id_null_or_invalid"


def test_several_invalid_ids_are_all_quarantined_not_merged(make_df):
    _, rejected, log = run(make_df({"id": "abc"}, {"id": "def"}, {"id": ""}))
    assert log["rows_quarantined"] == 3
    assert log["rows_clean"] == 0


# --- Corrections de valeurs ---------------------------------------------------------------

def test_review_score_of_zero_becomes_missing(make_df):
    clean, _, log = run(make_df({"review_scores_rating": "0.0"}, {"review_scores_rating": "4.5"}))
    assert clean[1]["review_scores_rating"] is None
    assert clean[2]["review_scores_rating"] == 4.5
    assert log["review_score_zeros_set_to_null"] == 1


def test_host_about_made_only_of_digits_is_removed(make_df):
    clean, _, log = run(
        make_df(
            {"host_about": "75019"},
            {"host_about": "+33 6 12 34 56 78"},
            {"host_about": "Room 42, near the metro"},
        )
    )
    assert clean[1]["host_about"] is None
    assert clean[2]["host_about"] is None
    assert clean[3]["host_about"] == "Room 42, near the metro"
    assert log["host_about_numeric_set_to_null"] == 2


def test_long_minimum_nights_is_flagged_but_not_quarantined(make_df):
    clean, rejected, _ = run(make_df({"minimum_nights": "1000"}, {"minimum_nights": "30"}))
    assert clean[1]["flag_minimum_nights_gt_365"] is True
    assert clean[2]["flag_minimum_nights_gt_365"] is False
    assert not rejected


def test_text_is_trimmed(make_df):
    clean, _, _ = run(make_df({"host_about": "  Hello  "}))
    assert clean[1]["host_about"] == "Hello"


# --- Doublons et coherence du comptage -----------------------------------------------------

def test_duplicate_ids_are_removed(make_df):
    clean, _, log = run(make_df({"id": "7"}, {"id": "7"}, {"id": "8"}))
    assert sorted(clean) == [7, 8]
    assert log["duplicate_ids_removed"] == 1


def test_no_row_is_lost_or_invented(make_df):
    df = make_df(
        {},                                          # valide
        {"price": f"${MAX_PRICE + 1}.00"},           # quarantaine
        {"id": "1"},                                 # doublon de l'id 1
        {"id": "abc"},                               # id invalide -> quarantaine
        {"beds": str(MAX_BEDS + 1)},                 # quarantaine
    )
    clean, rejected, log = run(df)
    assert log["rows_in"] == 5
    assert log["rows_clean"] + log["rows_quarantined"] == log["rows_in"] - log["duplicate_ids_removed"]
    assert len(clean) + len(rejected) == log["rows_clean"] + log["rows_quarantined"]
