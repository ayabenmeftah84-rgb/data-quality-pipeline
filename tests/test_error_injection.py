"""Tests de l'injection d'erreurs et de la mesure de detection."""

import pytest

from cleaning import clean_dataframe
from error_injection import count_collateral, error_types, evaluate_detection, inject_errors

N_ROWS = 60   # lignes de depart, toutes valides
N = 3         # erreurs injectees par type


@pytest.fixture
def base_df(make_df):
    # Prix a 500 $ : multiplie par 100 = 50 000 $, donc clairement au-dessus du seuil
    return make_df(*[{"price": "$500.00"} for _ in range(N_ROWS)])


def collect_by_id(df):
    return {r["id"]: r.asDict() for r in df.collect()}


def test_injection_is_reproducible_for_a_given_seed(base_df):
    _, _, first = inject_errors(base_df, base_df, n=N, seed=1)
    _, _, second = inject_errors(base_df, base_df, n=N, seed=1)
    assert first == second


def test_a_different_seed_picks_different_rows(base_df):
    _, _, first = inject_errors(base_df, base_df, n=N, seed=1)
    _, _, second = inject_errors(base_df, base_df, n=N, seed=2)
    assert first != second


def test_each_row_receives_at_most_one_error(base_df):
    _, _, injected = inject_errors(base_df, base_df, n=N, seed=1)
    all_ids = [i for ids in injected.values() for i in ids]
    assert len(all_ids) == len(set(all_ids)) == N * len(injected)


def test_price_x100_changes_only_the_targeted_rows(base_df):
    corrupted, _, injected = inject_errors(base_df, base_df, n=N, seed=1)
    rows = collect_by_id(corrupted.dropDuplicates(["id"]))
    for i in injected["price_x100"]:
        assert rows[i]["price"] == "$50,000.00"
    untouched = set(rows) - set(injected["price_x100"]) - set(injected["price_negative"])
    assert all(rows[i]["price"] == "$500.00" for i in untouched)


def test_dates_are_really_swapped(base_df):
    corrupted, _, injected = inject_errors(base_df, base_df, n=N, seed=1)
    rows = collect_by_id(corrupted.dropDuplicates(["id"]))
    for i in injected["dates_swapped"]:
        assert rows[i]["first_review"] == "2024-01-01"
        assert rows[i]["last_review"] == "2020-01-01"


def test_duplicates_are_appended(base_df):
    corrupted, _, injected = inject_errors(base_df, base_df, n=N, seed=1)
    assert corrupted.count() == N_ROWS + N
    assert len(injected["duplicate_rows"]) == N


def test_error_types_needing_missing_columns_are_skipped(base_df):
    without_gps = base_df.drop("latitude", "longitude")
    _, types, injected = inject_errors(without_gps, without_gps, n=N, seed=1)
    assert "gps_far_shift" not in injected and "gps_small_shift" not in injected
    assert all("latitude" not in t.requires for t in types)


def test_only_valid_rows_are_corrupted(base_df):
    valid = base_df.filter("CAST(id AS INT) <= 30")
    _, _, injected = inject_errors(base_df, valid, n=N, seed=1)
    assert all(int(i) <= 30 for ids in injected.values() for i in ids)


def test_end_to_end_gross_errors_are_all_detected_and_subtle_ones_are_not(base_df):
    corrupted, types, injected = inject_errors(base_df, base_df, n=N, seed=1)
    clean, rejected, log = clean_dataframe(corrupted)
    results = {
        r["error_type"]: r
        for r in evaluate_detection(clean, rejected, types, injected, log["duplicate_ids_removed"])
    }
    # Erreurs grossieres : tout est detecte
    for name in ["price_x100", "price_negative", "rating_above_5", "dates_swapped",
                 "gps_far_shift", "host_about_phone", "duplicate_rows"]:
        assert results[name]["recall_pct"] == 100.0, name
    # Erreur subtile : un decalage de 5 km reste dans la boite de Paris, donc aucune detection
    assert results["gps_small_shift"]["recall_pct"] == 0.0
    # Aucune ligne n'est en quarantaine sans avoir ete corrompue
    assert count_collateral(rejected, injected, baseline_rejected_ids=[]) == 0


def test_every_error_type_has_a_detection_rule():
    for t in error_types():
        assert t.detection in {"quarantine", "nullified", "duplicates"}
        if t.detection == "quarantine":
            assert t.reason
