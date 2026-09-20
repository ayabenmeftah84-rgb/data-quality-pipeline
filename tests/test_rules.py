"""Tests de la construction des regles Great Expectations (sans calcul Spark)."""

from validate_data import build_rules


def labels(columns):
    return [label for label, _ in build_rules(columns)]


def test_no_rule_is_built_for_unknown_columns():
    assert labels({"name"}) == []


def test_id_rules_need_the_id_column():
    assert labels({"id"}) == ["id non nul", "id unique"]


def test_price_gets_a_range_rule_and_a_completeness_rule():
    result = labels({"price"})
    assert "price > 0 et <= 10 000" in result
    assert "price renseigne dans au moins 95 % des lignes" in result


def test_every_review_score_column_gets_its_own_rule():
    result = labels({"review_scores_rating", "review_scores_value"})
    assert result == ["review_scores_rating dans ]0, 5]", "review_scores_value dans ]0, 5]"]


def test_date_order_rule_needs_both_date_columns():
    assert labels({"first_review"}) == []
    assert labels({"first_review", "last_review"}) == ["last_review >= first_review"]


def test_full_column_set_builds_all_rules():
    columns = {"id", "price", "beds", "bedrooms", "bathrooms", "first_review", "last_review",
               "latitude", "longitude", "host_about", "review_scores_rating"}
    # 2 (id) + 2 (price) + 3 (beds, bedrooms, bathrooms) + 1 (une note) + 1 (dates)
    # + 2 (latitude, longitude) + 1 (host_about) = 12
    assert len(build_rules(columns)) == 12
