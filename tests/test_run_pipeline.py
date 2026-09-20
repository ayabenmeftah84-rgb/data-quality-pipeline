"""Tests du lanceur de pipeline (sans lancer Spark)."""

import pytest

import run_pipeline


def test_default_plan_runs_every_step_in_order():
    assert run_pipeline.build_plan() == ["profile", "clean", "validate"]


def test_plan_keeps_pipeline_order_whatever_the_request_order():
    assert run_pipeline.build_plan(["validate", "clean"]) == ["clean", "validate"]


def test_unknown_step_is_rejected():
    with pytest.raises(ValueError, match="inconnue"):
        run_pipeline.build_plan(["clean", "deploy"])


def test_every_step_points_to_an_existing_script():
    for _, script in run_pipeline.STEPS.values():
        assert (run_pipeline.ROOT / script).is_file()


def test_validation_alone_requires_clean_data(monkeypatch, tmp_path):
    raw = tmp_path / "listings.csv.gz"
    raw.write_text("x")
    monkeypatch.setattr(run_pipeline, "RAW_FILE", raw)
    monkeypatch.setattr(run_pipeline, "CLEAN_DIR", tmp_path / "missing")
    assert run_pipeline.check_inputs(["validate"]) is not None
    assert run_pipeline.check_inputs(["clean", "validate"]) is None


def test_missing_raw_file_is_reported(monkeypatch, tmp_path):
    monkeypatch.setattr(run_pipeline, "RAW_FILE", tmp_path / "nope.csv.gz")
    message = run_pipeline.check_inputs(["clean"])
    assert message is not None and "introuvable" in message
