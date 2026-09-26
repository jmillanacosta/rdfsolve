"""Question-level estimates, paired tests, variance components and simulated power."""

import math

import numpy as np
import pandas as pd
import pytest
from rdfsolve.evaluation.statistics import (
    Components,
    bootstrap,
    compare,
    design_effect,
    fit_components,
    holm,
    icc,
    minimum_detectable,
    power_table,
    sign_flip,
    simulate_power,
)


def simulated(components, questions, repeats, seed=1):
    """Draw 0/1 attempts of conditions A and B from the planning model."""
    rng = np.random.default_rng(seed)
    rows = []
    for q in range(questions):
        u, w = rng.normal(0, components.sigma_u), rng.normal(0, components.sigma_w)
        for condition, logit in (("A", components.mu + u), ("B", components.mu + components.delta + u + w)):
            for s in range(repeats):
                rows.append({"question": f"q{q}", "condition": condition, "seed": s,
                             "success": float(rng.random() < 1 / (1 + math.exp(-logit)))})
    return pd.DataFrame(rows)


def test_sign_flip_exact_and_monte_carlo_and_holm():
    assert sign_flip([1, 1, 1, 1]) == pytest.approx(2 / 16)
    assert sign_flip([0, 0, 0]) == 1.0 and sign_flip([]) == 1.0
    assert sign_flip([1, -1, 1, -1]) == 1.0
    assert sign_flip(np.ones(30), draws=20000) < 0.001
    assert holm({"a": 0.01, "b": 0.04, "c": 0.03}) == pytest.approx({"a": 0.03, "b": 0.06, "c": 0.06})


def test_bootstrap_and_comparison_are_paired_by_question():
    rows = [{"question": f"q{q}", "condition": c, "seed": s, "f1": q / 10 + (0.3 if c == "B" else 0)}
            for q in range(10) for c in "AB" for s in range(3)]
    attempts = pd.DataFrame(rows)
    table = bootstrap(attempts, "f1", draws=2000).set_index("comparison")
    assert table.loc["B - A", "estimate"] == pytest.approx(0.3)
    assert table.loc["B - A", "low"] == pytest.approx(0.3) and table.loc["B - A", "se"] < 1e-9
    assert table.loc["A", "low"] < 0.45 < table.loc["A", "high"]
    row = compare(attempts, "f1").iloc[0]
    assert (row.wins, row.ties, row.losses, row.p, row.p_holm) == (10, 0, 0, pytest.approx(2 / 1024), pytest.approx(2 / 1024))


def test_intraclass_correlation_and_design_effect():
    same = pd.DataFrame([{"question": q, "condition": "A", "x": float(q % 2)} for q in range(8) for _ in range(3)])
    assert icc(same, "x", "A") == pytest.approx(1.0)
    assert math.isnan(icc(same.drop_duplicates("question"), "x", "A"))
    assert design_effect(5, 0.5) == 3.0


def test_fitted_components_recover_the_pilot_model():
    truth = Components(mu=-0.5, delta=1.2, sigma_u=1.5, sigma_w=0.2)
    fitted = fit_components(simulated(truth, 400, 5), "success", "A", "B")
    assert fitted.mu == pytest.approx(truth.mu, abs=0.3)
    assert fitted.delta == pytest.approx(truth.delta, abs=0.3)
    assert fitted.sigma_u == pytest.approx(truth.sigma_u, abs=0.35)


def test_simulated_power_is_calibrated_and_grows_with_questions():
    model = Components(mu=0.0, delta=0.0, sigma_u=1.5, sigma_w=0.5)
    assert simulate_power(model, 20, 3, sims=1500, flips=500) <= 0.075, "Type I error near alpha"
    table = power_table(model, [10, 60], [3], [0.0, 1.5], sims=400, flips=400)
    strong = table[table.delta == 1.5].set_index("questions").power
    assert strong[60] > 0.9 and strong[60] > strong[10]
    assert table.difference.iloc[0] == pytest.approx(0.0, abs=0.01)
    found = minimum_detectable(table)
    assert list(found.questions) == [60] and found.difference.iloc[0] > 0.2
