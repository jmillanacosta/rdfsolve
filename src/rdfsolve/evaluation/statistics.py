"""Estimate, compare and plan evaluations of systems that answer questions.

The unit of analysis is the question. Each system (a condition) makes repeated attempts at
each question. A condition is estimated by the mean over questions of its mean score per
question. Intervals come from a two-stage bootstrap: questions, then attempts within each
question. Two conditions are compared by a sign-flip test on the differences per question
(exact for few questions, else Monte Carlo), with the correction of Holm for several pairs.

For planning, the success of an attempt is modelled on the logit scale::

    logit P(success | question q, condition A) = mu + u_q
    logit P(success | question q, condition B) = mu + delta + u_q + w_q

with u_q ~ N(0, sigma_u^2), the difficulty of the question, and w_q ~ N(0, sigma_w^2), the
part of the effect that changes with the question. The parameters are fitted to a pilot by
maximum marginal likelihood, and the power of the sign-flip test is found by simulation.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from itertools import combinations, product

import numpy as np
import pandas as pd

Vector = np.ndarray


def question_means(attempts: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Give the mean score per question (rows) and condition (columns)."""
    return attempts.pivot_table(
        index="question", columns="condition", values=metric, aggfunc="mean"
    )


def bootstrap(
    attempts: pd.DataFrame, metric: str, *, draws: int = 10000, seed: int = 0, level: float = 0.95
) -> pd.DataFrame:
    """Give the estimate and a two-stage bootstrap interval of each condition and each pair.

    Only questions with attempts in all conditions are used. The pairs are paired by question.
    """
    rng = np.random.default_rng(seed)
    table = question_means(attempts, metric).dropna()
    questions, conditions = list(table.index), list(table.columns)
    picks = rng.integers(len(questions), size=(draws, len(questions)))
    samples: dict[str, Vector] = {}
    for condition in conditions:
        inner = np.empty((len(questions), draws))
        for i, question in enumerate(questions):
            chosen = (attempts.question == question) & (attempts.condition == condition)
            values = attempts.loc[chosen, metric].to_numpy(float)
            inner[i] = values[rng.integers(len(values), size=(draws, len(values)))].mean(axis=1)
        samples[condition] = inner[picks, np.arange(draws)[:, None]].mean(axis=1)
    tail = (1 - level) / 2
    rows = []
    pairs = [(c, table[c], samples[c]) for c in conditions] + [
        (f"{b} - {a}", table[b] - table[a], samples[b] - samples[a])
        for a, b in combinations(conditions, 2)
    ]
    for name, values, sample in pairs:
        rows.append(
            {
                "comparison": name,
                "questions": len(values),
                "estimate": float(values.mean()),
                "low": float(np.quantile(sample, tail)),
                "high": float(np.quantile(sample, 1 - tail)),
                "se": float(sample.std(ddof=1)),
            }
        )
    return pd.DataFrame(rows)


def sign_flip(
    differences: Sequence[float] | Vector, *, draws: int = 100000, seed: int = 0, exact: int = 16
) -> float:
    """Give the two-sided p value of a sign-flip test that the mean difference is zero.

    All sign patterns are used for up to `exact` differences; else `draws` random patterns.
    """
    values = np.asarray(differences, float)
    values = values[~np.isnan(values)]
    if not values.size or not values.any():
        return 1.0
    observed = abs(values.mean()) - 1e-12
    if values.size <= exact:
        signs = np.array(list(product((1, -1), repeat=values.size)), dtype=float)
        return float(np.mean(np.abs(signs @ values) / values.size >= observed))
    signs = np.random.default_rng(seed).choice((-1.0, 1.0), size=(draws, values.size))
    extreme = np.sum(np.abs(signs @ values) / values.size >= observed)
    return float((extreme + 1) / (draws + 1))


def holm(pvalues: dict[str, float]) -> dict[str, float]:
    """Give p values adjusted by the step-down method of Holm."""
    adjusted, running = {}, 0.0
    order = sorted(pvalues, key=lambda name: pvalues[name])
    for rank, name in enumerate(order):
        running = max(running, min(1.0, (len(order) - rank) * pvalues[name]))
        adjusted[name] = running
    return adjusted


def compare(attempts: pd.DataFrame, metric: str, *, seed: int = 0) -> pd.DataFrame:
    """Compare each pair of conditions by question: difference, wins, ties, losses, p values."""
    table = question_means(attempts, metric).dropna()
    rows = []
    for a, b in combinations(table.columns, 2):
        difference = (table[b] - table[a]).to_numpy()
        rows.append(
            {
                "comparison": f"{b} - {a}",
                "questions": len(difference),
                "difference": float(difference.mean()),
                "wins": int((difference > 0).sum()),
                "ties": int((difference == 0).sum()),
                "losses": int((difference < 0).sum()),
                "p": sign_flip(difference, seed=seed),
            }
        )
    frame = pd.DataFrame(rows)
    if len(frame):
        frame["p_holm"] = frame["comparison"].map(
            holm(dict(zip(frame.comparison, frame.p, strict=True)))
        )
    return frame


def icc(attempts: pd.DataFrame, metric: str, condition: str) -> float:
    """Give the intraclass correlation of attempts within questions, ICC(1); nan without repeats."""
    groups = [
        g[metric].to_numpy(float)
        for _, g in attempts[attempts.condition == condition].groupby("question")
    ]
    total = sum(len(g) for g in groups)
    if total <= len(groups) or len(groups) < 2:
        return math.nan
    grand = np.concatenate(groups).mean()
    size = total / len(groups)
    between = sum(len(g) * (g.mean() - grand) ** 2 for g in groups) / (len(groups) - 1)
    within = sum(((g - g.mean()) ** 2).sum() for g in groups) / (total - len(groups))
    denominator = between + (size - 1) * within
    return float(max(0.0, (between - within) / denominator)) if denominator > 0 else 1.0


def design_effect(repeats: int, correlation: float) -> float:
    """Give the factor by which repeated attempts at one question lose information."""
    return 1 + (repeats - 1) * correlation


def _expit(x: Vector) -> Vector:
    result: Vector = 1 / (1 + np.exp(-x))
    return result


def _log(p: Vector) -> Vector:
    result: Vector = np.log(np.clip(p, 1e-12, 1.0))
    return result


def nelder_mead(
    function: Callable[[Vector], float], start: Sequence[float], *, steps: int = 4000
) -> Vector:
    """Minimize a function with the simplex method of Nelder and Mead."""
    simplex = [np.asarray(start, float)]
    for i in range(len(start)):
        point = simplex[0].copy()
        point[i] += 0.5
        simplex.append(point)
    values = [function(p) for p in simplex]
    for _ in range(steps):
        order = np.argsort(values)
        simplex, values = [simplex[i] for i in order], [values[i] for i in order]
        if abs(values[-1] - values[0]) < 1e-10:
            break
        centre = np.mean(simplex[:-1], axis=0)
        reflected = centre + (centre - simplex[-1])
        value = function(reflected)
        if value < values[0]:
            expanded = centre + 2 * (centre - simplex[-1])
            better = function(expanded)
            simplex[-1], values[-1] = (expanded, better) if better < value else (reflected, value)
        elif value < values[-2]:
            simplex[-1], values[-1] = reflected, value
        else:
            contracted = centre + 0.5 * (simplex[-1] - centre)
            smaller = function(contracted)
            if smaller < values[-1]:
                simplex[-1], values[-1] = contracted, smaller
            else:
                simplex = [simplex[0] + 0.5 * (p - simplex[0]) for p in simplex]
                values = [function(p) for p in simplex]
    return simplex[int(np.argmin(values))]


@dataclass(frozen=True)
class Components:
    """The parameters of the planning model (see the module documentation)."""

    mu: float
    delta: float
    sigma_u: float
    sigma_w: float

    def rates(
        self, delta: float | None = None, *, draws: int = 200000, seed: int = 0
    ) -> tuple[float, float]:
        """Give the mean success rates of condition A and B over questions."""
        rng = np.random.default_rng(seed)
        u, w = rng.normal(0, self.sigma_u, draws), rng.normal(0, self.sigma_w, draws)
        shift = self.delta if delta is None else delta
        return float(_expit(self.mu + u).mean()), float(_expit(self.mu + shift + u + w).mean())


def fit_components(
    attempts: pd.DataFrame, metric: str, a: str, b: str, nodes: int = 24
) -> Components:
    """Fit the planning model to 0/1 outcomes of conditions a and b by marginal likelihood."""
    counts = attempts[attempts.condition.isin([a, b])].groupby(["question", "condition"])[metric]
    table = counts.agg(["sum", "count"]).unstack("condition").dropna()
    y_a, k_a = table[("sum", a)].to_numpy(), table[("count", a)].to_numpy()
    y_b, k_b = table[("sum", b)].to_numpy(), table[("count", b)].to_numpy()
    z, weights = np.polynomial.hermite_e.hermegauss(nodes)
    log_weights = np.log(weights / math.sqrt(2 * math.pi))

    def loss(theta: Vector) -> float:
        """Give the negative log marginal likelihood of the pilot."""
        mu, delta, log_u, log_w = theta
        sigma_u, sigma_w = math.exp(np.clip(log_u, -5, 2.5)), math.exp(np.clip(log_w, -5, 2.5))
        p_a = _expit(mu + sigma_u * z)
        p_b = _expit(mu + delta + sigma_u * z[:, None] + sigma_w * z[None, :])
        part_a = y_a[:, None] * _log(p_a) + (k_a - y_a)[:, None] * _log(1 - p_a)
        part_b = y_b[:, None, None] * _log(p_b) + (k_b - y_b)[:, None, None] * _log(1 - p_b)
        terms = part_a[:, :, None] + part_b + log_weights[:, None] + log_weights[None, :]
        top = terms.max(axis=(1, 2), keepdims=True)
        return float(-np.sum(np.log(np.exp(terms - top).sum(axis=(1, 2))) + top[:, 0, 0]))

    rate_a = (y_a.sum() + 0.5) / (k_a.sum() + 1)
    rate_b = (y_b.sum() + 0.5) / (k_b.sum() + 1)
    start = [
        math.log(rate_a / (1 - rate_a)),
        math.log(rate_b / (1 - rate_b) * (1 - rate_a) / rate_a),
        0.0,
        -1.0,
    ]
    mu, delta, log_u, log_w = nelder_mead(loss, start)
    return Components(
        float(mu),
        float(delta),
        math.exp(np.clip(log_u, -5, 2.5)),
        math.exp(np.clip(log_w, -5, 2.5)),
    )


def simulate_power(
    components: Components,
    questions: int,
    repeats: int,
    *,
    delta: float | None = None,
    alpha: float = 0.05,
    sims: int = 2000,
    flips: int = 2000,
    seed: int = 0,
) -> float:
    """Give the share of simulated studies in which the sign-flip test rejects at alpha."""
    rng = np.random.default_rng(seed)
    shift = components.delta if delta is None else delta
    u = rng.normal(0, components.sigma_u, (sims, questions))
    w = rng.normal(0, components.sigma_w, (sims, questions))
    y_a = rng.binomial(repeats, _expit(components.mu + u))
    y_b = rng.binomial(repeats, _expit(components.mu + shift + u + w))
    differences = (y_b - y_a) / repeats
    signs = rng.choice((-1.0, 1.0), size=(flips, questions))
    observed = np.abs(differences.mean(axis=1)) - 1e-12
    extreme = (np.abs(differences @ signs.T) / questions >= observed[:, None]).sum(axis=1)
    return float(np.mean((extreme + 1) / (flips + 1) <= alpha))


def power_table(
    components: Components,
    questions: Sequence[int],
    repeats: Sequence[int],
    deltas: Sequence[float],
    **options: float,
) -> pd.DataFrame:
    """Give the power for each number of questions, of repeats, and each effect (logit scale)."""
    rows = []
    for delta in deltas:
        rate_a, rate_b = components.rates(delta)
        for n, k in product(questions, repeats):
            power = simulate_power(components, n, k, delta=delta, **options)  # type: ignore[arg-type]
            rows.append(
                {
                    "questions": n,
                    "repeats": k,
                    "delta": delta,
                    "rate_a": rate_a,
                    "rate_b": rate_b,
                    "difference": rate_b - rate_a,
                    "power": power,
                }
            )
    return pd.DataFrame(rows)


def minimum_detectable(table: pd.DataFrame, power: float = 0.8) -> pd.DataFrame:
    """Give the smallest difference in success rate that reaches the power, per design."""
    reached = table[(table.power >= power) & (table.delta > 0)]
    best = reached.sort_values("delta").groupby(["questions", "repeats"], as_index=False).first()
    return best[["questions", "repeats", "difference", "power"]]
