"""Score MCP experiment runs by RDF term and by resource, and report the statistics.

Usage (each run is LABEL=DIRECTORY; the label is added to the condition names)::

    python analysis.py baseline=RUN1 v1=RUN2 --examples examples/aopwiki.ttl \
        --endpoint https://aopwiki.rdf.bigcat-bioinformatics.org/sparql/ --out report \
        --plan endpoint:final rdfsolve:final

The primary outcome is the exact match at the resource level (resource_exact): each answer
row shows the same resources as one reference row, through any view (IRI, name, identifier,
page or key field). The term level gives the strict comparison of RDF terms; the linked level
also accepts records linked by owl:sameAs or skos:exactMatch (a sensitivity analysis).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mcp_experiment import load_examples  # noqa: E402

from rdfsolve.evaluation.scoring import LEVELS, score  # noqa: E402
from rdfsolve.evaluation.statistics import (  # noqa: E402
    bootstrap,
    compare,
    fit_components,
    icc,
    minimum_detectable,
    power_table,
    sensitivity,
    simulate_power,
)
from rdfsolve.evaluation.views import resolve, term_key  # noqa: E402

METRICS = [f"{level}_{name}" for level in LEVELS for name in ("f1", "exact")] + ["complete"]
FIGURE_METRICS = ["term_exact", "resource_exact", "linked_exact"]


def endpoint_select(endpoint):
    """Give a function that runs a SELECT on the endpoint and gives its bindings."""
    from rdfsolve.sparql_helper import SparqlHelper

    helper = SparqlHelper(endpoint, timeout=65)
    return lambda query: helper.select_with_fallback(query, purpose="evaluation views")["results"]["bindings"]


def load_run(label, run, cases):
    """Give the attempts of a run with their rows, and the reference rows by question."""
    run = Path(run)
    references = {}
    for index, case in enumerate(cases):
        path = run / "references" / f"before-{index}" / "result.json"
        if path.exists():
            data = json.loads(path.read_text())
            if data.get("state") == "complete":
                references[label, case["name"]] = data["bindings"]
    attempts = []
    for row in json.loads((run / "metrics.json").read_text()):
        if (label, row["question"]) not in references or row["state"] == "running":
            continue
        result_path = run / "attempts" / Path(row["directory"]).name / "result.json"
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        attempts.append(
            {
                "run": label,
                "question": row["question"],
                "condition": f"{row['condition']}:{label}",
                "seed": row["seed"],
                "temperature": row["temperature"],
                "state": row["state"],
                "bindings": result.get("bindings", []) if row["state"] == "complete" else [],
                "requests": (row.get("diagnostics") or {}).get("usage", {}).get("requests"),
                "seconds": (row.get("diagnostics") or {}).get("wall_seconds"),
            }
        )
    return attempts, references


def score_attempts(attempts, references, columns, select):
    """Add the scores at each level to each attempt, against the references of its run.

    The views of each value are resolved once and kept for all runs.
    """
    frame, views, seen, key_cache = [], {}, set(), {}
    for run, question in sorted({(a["run"], a["question"]) for a in attempts}):
        group = [a for a in attempts if (a["run"], a["question"]) == (run, question)]
        reference = references[run, question]
        cells = [
            row[c]
            for rows in [reference, *(a["bindings"] for a in group)]
            for row in rows
            for c in columns[question]
            if c in row and term_key(row[c]) not in seen
        ]
        views.update(resolve(cells, select, key_cache=key_cache))
        seen.update(term_key(cell) for cell in cells)
        for attempt in group:
            complete = attempt["state"] == "complete"
            record = {k: v for k, v in attempt.items() if k != "bindings"} | {"complete": float(complete)}
            for level in LEVELS:
                result = score(reference, attempt["bindings"], columns[question], level=level, views=views)
                record[f"{level}_f1"] = result.f1 if complete else 0.0
                record[f"{level}_exact"] = float(complete and result.exact)
                if level == "resource":
                    record["substitutions"] = dict(result.substitutions)
            frame.append(record)
    return pd.DataFrame(frame)


def report(frame, out, *, draws=10000, seed=20260926):
    """Write the estimates, the comparisons, the per-question scores and the substitutions."""
    out.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out / "attempts.csv", index=False)
    estimates, comparisons = [], []
    for metric in METRICS:
        estimates.append(bootstrap(frame, metric, draws=draws, seed=seed).assign(metric=metric))
        comparisons.append(compare(frame, metric, seed=seed).assign(metric=metric))
    estimates = pd.concat(estimates)
    comparisons = pd.concat(comparisons)
    estimates.to_csv(out / "estimates.csv", index=False)
    comparisons.to_csv(out / "comparisons.csv", index=False)
    per_question = frame.pivot_table(index="question", columns="condition", values=METRICS, aggfunc="mean")
    per_question.to_csv(out / "questions.csv")
    substitutions = Counter()
    for condition, row in zip(frame.condition, frame.substitutions, strict=True):
        for key, count in (row or {}).items():
            substitutions[(condition, key)] += count
    pd.DataFrame(
        [{"condition": c, "column": k.split(":")[0], "change": k.split(": ", 1)[1], "rows": n}
         for (c, k), n in substitutions.most_common()]
    ).to_csv(out / "substitutions.csv", index=False)
    correlations = pd.DataFrame(
        [{"condition": c, "icc_resource_exact": icc(frame, "resource_exact", c),
          "attempts_per_question": frame[frame.condition == c].groupby("question").size().mean()}
         for c in sorted(frame.condition.unique())]
    )
    correlations.to_csv(out / "correlations.csv", index=False)
    costs = frame.groupby("condition")[["requests", "seconds"]].mean()
    costs.to_csv(out / "costs.csv")
    figure(estimates, out)
    lines = ["# MCP evaluation report", "", "## Estimates (mean over questions, 95% two-stage bootstrap)", ""]
    lines += [estimates.to_markdown(index=False, floatfmt=".3f"), "", "## Paired comparisons (sign-flip, Holm)", ""]
    lines += [comparisons.to_markdown(index=False, floatfmt=".4f"), "", "## Intraclass correlation", ""]
    lines += [correlations.to_markdown(index=False, floatfmt=".3f"), "", "## Mean cost per attempt", ""]
    lines += [costs.to_markdown(floatfmt=".1f"), ""]
    (out / "report.md").write_text("\n".join(lines))
    return estimates, comparisons


def figure(estimates, out):
    """Draw the mean score of each condition with its interval, for each metric."""
    import matplotlib.pyplot as plt

    shown = estimates[~estimates.comparison.str.contains(" - ") & estimates.metric.isin(FIGURE_METRICS)]
    conditions = sorted(shown.comparison.unique())
    fig, ax = plt.subplots(figsize=(7, 0.5 + 0.45 * len(conditions) * 3), layout="constrained")
    for offset, metric in enumerate(FIGURE_METRICS):
        part = shown[shown.metric == metric].set_index("comparison").reindex(conditions)
        y = np.arange(len(conditions)) * 3.5 + offset * 0.9
        ax.errorbar(part.estimate, y, xerr=[part.estimate - part.low, part.high - part.estimate], fmt="o", label=metric, capsize=3)
    ax.set_yticks(np.arange(len(conditions)) * 3.5 + 1, conditions)
    ax.set(xlim=(-0.02, 1.02), xlabel="Mean over questions (95% interval)")
    ax.legend(frameon=False, loc="lower right")
    fig.savefig(out / "estimates.pdf")
    fig.savefig(out / "estimates.png", dpi=200)


def plan(frame, out, a, b, *, questions=(20, 40, 80, 160, 320), repeats=(1, 3, 5), sims=2000, seed=20260926):
    """Fit the planning model to conditions a and b, and write power and detectable effects.

    The fit uses weakly informative priors. Because a small pilot gives uncertain variance
    components, the smallest detectable difference is also given for other spreads of the
    difficulty of questions (sigma_u).
    """
    components = fit_components(frame, "resource_exact", a, b)
    deltas = np.round(np.arange(0.0, 4.01, 0.25), 2)
    table = power_table(components, questions, repeats, deltas, sims=sims, flips=1000, seed=seed)
    table.to_csv(out / "power.csv", index=False)
    detectable = minimum_detectable(table)
    detectable.to_csv(out / "detectable.csv", index=False)
    spread = sensitivity(components, questions, [3], (0.5, 1.0, 2.0, 3.0), deltas, sims=sims, flips=1000, seed=seed)
    spread.to_csv(out / "sensitivity.csv", index=False)
    observed = pd.DataFrame(
        [{"questions": n, "repeats": k, "power": simulate_power(components, n, k, sims=sims, flips=1000, seed=seed)}
         for n in questions for k in repeats]
    )
    observed.to_csv(out / "power-observed.csv", index=False)
    null = table[table.delta == 0][["questions", "repeats", "power"]].rename(columns={"power": "type_i_error"})
    rate_a, rate_b = components.rates()
    text = [
        "## Planning model (fitted to the pilot, weakly informative priors)", "",
        f"A = {a}, B = {b}: mu = {components.mu:.3f}, delta = {components.delta:.3f}, "
        f"sigma_u = {components.sigma_u:.3f}, sigma_w = {components.sigma_w:.3f}; "
        f"mean success rate A = {rate_a:.3f}, B = {rate_b:.3f}.", "",
        "### Power at the fitted effect", "", observed.to_markdown(index=False, floatfmt=".3f"), "",
        "### Type I error of the sign-flip test (simulated, delta = 0, alpha = 0.05)", "",
        null.to_markdown(index=False, floatfmt=".3f"), "",
        "### Smallest difference in success rate with power 0.8 (fitted variance)", "",
        detectable.to_markdown(index=False, floatfmt=".3f"), "",
        "### Sensitivity: smallest detectable difference for other sigma_u (3 attempts)", "",
        spread.to_markdown(index=False, floatfmt=".3f"), "",
    ]
    with (out / "report.md").open("a") as stream:
        stream.write("\n".join(text))
    return components, table


def main():
    """Read the runs, score them, and write the report."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("runs", nargs="+", help="LABEL=DIRECTORY")
    parser.add_argument("--examples", type=Path, required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--plan", nargs=2, metavar=("A", "B"))
    parser.add_argument("--draws", type=int, default=10000)
    args = parser.parse_args()
    cases, _ = load_examples(args.examples)
    columns = {case["name"]: case["columns"] for case in cases}
    attempts, references = [], {}
    for item in args.runs:
        label, run = item.split("=", 1)
        found, refs = load_run(label, run, cases)
        attempts += found
        references.update(refs)
    frame = score_attempts(attempts, references, columns, endpoint_select(args.endpoint))
    report(frame, args.out, draws=args.draws)
    if args.plan:
        plan(frame, args.out, *args.plan)
    print((args.out / "report.md").read_text())


if __name__ == "__main__":
    main()
