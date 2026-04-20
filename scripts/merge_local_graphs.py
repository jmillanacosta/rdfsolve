#!/usr/bin/env python3
"""
Merge locally-discovered graphs into discovered_graphs.yaml.

Usage:
    python scripts/merge_local_graphs.py --local local_graphs.yaml [--target discovered_graphs.yaml]
"""
import argparse
import yaml
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", required=True, help="Local discovery results YAML")
    parser.add_argument("--target", default="discovered_graphs.yaml")
    args = parser.parse_args()

    with open(args.local) as f:
        local = yaml.safe_load(f)
    with open(args.target) as f:
        target = yaml.safe_load(f)

    local_by_name = {d["name"]: d for d in local}
    updated = 0

    for d in target:
        name = d["name"]
        if name in local_by_name:
            loc = local_by_name[name]
            if loc.get("graphs") and not loc.get("error"):
                d["discovered_graphs"] = loc["graphs"]
                d.pop("error", None)
                updated += 1
                print(f"{name}: {len(loc['graphs'])} graphs")
            elif not d.get("discovered_graphs"):
                # Update error message
                if loc.get("error"):
                    d["error"] = f"local: {loc['error']}"
                    print(f"{name}: still failed locally")

    with open(args.target, "w") as f:
        yaml.dump(target, f, default_flow_style=False, sort_keys=False, width=200)

    print(f"\nMerged {updated} entries into {args.target}")


if __name__ == "__main__":
    main()
