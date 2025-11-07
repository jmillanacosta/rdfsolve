#!/usr/bin/env python3
import glob
import json
import os
import sys

# Change working dir to repo root (one level up from scripts/)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
os.chdir(ROOT)

files = glob.glob('**/*.json', recursive=True)
if not files:
    print('No JSON files found')
    sys.exit(0)

for f in sorted(files):
    try:
        with open(f, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        print('---', f, '---')
        # Print a compact representation to keep output readable
        try:
            print(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception:
            # Fallback to repr if data not JSON-serializable
            print(repr(data))
    except Exception as e:
        print('ERROR loading', f, e, file=sys.stderr)
