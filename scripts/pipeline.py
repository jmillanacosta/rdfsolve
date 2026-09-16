#!/usr/bin/env python
"""Run source mining and connectivity analysis."""

import logging

from pipeline_stages.cli import main

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    main()
