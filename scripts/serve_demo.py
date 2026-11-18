#!/usr/bin/env python3
"""Serve the demo dashboard without the background HubSpot sync.

`watchdog serve` starts a timer that syncs every 15 minutes; against the demo's
placeholder token that just logs failures and stamps an error on the header.
This serves the same app over the seeded database and leaves it alone.

    python scripts/serve_demo.py [--home demo] [--port 8077]
"""
import argparse
import dataclasses
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn  # noqa: E402

from crm.config import load_config  # noqa: E402
from crm.web import create_app  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--home", default="demo", type=Path)
ap.add_argument("--port", type=int, default=8077)
args = ap.parse_args()

# The dashboard reads signals.toml from $WATCHDOG_HOME; `watchdog` sets this
# from --home before dispatching, and going straight to create_app skips that.
os.environ["WATCHDOG_HOME"] = str(args.home.resolve())

cfg = load_config(args.home / "config.toml")
if not cfg.db_path.is_absolute():
    cfg = dataclasses.replace(cfg, db_path=args.home / cfg.db_path)
uvicorn.run(create_app(cfg), host="127.0.0.1", port=args.port)
