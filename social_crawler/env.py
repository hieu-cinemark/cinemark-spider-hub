"""Single place that calls load_dotenv(). Both settings.py and accounts.py
need .env loaded before they read os.getenv(...) at import time, but neither
can rely on the other having run first (either can be imported standalone).
Importing this module - from either of them, or anywhere else - loads .env
exactly once: Python only executes a module's top level the first time it's
imported, so whichever of them imports this first pays the read/parse cost,
and every later importer just gets the cached module back."""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()
