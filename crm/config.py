import os
import tomllib
from dataclasses import dataclass
from datetime import tzinfo
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass(frozen=True)
class Config:
    hubspot_token: str
    db_path: Path
    timezone: str
    week_starts_on: str
    ollama_model: str
    ollama_url: str
    sync_interval_minutes: int


def load_config(path: Path) -> Config:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    token = data.get("hubspot", {}).get("token")
    if not token:
        raise ValueError("config: [hubspot] token is required")
    local = data.get("local", {})
    ollama = data.get("ollama", {})
    return Config(
        hubspot_token=token,
        db_path=Path(local.get("db_path", "data/crm.db")),
        timezone=local.get("timezone", "local"),
        week_starts_on=local.get("week_starts_on", "monday"),
        ollama_model=ollama.get("model", "qwen2.5-coder:7b"),
        ollama_url=ollama.get("url", "http://localhost:11434"),
        sync_interval_minutes=int(data.get("sync", {}).get("interval_minutes", 15)),
    )


def load_thresholds(path: Path) -> dict[str, dict[str, float]]:
    with open(path, "rb") as f:
        return tomllib.load(f)


WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
            "sunday"]


def home_dir() -> Path:
    """Where config.toml and signals.toml live: $WATCHDOG_HOME, else the
    current directory. Not the package location, which is site-packages once
    installed."""
    return Path(os.environ.get("WATCHDOG_HOME") or Path.cwd())


def signal_settings(path: Path) -> tuple[tzinfo | None, str]:
    """(timezone, week start day) for signal evaluation, from config.toml.

    A missing file or key gives the defaults (machine-local, monday). "local"
    maps to None, meaning the machine's timezone. Both values are validated
    here, at the point of use: an unknown zone or weekday raises ValueError.
    """
    local = {}
    if Path(path).exists():
        with open(path, "rb") as f:
            local = tomllib.load(f).get("local", {})
    name = str(local.get("timezone", "local"))
    try:
        tz = None if name.lower() == "local" else ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"config: unknown timezone {name!r}") from exc
    week = str(local.get("week_starts_on", "monday")).strip().lower()
    if week not in WEEKDAYS:
        raise ValueError(f"config: week_starts_on must be a weekday, got {week!r}")
    return tz, week
