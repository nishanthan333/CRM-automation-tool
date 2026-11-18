import tomllib
from dataclasses import dataclass
from pathlib import Path


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
        ollama_model=ollama.get("model", "llama3.1"),
        ollama_url=ollama.get("url", "http://localhost:11434"),
        sync_interval_minutes=int(data.get("sync", {}).get("interval_minutes", 15)),
    )


def load_thresholds(path: Path) -> dict[str, dict[str, float]]:
    with open(path, "rb") as f:
        return tomllib.load(f)
