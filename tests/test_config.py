from pathlib import Path

import pytest

from crm.config import load_config, load_thresholds


def test_load_config_reads_token_and_defaults(tmp_path):
    (tmp_path / "config.toml").write_text('[hubspot]\ntoken = "pat-na1-abc"\n')
    cfg = load_config(tmp_path / "config.toml")
    assert cfg.hubspot_token == "pat-na1-abc"
    assert cfg.week_starts_on == "monday"  # spec default
    assert cfg.sync_interval_minutes == 15


def test_load_config_rejects_missing_token(tmp_path):
    (tmp_path / "config.toml").write_text("[hubspot]\n")
    with pytest.raises(ValueError, match="token"):
        load_config(tmp_path / "config.toml")


def test_load_thresholds_reads_shipped_signals_toml():
    t = load_thresholds(Path(__file__).parent.parent / "signals.toml")
    assert t["thresholds"]["stalled_days"] == 14
    assert t["thresholds"]["big_deal_value"] == 50000
