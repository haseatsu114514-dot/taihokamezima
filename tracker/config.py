from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

from tracker.models import HypothesisRecord, WatchlistMachineRecord


@dataclass(slots=True)
class StoreConfig:
    key: str
    name: str
    timezone: str
    listing_url: str
    all_units_query: str = "?kishu=all&sort=num"


@dataclass(slots=True)
class AnalysisConfig:
    lookback_days: int = 120
    watchlist_event_weight: float = 0.65
    recent_weight: float = 0.35


@dataclass(slots=True)
class AppConfig:
    config_path: Path
    database_path: Path
    snapshot_dir: Path
    report_dir: Path
    user_agent: str
    request_timeout_seconds: int
    request_delay_seconds: float
    max_listing_pages: int
    analysis: AnalysisConfig
    stores: list[StoreConfig] = field(default_factory=list)
    watchlist_machines: list[WatchlistMachineRecord] = field(default_factory=list)
    hypotheses: list[HypothesisRecord] = field(default_factory=list)


def _resolve_path(base_dir: Path, value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    base_dir = config_path.parent
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))

    analysis_raw = raw.get("analysis", {})
    stores = [
        StoreConfig(
            key=item["key"],
            name=item["name"],
            timezone=item.get("timezone", "Asia/Tokyo"),
            listing_url=item["listing_url"],
            all_units_query=item.get("all_units_query", "?kishu=all&sort=num"),
        )
        for item in raw.get("stores", [])
    ]
    watchlist = [
        WatchlistMachineRecord(
            store_key=item["store_key"],
            machine_name=item["machine_name"],
            display_name=item.get("display_name"),
            match_type=item.get("match_type", "exact"),
            match_value=item.get("match_value"),
            priority=int(item.get("priority", 100)),
            enabled=bool(item.get("enabled", True)),
            note=item.get("note", ""),
        )
        for item in raw.get("watchlist_machines", [])
    ]
    hypotheses = [
        HypothesisRecord(
            store_key=item["store_key"],
            hypothesis_key=item["hypothesis_key"],
            label=item["label"],
            tag_key=item["tag_key"],
            tag_label=item["tag_label"],
            rule_type=item["rule_type"],
            rule_value=str(item["rule_value"]),
            expected_direction=item.get("expected_direction", "up"),
            description=item.get("description", ""),
            enabled=bool(item.get("enabled", True)),
            tag_group=item.get("tag_group", "hypothesis"),
        )
        for item in raw.get("hypotheses", [])
    ]

    return AppConfig(
        config_path=config_path,
        database_path=_resolve_path(base_dir, raw["database_path"]),
        snapshot_dir=_resolve_path(base_dir, raw["snapshot_dir"]),
        report_dir=_resolve_path(base_dir, raw["report_dir"]),
        user_agent=raw.get("user_agent", "taihokamezima/1.0"),
        request_timeout_seconds=int(raw.get("request_timeout_seconds", 30)),
        request_delay_seconds=float(raw.get("request_delay_seconds", 0.0)),
        max_listing_pages=int(raw.get("max_listing_pages", 1)),
        analysis=AnalysisConfig(
            lookback_days=int(analysis_raw.get("lookback_days", 120)),
            watchlist_event_weight=float(analysis_raw.get("watchlist_event_weight", 0.65)),
            recent_weight=float(analysis_raw.get("recent_weight", 0.35)),
        ),
        stores=stores,
        watchlist_machines=watchlist,
        hypotheses=hypotheses,
    )
