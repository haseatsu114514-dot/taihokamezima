from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ListingEntry:
    store_key: str
    stat_date: str
    report_url: str
    list_position: int | None = None
    list_total_diff: int | None = None
    list_average_diff: int | None = None
    list_average_games: int | None = None
    raw_row: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DailySummaryRecord:
    store_key: str
    stat_date: str
    report_url: str
    all_units_url: str | None
    list_position: int | None = None
    list_total_diff: int | None = None
    list_average_diff: int | None = None
    list_average_games: int | None = None
    published_at: str | None = None
    total_diff: int | None = None
    average_diff: int | None = None
    average_games: int | None = None
    win_rate: float | None = None
    winners: int | None = None
    total_machines: int | None = None
    pay_rate: float | None = None
    official_event_text: str | None = None
    note: dict[str, Any] = field(default_factory=dict)
    collected_at: str = ""


@dataclass(slots=True)
class MachineSummaryRecord:
    store_key: str
    stat_date: str
    machine_name: str
    machine_count: int | None = None
    average_diff: int | None = None
    average_games: int | None = None
    win_rate: float | None = None
    winners: int | None = None
    pay_rate: float | None = None
    source_section: str = "machine_summary"
    raw_row: dict[str, Any] = field(default_factory=dict)
    collected_at: str = ""


@dataclass(slots=True)
class SueoSummaryRecord:
    store_key: str
    stat_date: str
    suffix_key: str
    suffix_label: str
    machine_count: int | None = None
    average_diff: int | None = None
    average_games: int | None = None
    win_rate: float | None = None
    winners: int | None = None
    pay_rate: float | None = None
    raw_row: dict[str, Any] = field(default_factory=dict)
    collected_at: str = ""


@dataclass(slots=True)
class MachineUnitResultRecord:
    store_key: str
    stat_date: str
    machine_name: str
    unit_number: str
    diff: int | None = None
    games: int | None = None
    pay_rate: float | None = None
    is_variety: bool = False
    raw_row: dict[str, Any] = field(default_factory=dict)
    collected_at: str = ""


@dataclass(slots=True)
class CalendarTagRecord:
    store_key: str
    tag_date: str
    tag_group: str
    tag_key: str
    tag_label: str
    rule_source: str
    detail: dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    collected_at: str = ""


@dataclass(slots=True)
class WatchlistMachineRecord:
    store_key: str
    machine_name: str
    display_name: str | None = None
    match_type: str = "exact"
    match_value: str | None = None
    priority: int = 100
    enabled: bool = True
    note: str = ""


@dataclass(slots=True)
class HypothesisRecord:
    store_key: str
    hypothesis_key: str
    label: str
    tag_key: str
    tag_label: str
    rule_type: str
    rule_value: str
    expected_direction: str = "up"
    description: str = ""
    enabled: bool = True
    tag_group: str = "hypothesis"


@dataclass(slots=True)
class RawPageRecord:
    store_key: str
    page_kind: str
    page_url: str
    page_date: str | None
    fetched_at: str
    status_code: int
    body_sha256: str
    file_path: str
    content_type: str | None = None


@dataclass(slots=True)
class ParseErrorRecord:
    store_key: str
    page_kind: str
    page_url: str
    page_date: str | None
    error_kind: str
    error_message: str
    body_sha256: str | None = None
    file_path: str | None = None
    created_at: str = ""


@dataclass(slots=True)
class ListingPageParseResult:
    entries: list[ListingEntry] = field(default_factory=list)
    next_page_url: str | None = None


@dataclass(slots=True)
class DailyPageParseResult:
    summary: DailySummaryRecord
    machine_summaries: list[MachineSummaryRecord] = field(default_factory=list)
    sueo_summaries: list[SueoSummaryRecord] = field(default_factory=list)
    variety_unit_results: list[MachineUnitResultRecord] = field(default_factory=list)


@dataclass(slots=True)
class AllUnitsParseResult:
    unit_results: list[MachineUnitResultRecord] = field(default_factory=list)
