from __future__ import annotations

from datetime import date
import re

from tracker.models import CalendarTagRecord, HypothesisRecord


def is_three_day(day: date) -> bool:
    return day.day % 10 == 3 and day.day not in {30, 31}


def is_first_saturday(day: date) -> bool:
    return day.weekday() == 5 and day.day <= 7


def hypothesis_matches(day: date, hypothesis: HypothesisRecord) -> bool:
    if not hypothesis.enabled:
        return False
    if hypothesis.rule_type == "day_digit":
        digits = {item.strip() for item in hypothesis.rule_value.split(",") if item.strip()}
        return str(day.day % 10) in digits
    if hypothesis.rule_type == "weekday":
        targets = {item.strip().lower() for item in hypothesis.rule_value.split(",") if item.strip()}
        labels = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        return labels[day.weekday()] in targets
    if hypothesis.rule_type == "day_of_month":
        targets = {int(item.strip()) for item in hypothesis.rule_value.split(",") if item.strip()}
        return day.day in targets
    return False


def extract_page_status_labels(status_text: str | None) -> list[str]:
    if not status_text:
        return []
    compact = re.sub(r"\s+", "", status_text)
    labels: list[str] = []
    if "3のつく日" in compact:
        labels.append("3のつく日")
    if "第1土曜" in compact or "第1土曜日" in compact:
        labels.append("第1土曜日")
    if "4のつく日" in compact:
        labels.append("4のつく日")
    return labels


def generate_calendar_tags(
    store_key: str,
    stat_date: str,
    hypotheses: list[HypothesisRecord],
    collected_at: str,
    page_status_text: str | None = None,
) -> list[CalendarTagRecord]:
    day = date.fromisoformat(stat_date)
    tags: list[CalendarTagRecord] = []

    if is_three_day(day):
        tags.append(
            CalendarTagRecord(
                store_key=store_key,
                tag_date=stat_date,
                tag_group="official",
                tag_key="official_3day",
                tag_label="3のつく日",
                rule_source="rule",
                detail={"rule": "day_digit", "excluded_days": [30, 31]},
                collected_at=collected_at,
            )
        )
    if is_first_saturday(day):
        tags.append(
            CalendarTagRecord(
                store_key=store_key,
                tag_date=stat_date,
                tag_group="official",
                tag_key="official_first_saturday",
                tag_label="第1土曜日",
                rule_source="rule",
                detail={"rule": "first_saturday"},
                collected_at=collected_at,
            )
        )

    for hypothesis in hypotheses:
        if hypothesis_matches(day, hypothesis):
            tags.append(
                CalendarTagRecord(
                    store_key=store_key,
                    tag_date=stat_date,
                    tag_group=hypothesis.tag_group,
                    tag_key=hypothesis.tag_key,
                    tag_label=hypothesis.tag_label,
                    rule_source="config",
                    detail={
                        "hypothesis_key": hypothesis.hypothesis_key,
                        "rule_type": hypothesis.rule_type,
                        "rule_value": hypothesis.rule_value,
                    },
                    collected_at=collected_at,
                )
            )

    for label in extract_page_status_labels(page_status_text):
        safe_key = (
            label.replace("の", "_")
            .replace("つく日", "day")
            .replace("第1土曜日", "first_saturday")
            .replace("第1土曜", "first_saturday")
        )
        tags.append(
            CalendarTagRecord(
                store_key=store_key,
                tag_date=stat_date,
                tag_group="page",
                tag_key=f"page_{safe_key}",
                tag_label=label,
                rule_source="page",
                detail={"status_text": page_status_text},
                collected_at=collected_at,
            )
        )

    unique: dict[tuple[str, str, str], CalendarTagRecord] = {}
    for row in tags:
        unique[(row.tag_group, row.tag_key, row.tag_label)] = row
    return list(unique.values())
