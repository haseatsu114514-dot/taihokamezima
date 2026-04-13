from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
import json
import re
import sqlite3
from statistics import mean
from typing import Any


def _avg(values: list[float | int | None]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return round(mean(cleaned), 2)


def _delta(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return round(current - previous, 2)


def _label_score(score: float | None) -> str:
    if score is None:
        return "unknown"
    if score >= 60:
        return "strong"
    if score <= 40:
        return "weak"
    return "neutral"


def _component_score(value: float | None, baseline: float | None, stretch: float) -> float | None:
    if value is None or baseline is None or baseline == 0:
        return None
    score = 50 + ((value - baseline) / stretch)
    return max(0.0, min(100.0, round(score, 2)))


def _json_load(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _load_days(conn: sqlite3.Connection, store_key: str, lookback_days: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM daily_summary
        WHERE store_key = ?
        ORDER BY stat_date ASC
        """,
        (store_key,),
    ).fetchall()
    if not rows:
        return []

    latest_date = date.fromisoformat(rows[-1]["stat_date"])
    cutoff = latest_date - timedelta(days=lookback_days - 1)
    result: list[dict[str, Any]] = []
    for row in rows:
        row_date = date.fromisoformat(row["stat_date"])
        if row_date < cutoff:
            continue
        item = dict(row)
        item["note"] = _json_load(item.get("note_json"))
        result.append(item)
    return result


def _load_tag_map(conn: sqlite3.Connection, store_key: str) -> dict[str, list[dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT tag_date, tag_group, tag_key, tag_label, rule_source, detail_json
        FROM calendar_tags
        WHERE store_key = ? AND is_active = 1
        ORDER BY tag_date ASC, tag_group ASC, tag_key ASC
        """,
        (store_key,),
    ).fetchall()
    mapping: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        item = dict(row)
        item["detail"] = _json_load(item.get("detail_json"))
        mapping[row["tag_date"]].append(item)
    return mapping


def _load_watchlist(conn: sqlite3.Connection, store_key: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT machine_name, display_name, match_type, match_value, priority, enabled, note
        FROM watchlist_machines
        WHERE store_key = ? AND enabled = 1
        ORDER BY priority ASC, machine_name ASC
        """,
        (store_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_hypotheses(conn: sqlite3.Connection, store_key: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT hypothesis_key, label, tag_key, tag_label, expected_direction, description, enabled
        FROM hypotheses
        WHERE store_key = ? AND enabled = 1
        ORDER BY hypothesis_key ASC
        """,
        (store_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_machine_rows(conn: sqlite3.Connection, store_key: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT stat_date, machine_name, machine_count, average_diff, average_games, win_rate, pay_rate
        FROM machine_summary
        WHERE store_key = ?
        ORDER BY stat_date ASC
        """,
        (store_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_sueo_rows(conn: sqlite3.Connection, store_key: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT stat_date, suffix_key, suffix_label, machine_count, average_diff, average_games, win_rate, pay_rate
        FROM sueo_summary
        WHERE store_key = ?
        ORDER BY stat_date ASC
        """,
        (store_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_parse_errors(conn: sqlite3.Connection, store_key: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT page_kind, page_url, page_date, error_kind, error_message, created_at
        FROM parse_errors
        WHERE store_key = ?
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (store_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def _has_tag(tag_map: dict[str, list[dict[str, Any]]], stat_date: str, tag_key: str) -> bool:
    return any(item["tag_key"] == tag_key for item in tag_map.get(stat_date, []))


def _has_group(tag_map: dict[str, list[dict[str, Any]]], stat_date: str, tag_group: str) -> bool:
    return any(item["tag_group"] == tag_group for item in tag_map.get(stat_date, []))


def _summarize_days(days: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "days": len(days),
        "average_games": _avg([day["average_games"] for day in days]),
        "average_diff": _avg([day["average_diff"] for day in days]),
        "win_rate": _avg([day["win_rate"] for day in days]),
    }


def _build_intensity(latest_day: dict[str, Any], baseline_days: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_games = _avg([item["average_games"] for item in baseline_days])
    baseline_diff = _avg([item["average_diff"] for item in baseline_days])
    baseline_win_rate = _avg([item["win_rate"] for item in baseline_days])

    components = [
        _component_score(latest_day.get("average_games"), baseline_games, 50.0),
        _component_score(latest_day.get("average_diff"), baseline_diff, 20.0),
        _component_score(latest_day.get("win_rate"), baseline_win_rate, 5.0),
    ]
    cleaned = [component for component in components if component is not None]
    overall = round(mean(cleaned), 2) if cleaned else None
    return {
        "score": overall,
        "label": _label_score(overall),
        "components": {
            "average_games": components[0],
            "average_diff": components[1],
            "win_rate": components[2],
        },
        "baseline": {
            "average_games": baseline_games,
            "average_diff": baseline_diff,
            "win_rate": baseline_win_rate,
        },
    }


def _event_comparisons(
    days: list[dict[str, Any]],
    tag_map: dict[str, list[dict[str, Any]]],
    hypotheses: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    comparisons = [
        ("official_3day", "3のつく日"),
        ("official_first_saturday", "第1土曜日"),
    ]
    comparisons.extend((item["tag_key"], item["label"]) for item in hypotheses)

    result: list[dict[str, Any]] = []
    for tag_key, label in comparisons:
        tagged = [day for day in days if _has_tag(tag_map, day["stat_date"], tag_key)]
        normal = [day for day in days if not _has_tag(tag_map, day["stat_date"], tag_key)]
        summary_tagged = _summarize_days(tagged)
        summary_normal = _summarize_days(normal)
        result.append(
            {
                "tag_key": tag_key,
                "label": label,
                "tagged": summary_tagged,
                "normal": summary_normal,
                "delta": {
                    "average_games": _delta(
                        summary_tagged["average_games"],
                        summary_normal["average_games"],
                    ),
                    "average_diff": _delta(
                        summary_tagged["average_diff"],
                        summary_normal["average_diff"],
                    ),
                    "win_rate": _delta(
                        summary_tagged["win_rate"],
                        summary_normal["win_rate"],
                    ),
                },
            }
        )
    return result


def _watchlist_priorities(
    machine_rows: list[dict[str, Any]],
    watchlist: list[dict[str, Any]],
    tag_map: dict[str, list[dict[str, Any]]],
    watchlist_event_weight: float,
    recent_weight: float,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for watch in watchlist:
        rows = [row for row in machine_rows if _watch_matches(row["machine_name"], watch)]
        if not rows:
            ranked.append(
                {
                    "machine_name": watch.get("display_name") or watch["machine_name"],
                    "matched_machine_names": [],
                    "priority": watch["priority"],
                    "score": 0.0,
                    "sample_days": 0,
                    "event_average_games": None,
                    "overall_average_games": None,
                    "event_average_diff": None,
                    "overall_average_diff": None,
                    "reason": "データ未取得",
                    "note": watch.get("note", ""),
                }
            )
            continue

        rows = sorted(rows, key=lambda item: item["stat_date"])
        recent_rows = rows[-10:]
        event_rows = [
            row
            for row in rows
            if _has_group(tag_map, row["stat_date"], "official")
            or _has_group(tag_map, row["stat_date"], "hypothesis")
        ]

        overall_avg_games = _avg([row["average_games"] for row in rows])
        overall_avg_diff = _avg([row["average_diff"] for row in rows])
        event_avg_games = _avg([row["average_games"] for row in event_rows])
        event_avg_diff = _avg([row["average_diff"] for row in event_rows])
        recent_avg_games = _avg([row["average_games"] for row in recent_rows])

        event_boost = 1.0
        if event_avg_games and overall_avg_games:
            event_boost = event_avg_games / overall_avg_games
        recent_boost = 1.0
        if recent_avg_games and overall_avg_games:
            recent_boost = recent_avg_games / overall_avg_games

        score = round(
            (event_boost * 100 * watchlist_event_weight)
            + (recent_boost * 100 * recent_weight)
            + max(0, 100 - watch["priority"]),
            2,
        )
        reason = "イベント日に伸びやすい"
        if event_boost < 1:
            reason = "通常日比ではイベント上振れが薄い"
        if recent_boost > event_boost:
            reason = "直近稼働が上向き"

        ranked.append(
            {
                "machine_name": watch.get("display_name") or watch["machine_name"],
                "matched_machine_names": sorted({row["machine_name"] for row in rows}),
                "priority": watch["priority"],
                "score": score,
                "sample_days": len(rows),
                "event_average_games": event_avg_games,
                "overall_average_games": overall_avg_games,
                "event_average_diff": event_avg_diff,
                "overall_average_diff": overall_avg_diff,
                "reason": reason,
                "note": watch.get("note", ""),
            }
        )

    return sorted(ranked, key=lambda item: (-item["score"], item["priority"], item["machine_name"]))


def _watch_matches(machine_name: str, watch: dict[str, Any]) -> bool:
    match_type = (watch.get("match_type") or "exact").lower()
    match_value = watch.get("match_value") or watch.get("machine_name") or ""
    if match_type == "contains":
        return match_value in machine_name
    if match_type == "startswith":
        return machine_name.startswith(match_value)
    if match_type == "regex":
        try:
            return re.search(match_value, machine_name) is not None
        except re.error:
            return False
    return machine_name == match_value


def _suffix_candidates(
    sueo_rows: list[dict[str, Any]],
    tag_map: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sueo_rows:
        by_date[row["stat_date"]].append(row)

    candidate_stats: dict[str, dict[str, Any]] = {}
    for stat_date, rows in by_date.items():
        rows = sorted(rows, key=lambda item: (item["average_games"] or -1), reverse=True)
        for hit_rank, row in enumerate(rows[:3], start=1):
            stats = candidate_stats.setdefault(
                row["suffix_key"],
                {
                    "suffix_key": row["suffix_key"],
                    "suffix_label": row["suffix_label"],
                    "top3_hits": 0,
                    "event_rows": [],
                    "all_rows": [],
                },
            )
            stats["top3_hits"] += 1
            stats["all_rows"].append(row)
            if _has_group(tag_map, stat_date, "official"):
                stats["event_rows"].append(row)
        for row in rows[3:]:
            stats = candidate_stats.setdefault(
                row["suffix_key"],
                {
                    "suffix_key": row["suffix_key"],
                    "suffix_label": row["suffix_label"],
                    "top3_hits": 0,
                    "event_rows": [],
                    "all_rows": [],
                },
            )
            stats["all_rows"].append(row)
            if _has_group(tag_map, stat_date, "official"):
                stats["event_rows"].append(row)

    ranked: list[dict[str, Any]] = []
    for suffix_key, stats in candidate_stats.items():
        event_avg_games = _avg([row["average_games"] for row in stats["event_rows"]])
        event_avg_diff = _avg([row["average_diff"] for row in stats["event_rows"]])
        all_avg_games = _avg([row["average_games"] for row in stats["all_rows"]])
        score = (
            (event_avg_games or all_avg_games or 0) / 50.0
            + max(event_avg_diff or 0, 0) / 20.0
            + stats["top3_hits"] * 5
        )
        ranked.append(
            {
                "suffix_key": suffix_key,
                "suffix_label": stats["suffix_label"],
                "score": round(score, 2),
                "top3_hits": stats["top3_hits"],
                "event_average_games": event_avg_games,
                "event_average_diff": event_avg_diff,
                "overall_average_games": all_avg_games,
            }
        )
    return sorted(ranked, key=lambda item: (-item["score"], item["suffix_label"]))[:5]


def _previous_event_comparison(
    days: list[dict[str, Any]],
    tag_map: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    if not days:
        return None

    latest = days[-1]
    latest_tags = tag_map.get(latest["stat_date"], [])
    preferred = None
    for tag in latest_tags:
        if tag["tag_group"] == "official":
            preferred = tag
            break
    if preferred is None and latest_tags:
        preferred = latest_tags[0]

    previous = None
    if preferred is not None:
        for day_row in reversed(days[:-1]):
            if _has_tag(tag_map, day_row["stat_date"], preferred["tag_key"]):
                previous = day_row
                break
    if previous is None:
        for day_row in reversed(days[:-1]):
            if _has_group(tag_map, day_row["stat_date"], "official"):
                previous = day_row
                preferred = {"tag_key": "official", "tag_label": "直近イベント"}
                break
    if previous is None:
        return None

    return {
        "tag_key": preferred["tag_key"],
        "tag_label": preferred["tag_label"],
        "current_date": latest["stat_date"],
        "previous_date": previous["stat_date"],
        "current": {
            "average_games": latest["average_games"],
            "average_diff": latest["average_diff"],
            "win_rate": latest["win_rate"],
        },
        "previous": {
            "average_games": previous["average_games"],
            "average_diff": previous["average_diff"],
            "win_rate": previous["win_rate"],
        },
        "delta": {
            "average_games": _delta(latest["average_games"], previous["average_games"]),
            "average_diff": _delta(latest["average_diff"], previous["average_diff"]),
            "win_rate": _delta(latest["win_rate"], previous["win_rate"]),
        },
    }


def _hypothesis_results(
    days: list[dict[str, Any]],
    tag_map: dict[str, list[dict[str, Any]]],
    hypotheses: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in hypotheses:
        tagged = [day for day in days if _has_tag(tag_map, day["stat_date"], item["tag_key"])]
        untagged = [day for day in days if not _has_tag(tag_map, day["stat_date"], item["tag_key"])]
        tagged_summary = _summarize_days(tagged)
        untagged_summary = _summarize_days(untagged)

        avg_games_delta = _delta(tagged_summary["average_games"], untagged_summary["average_games"])
        win_rate_delta = _delta(tagged_summary["win_rate"], untagged_summary["win_rate"])
        verdict = "保留"
        if (avg_games_delta or 0) >= 150 or (win_rate_delta or 0) >= 2:
            verdict = "支持寄り"
        elif (avg_games_delta or 0) <= -150 or (win_rate_delta or 0) <= -2:
            verdict = "否定寄り"

        results.append(
            {
                "hypothesis_key": item["hypothesis_key"],
                "label": item["label"],
                "description": item["description"],
                "tagged": tagged_summary,
                "untagged": untagged_summary,
                "delta": {
                    "average_games": avg_games_delta,
                    "average_diff": _delta(
                        tagged_summary["average_diff"],
                        untagged_summary["average_diff"],
                    ),
                    "win_rate": win_rate_delta,
                },
                "verdict": verdict,
            }
        )
    return results


def build_analysis(
    conn: sqlite3.Connection,
    store_key: str,
    lookback_days: int,
    watchlist_event_weight: float,
    recent_weight: float,
) -> dict[str, Any]:
    days = _load_days(conn, store_key, lookback_days)
    tag_map = _load_tag_map(conn, store_key)
    watchlist = _load_watchlist(conn, store_key)
    hypotheses = _load_hypotheses(conn, store_key)
    machine_rows = _load_machine_rows(conn, store_key)
    sueo_rows = _load_sueo_rows(conn, store_key)
    parse_errors = _load_parse_errors(conn, store_key)

    if not days:
        return {
            "store_key": store_key,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "sample_days": 0,
            "message": "データがまだありません。",
            "warnings": parse_errors,
        }

    latest_day = days[-1]
    latest_day["tags"] = tag_map.get(latest_day["stat_date"], [])
    intensity = _build_intensity(latest_day, days)

    return {
        "store_key": store_key,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "sample_days": len(days),
        "latest_day": latest_day,
        "today_summary": {
            "stat_date": latest_day["stat_date"],
            "intensity": intensity,
            "headline": f"{latest_day['stat_date']} は {_label_score(intensity['score'])} 判定",
        },
        "event_comparisons": _event_comparisons(days, tag_map, hypotheses),
        "watchlist_priorities": _watchlist_priorities(
            machine_rows,
            watchlist,
            tag_map,
            watchlist_event_weight,
            recent_weight,
        ),
        "suffix_candidates": _suffix_candidates(sueo_rows, tag_map),
        "previous_event_comparison": _previous_event_comparison(days, tag_map),
        "hypothesis_results": _hypothesis_results(days, tag_map, hypotheses),
        "warnings": parse_errors,
    }
