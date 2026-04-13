from __future__ import annotations

from datetime import date, datetime
import hashlib
import re
import ssl
from time import sleep
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup, Tag

from tracker.models import (
    AllUnitsParseResult,
    DailyPageParseResult,
    DailySummaryRecord,
    ListingEntry,
    ListingPageParseResult,
    MachineSummaryRecord,
    MachineUnitResultRecord,
    SueoSummaryRecord,
)


HEADING_TAGS = {"h1", "h2", "h3", "h4"}
DATE_LABEL_RE = re.compile(r"^(?:(?P<year>\d{4})/)?(?P<month>\d{1,2})/(?P<day>\d{1,2})")
TITLE_DATE_RE = re.compile(r"^(?:(?P<year>\d{4})/)?(?P<month>\d{1,2})/(?P<day>\d{1,2})\([^)]+\)")
PUBLISHED_DATE_RE = re.compile(r"^(?P<year>\d{4})年(?P<month>\d{1,2})月(?P<day>\d{1,2})日$")
WIN_RATE_RE = re.compile(r"(?P<winners>\d+)\s*/\s*(?P<total>\d+)")
SPACE_RE = re.compile(r"\s+")


class ParseStructureError(RuntimeError):
    pass


def fetch_url(url: str, user_agent: str, timeout_seconds: int) -> tuple[int, str | None, bytes]:
    request = Request(url, headers={"User-Agent": user_agent})
    ssl_context = ssl.create_default_context()
    with urlopen(request, timeout=timeout_seconds, context=ssl_context) as response:
        body = response.read()
        status_code = getattr(response, "status", 200)
        content_type = response.headers.get("Content-Type")
        return status_code, content_type, body


def maybe_wait(delay_seconds: float) -> None:
    if delay_seconds > 0:
        sleep(delay_seconds)


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def build_all_units_url(report_url: str, query_string: str) -> str:
    parsed = urlparse(report_url)
    query = dict(parse_qsl(parsed.query))
    for key, value in parse_qsl(query_string.lstrip("?")):
        query[key] = value
    return urlunparse(parsed._replace(query=urlencode(query)))


def normalize_space(text: str) -> str:
    return SPACE_RE.sub(" ", text.replace("\xa0", " ")).strip()


def text_lines(html_text: str) -> list[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    return [normalize_space(line) for line in soup.get_text("\n").splitlines() if normalize_space(line)]


def parse_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    cleaned = raw.replace(",", "").replace("+", "").replace("枚", "").replace("G", "").strip()
    if cleaned in {"", "-", "--", "—"}:
        return None
    if cleaned.startswith("-") and cleaned[1:].isdigit():
        return int(cleaned)
    if cleaned.isdigit():
        return int(cleaned)
    return None


def parse_percent(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = raw.replace("%", "").strip()
    if cleaned in {"", "-", "--", "—"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_win_rate(raw: str | None) -> tuple[float | None, int | None, int | None]:
    if raw is None:
        return None, None, None
    match = WIN_RATE_RE.search(raw)
    if not match:
        return None, None, None
    winners = int(match.group("winners"))
    total = int(match.group("total"))
    rate = round((winners / total) * 100, 2) if total else None
    return rate, winners, total


def next_value_after(lines: list[str], label: str) -> str | None:
    for index, line in enumerate(lines):
        if line == label or line.startswith(label):
            remainder = line.replace(label, "", 1).strip(" :|：")
            if remainder:
                return remainder
            for next_line in lines[index + 1 :]:
                if next_line != label:
                    return next_line
    return None


def normalize_listing_date(
    label: str,
    previous_date: date | None,
    default_year: int,
) -> str:
    match = DATE_LABEL_RE.search(label)
    if not match:
        raise ParseStructureError(f"could not parse listing date label: {label}")

    explicit_year = match.group("year")
    month = int(match.group("month"))
    day_value = int(match.group("day"))
    if explicit_year:
        return date(int(explicit_year), month, day_value).isoformat()

    year = previous_date.year if previous_date else default_year
    candidate = date(year, month, day_value)
    if previous_date and candidate > previous_date:
        candidate = date(year - 1, month, day_value)
    return candidate.isoformat()


def parse_title_date(title_text: str, fallback_year: int) -> str | None:
    match = TITLE_DATE_RE.search(title_text)
    if not match:
        return None
    year = int(match.group("year") or fallback_year)
    return date(year, int(match.group("month")), int(match.group("day"))).isoformat()


def _find_tables_by_header_keywords(soup: BeautifulSoup, keywords: list[str]) -> list[Tag]:
    matched: list[Tag] = []
    for table in soup.find_all("table"):
        header_text = normalize_space(table.get_text(" ", strip=True))
        if all(keyword in header_text for keyword in keywords):
            matched.append(table)
    return matched


def _find_tables_after_heading(soup: BeautifulSoup, heading_keyword: str) -> list[Tag]:
    for tag in soup.find_all(HEADING_TAGS):
        heading_text = normalize_space(tag.get_text(" ", strip=True))
        if heading_keyword not in heading_text:
            continue

        tables: list[Tag] = []
        cursor = tag
        while True:
            cursor = cursor.find_next()
            if cursor is None:
                break
            if isinstance(cursor, Tag) and cursor.name in HEADING_TAGS and cursor is not tag:
                break
            if isinstance(cursor, Tag) and cursor.name == "table":
                tables.append(cursor)
        return tables
    return []


def _extract_table_rows(tables: list[Tag]) -> list[list[str]]:
    rows: list[list[str]] = []
    for table in tables:
        for tr in table.find_all("tr"):
            cells = [normalize_space(cell.get_text(" ", strip=True)) for cell in tr.find_all(["th", "td"])]
            if cells:
                rows.append(cells)
    return rows


def _find_report_rows(soup: BeautifulSoup) -> list[Tag]:
    rows: list[Tag] = []
    for table in _find_tables_by_header_keywords(soup, ["日付", "平均G"]):
        for tr in table.find_all("tr"):
            link = tr.find("a", href=True)
            if link and DATE_LABEL_RE.search(normalize_space(link.get_text(" ", strip=True))):
                rows.append(tr)
    return rows


def parse_listing_page(
    store_key: str,
    html_text: str,
    page_url: str,
    fetched_at: str,
) -> ListingPageParseResult:
    soup = BeautifulSoup(html_text, "html.parser")
    report_rows = _find_report_rows(soup)
    if not report_rows:
        raise ParseStructureError("listing page table was not found")

    default_year = datetime.fromisoformat(fetched_at).year
    entries: list[ListingEntry] = []
    previous_date: date | None = None
    for position, row in enumerate(report_rows, start=1):
        link = row.find("a", href=True)
        if link is None:
            continue
        date_label = normalize_space(link.get_text(" ", strip=True))
        stat_date = normalize_listing_date(date_label, previous_date, default_year)
        previous_date = date.fromisoformat(stat_date)
        cells = [normalize_space(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
        values = cells[1:4] if len(cells) >= 4 else []
        entries.append(
            ListingEntry(
                store_key=store_key,
                stat_date=stat_date,
                report_url=urljoin(page_url, link["href"]),
                list_position=position,
                list_total_diff=parse_int(values[0]) if len(values) >= 1 else None,
                list_average_diff=parse_int(values[1]) if len(values) >= 2 else None,
                list_average_games=parse_int(values[2]) if len(values) >= 3 else None,
                raw_row={"cells": cells},
            )
        )

    next_page_url: str | None = None
    for link in soup.find_all("a", href=True):
        label = normalize_space(link.get_text(" ", strip=True))
        if label in {"»", "次へ"}:
            next_page_url = urljoin(page_url, link["href"])
            break

    return ListingPageParseResult(entries=entries, next_page_url=next_page_url)


def _extract_published_at(lines: list[str]) -> str | None:
    for line in lines:
        match = PUBLISHED_DATE_RE.match(line)
        if match:
            return date(
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
            ).isoformat()
    return None


def _extract_page_status(lines: list[str]) -> str | None:
    for index, line in enumerate(lines):
        if line != "状況":
            continue
        for next_line in lines[index + 1 : index + 4]:
            if "旧イベント日" in next_line or "つく日" in next_line or "土曜" in next_line:
                return next_line
    for line in lines:
        if line.startswith("状況") and ("つく日" in line or "土曜" in line):
            return line
    return None


def parse_daily_page(
    store_key: str,
    html_text: str,
    report_url: str,
    stat_date: str,
    all_units_url: str,
    collected_at: str,
    listing_entry: ListingEntry | None = None,
) -> DailyPageParseResult:
    soup = BeautifulSoup(html_text, "html.parser")
    lines = text_lines(html_text)
    page_title = normalize_space(soup.find("h1").get_text(" ", strip=True)) if soup.find("h1") else ""

    inferred_date = parse_title_date(page_title, date.fromisoformat(stat_date).year)
    if inferred_date and inferred_date != stat_date:
        raise ParseStructureError(f"listing date {stat_date} and page title {inferred_date} mismatch")

    win_rate, winners, total_machines = parse_win_rate(next_value_after(lines, "勝率"))
    note = {
        "page_title": page_title,
        "page_status": _extract_page_status(lines),
    }
    summary = DailySummaryRecord(
        store_key=store_key,
        stat_date=stat_date,
        report_url=report_url,
        all_units_url=all_units_url,
        list_position=listing_entry.list_position if listing_entry else None,
        list_total_diff=listing_entry.list_total_diff if listing_entry else None,
        list_average_diff=listing_entry.list_average_diff if listing_entry else None,
        list_average_games=listing_entry.list_average_games if listing_entry else None,
        published_at=_extract_published_at(lines),
        total_diff=parse_int(next_value_after(lines, "総差枚")),
        average_diff=parse_int(next_value_after(lines, "平均差枚")),
        average_games=parse_int(next_value_after(lines, "平均G数")),
        win_rate=win_rate,
        winners=winners,
        total_machines=total_machines,
        pay_rate=parse_percent(next_value_after(lines, "出率")),
        official_event_text=next_value_after(lines, "旧イベント日"),
        note=note,
        collected_at=collected_at,
    )

    machine_tables = _find_tables_after_heading(soup, "機種別データ")
    if not machine_tables:
        machine_tables = _find_tables_by_header_keywords(soup, ["機種", "平均G数", "勝率"])
    machine_rows: list[MachineSummaryRecord] = []
    for row in _extract_table_rows(machine_tables):
        if len(row) < 5 or row[0] == "機種":
            continue
        machine_name = row[0]
        if machine_name in {"-----スポンサーリンク-----", "スポンサーリンク"}:
            continue
        rate, row_winners, row_total = parse_win_rate(row[3])
        machine_rows.append(
            MachineSummaryRecord(
                store_key=store_key,
                stat_date=stat_date,
                machine_name=machine_name,
                machine_count=row_total,
                average_diff=parse_int(row[1]),
                average_games=parse_int(row[2]),
                win_rate=rate,
                winners=row_winners,
                pay_rate=parse_percent(row[4]),
                collected_at=collected_at,
                raw_row={"cells": row},
            )
        )
    if not machine_rows:
        raise ParseStructureError("machine summary rows were not found on daily page")

    variety_tables = _find_tables_after_heading(soup, "バラエティ")
    variety_rows: list[MachineUnitResultRecord] = []
    for row in _extract_table_rows(variety_tables):
        if len(row) < 5 or row[0] == "機種":
            continue
        variety_rows.append(
            MachineUnitResultRecord(
                store_key=store_key,
                stat_date=stat_date,
                machine_name=row[0],
                unit_number=row[1],
                diff=parse_int(row[2]),
                games=parse_int(row[3]),
                pay_rate=parse_percent(row[4]),
                is_variety=True,
                collected_at=collected_at,
                raw_row={"cells": row, "source": "variety"},
            )
        )

    sueo_tables = _find_tables_after_heading(soup, "末尾別データ")
    sueo_rows: list[SueoSummaryRecord] = []
    for row in _extract_table_rows(sueo_tables):
        if len(row) < 5 or row[0] == "末尾":
            continue
        rate, row_winners, row_total = parse_win_rate(row[3])
        suffix_label = row[0]
        suffix_key = suffix_label.replace(" ", "").replace("　", "")
        if "ゾロ目" in suffix_key:
            suffix_key = "double"
        sueo_rows.append(
            SueoSummaryRecord(
                store_key=store_key,
                stat_date=stat_date,
                suffix_key=suffix_key,
                suffix_label=suffix_label,
                machine_count=row_total,
                average_diff=parse_int(row[1]),
                average_games=parse_int(row[2]),
                win_rate=rate,
                winners=row_winners,
                pay_rate=parse_percent(row[4]),
                collected_at=collected_at,
                raw_row={"cells": row},
            )
        )
    if not sueo_rows:
        raise ParseStructureError("suffix summary rows were not found on daily page")

    return DailyPageParseResult(
        summary=summary,
        machine_summaries=machine_rows,
        sueo_summaries=sueo_rows,
        variety_unit_results=variety_rows,
    )


def parse_all_units_page(
    store_key: str,
    html_text: str,
    stat_date: str,
    collected_at: str,
) -> AllUnitsParseResult:
    soup = BeautifulSoup(html_text, "html.parser")
    tables = _find_tables_after_heading(soup, "全台")
    if not tables:
        tables = _find_tables_by_header_keywords(soup, ["機種", "台番", "G数"])
    rows = _extract_table_rows(tables)
    unit_results: dict[str, MachineUnitResultRecord] = {}
    for row in rows:
        if len(row) < 5 or row[0] == "機種":
            continue
        record = MachineUnitResultRecord(
            store_key=store_key,
            stat_date=stat_date,
            machine_name=row[0],
            unit_number=row[1],
            diff=parse_int(row[2]),
            games=parse_int(row[3]),
            pay_rate=parse_percent(row[4]),
            is_variety=False,
            collected_at=collected_at,
            raw_row={"cells": row, "source": "all_units"},
        )
        unit_results[record.unit_number] = record

    if not unit_results:
        raise ParseStructureError("all-unit rows were not found")
    return AllUnitsParseResult(unit_results=list(unit_results.values()))
