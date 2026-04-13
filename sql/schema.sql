PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS raw_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    page_kind TEXT NOT NULL,
    page_url TEXT NOT NULL,
    page_date TEXT,
    fetched_at TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    body_sha256 TEXT NOT NULL,
    file_path TEXT NOT NULL,
    content_type TEXT,
    UNIQUE(store_key, page_kind, page_url, body_sha256)
);

CREATE TABLE IF NOT EXISTS daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    stat_date TEXT NOT NULL,
    report_url TEXT NOT NULL,
    all_units_url TEXT,
    list_position INTEGER,
    list_total_diff INTEGER,
    list_average_diff INTEGER,
    list_average_games INTEGER,
    published_at TEXT,
    total_diff INTEGER,
    average_diff INTEGER,
    average_games INTEGER,
    win_rate REAL,
    winners INTEGER,
    total_machines INTEGER,
    pay_rate REAL,
    official_event_text TEXT,
    note_json TEXT NOT NULL DEFAULT '{}',
    collected_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_key, stat_date)
);

CREATE TABLE IF NOT EXISTS machine_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    stat_date TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    machine_count INTEGER,
    average_diff INTEGER,
    average_games INTEGER,
    win_rate REAL,
    winners INTEGER,
    pay_rate REAL,
    source_section TEXT NOT NULL DEFAULT 'machine_summary',
    raw_row_json TEXT NOT NULL DEFAULT '{}',
    collected_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_key, stat_date, machine_name, source_section)
);

CREATE TABLE IF NOT EXISTS sueo_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    stat_date TEXT NOT NULL,
    suffix_key TEXT NOT NULL,
    suffix_label TEXT NOT NULL,
    machine_count INTEGER,
    average_diff INTEGER,
    average_games INTEGER,
    win_rate REAL,
    winners INTEGER,
    pay_rate REAL,
    raw_row_json TEXT NOT NULL DEFAULT '{}',
    collected_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_key, stat_date, suffix_key)
);

CREATE TABLE IF NOT EXISTS machine_unit_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    stat_date TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    unit_number TEXT NOT NULL,
    diff INTEGER,
    games INTEGER,
    pay_rate REAL,
    is_variety INTEGER NOT NULL DEFAULT 0,
    raw_row_json TEXT NOT NULL DEFAULT '{}',
    collected_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_key, stat_date, unit_number)
);

CREATE TABLE IF NOT EXISTS calendar_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    tag_date TEXT NOT NULL,
    tag_group TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_label TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    rule_source TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    collected_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_key, tag_date, tag_group, tag_key)
);

CREATE TABLE IF NOT EXISTS watchlist_machines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    display_name TEXT,
    match_type TEXT NOT NULL DEFAULT 'exact',
    match_value TEXT,
    priority INTEGER NOT NULL DEFAULT 100,
    enabled INTEGER NOT NULL DEFAULT 1,
    note TEXT NOT NULL DEFAULT '',
    source_config TEXT NOT NULL DEFAULT 'config',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_key, machine_name)
);

CREATE TABLE IF NOT EXISTS hypotheses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    hypothesis_key TEXT NOT NULL,
    label TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_label TEXT NOT NULL,
    tag_group TEXT NOT NULL DEFAULT 'hypothesis',
    rule_type TEXT NOT NULL,
    rule_value TEXT NOT NULL,
    expected_direction TEXT NOT NULL DEFAULT 'up',
    description TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    source_config TEXT NOT NULL DEFAULT 'config',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(store_key, hypothesis_key)
);

CREATE TABLE IF NOT EXISTS parse_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_key TEXT NOT NULL,
    page_kind TEXT NOT NULL,
    page_url TEXT NOT NULL,
    page_date TEXT,
    error_kind TEXT NOT NULL,
    error_message TEXT NOT NULL,
    body_sha256 TEXT,
    file_path TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_store_date
ON daily_summary (store_key, stat_date);

CREATE INDEX IF NOT EXISTS idx_machine_summary_store_date
ON machine_summary (store_key, stat_date);

CREATE INDEX IF NOT EXISTS idx_sueo_summary_store_date
ON sueo_summary (store_key, stat_date);

CREATE INDEX IF NOT EXISTS idx_machine_unit_results_store_date
ON machine_unit_results (store_key, stat_date);

CREATE INDEX IF NOT EXISTS idx_calendar_tags_store_date
ON calendar_tags (store_key, tag_date);

CREATE INDEX IF NOT EXISTS idx_parse_errors_store_date
ON parse_errors (store_key, page_date, created_at);
