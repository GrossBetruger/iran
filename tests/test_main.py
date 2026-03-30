import csv
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main


def test_build_rows_merges_series_and_computes_totals(monkeypatch) -> None:
    series_by_name = {
        "Waves of attacks from Iran": {
            "3/10/2026": 7,
            "3/11/2026": 3,
        },
        "Waves of attacks from Lebanon": {
            "3/11/2026": 2,
        },
        "Waves of attacks from Yemen": {
            "3/11/2026": 1,
        },
    }
    fixed_now = datetime(2026, 3, 11, 15, 38, 2, tzinfo=timezone(timedelta(hours=2)))
    monkeypatch.setattr(main, "get_scraped_now_local", lambda: fixed_now)

    rows = main.build_rows(series_by_name)

    assert [row.event_date for row in rows] == ["2026-03-10", "2026-03-11"]
    assert rows[0].iran_barrage_count == 7
    assert rows[0].lebanon_barrage_count is None
    assert rows[0].yemen_barrage_count is None
    assert rows[0].total_barrage_count == 7
    assert rows[1].iran_barrage_count == 3
    assert rows[1].lebanon_barrage_count == 2
    assert rows[1].yemen_barrage_count == 1
    assert rows[1].total_barrage_count == 6
    assert all(row.snapshot_date == "2026-03-11" for row in rows)
    assert all(row.scraped_at_local == "2026-03-11T15:38:02+02:00" for row in rows)
    assert all(row.scraped_at_utc == "2026-03-11T13:38:02+00:00" for row in rows)


def test_normalize_date_returns_iso_date() -> None:
    assert main.normalize_date("3/11/2026") == "2026-03-11"


def test_get_snapshot_date_uses_existing_snapshot_date() -> None:
    row = {
        "snapshot_date": "2026-03-11",
        "scraped_at_utc": "2026-03-09T13:38:02+00:00",
    }

    assert main.get_snapshot_date(row) == "2026-03-11"


def test_get_snapshot_date_falls_back_to_scraped_at_utc() -> None:
    row = {
        "event_date": "2026-03-11",
        "scraped_at_utc": "2026-03-09T13:38:02+00:00",
    }

    assert main.get_snapshot_date(row) == "2026-03-09"


def test_get_latest_snapshot_rows_returns_only_latest_snapshot_sorted() -> None:
    rows = [
        {
            "snapshot_date": "2026-03-10",
            "event_date": "2026-03-11",
            "iran_barrage_count": "1",
            "lebanon_barrage_count": "2",
            "total_barrage_count": "3",
        },
        {
            "snapshot_date": "2026-03-11",
            "event_date": "2026-03-11",
            "iran_barrage_count": "3",
            "lebanon_barrage_count": "2",
            "total_barrage_count": "5",
        },
        {
            "snapshot_date": "2026-03-11",
            "event_date": "2026-03-10",
            "iran_barrage_count": "7",
            "lebanon_barrage_count": "21",
            "total_barrage_count": "28",
        },
    ]

    latest_rows = main.get_latest_snapshot_rows(rows)

    assert [row["snapshot_date"] for row in latest_rows] == ["2026-03-11", "2026-03-11"]
    assert [row["event_date"] for row in latest_rows] == ["2026-03-10", "2026-03-11"]


def test_parse_optional_int_returns_none_for_blank_values() -> None:
    assert main.parse_optional_int("") is None
    assert main.parse_optional_int("  ") is None
    assert main.parse_optional_int("12") == 12


def test_normalize_target_label_returns_english_key_and_label() -> None:
    assert main.normalize_target_label('מכ"ם') == ("radar", "Radar")
    assert main.normalize_target_label("Missions Foreign") == (
        "missions_foreign",
        "Missions Foreign",
    )
    assert main.normalize_target_label("Oil&Fuel facilities") == (
        "oil_fuel_facilities",
        "Oil&Fuel facilities",
    )


def test_build_target_rows_normalizes_and_sorts_categories(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 11, 15, 38, 2, tzinfo=timezone(timedelta(hours=2)))
    monkeypatch.setattr(main, "get_scraped_now_local", lambda: fixed_now)

    rows = main.build_target_rows(
        {
            "Ports": 3,
            'מכ"ם': 6,
            "US Military Bases": 13,
        }
    )

    assert [(row.target_key, row.strike_count) for row in rows] == [
        ("us_military_bases", 13),
        ("radar", 6),
        ("ports", 3),
    ]
    assert all(row.snapshot_date == "2026-03-11" for row in rows)
    assert all(row.metric_key == main.KEY_TARGETS_METRIC_KEY for row in rows)


def test_upsert_rows_replaces_same_day_snapshot_and_keeps_previous_days(tmp_path) -> None:
    path = tmp_path / "barrages.csv"
    first_snapshot = main.ScrapedRow(
        snapshot_date="2026-03-10",
        event_date="2026-03-11",
        iran_barrage_count=1,
        lebanon_barrage_count=2,
        yemen_barrage_count=None,
        total_barrage_count=3,
        chart_title=main.BARRAGE_VISUAL_TITLE,
        metric_key=main.BARRAGE_METRIC_KEY,
        source_page_url=main.SOURCE_PAGE_URL,
        dashboard_url=main.DASHBOARD_URL,
        scraped_at_local="2026-03-10T15:00:00+02:00",
        scraped_at_utc="2026-03-10T13:00:00+00:00",
    )
    same_day_replacement = main.ScrapedRow(
        snapshot_date="2026-03-11",
        event_date="2026-03-11",
        iran_barrage_count=3,
        lebanon_barrage_count=2,
        yemen_barrage_count=None,
        total_barrage_count=5,
        chart_title=main.BARRAGE_VISUAL_TITLE,
        metric_key=main.BARRAGE_METRIC_KEY,
        source_page_url=main.SOURCE_PAGE_URL,
        dashboard_url=main.DASHBOARD_URL,
        scraped_at_local="2026-03-11T15:30:00+02:00",
        scraped_at_utc="2026-03-11T13:30:00+00:00",
    )
    same_day_newer = main.ScrapedRow(
        snapshot_date="2026-03-11",
        event_date="2026-03-11",
        iran_barrage_count=4,
        lebanon_barrage_count=1,
        yemen_barrage_count=None,
        total_barrage_count=5,
        chart_title=main.BARRAGE_VISUAL_TITLE,
        metric_key=main.BARRAGE_METRIC_KEY,
        source_page_url=main.SOURCE_PAGE_URL,
        dashboard_url=main.DASHBOARD_URL,
        scraped_at_local="2026-03-11T16:00:00+02:00",
        scraped_at_utc="2026-03-11T14:00:00+00:00",
    )

    main.upsert_rows(path, [first_snapshot])
    main.upsert_rows(path, [same_day_replacement])
    main.upsert_rows(path, [same_day_newer])

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert [(row["snapshot_date"], row["event_date"]) for row in rows] == [
        ("2026-03-10", "2026-03-11"),
        ("2026-03-11", "2026-03-11"),
    ]
    assert rows[1]["iran_barrage_count"] == "4"
    assert rows[1]["lebanon_barrage_count"] == "1"
    assert rows[1]["scraped_at_utc"] == "2026-03-11T14:00:00+00:00"


def test_upsert_rows_migrates_legacy_rows_without_snapshot_date(tmp_path) -> None:
    path = tmp_path / "barrages.csv"
    fieldnames = [
        "event_date",
        "iran_barrage_count",
        "lebanon_barrage_count",
        "total_barrage_count",
        "chart_title",
        "metric_key",
        "source_page_url",
        "dashboard_url",
        "scraped_at_local",
        "scraped_at_utc",
    ]

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "event_date": "2026-03-11",
                "iran_barrage_count": "3",
                "lebanon_barrage_count": "2",
                "total_barrage_count": "5",
                "chart_title": main.BARRAGE_VISUAL_TITLE,
                "metric_key": main.BARRAGE_METRIC_KEY,
                "source_page_url": main.SOURCE_PAGE_URL,
                "dashboard_url": main.DASHBOARD_URL,
                "scraped_at_local": "2026-03-11T15:38:02+02:00",
                "scraped_at_utc": "2026-03-11T13:38:02+00:00",
            }
        )

    new_snapshot = main.ScrapedRow(
        snapshot_date="2026-03-12",
        event_date="2026-03-11",
        iran_barrage_count=6,
        lebanon_barrage_count=1,
        yemen_barrage_count=None,
        total_barrage_count=7,
        chart_title=main.BARRAGE_VISUAL_TITLE,
        metric_key=main.BARRAGE_METRIC_KEY,
        source_page_url=main.SOURCE_PAGE_URL,
        dashboard_url=main.DASHBOARD_URL,
        scraped_at_local="2026-03-12T15:00:00+02:00",
        scraped_at_utc="2026-03-12T13:00:00+00:00",
    )

    main.upsert_rows(path, [new_snapshot])

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert rows[0]["snapshot_date"] == "2026-03-11"
    assert rows[0]["event_date"] == "2026-03-11"
    assert rows[1]["snapshot_date"] == "2026-03-12"
    assert rows[1]["iran_barrage_count"] == "6"


def test_upsert_target_rows_replaces_same_day_snapshot_and_keeps_previous_days(tmp_path) -> None:
    path = tmp_path / "targets.csv"
    first_snapshot = main.TargetStrikeRow(
        snapshot_date="2026-03-10",
        target_key="radar",
        target_label="Radar",
        strike_count=6,
        chart_title=main.KEY_TARGETS_VISUAL_TITLE,
        metric_key=main.KEY_TARGETS_METRIC_KEY,
        source_page_url=main.SOURCE_PAGE_URL,
        dashboard_url=main.DASHBOARD_URL,
        scraped_at_local="2026-03-10T15:00:00+02:00",
        scraped_at_utc="2026-03-10T13:00:00+00:00",
    )
    same_day_replacement = main.TargetStrikeRow(
        snapshot_date="2026-03-11",
        target_key="radar",
        target_label="Radar",
        strike_count=7,
        chart_title=main.KEY_TARGETS_VISUAL_TITLE,
        metric_key=main.KEY_TARGETS_METRIC_KEY,
        source_page_url=main.SOURCE_PAGE_URL,
        dashboard_url=main.DASHBOARD_URL,
        scraped_at_local="2026-03-11T15:00:00+02:00",
        scraped_at_utc="2026-03-11T13:00:00+00:00",
    )
    same_day_newer = main.TargetStrikeRow(
        snapshot_date="2026-03-11",
        target_key="radar",
        target_label="Radar",
        strike_count=8,
        chart_title=main.KEY_TARGETS_VISUAL_TITLE,
        metric_key=main.KEY_TARGETS_METRIC_KEY,
        source_page_url=main.SOURCE_PAGE_URL,
        dashboard_url=main.DASHBOARD_URL,
        scraped_at_local="2026-03-11T16:00:00+02:00",
        scraped_at_utc="2026-03-11T14:00:00+00:00",
    )

    main.upsert_target_rows(path, [first_snapshot])
    main.upsert_target_rows(path, [same_day_replacement])
    main.upsert_target_rows(path, [same_day_newer])

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert [(row["snapshot_date"], row["target_key"]) for row in rows] == [
        ("2026-03-10", "radar"),
        ("2026-03-11", "radar"),
    ]
    assert rows[1]["strike_count"] == "8"



def test_write_latest_snapshot_graph_creates_html_for_latest_snapshot_only(tmp_path) -> None:
    csv_path = tmp_path / "barrages.csv"
    graph_path = tmp_path / "barrages.html"
    fieldnames = [
        "snapshot_date",
        "event_date",
        "iran_barrage_count",
        "lebanon_barrage_count",
        "total_barrage_count",
        "chart_title",
        "metric_key",
        "source_page_url",
        "dashboard_url",
        "scraped_at_local",
        "scraped_at_utc",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "snapshot_date": "2026-03-10",
                    "event_date": "2026-03-10",
                    "iran_barrage_count": "1",
                    "lebanon_barrage_count": "2",
                    "total_barrage_count": "3",
                    "chart_title": main.BARRAGE_VISUAL_TITLE,
                    "metric_key": main.BARRAGE_METRIC_KEY,
                    "source_page_url": main.SOURCE_PAGE_URL,
                    "dashboard_url": main.DASHBOARD_URL,
                    "scraped_at_local": "2026-03-10T15:00:00+02:00",
                    "scraped_at_utc": "2026-03-10T13:00:00+00:00",
                },
                {
                    "snapshot_date": "2026-03-11",
                    "event_date": "2026-03-10",
                    "iran_barrage_count": "7",
                    "lebanon_barrage_count": "21",
                    "total_barrage_count": "28",
                    "chart_title": main.BARRAGE_VISUAL_TITLE,
                    "metric_key": main.BARRAGE_METRIC_KEY,
                    "source_page_url": main.SOURCE_PAGE_URL,
                    "dashboard_url": main.DASHBOARD_URL,
                    "scraped_at_local": "2026-03-11T15:00:00+02:00",
                    "scraped_at_utc": "2026-03-11T13:00:00+00:00",
                },
                {
                    "snapshot_date": "2026-03-11",
                    "event_date": "2026-03-11",
                    "iran_barrage_count": "3",
                    "lebanon_barrage_count": "2",
                    "total_barrage_count": "5",
                    "chart_title": main.BARRAGE_VISUAL_TITLE,
                    "metric_key": main.BARRAGE_METRIC_KEY,
                    "source_page_url": main.SOURCE_PAGE_URL,
                    "dashboard_url": main.DASHBOARD_URL,
                    "scraped_at_local": "2026-03-11T15:00:00+02:00",
                    "scraped_at_utc": "2026-03-11T13:00:00+00:00",
                },
            ]
        )

    main.write_latest_snapshot_graph(csv_path, graph_path)

    html = graph_path.read_text(encoding="utf-8")
    assert "Latest Snapshot: 2026-03-11" in html
    assert "2026-03-10" in html
    assert "2026-03-11" in html


def test_write_latest_target_graph_creates_html_for_latest_snapshot_only(tmp_path) -> None:
    csv_path = tmp_path / "targets.csv"
    graph_path = tmp_path / "targets.html"
    fieldnames = [
        "snapshot_date",
        "target_key",
        "target_label",
        "strike_count",
        "chart_title",
        "metric_key",
        "source_page_url",
        "dashboard_url",
        "scraped_at_local",
        "scraped_at_utc",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "snapshot_date": "2026-03-10",
                    "target_key": "radar",
                    "target_label": "Radar",
                    "strike_count": "6",
                    "chart_title": main.KEY_TARGETS_VISUAL_TITLE,
                    "metric_key": main.KEY_TARGETS_METRIC_KEY,
                    "source_page_url": main.SOURCE_PAGE_URL,
                    "dashboard_url": main.DASHBOARD_URL,
                    "scraped_at_local": "2026-03-10T15:00:00+02:00",
                    "scraped_at_utc": "2026-03-10T13:00:00+00:00",
                },
                {
                    "snapshot_date": "2026-03-11",
                    "target_key": "us_military_bases",
                    "target_label": "US Military Bases",
                    "strike_count": "13",
                    "chart_title": main.KEY_TARGETS_VISUAL_TITLE,
                    "metric_key": main.KEY_TARGETS_METRIC_KEY,
                    "source_page_url": main.SOURCE_PAGE_URL,
                    "dashboard_url": main.DASHBOARD_URL,
                    "scraped_at_local": "2026-03-11T15:00:00+02:00",
                    "scraped_at_utc": "2026-03-11T13:00:00+00:00",
                },
                {
                    "snapshot_date": "2026-03-11",
                    "target_key": "radar",
                    "target_label": "Radar",
                    "strike_count": "6",
                    "chart_title": main.KEY_TARGETS_VISUAL_TITLE,
                    "metric_key": main.KEY_TARGETS_METRIC_KEY,
                    "source_page_url": main.SOURCE_PAGE_URL,
                    "dashboard_url": main.DASHBOARD_URL,
                    "scraped_at_local": "2026-03-11T15:00:00+02:00",
                    "scraped_at_utc": "2026-03-11T13:00:00+00:00",
                },
            ]
        )

    main.write_latest_target_graph(csv_path, graph_path)

    html = graph_path.read_text(encoding="utf-8")
    assert "Latest Snapshot: 2026-03-11" in html
    assert "US Military Bases" in html
    assert "Radar" in html
