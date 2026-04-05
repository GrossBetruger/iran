from __future__ import annotations

import concurrent.futures
import csv
import io
import json
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, TypeVar

import plotly.graph_objects as go
from PIL import Image
from playwright.sync_api import Locator, Page, Playwright, sync_playwright

SOURCE_PAGE_URL = "https://www.inss.org.il/publication/lions-roar-data/"
DASHBOARD_URL = (
    "https://app.powerbi.com/view?"
    "r=eyJrIjoiYmI3MzIzMTAtMTdjNC00NTY1LWFiM2YtNjI0NTk5MWEyYTI5IiwidCI6"
    "IjgwOGNmNGIzLTFhOTYtNDEzZi1iMDZiLTlkZTZjOThmNTQ2OSJ9"
)
BARRAGE_VISUAL_TITLE = "Waves of Attacks on Israel"
BARRAGE_METRIC_KEY = "barrages_to_israel"
BARRAGE_OUTPUT_PATH = Path("data/barrages_to_israel_daily.csv")
BARRAGE_GRAPH_OUTPUT_PATH = Path("data/barrages_to_israel_latest_snapshot.html")
KEY_TARGETS_VISUAL_TITLE = "Key Targets Struck by Iran in the ME"
KEY_TARGETS_METRIC_KEY = "key_target_strikes_middle_east"
KEY_TARGETS_OUTPUT_PATH = Path("data/key_target_strikes_middle_east.csv")
KEY_TARGETS_GRAPH_OUTPUT_PATH = Path("data/key_target_strikes_middle_east_latest_snapshot.html")

IRAN_NORTH, IRAN_SOUTH = 39.8, 25.0
IRAN_WEST, IRAN_EAST = 44.0, 63.5

GIBS_LAYER = "VIIRS_SNPP_CorrectedReflectance_TrueColor"
GIBS_TILE_MATRIX_SET = "250m"
GIBS_ZOOM = 5
GIBS_TILE_SIZE = 512
GIBS_TILE_DEG = 360 / 40  # 9° per tile at zoom 5 (40 cols × 20 rows)
GIBS_ROW_RANGE = range(5, 8)   # rows 5,6,7
GIBS_COL_RANGE = range(24, 28) # cols 24,25,26,27
GIBS_OUTPUT_DIR = Path("data/satellite/nasa")

HEBREW_LABEL_MAP: dict[str, str] = {
    'מכ"ם': "Radar",
    "סאטקום": "Satellite Communications",
}

T = TypeVar("T")


@dataclass(frozen=True)
class ScrapedRow:
    snapshot_date: str
    event_date: str
    iran_barrage_count: int | None
    lebanon_barrage_count: int | None
    yemen_barrage_count: int | None
    total_barrage_count: int
    chart_title: str
    metric_key: str
    source_page_url: str
    dashboard_url: str
    scraped_at_local: str
    scraped_at_utc: str

    def to_dict(self) -> dict[str, str]:
        return {
            "snapshot_date": self.snapshot_date,
            "event_date": self.event_date,
            "iran_barrage_count": serialize_optional_int(self.iran_barrage_count),
            "lebanon_barrage_count": serialize_optional_int(self.lebanon_barrage_count),
            "yemen_barrage_count": serialize_optional_int(self.yemen_barrage_count),
            "total_barrage_count": str(self.total_barrage_count),
            "chart_title": self.chart_title,
            "metric_key": self.metric_key,
            "source_page_url": self.source_page_url,
            "dashboard_url": self.dashboard_url,
            "scraped_at_local": self.scraped_at_local,
            "scraped_at_utc": self.scraped_at_utc,
        }


@dataclass(frozen=True)
class TargetStrikeRow:
    snapshot_date: str
    target_key: str
    target_label: str
    strike_count: int
    chart_title: str
    metric_key: str
    source_page_url: str
    dashboard_url: str
    scraped_at_local: str
    scraped_at_utc: str

    def to_dict(self) -> dict[str, str]:
        return {
            "snapshot_date": self.snapshot_date,
            "target_key": self.target_key,
            "target_label": self.target_label,
            "strike_count": str(self.strike_count),
            "chart_title": self.chart_title,
            "metric_key": self.metric_key,
            "source_page_url": self.source_page_url,
            "dashboard_url": self.dashboard_url,
            "scraped_at_local": self.scraped_at_local,
            "scraped_at_utc": self.scraped_at_utc,
        }


def serialize_optional_int(value: int | None) -> str:
    return "" if value is None else str(value)


def parse_optional_int(value: str) -> int | None:
    stripped = value.strip()
    return int(stripped) if stripped else None


def detect_browser_executable() -> str:
    candidates = [
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
        Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
        Path("/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"),
    ]

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    joined_candidates = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "No supported local browser executable was found. "
        f"Checked: {joined_candidates}"
    )


def open_dashboard(playwright: Playwright) -> tuple[Any, Page]:
    browser = playwright.chromium.launch(
        headless=True,
        executable_path=detect_browser_executable(),
    )
    page = browser.new_page(locale="en-US", viewport={"width": 1600, "height": 1400})
    page.goto(DASHBOARD_URL, wait_until="networkidle", timeout=180_000)
    page.wait_for_timeout(20_000)
    return browser, page


def locate_visual(page: Page, title: str, wait_selector: str = "svg") -> Locator:
    title_locator = page.locator(".visualsEnterHint", has_text=title)
    title_locator.first.wait_for(state="attached", timeout=120_000)
    visual = page.locator(".visualContainer").filter(has=title_locator).first
    visual.wait_for(timeout=120_000)
    visual.locator(wait_selector).first.wait_for(timeout=120_000)
    return visual


_LINE_CHART_JS = """(node) => {
    const parseTr = (val) => {
        const m = /translate\\(([-0-9.]+),?\\s*([-0-9.]+)?/.exec(val || "");
        return m ? [parseFloat(m[1]), parseFloat(m[2] || 0)] : null;
    };

    const ticks = [...node.querySelectorAll(".x.axis .tick")].map((t) => {
        const titleEl = t.querySelector("title");
        const textEl = t.querySelector("text");
        const pos = parseTr(t.getAttribute("transform"));
        return {
            date: (titleEl?.textContent || textEl?.textContent || "").trim(),
            x: pos?.[0],
        };
    }).filter((t) => t.date && t.x !== null);

    const legends = [...node.querySelectorAll(".legend-item")].map((l) =>
        l.getAttribute("aria-label") || l.textContent?.trim() || ""
    ).filter(Boolean);

    const lines = [...node.querySelectorAll("path.line")].map((l) =>
        l.getAttribute("d") || ""
    );

    const labels = [...node.querySelectorAll("g.label-container")].map((g) => {
        const pos = parseTr(g.getAttribute("transform"));
        const txt = g.querySelector("text.label")?.textContent?.trim() || "";
        return { x: pos?.[0], y: pos?.[1], val: parseInt(txt) || 0 };
    }).filter((l) => l.x !== null && l.val > 0);

    return { ticks, legends, lines, labels };
}"""


def extract_line_chart_raw(visual: Locator) -> dict[str, Any]:
    return visual.evaluate(_LINE_CHART_JS)


def extract_line_chart_series(visual: Locator) -> dict[str, dict[str, int]]:
    return _parse_line_chart_payload(extract_line_chart_raw(visual))


def _parse_line_chart_payload(
    raw: dict[str, Any],
) -> dict[str, dict[str, int]]:
    import re as _re

    ticks = raw["ticks"]
    legends = raw["legends"]

    all_line_points: list[list[tuple[float, float]]] = []
    for path_d in raw["lines"]:
        if not path_d:
            all_line_points.append([])
            continue
        coords = _re.findall(r"([-0-9.]+),([-0-9.]+)", path_d)
        all_line_points.append([(float(x), float(y)) for x, y in coords])

    def _points_at_x(
        target_x: float, tol: float = 2.0
    ) -> list[tuple[int, float]]:
        hits = []
        for li, pts in enumerate(all_line_points):
            for px, py in pts:
                if abs(px - target_x) < tol:
                    hits.append((li, py))
        return hits

    calibration_pairs: list[tuple[float, int]] = []
    for lab in raw["labels"]:
        hits = _points_at_x(lab["x"])
        if len(hits) == 1:
            calibration_pairs.append((hits[0][1], lab["val"]))

    if len(calibration_pairs) < 2:
        raise RuntimeError(
            f"Need at least 2 unambiguous calibration labels, got {len(calibration_pairs)}"
        )

    y1, v1 = calibration_pairs[0]
    y2, v2 = calibration_pairs[1]
    scale = (v1 - v2) / (y1 - y2)
    intercept = v1 - scale * y1

    def _nearest_date(px: float) -> str | None:
        best = None
        for t in ticks:
            d = abs(px - t["x"])
            if not best or d < best[0]:
                best = (d, t["date"])
        return best[1] if best else None

    series: dict[str, dict[str, int]] = {}
    for li, pts in enumerate(all_line_points):
        name = legends[li] if li < len(legends) else f"series_{li}"
        values: dict[str, int] = {}
        for px, py in pts:
            date = _nearest_date(px)
            if date:
                values[date] = round(scale * py + intercept)
        series[name] = values

    return series


def extract_categorical_chart_values(visual: Locator) -> dict[str, int]:
    payload = visual.evaluate(
        """(node) => {
            const toNumber = (value) => Number.parseFloat(value);
            const parseTranslateX = (value) => {
                const match = /translate\\(([-0-9.]+)/.exec(value || "");
                return match ? Number.parseFloat(match[1]) : null;
            };

            const ticks = [...node.querySelectorAll(".x.axis .tick")].map((tick) => {
                const titleEl = tick.querySelector("title");
                const textEl = tick.querySelector("text");
                return {
                    label: (titleEl?.textContent || textEl?.textContent || "").trim(),
                    x: parseTranslateX(tick.getAttribute("transform")),
                };
            }).filter((tick) => tick.label && tick.x !== null);

            const nearestTickLabel = (centerX) => {
                let nearest = null;
                for (const tick of ticks) {
                    const distance = Math.abs(centerX - tick.x);
                    if (!nearest || distance < nearest.distance) {
                        nearest = { label: tick.label, distance };
                    }
                }
                return nearest?.label || null;
            };

            const values = {};
            for (const bar of node.querySelectorAll("g.series rect.column")) {
                const width = toNumber(bar.getAttribute("width"));
                const x = toNumber(bar.getAttribute("x"));
                const value = Number.parseInt(bar.getAttribute("aria-label") || "", 10);
                if (Number.isNaN(width) || Number.isNaN(x) || Number.isNaN(value)) {
                    continue;
                }
                const label = nearestTickLabel(x + width / 2);
                if (label) {
                    values[label] = value;
                }
            }

            return values;
        }"""
    )

    return payload


def normalize_date(date_text: str) -> str:
    return datetime.strptime(date_text, "%m/%d/%Y").date().isoformat()


def get_scraped_now_local() -> datetime:
    return datetime.now().astimezone().replace(microsecond=0)


def get_scrape_timestamps() -> tuple[str, str, str]:
    scraped_now_local = get_scraped_now_local()
    return (
        scraped_now_local.date().isoformat(),
        scraped_now_local.isoformat(),
        scraped_now_local.astimezone(UTC).isoformat(),
    )


def scrape_visual_payload(
    visual_title: str,
    extractor: Callable[[Locator], T],
    wait_selector: str = "svg",
) -> T:
    with sync_playwright() as playwright:
        browser, page = open_dashboard(playwright)
        try:
            visual = locate_visual(page, visual_title, wait_selector)
            return extractor(visual)
        finally:
            browser.close()


def build_rows(
    series_by_name: dict[str, dict[str, int]],
) -> list[ScrapedRow]:
    iran_series = series_by_name.get("Waves of attacks from Iran", {})
    lebanon_series = series_by_name.get("Waves of attacks from Lebanon", {})
    yemen_series = series_by_name.get("Waves of attacks from Yemen", {})
    all_dates = sorted(
        {normalize_date(d) for d in iran_series}
        | {normalize_date(d) for d in lebanon_series}
        | {normalize_date(d) for d in yemen_series}
    )
    iran_normalized = {normalize_date(k): v for k, v in iran_series.items()}
    lebanon_normalized = {normalize_date(k): v for k, v in lebanon_series.items()}
    yemen_normalized = {normalize_date(k): v for k, v in yemen_series.items()}
    snapshot_date, scraped_at_local, scraped_at_utc = get_scrape_timestamps()

    rows: list[ScrapedRow] = []
    for event_date in all_dates:
        iran_count = iran_normalized.get(event_date)
        lebanon_count = lebanon_normalized.get(event_date)
        yemen_count = yemen_normalized.get(event_date)
        total_count = (iran_count or 0) + (lebanon_count or 0) + (yemen_count or 0)
        rows.append(
            ScrapedRow(
                snapshot_date=snapshot_date,
                event_date=event_date,
                iran_barrage_count=iran_count,
                lebanon_barrage_count=lebanon_count,
                yemen_barrage_count=yemen_count,
                total_barrage_count=total_count,
                chart_title=BARRAGE_VISUAL_TITLE,
                metric_key=BARRAGE_METRIC_KEY,
                source_page_url=SOURCE_PAGE_URL,
                dashboard_url=DASHBOARD_URL,
                scraped_at_local=scraped_at_local,
                scraped_at_utc=scraped_at_utc,
            )
        )

    return rows


def normalize_target_label(raw_label: str) -> tuple[str, str]:
    import re as _re

    stripped = raw_label.strip()
    label = HEBREW_LABEL_MAP.get(stripped, stripped)
    key = _re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return (key, label)


def build_target_rows(category_values: dict[str, int]) -> list[TargetStrikeRow]:
    snapshot_date, scraped_at_local, scraped_at_utc = get_scrape_timestamps()
    rows: list[TargetStrikeRow] = []

    for raw_label, strike_count in sorted(category_values.items()):
        target_key, target_label = normalize_target_label(raw_label)
        rows.append(
            TargetStrikeRow(
                snapshot_date=snapshot_date,
                target_key=target_key,
                target_label=target_label,
                strike_count=strike_count,
                chart_title=KEY_TARGETS_VISUAL_TITLE,
                metric_key=KEY_TARGETS_METRIC_KEY,
                source_page_url=SOURCE_PAGE_URL,
                dashboard_url=DASHBOARD_URL,
                scraped_at_local=scraped_at_local,
                scraped_at_utc=scraped_at_utc,
            )
        )

    return sorted(rows, key=lambda row: (-row.strike_count, row.target_label))


def get_snapshot_date(row: dict[str, str]) -> str:
    snapshot_date = (row.get("snapshot_date") or "").strip()
    if snapshot_date:
        return snapshot_date

    scraped_at_utc = (row.get("scraped_at_utc") or "").strip()
    if scraped_at_utc:
        try:
            return datetime.fromisoformat(scraped_at_utc).date().isoformat()
        except ValueError:
            pass

    raise ValueError("Row is missing both snapshot_date and a valid scraped_at_utc value.")


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as csv_file:
        return list(csv.DictReader(csv_file))


def get_latest_snapshot_rows(
    rows: list[dict[str, str]], sort_key: str = "event_date"
) -> list[dict[str, str]]:
    if not rows:
        return []

    rows_with_snapshot = [
        {**row, "snapshot_date": get_snapshot_date(row)}
        for row in rows
        if row.get(sort_key)
    ]
    if not rows_with_snapshot:
        return []

    latest_snapshot_date = max(row["snapshot_date"] for row in rows_with_snapshot)
    latest_rows = [
        row for row in rows_with_snapshot if row["snapshot_date"] == latest_snapshot_date
    ]
    return sorted(latest_rows, key=lambda row: row[sort_key])


def write_latest_graph(
    csv_path: Path,
    graph_path: Path,
    sort_key: str,
    build_figure: Callable[[list[dict[str, str]]], go.Figure],
) -> None:
    latest_rows = get_latest_snapshot_rows(load_csv_rows(csv_path), sort_key=sort_key)
    if not latest_rows:
        raise RuntimeError("No CSV rows available for graph generation.")

    figure = build_figure(latest_rows)
    graph_path.parent.mkdir(parents=True, exist_ok=True)
    figure.write_html(graph_path, include_plotlyjs="cdn")


def write_latest_snapshot_graph(csv_path: Path, graph_path: Path) -> None:
    def build_figure(latest_rows: list[dict[str, str]]) -> go.Figure:
        event_dates = [row["event_date"] for row in latest_rows]
        iran_counts = [
            parse_optional_int(row["iran_barrage_count"]) or 0 for row in latest_rows
        ]
        lebanon_counts = [
            parse_optional_int(row.get("lebanon_barrage_count", "")) or 0
            for row in latest_rows
        ]
        yemen_counts = [
            parse_optional_int(row.get("yemen_barrage_count", "")) or 0
            for row in latest_rows
        ]
        total_counts = [int(row["total_barrage_count"]) for row in latest_rows]
        snapshot_date = latest_rows[0]["snapshot_date"]

        figure = go.Figure()
        figure.add_trace(go.Bar(name="Iran", x=event_dates, y=iran_counts))
        figure.add_trace(go.Bar(name="Lebanon", x=event_dates, y=lebanon_counts))
        figure.add_trace(go.Bar(name="Yemen", x=event_dates, y=yemen_counts))
        figure.add_trace(
            go.Scatter(
                name="Total",
                x=event_dates,
                y=total_counts,
                mode="lines+markers",
            )
        )
        figure.update_layout(
            title=f"Waves of Attacks on Israel by Day (Latest Snapshot: {snapshot_date})",
            xaxis_title="Event date",
            yaxis_title="Attack count",
            barmode="stack",
            template="plotly_white",
            legend_title="Series",
        )
        return figure

    write_latest_graph(
        csv_path=csv_path,
        graph_path=graph_path,
        sort_key="event_date",
        build_figure=build_figure,
    )


def write_latest_target_graph(csv_path: Path, graph_path: Path) -> None:
    def build_figure(latest_rows: list[dict[str, str]]) -> go.Figure:
        target_labels = [row["target_label"] for row in latest_rows]
        strike_counts = [int(row["strike_count"]) for row in latest_rows]
        snapshot_date = latest_rows[0]["snapshot_date"]

        figure = go.Figure(
            data=[
                go.Bar(
                    x=target_labels,
                    y=strike_counts,
                    name="Strike count",
                )
            ]
        )
        figure.update_layout(
            title=f"Key Target Strikes in Middle East (Latest Snapshot: {snapshot_date})",
            xaxis_title="Target category",
            yaxis_title="Strike count",
            template="plotly_white",
        )
        return figure

    write_latest_graph(
        csv_path=csv_path,
        graph_path=graph_path,
        sort_key="target_label",
        build_figure=build_figure,
    )


def upsert_rows(path: Path, rows: list[ScrapedRow]) -> None:
    upsert_snapshot_rows(
        path=path,
        rows=[row.to_dict() for row in rows],
        unique_key_field="event_date",
    )


def upsert_target_rows(path: Path, rows: list[TargetStrikeRow]) -> None:
    upsert_snapshot_rows(
        path=path,
        rows=[row.to_dict() for row in rows],
        unique_key_field="target_key",
    )


def upsert_snapshot_rows(
    path: Path, rows: list[dict[str, str]], unique_key_field: str
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    by_snapshot_and_key: dict[tuple[str, str], dict[str, str]] = {}

    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as existing_file:
            reader = csv.DictReader(existing_file)
            for row in reader:
                unique_key = row.get(unique_key_field)
                if unique_key:
                    snapshot_date = get_snapshot_date(row)
                    normalized_row = {"snapshot_date": snapshot_date, **row}
                    by_snapshot_and_key[(snapshot_date, unique_key)] = normalized_row

    for row in rows:
        snapshot_date = row["snapshot_date"]
        unique_key = row[unique_key_field]
        by_snapshot_and_key[(snapshot_date, unique_key)] = row

    sorted_rows = [
        by_snapshot_and_key[key] for key in sorted(by_snapshot_and_key)
    ]

    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_rows)


def _merge_series(
    target: dict[str, dict[str, int]], source: dict[str, dict[str, int]]
) -> None:
    for name, values in source.items():
        if name not in target:
            target[name] = {}
        target[name].update(values)


def _count_all_dates(series: dict[str, dict[str, int]]) -> int:
    return len({d for vals in series.values() for d in vals})


def _merge_series(
    target: dict[str, dict[str, int]], source: dict[str, dict[str, int]]
) -> None:
    for name, values in source.items():
        if name not in target:
            target[name] = {}
        target[name].update(values)


def _count_all_dates(series: dict[str, dict[str, int]]) -> int:
    return len({d for vals in series.values() for d in vals})


def _extract_table_series(page: Page, visual: Locator) -> dict[str, dict[str, int]]:
    line = visual.locator("path.line").first
    box = line.bounding_box()
    if not box:
        raise RuntimeError("Could not get bounding box for line element")

    page.mouse.click(
        box["x"] + box["width"] / 2,
        box["y"] + box["height"] / 2,
        button="right",
    )
    page.wait_for_timeout(2_000)
    page.get_by_role("menuitem", name="Show as a table").click()
    page.wait_for_timeout(5_000)

    rows = page.evaluate(
        """() => {
            const table = document.querySelector('[role="grid"], table');
            if (!table) return [];
            return [...table.querySelectorAll('tr, [role="row"]')].map(row => {
                const cells = [...row.querySelectorAll(
                    'th, td, [role="columnheader"], [role="rowheader"], [role="gridcell"]'
                )];
                return cells.map(c => c.textContent.trim());
            });
        }"""
    )

    if len(rows) < 3:
        raise RuntimeError(f"Table too small ({len(rows)} rows)")

    headers = rows[0]
    series: dict[str, dict[str, int]] = {}
    for col_idx in range(1, len(headers)):
        series[headers[col_idx]] = {}

    for row in rows[1:]:
        date_str = row[0] if row[0] else ""
        if not date_str or "/" not in date_str:
            continue
        for col_idx in range(1, min(len(headers), len(row))):
            val = row[col_idx].strip()
            if val:
                series[headers[col_idx]][date_str] = int(val)

    return series


def scrape_barrages_to_israel() -> list[ScrapedRow]:
    with sync_playwright() as playwright:
        browser, page = open_dashboard(playwright)
        try:
            visual = locate_visual(
                page, BARRAGE_VISUAL_TITLE, wait_selector="path.line"
            )
            series = _extract_table_series(page, visual)
            return build_rows(series_by_name=series)
        finally:
            browser.close()


def scrape_key_target_strikes_middle_east() -> list[TargetStrikeRow]:
    category_values = scrape_visual_payload(
        KEY_TARGETS_VISUAL_TITLE,
        extract_categorical_chart_values,
        wait_selector="g.series rect.column",
    )
    return build_target_rows(category_values=category_values)


GIBS_FALLBACK_LAYER = "MODIS_Terra_CorrectedReflectance_TrueColor"


def fetch_gibs_tile(
    date: str, row: int, col: int, layer: str | None = None
) -> tuple[int, int, bytes]:
    layer = layer or GIBS_LAYER
    url = (
        f"https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/"
        f"{layer}/default/{date}/{GIBS_TILE_MATRIX_SET}/"
        f"{GIBS_ZOOM}/{row}/{col}.jpeg"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        return row, col, resp.read()


def fetch_and_crop_gibs(date: str, layer: str | None = None) -> Image.Image:
    layer = layer or GIBS_LAYER
    tiles: dict[tuple[int, int], bytes] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = [
            pool.submit(fetch_gibs_tile, date, r, c, layer)
            for r in GIBS_ROW_RANGE
            for c in GIBS_COL_RANGE
        ]
        for f in concurrent.futures.as_completed(futures):
            row, col, data = f.result()
            tiles[(row, col)] = data

    n_cols = len(GIBS_COL_RANGE)
    n_rows = len(GIBS_ROW_RANGE)
    canvas = Image.new("RGB", (n_cols * GIBS_TILE_SIZE, n_rows * GIBS_TILE_SIZE))
    for ri, row in enumerate(GIBS_ROW_RANGE):
        for ci, col in enumerate(GIBS_COL_RANGE):
            tile = Image.open(io.BytesIO(tiles[(row, col)]))
            canvas.paste(tile, (ci * GIBS_TILE_SIZE, ri * GIBS_TILE_SIZE))

    grid_west = min(GIBS_COL_RANGE) * GIBS_TILE_DEG - 180
    grid_north = 90 - min(GIBS_ROW_RANGE) * GIBS_TILE_DEG
    ppd = GIBS_TILE_SIZE / GIBS_TILE_DEG

    crop_box = (
        int((IRAN_WEST - grid_west) * ppd),
        int((grid_north - IRAN_NORTH) * ppd),
        int((IRAN_EAST - grid_west) * ppd),
        int((grid_north - IRAN_SOUTH) * ppd),
    )
    return canvas.crop(crop_box)


def download_gibs_image(date: str) -> Path:
    import numpy as np

    image = fetch_and_crop_gibs(date, GIBS_LAYER)
    arr = np.array(image.convert("L"))
    if float(np.mean(arr < 10)) > 0.5:
        print(f"  VIIRS blank for {date}, falling back to MODIS Terra")
        image = fetch_and_crop_gibs(date, GIBS_FALLBACK_LAYER)

    GIBS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = GIBS_OUTPUT_DIR / f"iran_satellite_{date}.jpeg"
    image.save(output_path, "JPEG", quality=90)
    return output_path


def backfill_gibs_from_csv() -> list[Path]:
    csv_rows = load_csv_rows(BARRAGE_OUTPUT_PATH)
    event_dates = sorted({row["event_date"] for row in csv_rows if row.get("event_date")})

    saved: list[Path] = []
    for date in event_dates:
        target = GIBS_OUTPUT_DIR / f"iran_satellite_{date}.jpeg"
        if target.exists():
            continue
        try:
            path = download_gibs_image(date)
            print(f"  NASA GIBS: saved {path}")
            saved.append(path)
        except Exception as exc:
            print(f"  NASA GIBS: failed for {date}: {exc}")

    return saved


ALERTS_RAW_URL = (
    "https://raw.githubusercontent.com/dleshem/israel-alerts-data/main/israel-alerts.csv"
)
ALERTS_RAW_PATH = Path("data/israel_alerts_raw.csv")
BALLISTIC_ALERTS_PATH = Path("data/ballistic_alerts_daily.csv")
BALLISTIC_ALERT_CATEGORY = "בדקות הקרובות צפויות להתקבל התרעות באזורך"


def refresh_ballistic_alerts() -> Path:
    print("  Downloading israel-alerts-data …")
    req = urllib.request.Request(ALERTS_RAW_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        ALERTS_RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
        ALERTS_RAW_PATH.write_bytes(resp.read())

    from collections import Counter

    daily: Counter[str] = Counter()
    with open(ALERTS_RAW_PATH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            date = row.get("alertDate", "")[:10]
            if date < "2026-02-28":
                continue
            if row.get("category_desc", "").strip() == BALLISTIC_ALERT_CATEGORY:
                daily[date] += 1

    with open(BALLISTIC_ALERTS_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "ballistic_alert_count"])
        for d in sorted(daily):
            writer.writerow([d, daily[d]])

    total = sum(daily.values())
    print(f"  Ballistic alerts: {len(daily)} days, {total} total alerts")
    return BALLISTIC_ALERTS_PATH


def main() -> None:
    barrage_rows = scrape_barrages_to_israel()
    target_rows = scrape_key_target_strikes_middle_east()
    if not barrage_rows:
        raise RuntimeError("No barrage rows were extracted from the dashboard.")
    if not target_rows:
        raise RuntimeError("No key target rows were extracted from the dashboard.")

    upsert_rows(BARRAGE_OUTPUT_PATH, barrage_rows)
    write_latest_snapshot_graph(BARRAGE_OUTPUT_PATH, BARRAGE_GRAPH_OUTPUT_PATH)
    upsert_target_rows(KEY_TARGETS_OUTPUT_PATH, target_rows)
    write_latest_target_graph(KEY_TARGETS_OUTPUT_PATH, KEY_TARGETS_GRAPH_OUTPUT_PATH)

    today = barrage_rows[0].snapshot_date
    today_gibs = GIBS_OUTPUT_DIR / f"iran_satellite_{today}.jpeg"
    if not today_gibs.exists():
        try:
            download_gibs_image(today)
            print(f"  NASA GIBS: saved {today_gibs}")
        except Exception as exc:
            print(f"  NASA GIBS: failed for today ({today}): {exc}")

    backfilled = backfill_gibs_from_csv()

    try:
        refresh_ballistic_alerts()
    except Exception as exc:
        print(f"  Ballistic alerts refresh failed: {exc}")

    summary = {
        "snapshot_date": barrage_rows[0].snapshot_date,
        "saved_barrage_snapshot_rows": len(barrage_rows),
        "saved_key_target_snapshot_rows": len(target_rows),
        "barrage_output_path": str(BARRAGE_OUTPUT_PATH),
        "barrage_graph_output_path": str(BARRAGE_GRAPH_OUTPUT_PATH),
        "key_targets_output_path": str(KEY_TARGETS_OUTPUT_PATH),
        "key_targets_graph_output_path": str(KEY_TARGETS_GRAPH_OUTPUT_PATH),
        "gibs_backfilled_count": len(backfilled),
        "latest_barrage_row": barrage_rows[-1].to_dict(),
        "latest_key_target_row": target_rows[0].to_dict(),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
