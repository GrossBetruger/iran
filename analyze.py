from __future__ import annotations

import base64
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
from PIL import Image, ImageDraw
from scipy import stats

IRAN_GEOJSON_PATH = Path("data/iran_border.geojson")
NASA_SATELLITE_DIR = Path("data/satellite/nasa")
OUTPUT_DIR = Path("data/analysis")

IMG_W, IMG_H = 1109, 842
LON_MIN, LON_MAX = 44.0, 63.5
LAT_MAX, LAT_MIN = 39.8, 25.0

BRIGHTNESS_THRESHOLD = 200
N_PERMUTATIONS = 10_000


@dataclass
class MissileSource:
    name: str
    slug: str
    y_label: str
    load: callable  # () -> dict[str, int]


def _load_inss_barrage() -> dict[str, int]:
    path = Path("data/barrages_to_israel_daily.csv")
    by_date: dict[str, int] = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            date = row.get("event_date", "").strip()
            val = row.get("iran_barrage_count", "").strip()
            if date and val:
                by_date[date] = int(val)
    return by_date


def _load_ballistic_alerts() -> dict[str, int]:
    path = Path("data/ballistic_alerts_daily.csv")
    by_date: dict[str, int] = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            date = row.get("date", "").strip()
            val = row.get("ballistic_alert_count", "").strip()
            if date and val:
                by_date[date] = int(val)
    return by_date


SOURCES: list[MissileSource] = [
    MissileSource(
        name="INSS Iran Barrage Count",
        slug="inss_barrage",
        y_label="Iran barrage count (INSS)",
        load=_load_inss_barrage,
    ),
    MissileSource(
        name="Pikud HaOref Ballistic Alerts",
        slug="ballistic_alerts",
        y_label="Ballistic advance-warning alerts",
        load=_load_ballistic_alerts,
    ),
]


# ---------------------------------------------------------------------------
# Cloud cover helpers
# ---------------------------------------------------------------------------

def geo_to_px(lon: float, lat: float) -> tuple[int, int]:
    x = (lon - LON_MIN) / (LON_MAX - LON_MIN) * IMG_W
    y = (LAT_MAX - lat) / (LAT_MAX - LAT_MIN) * IMG_H
    return int(x), int(y)


def build_iran_mask() -> Image.Image:
    with open(IRAN_GEOJSON_PATH) as f:
        geo = json.load(f)

    coords = geo["features"][0]["geometry"]["coordinates"][0]
    pixel_polygon = [geo_to_px(lon, lat) for lon, lat in coords]

    mask = Image.new("L", (IMG_W, IMG_H), 0)
    ImageDraw.Draw(mask).polygon(pixel_polygon, fill=255)
    return mask


def compute_cloud_pct(image_path: Path, mask_arr: np.ndarray) -> float | None:
    img = Image.open(image_path).convert("L")
    img_arr = np.array(img)

    iran_pixels = img_arr[mask_arr > 0]
    if iran_pixels.size == 0:
        return None

    black_ratio = np.mean(iran_pixels < 10)
    if black_ratio > 0.5:
        return None

    cloud_pixels = np.sum(iran_pixels >= BRIGHTNESS_THRESHOLD)
    return float(cloud_pixels / iran_pixels.size * 100)


def classify_cloud(pct: float) -> str:
    if pct < 25:
        return "clear"
    if pct < 55:
        return "partly_cloudy"
    return "cloudy"


def compute_cloud_by_date(
    mask_arr: np.ndarray,
) -> dict[str, tuple[float, str]]:
    """Return {date: (cloud_pct, category)} for every satellite image."""
    result: dict[str, tuple[float, str]] = {}
    for image_path in sorted(NASA_SATELLITE_DIR.glob("iran_satellite_*.jpeg")):
        date = image_path.stem.replace("iran_satellite_", "")
        pct = compute_cloud_pct(image_path, mask_arr)
        if pct is not None:
            result[date] = (pct, classify_cloud(pct))
    return result


# ---------------------------------------------------------------------------
# Analysis for a single source
# ---------------------------------------------------------------------------

def run_analysis(
    source: MissileSource,
    cloud_by_date: dict[str, tuple[float, str]],
    out_dir: Path,
) -> dict:
    """Run correlation analysis for one missile data source. Returns stats."""
    missile_by_date = source.load()

    dates: list[str] = []
    cloud_pcts: list[float] = []
    missile_counts: list[int] = []
    categories: list[str] = []

    for date in sorted(cloud_by_date):
        pct, cat = cloud_by_date[date]
        missile = missile_by_date.get(date)
        print(f"  {date}: cloud={pct:.1f}%  category={cat}  {source.slug}={missile}")
        if missile is not None:
            dates.append(date)
            cloud_pcts.append(pct)
            missile_counts.append(missile)
            categories.append(cat)

    n = len(dates)
    print(f"\n  [{source.slug}] Matched {n} days\n")
    if n < 5:
        print(f"  [{source.slug}] Too few data points, skipping.\n")
        return {}

    cloud_arr = np.array(cloud_pcts)
    missile_arr = np.array(missile_counts)

    spearman_r, spearman_p = stats.spearmanr(cloud_arr, missile_arr)
    pearson_r, pearson_p = stats.pearsonr(cloud_arr, missile_arr)
    print(f"  Spearman r={spearman_r:.3f}  p={spearman_p:.4f}")
    print(f"  Pearson  r={pearson_r:.3f}  p={pearson_p:.4f}")

    rng = np.random.default_rng(42)
    count_extreme = sum(
        1
        for _ in range(N_PERMUTATIONS)
        if abs(stats.spearmanr(cloud_arr, rng.permutation(missile_arr))[0])
        >= abs(spearman_r)
    )
    perm_p = count_extreme / N_PERMUTATIONS
    print(f"  Permutation test p={perm_p:.4f} ({N_PERMUTATIONS} permutations)\n")

    # --- Scatter plot ---
    scatter_fig = go.Figure()
    scatter_fig.add_trace(go.Scatter(
        x=cloud_pcts, y=missile_counts, mode="markers+text",
        text=dates, textposition="top center", textfont=dict(size=8),
        marker=dict(size=10, color=cloud_pcts, colorscale="Blues", showscale=True,
                    colorbar=dict(title="Cloud %")),
    ))
    slope, intercept = np.polyfit(cloud_arr, missile_arr, 1)
    x_line = np.linspace(cloud_arr.min(), cloud_arr.max(), 50)
    scatter_fig.add_trace(go.Scatter(
        x=x_line.tolist(), y=(slope * x_line + intercept).tolist(),
        mode="lines", name="Trend",
        line=dict(dash="dash", color="red", width=2),
    ))
    scatter_fig.update_layout(
        title=(f"Cloud Cover vs {source.name}<br>"
               f"<sub>Spearman r={spearman_r:.3f} (p={spearman_p:.4f}) | "
               f"Permutation p={perm_p:.4f}</sub>"),
        xaxis_title="Cloud cover over Iran (%)",
        yaxis_title=source.y_label,
        template="plotly_white",
        showlegend=False,
    )
    scatter_path = out_dir / f"scatter_{source.slug}.html"
    scatter_fig.write_html(scatter_path, include_plotlyjs="cdn")
    print(f"  Saved {scatter_path}")

    # --- Box plot by category ---
    cat_order = ["clear", "partly_cloudy", "cloudy"]
    box_fig = go.Figure()
    for cat in cat_order:
        vals = [m for m, c in zip(missile_counts, categories) if c == cat]
        box_fig.add_trace(go.Box(
            y=vals, name=cat.replace("_", " ").title(),
            boxpoints="all", jitter=0.3, pointpos=-1.5,
        ))
    box_fig.update_layout(
        title=f"{source.name} by Cloud Cover Category",
        yaxis_title=source.y_label,
        template="plotly_white",
        showlegend=False,
    )
    box_path = out_dir / f"boxplot_{source.slug}.html"
    box_fig.write_html(box_path, include_plotlyjs="cdn")
    print(f"  Saved {box_path}")

    # --- Joined CSV ---
    csv_path = out_dir / f"joined_{source.slug}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "cloud_pct", "cloud_category", source.slug])
        for d, c, cat, m in zip(dates, cloud_pcts, categories, missile_counts):
            writer.writerow([d, f"{c:.1f}", cat, m])
    print(f"  Saved {csv_path}")

    return {
        "source": source.name,
        "matched_days": n,
        "spearman_r": spearman_r,
        "spearman_p": spearman_p,
        "pearson_r": pearson_r,
        "pearson_p": pearson_p,
        "perm_p": perm_p,
        "missile_by_date": missile_by_date,
    }


# ---------------------------------------------------------------------------
# Gallery (shows all sources side by side per image card)
# ---------------------------------------------------------------------------

def build_gallery(
    cloud_by_date: dict[str, tuple[float, str]],
    mask_arr: np.ndarray,
    source_data: list[tuple[MissileSource, dict[str, int]]],
    out_dir: Path,
) -> None:
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    hours_elapsed = now_utc.hour + now_utc.minute / 60

    all_entries: list[dict] = []
    for image_path in sorted(NASA_SATELLITE_DIR.glob("iran_satellite_*.jpeg")):
        date = image_path.stem.replace("iran_satellite_", "")
        pct = compute_cloud_pct(image_path, mask_arr)
        with open(image_path, "rb") as img_f:
            b64 = base64.b64encode(img_f.read()).decode()
        is_partial = date == today_str
        entry = {
            "date": date,
            "cloud_pct": f"{pct:.1f}" if pct is not None else "N/A",
            "category": classify_cloud(pct) if pct is not None else "no_data",
            "b64": b64,
            "partial": is_partial,
            "hours_elapsed": f"{hours_elapsed:.0f}" if is_partial else None,
        }
        for src, by_date in source_data:
            val = by_date.get(date)
            entry[src.slug] = str(val) if val is not None else "N/A"
        all_entries.append(entry)

    source_spans = "".join(
        f"<span>{src.name}: {{e['{src.slug}']}}</span>"
        for src, _ in source_data
    )

    html_parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Cloud Gallery</title>",
        "<style>",
        "body{font-family:sans-serif;background:#111;color:#eee;margin:20px}",
        ".grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px}",
        ".card{background:#222;border-radius:8px;overflow:hidden}",
        ".card img{width:100%;display:block}",
        ".card .info{padding:10px;font-size:13px;line-height:1.7}",
        ".card .info span{display:inline-block;margin-right:12px}",
        ".tag{padding:2px 8px;border-radius:4px;font-size:12px;font-weight:bold}",
        ".clear{background:#2e7d32;color:#fff}",
        ".partly_cloudy{background:#f9a825;color:#000}",
        ".cloudy{background:#1565c0;color:#fff}",
        ".no_data{background:#555;color:#ccc}",
        ".partial{background:#e65100;color:#fff}",
        "h1{margin-bottom:20px}",
        "</style></head><body>",
        "<h1>Iran Satellite Cloud Gallery</h1>",
        "<div class='grid'>",
    ]
    for e in all_entries:
        source_lines = "".join(
            f"<span>{src.name}: {e[src.slug]}</span>"
            for src, _ in source_data
        )
        partial_badge = (
            f" <span class='tag partial'>PARTIAL — {e['hours_elapsed']}h of 24h</span>"
            if e["partial"]
            else ""
        )
        html_parts.append(
            f"<div class='card'>"
            f"<img src='data:image/jpeg;base64,{e['b64']}' alt='{e['date']}'>"
            f"<div class='info'>"
            f"<span><b>{e['date']}</b></span>"
            f"<span class='tag {e['category']}'>{e['category'].replace('_',' ')}</span>"
            f"<span>Cloud: {e['cloud_pct']}%</span>"
            f"{partial_badge}<br>"
            f"{source_lines}"
            f"</div></div>"
        )
    html_parts.append("</div></body></html>")

    gallery_path = out_dir / "cloud_gallery.html"
    gallery_path.write_text("".join(html_parts), encoding="utf-8")
    print(f"  Saved {gallery_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    mask = build_iran_mask()
    mask_arr = np.array(mask)
    cloud_by_date = compute_cloud_by_date(mask_arr)

    results: list[dict] = []
    loaded_sources: list[tuple[MissileSource, dict[str, int]]] = []

    for source in SOURCES:
        print(f"\n{'='*60}")
        print(f"  Source: {source.name}")
        print(f"{'='*60}\n")
        result = run_analysis(source, cloud_by_date, OUTPUT_DIR)
        if result:
            results.append(result)
            loaded_sources.append((source, result["missile_by_date"]))

    build_gallery(cloud_by_date, mask_arr, loaded_sources, OUTPUT_DIR)

    if len(results) > 1:
        print(f"\n{'='*60}")
        print("  Summary across all sources")
        print(f"{'='*60}\n")
        for r in results:
            print(
                f"  {r['source']:<40} "
                f"n={r['matched_days']:>3}  "
                f"Spearman r={r['spearman_r']:>7.3f} (p={r['spearman_p']:.4f})  "
                f"Perm p={r['perm_p']:.4f}"
            )
        print()


if __name__ == "__main__":
    main()
