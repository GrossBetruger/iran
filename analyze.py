from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
from PIL import Image, ImageDraw
from scipy import stats

IRAN_GEOJSON_PATH = Path("data/iran_border.geojson")
NASA_SATELLITE_DIR = Path("data/satellite/nasa")
BARRAGE_CSV_PATH = Path("data/barrages_to_israel_daily.csv")
OUTPUT_DIR = Path("data/analysis")

IMG_W, IMG_H = 1109, 842
LON_MIN, LON_MAX = 44.0, 63.5
LAT_MAX, LAT_MIN = 39.8, 25.0

BRIGHTNESS_THRESHOLD = 200


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


def load_barrage_by_date() -> dict[str, int]:
    by_date: dict[str, int] = {}
    with open(BARRAGE_CSV_PATH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            date = row.get("event_date", "").strip()
            iran = row.get("iran_barrage_count", "").strip()
            if date and iran:
                by_date[date] = int(iran)
    return by_date


def classify_cloud(pct: float) -> str:
    if pct < 25:
        return "clear"
    if pct < 55:
        return "partly_cloudy"
    return "cloudy"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    mask = build_iran_mask()
    mask_arr = np.array(mask)
    barrage_by_date = load_barrage_by_date()

    dates: list[str] = []
    cloud_pcts: list[float] = []
    barrage_counts: list[int] = []
    categories: list[str] = []

    for image_path in sorted(NASA_SATELLITE_DIR.glob("iran_satellite_*.jpeg")):
        date = image_path.stem.replace("iran_satellite_", "")
        pct = compute_cloud_pct(image_path, mask_arr)
        if pct is None:
            print(f"  {date}: no data (skipped)")
            continue

        barrage = barrage_by_date.get(date)
        cat = classify_cloud(pct)
        print(f"  {date}: cloud={pct:.1f}%  category={cat}  iran_barrage={barrage}")

        if barrage is not None:
            dates.append(date)
            cloud_pcts.append(pct)
            barrage_counts.append(barrage)
            categories.append(cat)

    print(f"\n  Matched {len(dates)} days with both cloud data and barrage data\n")

    cloud_arr = np.array(cloud_pcts)
    barrage_arr = np.array(barrage_counts)

    spearman_r, spearman_p = stats.spearmanr(cloud_arr, barrage_arr)
    pearson_r, pearson_p = stats.pearsonr(cloud_arr, barrage_arr)

    print(f"  Spearman r={spearman_r:.3f}  p={spearman_p:.4f}")
    print(f"  Pearson  r={pearson_r:.3f}  p={pearson_p:.4f}")

    n_permutations = 10_000
    observed_r = spearman_r
    rng = np.random.default_rng(42)
    count_extreme = 0
    for _ in range(n_permutations):
        shuffled = rng.permutation(barrage_arr)
        r, _ = stats.spearmanr(cloud_arr, shuffled)
        if abs(r) >= abs(observed_r):
            count_extreme += 1
    perm_p = count_extreme / n_permutations
    print(f"  Permutation test p={perm_p:.4f} ({n_permutations} permutations)\n")

    # --- Scatter plot ---
    scatter_fig = go.Figure()
    scatter_fig.add_trace(go.Scatter(
        x=cloud_pcts, y=barrage_counts, mode="markers+text",
        text=dates, textposition="top center", textfont=dict(size=8),
        marker=dict(size=10, color=cloud_pcts, colorscale="Blues", showscale=True,
                    colorbar=dict(title="Cloud %")),
    ))
    slope, intercept = np.polyfit(cloud_arr, barrage_arr, 1)
    x_line = np.linspace(cloud_arr.min(), cloud_arr.max(), 50)
    scatter_fig.add_trace(go.Scatter(
        x=x_line.tolist(), y=(slope * x_line + intercept).tolist(),
        mode="lines", name="Trend",
        line=dict(dash="dash", color="red", width=2),
    ))
    scatter_fig.update_layout(
        title=(f"Cloud Cover vs Barrage Count<br>"
               f"<sub>Spearman r={spearman_r:.3f} (p={spearman_p:.4f}) | "
               f"Permutation p={perm_p:.4f}</sub>"),
        xaxis_title="Cloud cover over Iran (%)",
        yaxis_title="Iran barrage count",
        template="plotly_white",
        showlegend=False,
    )
    scatter_path = OUTPUT_DIR / "cloud_vs_barrage_scatter.html"
    scatter_fig.write_html(scatter_path, include_plotlyjs="cdn")
    print(f"  Saved {scatter_path}")

    # --- Box plot by category ---
    cat_order = ["clear", "partly_cloudy", "cloudy"]
    box_fig = go.Figure()
    for cat in cat_order:
        vals = [b for b, c in zip(barrage_counts, categories) if c == cat]
        box_fig.add_trace(go.Box(y=vals, name=cat.replace("_", " ").title(),
                                 boxpoints="all", jitter=0.3, pointpos=-1.5))
    box_fig.update_layout(
        title="Iran Barrage Count by Cloud Cover Category",
        yaxis_title="Iran barrage count",
        template="plotly_white",
        showlegend=False,
    )
    box_path = OUTPUT_DIR / "barrage_by_cloud_category.html"
    box_fig.write_html(box_path, include_plotlyjs="cdn")
    print(f"  Saved {box_path}")

    # --- Image gallery ---
    import base64

    all_entries: list[dict] = []
    for image_path in sorted(NASA_SATELLITE_DIR.glob("iran_satellite_*.jpeg")):
        date = image_path.stem.replace("iran_satellite_", "")
        pct = compute_cloud_pct(image_path, mask_arr)
        barrage = barrage_by_date.get(date)
        with open(image_path, "rb") as img_f:
            b64 = base64.b64encode(img_f.read()).decode()
        all_entries.append({
            "date": date,
            "cloud_pct": f"{pct:.1f}" if pct is not None else "N/A",
            "category": classify_cloud(pct) if pct is not None else "no_data",
            "barrage": str(barrage) if barrage is not None else "N/A",
            "b64": b64,
        })

    gallery_html_parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Cloud Gallery</title>",
        "<style>",
        "body{font-family:sans-serif;background:#111;color:#eee;margin:20px}",
        ".grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px}",
        ".card{background:#222;border-radius:8px;overflow:hidden}",
        ".card img{width:100%;display:block}",
        ".card .info{padding:10px;font-size:14px}",
        ".card .info span{display:inline-block;margin-right:12px}",
        ".tag{padding:2px 8px;border-radius:4px;font-size:12px;font-weight:bold}",
        ".clear{background:#2e7d32;color:#fff}",
        ".partly_cloudy{background:#f9a825;color:#000}",
        ".cloudy{background:#1565c0;color:#fff}",
        ".no_data{background:#555;color:#ccc}",
        "h1{margin-bottom:20px}",
        "</style></head><body>",
        "<h1>Iran Satellite Cloud Gallery</h1>",
        "<div class='grid'>",
    ]
    for e in all_entries:
        gallery_html_parts.append(
            f"<div class='card'>"
            f"<img src='data:image/jpeg;base64,{e['b64']}' alt='{e['date']}'>"
            f"<div class='info'>"
            f"<span><b>{e['date']}</b></span>"
            f"<span class='tag {e['category']}'>{e['category'].replace('_',' ')}</span>"
            f"<span>Cloud: {e['cloud_pct']}%</span>"
            f"<span>Iran barrage: {e['barrage']}</span>"
            f"</div></div>"
        )
    gallery_html_parts.append("</div></body></html>")

    gallery_path = OUTPUT_DIR / "cloud_gallery.html"
    gallery_path.write_text("".join(gallery_html_parts), encoding="utf-8")
    print(f"  Saved {gallery_path}")

    # --- Save CSV of joined data ---
    csv_path = OUTPUT_DIR / "cloud_barrage_joined.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "cloud_pct", "cloud_category", "iran_barrage_count"])
        for d, c, cat, b in zip(dates, cloud_pcts, categories, barrage_counts):
            writer.writerow([d, f"{c:.1f}", cat, b])
    print(f"  Saved {csv_path}")


if __name__ == "__main__":
    main()
