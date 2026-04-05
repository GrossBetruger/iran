# Iran–Israel Conflict: Cloud Cover & Missile Correlation

A data pipeline that scrapes daily military campaign metrics, downloads satellite
imagery of Iran, collects Home Front Command alert data, and runs statistical
analysis to test whether cloud cover over Iran correlates with missile barrage
activity toward Israel.

## Hypothesis

Iran may launch more missile barrages on cloudier days, when satellite
reconnaissance is degraded. This project collects the data needed to evaluate
that hypothesis and tracks the correlation over time.

## Data Sources

| Source | What it provides |
|--------|------------------|
| [INSS Power BI Dashboard](https://www.inss.org.il/publication/lions-roar-data/) | Daily barrage counts (Iran, Lebanon, Yemen) and key target strike categories |
| [NASA GIBS](https://gibs.earthdata.nasa.gov/) (WMTS) | Daily true-color satellite imagery of Iran (VIIRS SNPP, MODIS Terra fallback) |
| [Pikud HaOref alerts](https://github.com/dleshem/israel-alerts-data) (via GitHub) | Historical Israeli Home Front Command alerts, filtered for ballistic advance-warnings |
| `data/iran_border.geojson` | Iran border polygon used to mask satellite images |

## Quick Start

```bash
# Install dependencies (requires uv)
uv sync

# Install Playwright browsers (first time only)
uv run playwright install chromium

# Run the full daily pipeline
uv run python main.py

# Run the cloud-cover analysis
uv run python analyze.py
```

## Pipeline Overview

### `main.py` — Daily Scraper

Runs four stages, scheduled daily at 08:00 via macOS LaunchAgent:

1. **INSS dashboard scrape** — launches headless Chromium via Playwright, navigates
   to the Power BI report, and extracts two datasets:
   - *Waves of Attacks on Israel*: daily barrage counts by origin (Iran, Lebanon,
     Yemen), extracted via the Power BI "Show as a table" feature.
   - *Key Targets Struck by Iran in the ME*: categorical strike counts (Radar,
     Airports, etc.), scraped from SVG bar elements.

2. **CSV upsert** — writes scraped data into append-only CSVs keyed by
   `(snapshot_date, event_date)` or `(snapshot_date, target_key)`. Re-running on
   the same day replaces that day's snapshot; previous snapshots are preserved.

3. **NASA GIBS satellite download** — fetches a grid of WMTS tiles covering Iran
   (lat 25–39.8°N, lon 44–63.5°E), stitches them with Pillow, and crops to Iran's
   bounding box. Uses VIIRS SNPP by default; falls back to MODIS Terra when VIIRS
   returns a blank (night-side) image. Backfills any missing dates.

4. **Ballistic alert refresh** — downloads the full Pikud HaOref alert dataset
   (~540K rows), filters for the ballistic advance-warning category
   (`בדקות הקרובות צפויות להתקבל התרעות באזורך`), and aggregates into daily
   counts.

### `analyze.py` — Statistical Analysis

Computes cloud cover from satellite imagery and correlates it with missile
activity:

- **Cloud cover quantification** — loads Iran's border polygon as a pixel mask,
  computes the fraction of bright pixels (brightness ≥ 200) within the mask, and
  classifies each day as `clear` (<25%), `partly_cloudy` (25–55%), or `cloudy`
  (>55%). Blank/night images (>50% near-black pixels) are skipped.

- **Correlation analysis** — for each missile data source (INSS barrages, Pikud
  HaOref ballistic alerts), computes Spearman and Pearson correlations, runs a
  10,000-iteration permutation test, and reports windowed results (last 10, 12,
  15 days).

- **Outputs** — scatter plots, box plots (grouped by cloud category), joined
  CSVs, and an HTML image gallery showing each day's satellite image with cloud
  percentage, category badge, and missile counts from all sources.

## Project Structure

```
├── main.py                        # Daily scraper + satellite downloader
├── analyze.py                     # Cloud-cover correlation analysis
├── pyproject.toml                 # Dependencies
├── com.iran.scraper.plist         # macOS LaunchAgent (daily at 08:00)
├── copy_plist_to_cronjob_dest.sh  # Installs plist to ~/Library/LaunchAgents/
├── load_and_run_cronjob.sh        # Loads and starts the LaunchAgent
├── tests/
│   └── test_main.py               # Unit tests for main.py
├── tagged_data/                   # Manually classified satellite images
│   ├── clear/                     #   (ground truth for cloud thresholds)
│   ├── cloudy/
│   └── partly_cloudy/
└── data/
    ├── barrages_to_israel_daily.csv
    ├── barrages_to_israel_latest_snapshot.html
    ├── key_target_strikes_middle_east.csv
    ├── key_target_strikes_middle_east_latest_snapshot.html
    ├── ballistic_alerts_daily.csv
    ├── israel_alerts_raw.csv              (Git LFS)
    ├── iran_border.geojson
    ├── satellite/
    │   └── nasa/                          (Git LFS)
    │       └── iran_satellite_YYYY-MM-DD.jpeg
    └── analysis/
        ├── scatter_inss_barrage.html
        ├── scatter_ballistic_alerts.html
        ├── boxplot_inss_barrage.html
        ├── boxplot_ballistic_alerts.html
        ├── joined_inss_barrage.csv
        ├── joined_ballistic_alerts.csv
        └── cloud_gallery.html             (Git LFS)
```

## Scheduling

The scraper runs daily via macOS `launchd`. To install:

```bash
bash copy_plist_to_cronjob_dest.sh
bash load_and_run_cronjob.sh
```

This registers a LaunchAgent that runs `uv run python main.py` at 08:00 daily,
with output logged to `data/cron.log`.

## Missile Data Sources

The analysis is modular — `analyze.py` defines a `MissileSource` dataclass and
iterates over a `SOURCES` list, so new data feeds can be added with minimal code:

| Source | File | Description |
|--------|------|-------------|
| INSS Iran Barrage Count | `barrages_to_israel_daily.csv` | Iran-origin barrage count from the INSS dashboard |
| Pikud HaOref Ballistic Alerts | `ballistic_alerts_daily.csv` | Daily count of advance-warning alerts for ballistic missiles |

## Git LFS

Large files are tracked with Git LFS:

- `data/israel_alerts_raw.csv` (~59 MB raw alert history)
- `data/analysis/cloud_gallery.html`
- `data/satellite/**/*.jpeg` (NASA GIBS images)

Run `git lfs install` before cloning if you need these files.

## Dependencies

| Package | Purpose |
|---------|---------|
| playwright | Headless browser for Power BI scraping |
| plotly | Interactive HTML chart generation |
| pillow | Satellite tile stitching and cropping |
| numpy | Array operations for cloud cover computation |
| scipy | Spearman/Pearson correlation and permutation tests |
| pytest | Unit tests |

## Tests

```bash
uv run pytest
```
