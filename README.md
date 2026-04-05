## INSS Daily Scraper

This project scrapes the public INSS Iran campaign dashboard and stores:

- daily `Barrage against Israel` data
- snapshot `Key Target Strikes in Middle East` data

All outputs use English column names.

### What it saves

`data/barrages_to_israel_daily.csv` rows include:

- `snapshot_date`
- `event_date`
- `iran_barrage_count`
- `lebanon_barrage_count`
- `total_barrage_count`
- `chart_title`
- `metric_key`
- `source_page_url`
- `dashboard_url`
- `scraped_at_local`
- `scraped_at_utc`

`data/key_target_strikes_middle_east.csv` rows include:

- `snapshot_date`
- `target_key`
- `target_label`
- `strike_count`
- `chart_title`
- `metric_key`
- `source_page_url`
- `dashboard_url`
- `scraped_at_local`
- `scraped_at_utc`

### Run

```bash
uv run python main.py
```

The scraper writes or updates:

```text
data/barrages_to_israel_daily.csv
data/barrages_to_israel_latest_snapshot.html
data/key_target_strikes_middle_east.csv
data/key_target_strikes_middle_east_latest_snapshot.html
```

Rows are keyed by:

- `snapshot_date + event_date` for barrage history
- `snapshot_date + target_key` for key target history

Rerunning on the same day replaces that day's snapshot while keeping previous
daily snapshots intact.

The barrage HTML graph uses Plotly and visualizes the latest available snapshot
with:

- stacked bars for `Iran` and `Lebanon`
- a line for `Total`

The key target HTML graph uses Plotly and visualizes the latest available
snapshot as a category bar chart.

### Notes

- The source page is [INSS Dashboard: The Military Campaign Against Iran](https://www.inss.org.il/publication/lions-roar-data/).
- The live dashboard is a public [Power BI report](https://app.powerbi.com/view?r=eyJrIjoiYmI3MzIzMTAtMTdjNC00NTY1LWFiM2YtNjI0NTk5MWEyYTI5IiwidCI6IjgwOGNmNGIzLTFhOTYtNDEzZi1iMDZiLTlkZTZjOThmNTQ2OSJ9).
- The scraper uses Playwright with an already installed local browser.
