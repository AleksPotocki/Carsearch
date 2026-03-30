# Honda CR-V Scraper (Poland)

Scrapes Honda CR-V listings from official Honda dealer pages across Poland.
Runs daily at 9:00 via GitHub Actions, commits new results automatically.

## What it does

- Reads dealer URLs from `CSV adresy dealerów - Arkusz1.csv`
- Scrapes each dealer's "od ręki" (available now) and "używane" (used) pages
- Filters for CR-V listings, marks which are PHEV
- Stores results in SQLite (`output/dealers.db`) and CSV (`output/dealers_latest.csv`)
- Tracks `first_seen_utc` so you know when an offer first appeared

## Output columns

| Column | Description |
|--------|-------------|
| dealer_name | Dealer name |
| city | Dealer city |
| title | Car listing title |
| price_pln | Price in PLN |
| year | Production year |
| engine_type | Engine type from dealer page |
| trim | Trim level |
| is_phev | 1 = Plug-in Hybrid, 0 = regular Hybrid |
| url | Direct link to listing |
| source_page | "od_reki" or "uzywane" |
| date_scraped_utc | Last scrape timestamp |
| first_seen_utc | When first discovered |

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run manually

```bash
python -m scraper.honda_dealers
```

## Hosting

Runs automatically via GitHub Actions (`.github/workflows/scrape.yml`).
Cron: daily at 7:00 UTC (~9:00 Warsaw time).
Can also be triggered manually from the Actions tab.

## Legacy: Otomoto scraper

```bash
python -m scraper.otomoto_crv_phev
```
Scrapes Honda CR-V PHEV listings from Otomoto (requires Playwright).
