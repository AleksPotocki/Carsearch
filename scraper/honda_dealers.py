"""
Scraper for Honda CR-V PHEV listings from official Honda dealer pages in Poland.

Dealer pages use the standard Honda CMS template with div.c-teaser cards.
Results are stored in SQLite and exported to CSV.
"""

import csv
import os
import re
import sqlite3
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DEALERS = os.path.join(BASE_DIR, "CSV adresy dealerów - Arkusz1.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DB_PATH = os.path.join(OUTPUT_DIR, "dealers.db")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9",
}

# We look for CR-V in the listing title/model name.
# "CR-V" covers both e:HEV and e:PHEV variants; we further filter for PHEV
# by checking engine type specs when available.
CRV_PATTERN = re.compile(r"CR-?V", re.IGNORECASE)
PHEV_KEYWORDS = {"phev", "plug-in", "plug in", "plugin"}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DealerListing:
    dealer_name: str
    city: str
    title: str
    price_pln: Optional[int]
    year: Optional[int]
    engine_type: str
    trim: str
    is_phev: bool
    url: str
    source_page: str  # "od_reki" or "uzywane"
    date_scraped_utc: str


# ---------------------------------------------------------------------------
# CSV parsing – dealer list
# ---------------------------------------------------------------------------

@dataclass
class Dealer:
    name: str
    city: str
    url_od_reki: str
    url_uzywane: str


def load_dealers(csv_path: str = CSV_DEALERS) -> list[Dealer]:
    """Parse the dealer CSV."""
    dealers: list[Dealer] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            name, city, url_od_reki, url_uzywane = row[0], row[1], row[2], row[3]
            # Skip header row
            if "nazwa" in name.lower() and "dealer" in name.lower():
                continue
            dealers.append(Dealer(name, city, url_od_reki, url_uzywane))
    return dealers


# ---------------------------------------------------------------------------
# HTML scraping – standard Honda CMS template
# ---------------------------------------------------------------------------

def fetch_page(url: str, timeout: int = 30) -> Optional[BeautifulSoup]:
    """Fetch a page and return parsed soup, or None on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"  [WARN] Failed to fetch {url}: {e}")
        return None


def parse_price(text: str) -> Optional[int]:
    """Extract integer price from strings like '189 900 zł'."""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_year(text: str) -> Optional[int]:
    """Extract production year from text."""
    m = re.search(r"\b(20[0-3]\d)\b", text or "")
    if m:
        return int(m.group(1))
    return None


def is_crv(title: str, card_text: str) -> bool:
    """Check if a listing is a CR-V (any variant)."""
    return bool(CRV_PATTERN.search(title) or CRV_PATTERN.search(card_text))


def detect_phev(title: str, engine_type: str, card_text: str) -> bool:
    """Check if a CR-V listing is specifically the PHEV variant."""
    combined = f"{title} {engine_type} {card_text}".lower()
    return any(kw in combined for kw in PHEV_KEYWORDS)


def extract_spec(card: BeautifulSoup, spec_name: str) -> str:
    """Extract a specification value from a card's feature list."""
    for item in card.select("li.c-features-list__item"):
        label_el = item.select_one("p.c-features-list__name")
        value_el = item.select_one("p.c-features-list__desc strong")
        if label_el and value_el:
            if spec_name.lower() in label_el.get_text(strip=True).lower():
                return value_el.get_text(strip=True)
    return ""


def scrape_honda_cms_page(
    url: str, dealer: Dealer, source_page: str
) -> list[DealerListing]:
    """Scrape a single Honda CMS page (may have pagination)."""
    listings: list[DealerListing] = []
    page_url: Optional[str] = url
    seen_urls: set[str] = set()

    while page_url:
        soup = fetch_page(page_url)
        if not soup:
            break

        cards = soup.select("div.c-teaser")
        if not cards:
            # Some dealers might use different container
            cards = soup.select(".c-teasers-grid .c-teaser")

        for card in cards:
            title_el = card.select_one("h3.c-teaser__title")
            title = title_el.get_text(strip=True) if title_el else ""

            price_el = card.select_one("p.c-teaser__price")
            price_text = price_el.get_text(strip=True) if price_el else ""

            link_el = card.select_one("div.c-teaser__cta a.c-btn")
            href = link_el.get("href", "") if link_el else ""
            listing_url = urljoin(page_url, href) if href else ""

            engine_type = extract_spec(card, "silnik")
            trim = extract_spec(card, "wersja")
            year_text = extract_spec(card, "rok")
            card_text = card.get_text(" ", strip=True)

            if not is_crv(title, card_text):
                continue

            if listing_url in seen_urls:
                continue
            seen_urls.add(listing_url)

            listings.append(
                DealerListing(
                    dealer_name=dealer.name,
                    city=dealer.city,
                    title=title,
                    price_pln=parse_price(price_text),
                    year=parse_year(year_text) or parse_year(card_text),
                    engine_type=engine_type,
                    trim=trim,
                    is_phev=detect_phev(title, engine_type, card_text),
                    url=listing_url,
                    source_page=source_page,
                    date_scraped_utc=datetime.now(timezone.utc)
                    .replace(microsecond=0)
                    .isoformat(),
                )
            )

        # Pagination: look for next page link
        next_link = soup.select_one("a.c-pagination__btn--next")
        if next_link and next_link.get("href"):
            next_url = urljoin(page_url, next_link["href"])
            if next_url == page_url:
                break
            page_url = next_url
            time.sleep(random.uniform(1.0, 2.0))
        else:
            break

    return listings


# ---------------------------------------------------------------------------
# Scrape Otomoto dealer inventory pages (e.g. cmcmotors.otomoto.pl/inventory)
# ---------------------------------------------------------------------------

def scrape_otomoto_inventory(
    url: str, dealer: Dealer, source_page: str
) -> list[DealerListing]:
    """Scrape an Otomoto dealer inventory page."""
    listings: list[DealerListing] = []
    soup = fetch_page(url)
    if not soup:
        return listings

    # Cards use ooa- prefixed classes; find all links to /oferta/ within cards
    for card in soup.select("a[href*='/oferta/']"):
        parent = card.find_parent("div")
        if not parent:
            continue
        card_text = parent.get_text(" ", strip=True)
        if not CRV_PATTERN.search(card_text):
            continue

        listing_url = urljoin(url, card.get("href", ""))
        title = ""
        for el in parent.select("h2, h3, [class*='1v6v2we']"):
            title = el.get_text(strip=True)
            break
        if not title:
            title = card.get_text(strip=True)

        price_text = ""
        for el in parent.select("[class*='price'], [class*='1kbkia7']"):
            price_text = el.get_text(strip=True)
            break
        if not price_text:
            m = re.search(r"([\d\s]{3,12})\s*PLN", card_text)
            if m:
                price_text = m.group(0)

        listings.append(_make_listing(
            dealer, title, price_text, card_text, listing_url, source_page,
        ))

    return listings


# ---------------------------------------------------------------------------
# Scrape modeleodreki.honda.pl (national Honda "od ręki" catalog)
# ---------------------------------------------------------------------------

def scrape_honda_odreki_national(
    url: str, dealer: Dealer, source_page: str
) -> list[DealerListing]:
    """Scrape the national Honda 'od ręki' catalog page."""
    listings: list[DealerListing] = []
    page_url: str | None = url
    seen_urls: set[str] = set()

    while page_url:
        soup = fetch_page(page_url)
        if not soup:
            break

        # This page uses the same Honda CMS c-teaser cards
        cards = soup.select("div.c-teaser")
        for card in cards:
            title_el = card.select_one("h3.c-teaser__title")
            title = title_el.get_text(strip=True) if title_el else ""

            card_text = card.get_text(" ", strip=True)
            if not is_crv(title, card_text):
                continue

            price_el = card.select_one("p.c-teaser__price")
            price_text = price_el.get_text(strip=True) if price_el else ""

            link_el = card.select_one("div.c-teaser__cta a.c-btn")
            href = link_el.get("href", "") if link_el else ""
            listing_url = urljoin(page_url, href) if href else ""

            if listing_url in seen_urls:
                continue
            seen_urls.add(listing_url)

            engine_type = extract_spec(card, "silnik")
            trim = extract_spec(card, "wersja")
            year_text = extract_spec(card, "rok")

            listings.append(DealerListing(
                dealer_name=dealer.name,
                city=dealer.city,
                title=title,
                price_pln=parse_price(price_text),
                year=parse_year(year_text) or parse_year(card_text),
                engine_type=engine_type,
                trim=trim,
                is_phev=detect_phev(title, engine_type, card_text),
                url=listing_url,
                source_page=source_page,
                date_scraped_utc=datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
            ))

        # Pagination
        next_link = soup.select_one("a.c-pagination__btn--next")
        if next_link and next_link.get("href"):
            next_url = urljoin(page_url, next_link["href"])
            if next_url == page_url:
                break
            page_url = next_url
            time.sleep(random.uniform(1.0, 2.0))
        else:
            break

    return listings


# ---------------------------------------------------------------------------
# Scrape Karlik used cars platform
# ---------------------------------------------------------------------------

def scrape_karlik_used(
    url: str, dealer: Dealer, source_page: str
) -> list[DealerListing]:
    """Scrape Karlik's used car platform."""
    listings: list[DealerListing] = []
    soup = fetch_page(url)
    if not soup:
        return listings

    # Look for car cards with links to /pl/samochod/
    for link in soup.select("a[href*='/samochod/']"):
        card_text = ""
        parent = link.find_parent("div") or link.find_parent("article")
        if parent:
            card_text = parent.get_text(" ", strip=True)
        else:
            card_text = link.get_text(" ", strip=True)

        if not CRV_PATTERN.search(card_text):
            continue

        listing_url = urljoin(url, link.get("href", ""))
        title = link.get_text(strip=True)

        price_text = ""
        if parent:
            price_el = parent.select_one("h4, [class*='price']")
            if price_el:
                price_text = price_el.get_text(strip=True)

        listings.append(_make_listing(
            dealer, title, price_text, card_text, listing_url, source_page,
        ))

    return listings


# ---------------------------------------------------------------------------
# Scrape Odyssey / WordPress dealer pages
# ---------------------------------------------------------------------------

def scrape_wordpress_dealer(
    url: str, dealer: Dealer, source_page: str
) -> list[DealerListing]:
    """Scrape WordPress-based dealer pages."""
    listings: list[DealerListing] = []
    soup = fetch_page(url)
    if not soup:
        return listings

    for card in soup.select("article, .car-item, .vehicle-card, .product-card, .entry, .listing-item"):
        card_text = card.get_text(" ", strip=True)
        if not CRV_PATTERN.search(card_text):
            continue

        title = ""
        title_el = card.select_one("h2, h3, h4, .title, .car-title")
        if title_el:
            title = title_el.get_text(strip=True)

        link_el = card.select_one("a[href]")
        listing_url = urljoin(url, link_el.get("href", "")) if link_el else url

        price_text = ""
        price_el = card.select_one(".price, .car-price, [class*='price']")
        if price_el:
            price_text = price_el.get_text(strip=True)

        listings.append(_make_listing(
            dealer, title or "CR-V", price_text, card_text, listing_url, source_page,
        ))

    return listings


# ---------------------------------------------------------------------------
# Helper to build a DealerListing
# ---------------------------------------------------------------------------

def _make_listing(
    dealer: Dealer, title: str, price_text: str, card_text: str,
    listing_url: str, source_page: str,
) -> DealerListing:
    return DealerListing(
        dealer_name=dealer.name,
        city=dealer.city,
        title=title,
        price_pln=parse_price(price_text),
        year=parse_year(card_text),
        engine_type="",
        trim="",
        is_phev=detect_phev(title, "", card_text),
        url=listing_url,
        source_page=source_page,
        date_scraped_utc=datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    )


# ---------------------------------------------------------------------------
# URL-based scraper routing
# ---------------------------------------------------------------------------

def _pick_scraper(url: str):
    """Return the appropriate scraper function based on the URL pattern."""
    if "otomoto.pl/inventory" in url:
        return scrape_otomoto_inventory
    if "modeleodreki.honda.pl" in url:
        return scrape_honda_odreki_national
    if "uzywane.karlik" in url:
        return scrape_karlik_used
    if "odyssey-dealer-group.pl" in url:
        return scrape_wordpress_dealer
    # Default: standard Honda CMS
    return scrape_honda_cms_page


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_listings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          dealer_name TEXT NOT NULL,
          city TEXT NOT NULL,
          title TEXT NOT NULL,
          price_pln INTEGER,
          year INTEGER,
          engine_type TEXT,
          trim TEXT,
          is_phev INTEGER NOT NULL DEFAULT 0,
          url TEXT NOT NULL,
          source_page TEXT NOT NULL,
          date_scraped_utc TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL,
          UNIQUE(url)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dl_city ON dealer_listings(city)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dl_date ON dealer_listings(date_scraped_utc)"
    )
    conn.commit()


def upsert_listing(conn: sqlite3.Connection, l: DealerListing) -> bool:
    """Insert or update a listing. Returns True if this is a new listing."""
    # Check if it already exists
    existing = conn.execute(
        "SELECT id FROM dealer_listings WHERE url = ?",
        (l.url,),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE dealer_listings SET
              title=?, price_pln=?, year=?, engine_type=?, trim=?,
              source_page=?, date_scraped_utc=?, city=?
            WHERE url=?
            """,
            (
                l.title, l.price_pln, l.year, l.engine_type, l.trim,
                l.source_page, l.date_scraped_utc, l.city,
                l.url,
            ),
        )
        return False
    else:
        conn.execute(
            """
            INSERT INTO dealer_listings
              (dealer_name, city, title, price_pln, year, engine_type, trim,
               is_phev, url, source_page, date_scraped_utc, first_seen_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                l.dealer_name, l.city, l.title, l.price_pln, l.year,
                l.engine_type, l.trim, int(l.is_phev), l.url, l.source_page,
                l.date_scraped_utc, l.date_scraped_utc,
            ),
        )
        return True


def export_csv(conn: sqlite3.Connection, out_path: str) -> int:
    rows = conn.execute(
        """
        SELECT dealer_name, city, title, price_pln, year, engine_type, trim,
               is_phev, url, source_page, date_scraped_utc, first_seen_utc
        FROM dealer_listings
        ORDER BY first_seen_utc DESC
        """
    ).fetchall()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "dealer_name", "city", "title", "price_pln", "year",
            "engine_type", "trim", "is_phev", "url", "source_page",
            "date_scraped_utc", "first_seen_utc",
        ])
        w.writerows(rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def scrape_all_dealers() -> tuple[list[DealerListing], int]:
    """Scrape all dealers. Returns (all_listings, new_count)."""
    dealers = load_dealers()
    print(f"Loaded {len(dealers)} dealers from CSV")

    all_listings: list[DealerListing] = []

    for dealer in dealers:
        print(f"\n--- {dealer.name} ({dealer.city}) ---")

        for label, url, source in [
            ("od ręki", dealer.url_od_reki, "od_reki"),
            ("używane", dealer.url_uzywane, "uzywane"),
        ]:
            if not url:
                continue
            print(f"  Scraping {label}: {url}")
            scraper_fn = _pick_scraper(url)
            found = scraper_fn(url, dealer, source)
            print(f"  Found {len(found)} CR-V listing(s)")
            all_listings.extend(found)

        # Polite delay between dealers
        time.sleep(random.uniform(0.5, 1.5))

    # Save to DB
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    new_count = 0
    try:
        init_db(conn)
        for l in all_listings:
            if upsert_listing(conn, l):
                new_count += 1
        conn.commit()

        # Export CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(OUTPUT_DIR, f"dealers_{timestamp}.csv")
        total = export_csv(conn, csv_path)
        export_csv(conn, os.path.join(OUTPUT_DIR, "dealers_latest.csv"))
        print(f"\nExported {total} total listings to {csv_path}")
    finally:
        conn.close()

    return all_listings, new_count


def main() -> None:
    all_listings, new_count = scrape_all_dealers()

    print(f"\n{'='*60}")
    print(f"Total CR-V listings found this run: {len(all_listings)}")
    print(f"New listings (not seen before): {new_count}")

    if all_listings:
        print(f"\nListings by city:")
        cities: dict[str, list[DealerListing]] = {}
        for l in all_listings:
            cities.setdefault(l.city, []).append(l)
        for city, items in sorted(cities.items()):
            prices = [i.price_pln for i in items if i.price_pln]
            price_info = f", prices: {min(prices):,}-{max(prices):,} PLN" if prices else ""
            print(f"  {city}: {len(items)} listing(s){price_info}")
    else:
        print("\nNo CR-V listings found across any dealer.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
