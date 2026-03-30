"""
Scraper for Honda CR-V PHEV listings on Otomoto.pl.

Uses Playwright (headless Chromium) because Otomoto renders listings client-side.
Results are stored in the shared SQLite database alongside dealer scraper results.
"""

import csv
import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DB_PATH = os.path.join(OUTPUT_DIR, "otomoto.db")

SEARCH_URL = (
    "https://www.otomoto.pl/osobowe/honda/cr-v/od-2023"
    "?search%5Bfilter_enum_damaged%5D=0"
    "&search%5Bfilter_enum_fuel_type%5D=plugin-hybrid"
)


@dataclass(frozen=True)
class Listing:
    title: str
    price_pln: Optional[int]
    year: Optional[int]
    mileage_km: Optional[int]
    location: str
    url: str
    date_scraped_utc: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_int(value: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", value or "")
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_price(text: str) -> Optional[int]:
    if not text:
        return None
    lowered = text.lower()
    if "zapytaj" in lowered or "do negocjacji" in lowered:
        return None
    return parse_int(text)


def parse_year(text: str) -> Optional[int]:
    year = parse_int(text)
    if year and 2023 <= year <= datetime.now().year + 1:
        return year
    return None


# ---------------------------------------------------------------------------
# Cookie consent
# ---------------------------------------------------------------------------

def accept_cookies(page) -> None:
    candidates = [
        "#onetrust-accept-btn-handler",
        "button:has-text('Akceptuję')",
        "button:has-text('Akceptuj')",
        "button:has-text('Zgadzam się')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=2000):
                loc.click(timeout=3000)
                time.sleep(1)
                return
        except Exception:
            continue


# ---------------------------------------------------------------------------
# Card extraction
# ---------------------------------------------------------------------------

def absolute_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.otomoto.pl" + href
    return "https://www.otomoto.pl/" + href


def extract_listing(card) -> Optional[Listing]:
    url = ""
    title = ""

    # Link and title
    try:
        a = card.locator("a[href*='/oferta/']").first
        if a.count() > 0:
            url = absolute_url(a.get_attribute("href") or "")
    except Exception:
        pass

    try:
        title = card.locator("h2").first.inner_text().strip()
    except Exception:
        title = ""

    if not url:
        return None

    # Parse everything from full card text
    card_text = ""
    try:
        card_text = card.inner_text(timeout=2000) or ""
    except Exception:
        pass

    # Price: look for "NNN NNN\nPLN" or "NNN NNN PLN" pattern
    price_pln = None
    m_price = re.search(r"([\d\s\u00A0]{3,12})\s*\n?\s*PLN", card_text)
    if m_price:
        price_pln = parse_int(m_price.group(1))

    # Year: 4-digit year on its own line (2023+)
    year = None
    m_year = re.search(r"\b(202[3-9]|20[3-9]\d)\b", card_text)
    if m_year:
        year = int(m_year.group(1))

    # Mileage: "23 000 km"
    mileage_km = None
    m_mileage = re.search(r"([\d\s\u00A0]{1,9})\s*km\b", card_text)
    if m_mileage:
        mileage_km = parse_int(m_mileage.group(0))

    # Location: "City (Voivodeship)" pattern
    location = ""
    m_loc = re.search(r"([A-ZĄĆĘŁŃÓŚŹŻ][a-ząćęłńóśźż\s-]+)\s*\([A-ZĄĆĘŁŃÓŚŹŻ][a-ząćęłńóśźż]+\)", card_text)
    if m_loc:
        location = m_loc.group(0).strip()

    return Listing(
        title=title or "Honda CR-V PHEV",
        price_pln=price_pln,
        year=year,
        mileage_km=mileage_km,
        location=location,
        url=url,
        date_scraped_utc=utc_now_iso(),
    )


def locate_cards(page):
    # Otomoto wraps each listing in an <article> tag
    loc = page.locator("article:has(a[href*='/oferta/'])")
    try:
        if loc.count() >= 1:
            return loc
    except Exception:
        pass
    # Fallback
    return page.locator("a[href*='/oferta/']").locator("xpath=ancestor::article[1]")


def get_next_page_url(page) -> Optional[str]:
    try:
        rel_next = page.locator("a[rel='next']").first
        if rel_next.count() > 0:
            href = rel_next.get_attribute("href")
            if href:
                return absolute_url(href)
    except Exception:
        pass

    for sel in [
        "a[aria-label*='Następna']",
        "a:has-text('Następna')",
        "li[title='Next Page'] a",
    ]:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                href = loc.get_attribute("href")
                if href:
                    return absolute_url(href)
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape_otomoto() -> list[Listing]:
    listings: list[Listing] = []
    seen_urls: set[str] = set()

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            browser = p.firefox.launch(headless=True)

        context = browser.new_context(
            locale="pl-PL",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()
        page.add_init_script(
            'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
        )

        next_url: Optional[str] = SEARCH_URL
        page_no = 0

        while next_url:
            page_no += 1
            print(f"  Otomoto page {page_no}: {next_url}")
            page.goto(next_url, wait_until="networkidle", timeout=60000)
            time.sleep(random.uniform(3.0, 5.0))

            if page_no == 1:
                accept_cookies(page)
                time.sleep(2)

            try:
                page.wait_for_selector("article a[href*='/oferta/']", timeout=15000)
            except PlaywrightTimeoutError:
                print("  [WARN] No listing cards found, page may be blocked")
                break

            cards = locate_cards(page)
            try:
                n = cards.count()
            except Exception:
                n = 0

            count = 0
            for i in range(n):
                try:
                    card = cards.nth(i)
                    # Skip tiny articles (e.g. "Zobacz ogłoszenia" buttons)
                    text_len = len(card.inner_text(timeout=1000) or "")
                    if text_len < 50:
                        continue
                    listing = extract_listing(card)
                    if not listing or listing.url in seen_urls:
                        continue
                    seen_urls.add(listing.url)
                    listings.append(listing)
                    count += 1
                except Exception:
                    continue

            print(f"  Found {count} listing(s) on page {page_no}")

            next_candidate = get_next_page_url(page)
            if not next_candidate or next_candidate == next_url:
                break
            next_url = next_candidate

        context.close()
        browser.close()

    return listings


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS otomoto_listings (
          url TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          price_pln INTEGER,
          year INTEGER,
          mileage_km INTEGER,
          location TEXT,
          date_scraped_utc TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_oto_price ON otomoto_listings(price_pln)"
    )
    conn.commit()


def upsert_listing(conn: sqlite3.Connection, l: Listing) -> bool:
    """Returns True if this is a new listing."""
    existing = conn.execute(
        "SELECT url FROM otomoto_listings WHERE url = ?", (l.url,)
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE otomoto_listings SET
              title=?, price_pln=?, year=?, mileage_km=?,
              location=?, date_scraped_utc=?
            WHERE url=?
            """,
            (l.title, l.price_pln, l.year, l.mileage_km,
             l.location, l.date_scraped_utc, l.url),
        )
        return False
    else:
        conn.execute(
            """
            INSERT INTO otomoto_listings
              (url, title, price_pln, year, mileage_km, location,
               date_scraped_utc, first_seen_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (l.url, l.title, l.price_pln, l.year, l.mileage_km,
             l.location, l.date_scraped_utc, l.date_scraped_utc),
        )
        return True


def export_csv(conn: sqlite3.Connection, out_path: str) -> int:
    rows = conn.execute(
        """
        SELECT title, price_pln, year, mileage_km, location,
               url, date_scraped_utc, first_seen_utc
        FROM otomoto_listings
        ORDER BY first_seen_utc DESC
        """
    ).fetchall()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "title", "price_pln", "year", "mileage_km", "location",
            "url", "date_scraped_utc", "first_seen_utc",
        ])
        w.writerows(rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=== Otomoto CR-V PHEV Scraper ===")
    print(f"URL: {SEARCH_URL}\n")

    scraped = scrape_otomoto()

    conn = sqlite3.connect(DB_PATH)
    new_count = 0
    try:
        init_db(conn)
        for l in scraped:
            if upsert_listing(conn, l):
                new_count += 1
        conn.commit()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(OUTPUT_DIR, f"otomoto_{timestamp}.csv")
        total = export_csv(conn, csv_path)
        export_csv(conn, os.path.join(OUTPUT_DIR, "otomoto_latest.csv"))
    finally:
        conn.close()

    print(f"\n{'='*60}")
    print(f"Total Otomoto listings found: {len(scraped)}")
    print(f"New listings (not seen before): {new_count}")
    if scraped:
        prices = [l.price_pln for l in scraped if l.price_pln]
        if prices:
            print(f"Price range: {min(prices):,} - {max(prices):,} PLN")
    print(f"DB: {DB_PATH}")
    print(f"CSV: {os.path.join(OUTPUT_DIR, 'otomoto_latest.csv')}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
