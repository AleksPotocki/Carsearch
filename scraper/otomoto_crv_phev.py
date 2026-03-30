import csv
import os
import random
import re
import sqlite3
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


SEARCH_URL = "https://www.otomoto.pl/osobowe/honda/cr-v/hybrydowy-plug-in"
OUTPUT_DIR = os.path.join("output")
DB_PATH = os.path.join(OUTPUT_DIR, "listings.db")


@dataclass(frozen=True)
class Listing:
    title: str
    price_pln: Optional[int]
    year: Optional[int]
    mileage_km: Optional[int]
    location: str
    seller_type: str  # "dealer" | "private" | "unknown"
    url: str
    date_scraped_utc: str  # ISO8601


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def random_delay_seconds() -> float:
    return random.uniform(2.0, 3.0)


def parse_int(value: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", value or "")
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_price_pln(text: str) -> Optional[int]:
    # Examples: "189 900 PLN", "189 900 zł", "Zapytaj o cenę"
    if not text:
        return None
    lowered = text.lower()
    if "zapytaj" in lowered or "do negocjacji" in lowered:
        return None
    return parse_int(text)


def parse_mileage_km(text: str) -> Optional[int]:
    # Examples: "12 345 km"
    return parse_int(text)


def parse_year(text: str) -> Optional[int]:
    year = parse_int(text)
    if year and 1950 <= year <= datetime.now().year + 1:
        return year
    return None


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listings (
          url TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          price_pln INTEGER,
          year INTEGER,
          mileage_km INTEGER,
          location TEXT,
          seller_type TEXT,
          date_scraped_utc TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price_pln)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_year ON listings(year)")
    conn.commit()


def upsert_listing(conn: sqlite3.Connection, l: Listing) -> None:
    conn.execute(
        """
        INSERT INTO listings(url, title, price_pln, year, mileage_km, location, seller_type, date_scraped_utc)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
          title=excluded.title,
          price_pln=excluded.price_pln,
          year=excluded.year,
          mileage_km=excluded.mileage_km,
          location=excluded.location,
          seller_type=excluded.seller_type,
          date_scraped_utc=excluded.date_scraped_utc
        """
        ,
        (
            l.url,
            l.title,
            l.price_pln,
            l.year,
            l.mileage_km,
            l.location,
            l.seller_type,
            l.date_scraped_utc,
        ),
    )


def export_csv(conn: sqlite3.Connection, out_path: str) -> int:
    rows = conn.execute(
        """
        SELECT title, price_pln, year, mileage_km, location, seller_type, url, date_scraped_utc
        FROM listings
        ORDER BY date_scraped_utc DESC
        """
    ).fetchall()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            ["title", "price_pln", "year", "mileage_km", "location", "seller_type", "url", "date_scraped_utc"]
        )
        w.writerows(rows)
    return len(rows)


def accept_cookies_if_present(page) -> None:
    # Otomoto often shows a consent modal; try several common Polish labels.
    candidates = [
        "button:has-text('Akceptuję')",
        "button:has-text('Akceptuj')",
        "button:has-text('Zaakceptuj')",
        "button:has-text('Zgadzam się')",
        "button:has-text('Przejdź dalej')",
        "button:has-text('OK')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.is_visible():
                loc.click(timeout=1500)
                break
        except Exception:
            continue


def guess_seller_type(card_text: str) -> str:
    t = (card_text or "").lower()
    # Heuristics based on common labels seen on Otomoto cards.
    if "prywatn" in t:
        return "private"
    if "firma" in t or "dealer" in t or "salon" in t:
        return "dealer"
    return "unknown"


def absolute_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.otomoto.pl" + href
    return "https://www.otomoto.pl/" + href.lstrip("/")


def extract_listing_from_card(card) -> Optional[Listing]:
    # Try to be resilient to layout changes by using multiple fallbacks.
    title = ""
    url = ""
    price_text = ""
    location_text = ""
    year_text = ""
    mileage_text = ""

    try:
        a = card.locator("a[href*='/oferta/']").first
        if a.count() == 0:
            a = card.locator("a[href*='otomoto.pl']").first
        if a.count() > 0:
            href = a.get_attribute("href") or ""
            url = absolute_url(href)
            title = (a.get_attribute("title") or a.inner_text() or "").strip()
    except Exception:
        pass

    if not title:
        try:
            title = (card.locator("h2, h3").first.inner_text() or "").strip()
        except Exception:
            title = ""

    # Price
    try:
        # Often price is in element with currency; use broad text match.
        price_text = (card.locator("text=/\\b(PLN|zł)\\b/").first.inner_text() or "").strip()
    except Exception:
        price_text = ""

    # Year / mileage / location are typically in chips / params list
    # Pull full text and parse from it as a fallback.
    card_text = ""
    try:
        card_text = card.inner_text(timeout=1000) or ""
    except Exception:
        card_text = ""

    # Year: first 4-digit that looks plausible
    m_year = re.search(r"\b(19[5-9]\d|20[0-3]\d)\b", card_text)
    if m_year:
        year_text = m_year.group(0)

    # Mileage: something like "123 456 km"
    m_mileage = re.search(r"\b([\d\s\u00A0]{1,9})\s*km\b", card_text)
    if m_mileage:
        mileage_text = m_mileage.group(0)

    # Location: try explicit selector first, else last line-ish heuristic
    try:
        location_text = (card.locator("p:has-text(',')").first.inner_text() or "").strip()
    except Exception:
        location_text = ""
    if not location_text:
        # Many cards contain city/region line; pick a short line with comma.
        for line in [ln.strip() for ln in card_text.splitlines() if ln.strip()]:
            if "," in line and 3 <= len(line) <= 60:
                location_text = line
                break

    seller_type = guess_seller_type(card_text)
    price_pln = parse_price_pln(price_text)
    year = parse_year(year_text)
    mileage_km = parse_mileage_km(mileage_text)

    if not url:
        return None

    return Listing(
        title=title or "",
        price_pln=price_pln,
        year=year,
        mileage_km=mileage_km,
        location=location_text or "",
        seller_type=seller_type,
        url=url,
        date_scraped_utc=utc_now_iso(),
    )


def locate_listing_cards(page):
    # Try multiple patterns; Otomoto frequently uses article cards.
    candidates = [
        "article:has(a[href*='/oferta/'])",
        "[data-testid*='listing']",
        "li:has(a[href*='/oferta/'])",
    ]
    for sel in candidates:
        loc = page.locator(sel)
        try:
            if loc.count() >= 5:
                return loc
        except Exception:
            continue
    # Fallback (could be fewer on last page)
    return page.locator("a[href*='/oferta/']").locator("xpath=ancestor::*[self::article or self::li][1]")


def get_next_page_url(page) -> Optional[str]:
    # Prefer rel=next if present
    try:
        rel_next = page.locator("a[rel='next']").first
        if rel_next.count() > 0:
            href = rel_next.get_attribute("href")
            if href:
                return absolute_url(href)
    except Exception:
        pass

    # Otherwise try a "next" pagination control
    candidates = [
        "a[aria-label*='Następna']",
        "a:has-text('Następna')",
        "a:has-text('Next')",
        "button:has-text('Następna')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                href = loc.get_attribute("href")
                if href:
                    return absolute_url(href)
        except Exception:
            continue
    return None


def scrape_all_pages() -> list[Listing]:
    listings: list[Listing] = []
    visited_urls: set[str] = set()

    with sync_playwright() as p:
        # Chromium can occasionally crash in some constrained environments.
        # Prefer Chromium, but fall back to Firefox to keep the scraper runnable.
        try:
            browser = p.chromium.launch(headless=True)
        except Exception:
            browser = p.firefox.launch(headless=True)
        context = browser.new_context(
            locale="pl-PL",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        next_url: Optional[str] = SEARCH_URL
        page_no = 0

        while next_url:
            page_no += 1
            page.goto(next_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(random_delay_seconds())
            accept_cookies_if_present(page)

            # Wait a bit for cards
            try:
                page.wait_for_selector("a[href*='/oferta/']", timeout=15000)
            except PlaywrightTimeoutError:
                # Might be blocked by anti-bot or layout changed; capture nothing but still attempt pagination.
                pass

            cards = locate_listing_cards(page)
            count = 0
            try:
                n = cards.count()
            except Exception:
                n = 0

            for i in range(n):
                try:
                    card = cards.nth(i)
                    l = extract_listing_from_card(card)
                    if not l:
                        continue
                    if l.url in visited_urls:
                        continue
                    visited_urls.add(l.url)
                    listings.append(l)
                    count += 1
                except Exception:
                    continue

            # Determine next page
            next_candidate = get_next_page_url(page)
            if not next_candidate or next_candidate == next_url:
                break
            next_url = next_candidate

        context.close()
        browser.close()

    return listings


def summary_prices(listings: Iterable[Listing]) -> tuple[int, Optional[int], Optional[int], Optional[float]]:
    prices = [l.price_pln for l in listings if isinstance(l.price_pln, int)]
    if not prices:
        return 0, None, None, None
    return len(prices), min(prices), max(prices), statistics.mean(prices)


def main() -> None:
    ensure_output_dir()

    scraped = scrape_all_pages()

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        for l in scraped:
            upsert_listing(conn, l)
        conn.commit()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(OUTPUT_DIR, f"listings_{timestamp}.csv")
        export_csv(conn, csv_path)
        export_csv(conn, os.path.join(OUTPUT_DIR, "listings_latest.csv"))
    finally:
        conn.close()

    priced_count, pmin, pmax, pavg = summary_prices(scraped)
    total = len(scraped)

    print(f"Scraped listings: {total}")
    if priced_count == 0:
        print("Price summary: no numeric prices found.")
    else:
        print(
            "Price summary (PLN) over numeric prices: "
            f"count={priced_count}, min={pmin}, max={pmax}, avg={pavg:.2f}"
        )
    print(f"Saved SQLite: {DB_PATH}")
    print(f"Saved CSV: {csv_path}")


if __name__ == "__main__":
    main()

