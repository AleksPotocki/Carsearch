"""Run all scrapers: Honda dealers + Otomoto."""

from scraper.honda_dealers import main as run_dealers
from scraper.otomoto_crv_phev import main as run_otomoto


def main() -> None:
    print("\n" + "=" * 60)
    print("  HONDA DEALER SCRAPER")
    print("=" * 60 + "\n")
    run_dealers()

    print("\n" + "=" * 60)
    print("  OTOMOTO SCRAPER")
    print("=" * 60 + "\n")
    run_otomoto()


if __name__ == "__main__":
    main()
