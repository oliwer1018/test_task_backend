from app.core.config import settings
from app.core.db import apply_schema
from app.etl.load import import_raw, load_feeds_csv
from app.etl.distribute import run_distribution


def main():
    print("Initializing database schema...")
    apply_schema()

    print(f"Importing raw data from {settings.clicks_csv} and {settings.feeds_csv} ...")
    clicks, feeds = import_raw(settings.clicks_csv, settings.feeds_csv)

    print("Running distribution logic...")
    run_distribution(load_feeds_csv(settings.feeds_csv))

    print("ETL complete. Distributed stats are now available in the database.")


if __name__ == "__main__":
    main()
