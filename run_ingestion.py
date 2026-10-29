"""Run all three ingestion sources into the one landing zone."""

from sources import kamis, knbs_pdf, operational


def main():
    print("=" * 55)
    print("1/3  KAMIS market prices")
    print("=" * 55)
    kamis.ingest()

    print("=" * 55)
    print("2/3  KNBS CPI / inflation")
    print("=" * 55)
    knbs_pdf.ingest()

    print("=" * 55)
    print("3/3  Operational data")
    print("=" * 55)
    operational.ingest()

    print("\nDone. All sources are in data/landing/  :")


if __name__ == "__main__":
    main()
