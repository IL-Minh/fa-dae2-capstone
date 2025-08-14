from __future__ import annotations

import argparse
import random
from datetime import date, datetime
from pathlib import Path

import polars as pl
from faker import Faker


def generate_month(month: int, year: int, num_rows: int, out_dir: Path) -> Path:
    fake = Faker()
    rng = random.Random(year * 100 + month)

    categories = [
        "groceries",
        "rent",
        "salary",
        "entertainment",
        "utilities",
        "transport",
        "health",
    ]

    rows = []
    for _ in range(num_rows):
        amt = round(rng.uniform(-200.0, 2000.0), 2)
        ts = datetime(
            year,
            month,
            rng.randint(1, 28),
            rng.randint(0, 23),
            rng.randint(0, 59),
            rng.randint(0, 59),
        )
        rows.append(
            {
                "tx_id": fake.uuid4(),
                "user_id": rng.randint(1, 5),
                "amount": amt,
                "currency": "USD",
                "merchant": fake.company(),
                "category": rng.choice(categories),
                "timestamp": ts.isoformat(),
            }
        )

    df = pl.from_dicts(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"transactions_{year:04d}_{month:02d}.csv"
    df.write_csv(out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate monthly synthetic CSV data (no personal data)"
    )
    parser.add_argument("--year", type=int, default=date.today().year)
    parser.add_argument("--month", type=int, default=date.today().month)
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--out-dir", type=Path, default=Path("data/incoming"))
    args = parser.parse_args()

    out = generate_month(args.month, args.year, args.rows, args.out_dir)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
