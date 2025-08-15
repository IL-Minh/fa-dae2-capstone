from __future__ import annotations

import argparse
import logging
import random
from datetime import date, datetime
from pathlib import Path

import polars as pl
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def generate_monthly_transactions(
    month: int, year: int, num_rows: int, out_dir: Path, user_ids: list
) -> Path:
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
                "user_id": rng.choice(
                    user_ids
                ),  # Use existing user IDs from registration
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


def generate_monthly_users(
    month: int, year: int, num_users: int, out_dir: Path
) -> tuple[Path, list]:
    fake = Faker()
    rng = random.Random(year * 100 + month)

    # User segments and demographics
    age_ranges = [(18, 25), (26, 35), (36, 45), (46, 55), (56, 65), (66, 75)]

    preferred_categories = [
        "groceries",
        "entertainment",
        "travel",
        "technology",
        "fashion",
        "health",
        "education",
    ]

    rows = []
    user_ids = []  # Collect user IDs for transaction generation
    for _ in range(num_users):
        # Generate realistic user data
        age_range = rng.choice(age_ranges)
        age = rng.randint(age_range[0], age_range[1])

        # Income correlates with age and tier
        if age < 30:
            income = rng.choice(["low", "medium"])
        elif age < 50:
            income = rng.choice(["medium", "high"])
        else:
            income = rng.choice(["high", "very_high"])

        # Tier correlates with income
        if income == "low":
            tier = "bronze"
        elif income == "medium":
            tier = rng.choice(["bronze", "silver"])
        elif income == "high":
            tier = rng.choice(["silver", "gold"])
        else:
            tier = rng.choice(["gold", "platinum"])

        # Risk profile correlates with age and income
        if age < 30 and income in ["low", "medium"]:
            risk = "aggressive"
        elif age > 50 and income == "very_high":
            risk = "conservative"
        else:
            risk = rng.choice(["moderate", "conservative"])

        # Registration date within the month
        reg_date = datetime(
            year,
            month,
            rng.randint(1, 28),
            rng.randint(9, 17),  # Business hours
            rng.randint(0, 59),
            rng.randint(0, 59),
        )

        # Generate user preferences based on demographics
        num_preferences = rng.randint(2, 4)
        user_preferences = rng.sample(preferred_categories, num_preferences)

        user_id = fake.uuid4()
        user_ids.append(user_id)  # Store user ID for transaction generation

        rows.append(
            {
                "user_id": user_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "age": age,
                "income_bracket": income,
                "customer_tier": tier,
                "risk_profile": risk,
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": "USA",
                "registration_date": reg_date.isoformat(),
                "preferred_categories": ",".join(user_preferences),
                "is_active": rng.random() < 0.85,  # 85% active
                "source_system": "third_party_registration",
            }
        )

    df = pl.from_dicts(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"user_registrations_{year:04d}_{month:02d}.csv"
    df.write_csv(out_path)
    return out_path, user_ids


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate monthly synthetic CSV data for transactions and user registrations"
    )
    parser.add_argument("--year", type=int, default=date.today().year)
    parser.add_argument("--month", type=int, default=date.today().month)
    parser.add_argument("--transactions", type=int, default=500)
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--out-dir", type=Path, default=Path("data/incoming"))
    args = parser.parse_args()

    # Generate users first, then use their IDs for transactions
    user_out, user_ids = generate_monthly_users(
        args.month, args.year, args.users, args.out_dir
    )
    tx_out = generate_monthly_transactions(
        args.month, args.year, args.transactions, args.out_dir, user_ids
    )

    logger.info(f"Generated transaction data: {tx_out}")
    logger.info(f"Generated user registration data: {user_out}")


if __name__ == "__main__":
    main()
