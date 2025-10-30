import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


EXCLUDED_TRANSFER_SOURCES = {"SAVINGS_GOAL", "INTERNAL_TRANSFER"}


def calculate_spend_by_category(
    *,
    db_path,
    days: int,
    reference_time=None,
):
    if days <= 0:
        raise ValueError("days must be positive")

    reference = reference_time or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    else:
        reference = reference.astimezone(timezone.utc)

    window_start = reference - timedelta(days=days)
    window_end = reference + timedelta(seconds=1)

    database = Path(db_path)
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                SUBSTR(fi.transaction_time, 1, 7) AS spend_month,
                COALESCE(spend.name, space.name, 'Uncategorised') AS category_name,
                fi.currency AS currency,
                SUM(-fi.amount_minor_units) AS total_minor
            FROM feed_items fi
            LEFT JOIN categories space
              ON space.account_uid = fi.account_uid
             AND space.category_type = 'space'
             AND space.category_uid = fi.space_uid
            LEFT JOIN categories spend
              ON spend.account_uid = fi.account_uid
             AND spend.category_type = 'spending'
             AND spend.category_uid = fi.spending_category
            WHERE fi.amount_minor_units < 0
              AND fi.transaction_time >= ?
              AND fi.transaction_time < ?
              AND UPPER(COALESCE(fi.source, '')) NOT IN (?, ?)
            GROUP BY spend_month, category_name, fi.currency
            ORDER BY spend_month ASC, category_name ASC
            """,
            (
                window_start.isoformat(),
                window_end.isoformat(),
                *sorted(EXCLUDED_TRANSFER_SOURCES),
            ),
        ).fetchall()

    months = sorted({row["spend_month"] for row in rows})
    dates = [f"{month}-01" for month in months]
    categories = sorted({row["category_name"] for row in rows})

    values = {
        category: {
            "minor": [0 for _ in dates],
            "currency": None,
        }
        for category in categories
    }

    for row in rows:
        category = row["category_name"]
        day = f"{row['spend_month']}-01"
        idx = dates.index(day)
        values[category]["minor"][idx] = int(row["total_minor"])
        values[category]["currency"] = row["currency"] or values[category]["currency"]

    series = []
    for category in categories:
        minor_values = values[category]["minor"]
        major_values = [round(amount / 100, 2) for amount in minor_values]
        non_zero_months = [value for value in minor_values if value > 0]
        month_count = len(minor_values) if minor_values else 0
        avg_minor = int(sum(minor_values) / month_count) if month_count else 0
        avg_major = round(avg_minor / 100, 2)
        series.append(
            {
                "category": category,
                "values": major_values,
                "minorValues": minor_values,
                "totalMinorUnits": sum(minor_values),
                "currency": values[category]["currency"] or "GBP",
                "averageMinorUnits": avg_minor,
                "average": avg_major,
            }
        )

    return {
        "dates": dates,
        "series": series,
        "reference": reference.isoformat(),
        "days": days,
        "months": len(dates),
    }
