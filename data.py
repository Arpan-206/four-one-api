import polars as pl
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SCHEDULE_DIR = os.getenv("SCHEDULE_DIR", "data/schedule")
EMISSIONS_FILE = os.getenv("EMISSIONS_FILE", "data/emissions.csv")


def load_schedule(start_date: datetime, end_date: datetime) -> pl.LazyFrame:
    """Load schedule CSVs from the schedule directory between start_date and end_date."""
    frames = []
    current_date = start_date

    while current_date <= end_date:
        path = os.path.join(SCHEDULE_DIR, f"{current_date.year}/{current_date.month:02d}/{current_date.day:02d}.csv")
        if os.path.exists(path):
            df = pl.scan_csv(path, infer_schema_length=10000)
            frames.append(df)
        current_date += timedelta(days=1)

    if not frames:
        return pl.LazyFrame()

    return pl.concat(frames)


def load_emissions() -> pl.LazyFrame:
    """Load emissions CSV with all fields."""
    return pl.scan_csv(EMISSIONS_FILE, infer_schema_length=10000)


def join_flights_with_emissions(
    schedule: pl.LazyFrame,
    emissions: pl.LazyFrame,
) -> pl.DataFrame:
    """
    Join schedule and emissions data on CARRIER and FLTNO.

    Args:
        schedule: Schedule lazy frame
        emissions: Emissions lazy frame

    Returns:
        DataFrame with joined data sorted by CO2 emissions, keeping key fields and important data
    """
    result = (
        schedule.join(
            emissions,
            left_on=["CARRIER", "FLTNO"],
            right_on=["CARRIER_CODE", "FLIGHT_NUMBER"],
            how="inner"
        )
        .select([
            # Join keys and identifying fields
            pl.col("CARRIER"),
            pl.col("FLTNO"),
            pl.col("FLIGHT_DATE"),
            # Flight information
            pl.col("DEPAPT"),
            pl.col("ARRAPT"),
            pl.col("DEPTIM"),
            pl.col("ARRTIM"),
            pl.col("DISTANCE"),
            # Emissions data (very important)
            pl.col("ESTIMATED_FUEL_BURN_TOTAL_TONNES"),
            pl.col("ESTIMATED_CO2_TOTAL_TONNES"),
        ])
        .drop_nulls()
        .sort("ESTIMATED_CO2_TOTAL_TONNES")
    ).collect()

    return result


if __name__ == "__main__":
    from rich import print

    print("[bold blue]Loading schedule data...[/bold blue]")
    schedule = load_schedule(datetime(2025, 5, 1), datetime(2025, 5, 1))
    print(f"Schedule shape: {schedule.collect().shape}")
    print(f"Schedule columns: {schedule.collect_schema().names()}")

    print("\n[bold blue]Loading emissions data...[/bold blue]")
    emissions = load_emissions()
    print(f"Emissions shape: {emissions.collect().shape}")
    print(f"Emissions columns: {emissions.collect_schema().names()}")

    print("\n[bold blue]Joining schedule and emissions...[/bold blue]")
    joined = join_flights_with_emissions(schedule, emissions)
    print(f"Joined shape: {joined.shape}")
    print(f"Joined columns: {joined.columns}")
    print("\n[bold]First 5 joined rows:[/bold]")
    print(joined.head(5))
