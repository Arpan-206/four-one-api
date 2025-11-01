import polars as pl
import os
from datetime import datetime, timedelta
from rich import print

SCHEDULE_DIR = "data/schedule"
EMISSIONS_FILE = "data/emissions.csv"

# Columns to extract from schedule CSVs
# Mapping: Requested Name -> CSV Column Name
SCHEDULE_COLUMNS = {
    "Carrier Code": "CARRIER_CD_ICAO",
    "Service Number": "FLTNO",
    "Flight Data": "FLIGHT_DATE",
    "Departure Time": "DEPTIM",
    "Departure Port Code": "DEP_PORT_CD_ICAO",
    "Departure Airport": "DEPAPT",
    "Arrival Day": "ARRDAY",
    "Arrival Time": "ARRTIM",
    "Arrival Port Code": "ARR_PORT_CD_ICAO",
    "Arrival Airport": "ARRAPT",
    "Departure City Code": "DEPCITY",
    "Departure Country Code": "DEPCTRY",
    "Arrival City Code": "ARRCITY",
    "Arrival Country Code": "ARRCTRY",
    "Distance": "DISTANCE",
}

# Columns to extract from emissions CSV
EMISSIONS_COLUMNS = {
    "Carrier Code": "CARRIER_CODE",
    "Flight Number": "FLIGHT_NUMBER",
    "Estimated Fuel Burn Total Tonnes": "ESTIMATED_FUEL_BURN_TOTAL_TONNES",
    "Estimated CO2 Total Tonnes": "ESTIMATED_CO2_TOTAL_TONNES",
}

# Let's lazily load the schedule and emissions files
# Schedule is a directory of subdirectories, each being SCHEDULE_DIR/YYYY/MM/DD.csv

# We need to use scan_csv to lazily load the schedule files
def _load_schedule(start_date: datetime, end_date: datetime) -> pl.LazyFrame:
    """Load the schedule from the schedule directory, selecting only required columns and filtering out rows with NaN values."""
    columns = list(SCHEDULE_COLUMNS.values())
    frames = []

    current_date = start_date
    while current_date <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() not in [5, 6]:
            path = os.path.join(SCHEDULE_DIR, f"{current_date.year}/{current_date.month:02d}/{current_date.day:02d}.csv")
            if os.path.exists(path):
                # Select required columns and drop rows with any NaN values in those columns
                frames.append(
                    pl.scan_csv(path, infer_schema_length=0)
                    .select(columns)
                    .drop_nulls()
                )

        # Move to next day
        current_date += timedelta(days=1)

    return pl.concat(frames) if frames else pl.LazyFrame()


def _load_emissions() -> pl.LazyFrame:
    """Load the emissions from the emissions file, selecting required columns and filtering out rows with missing CO2 data."""
    columns = list(EMISSIONS_COLUMNS.values())
    df = pl.scan_csv(EMISSIONS_FILE, infer_schema_length=0)
    # Select required columns and filter out rows with null CO2 emissions
    return df.select(columns).drop_nulls()


def join_schedule_and_emissions(
    start_date: datetime,
    end_date: datetime,
    start_airport: str = None,
    end_airport: str = None,
) -> pl.DataFrame:
    """
    Load and join schedule and emissions data on carrier and flight number.

    Args:
        start_date: Start date for schedule data
        end_date: End date for schedule data
        start_airport: Optional filter by departure airport code (DEPAPT)
        end_airport: Optional filter by arrival airport code (ARRAPT)

    Returns:
        DataFrame: Joined data with schedule and emissions information
    """
    # Load schedule and emissions data as lazy frames
    schedule_df = _load_schedule(start_date, end_date)
    emissions_df = _load_emissions()

    # Join on carrier code and flight number
    joined = schedule_df.join(
        emissions_df,
        left_on=["CARRIER_CD_ICAO", "FLTNO"],
        right_on=["CARRIER_CODE", "FLIGHT_NUMBER"],
        how="inner"
    )

    # Apply airport filters if provided (filter by DEPAPT and ARRAPT)
    if start_airport:
        joined = joined.filter(pl.col("DEPAPT") == start_airport.upper())
    if end_airport:
        joined = joined.filter(pl.col("ARRAPT") == end_airport.upper())

    return joined.collect()

def main():
    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 5, 31)
    joined = join_schedule_and_emissions(start_date, end_date)
    print(joined)

if __name__ == "__main__":
    main()