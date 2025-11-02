import polars as pl
import os
from datetime import datetime, timedelta
from rich import print
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SCHEDULE_DIR = os.getenv("SCHEDULE_DIR", "data/schedule")
EMISSIONS_FILE = os.getenv("EMISSIONS_FILE", "data/emissions.csv")

# Check that required files/directories exist
if not os.path.exists(SCHEDULE_DIR):
    print(f"[red]Error: SCHEDULE_DIR not found at {SCHEDULE_DIR}[/red]")
if not os.path.exists(EMISSIONS_FILE):
    print(f"[red]Error: EMISSIONS_FILE not found at {EMISSIONS_FILE}[/red]")

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
    """Load the schedule from the schedule directory, selecting only required columns and filling nulls with mean."""
    columns = list(SCHEDULE_COLUMNS.values())
    frames = []

    current_date = start_date
    files_found = 0
    while current_date <= end_date:
        # Include all days (removed weekend skip)
        path = os.path.join(SCHEDULE_DIR, f"{current_date.year}/{current_date.month:02d}/{current_date.day:02d}.csv")
        if os.path.exists(path):
            # Select required columns
            df = pl.scan_csv(path, infer_schema_length=0).select(columns)
            frames.append(df)
            files_found += 1

        # Move to next day
        current_date += timedelta(days=1)

    print(f"[blue]Loaded {files_found} schedule files[/blue]")
    if not frames:
        print(f"[red]Warning: No schedule files found between {start_date} and {end_date}[/red]")
        return pl.LazyFrame()

    # Concatenate all frames
    combined = pl.concat(frames)

    # Fill numeric nulls with mean of that column
    # Convert string numeric columns to float first
    numeric_cols = [
        ("DEPTIM", pl.Int64),
        ("ARRTIM", pl.Int64),
        ("ARRDAY", pl.Int64),
        ("DISTANCE", pl.Float64),
    ]

    # Fill nulls with mean for numeric columns
    for col, dtype in numeric_cols:
        if col in columns:
            try:
                combined = combined.with_columns(
                    pl.col(col).cast(dtype)
                    .fill_null(pl.col(col).cast(dtype).mean())
                    .alias(col)
                )
            except:
                # If conversion fails, just keep original
                pass

    # For string columns, drop rows that have nulls in key fields
    combined = combined.filter(
        pl.col("CARRIER_CD_ICAO").is_not_null()
        & pl.col("FLTNO").is_not_null()
        & pl.col("DEPAPT").is_not_null()
        & pl.col("ARRAPT").is_not_null()
    )

    return combined


def _load_emissions() -> pl.LazyFrame:
    """Load the emissions from the emissions file, selecting required columns and filling nulls with mean."""
    columns = list(EMISSIONS_COLUMNS.values())

    # Check if emissions file exists
    if not os.path.exists(EMISSIONS_FILE):
        print(f"[red]Error: Emissions file not found at {EMISSIONS_FILE}[/red]")
        return pl.LazyFrame()

    # Load CSV with proper schema inference
    print(f"[blue]Loading emissions data from {EMISSIONS_FILE}[/blue]")
    df = pl.scan_csv(EMISSIONS_FILE, infer_schema_length=10000)

    # Select required columns
    try:
        df = df.select(columns)
    except Exception as e:
        print(f"Warning: Could not select columns {columns}: {e}")
        return pl.LazyFrame()

    # Drop rows with null in key identifier columns first
    df = df.filter(
        pl.col("CARRIER_CODE").is_not_null()
        & pl.col("FLIGHT_NUMBER").is_not_null()
    )

    # Fill numeric nulls with mean for emissions data
    numeric_cols = [
        "ESTIMATED_FUEL_BURN_TOTAL_TONNES",
        "ESTIMATED_CO2_TOTAL_TONNES",
    ]

    for col in numeric_cols:
        if col in columns:
            try:
                # Cast to float, calculate mean, and fill nulls
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.Float64)
                    .fill_null(pl.col(col).cast(pl.Float64).mean())
                    .alias(col)
                )
            except Exception as e:
                # If it fails, just keep the original
                print(f"Warning: Could not fill nulls for {col}: {e}")
                pass

    return df


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

    # Cast FLTNO to int64 to match FLIGHT_NUMBER type
    schedule_df = schedule_df.with_columns(
        pl.col("FLTNO").cast(pl.Int64)
    )

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

def find_connecting_flights(
    start_airport: str,
    end_airport: str,
    start_date: datetime,
    end_date: datetime,
    min_wait_mins: int = 40,
    max_wait_mins: int = 240,
) -> pl.DataFrame:
    """
    Find connecting flights between two airports with a connection point.

    Args:
        start_airport: Starting airport code (DEPAPT)
        end_airport: Final destination airport code (ARRAPT)
        start_date: Start date for flights
        end_date: End date for flights
        min_wait_mins: Minimum wait time in minutes (default 40)
        max_wait_mins: Maximum wait time in minutes (default 240 = 4 hours)

    Returns:
        DataFrame with connecting flight pairs
    """
    # Get all flights from start to any intermediate airport
    all_flights = join_schedule_and_emissions(start_date, end_date)

    # Convert to polars for easier manipulation
    df = pl.DataFrame(all_flights.to_dicts())

    # Find outbound flights (from start airport)
    outbound = df.filter(pl.col("DEPAPT") == start_airport.upper())

    # Find return flights (to end airport)
    inbound = df.filter(pl.col("ARRAPT") == end_airport.upper())

    if len(outbound) == 0 or len(inbound) == 0:
        return pl.DataFrame()

    # Find possible connections
    connections = []

    for out_row in outbound.to_dicts():
        # Arrival airport of outbound flight is the connection point
        connection_airport = out_row.get("ARRAPT")
        arrival_time_str = out_row.get("ARRTIM")  # e.g., "1015"

        # Parse arrival time
        if not arrival_time_str:
            continue

        try:
            arr_hours = int(str(arrival_time_str)[:2])
            arr_mins = int(str(arrival_time_str)[2:4])
            arrival_mins = arr_hours * 60 + arr_mins
        except (ValueError, IndexError):
            continue

        # Find inbound flights from this connection airport
        possible_connections = inbound.filter(
            pl.col("DEPAPT") == connection_airport.upper()
        )

        for in_row in possible_connections.to_dicts():
            departure_time_str = in_row.get("DEPTIM")  # e.g., "1115"

            if not departure_time_str:
                continue

            try:
                dep_hours = int(str(departure_time_str)[:2])
                dep_mins = int(str(departure_time_str)[2:4])
                departure_mins = dep_hours * 60 + dep_mins
            except (ValueError, IndexError):
                continue

            # Calculate wait time
            wait_mins = departure_mins - arrival_mins

            # Handle day boundary (if arrival is late and departure is early next day)
            if wait_mins < 0:
                wait_mins += 24 * 60

            # Check if wait time is within acceptable range
            if min_wait_mins <= wait_mins <= max_wait_mins:
                connections.append(
                    {
                        "outbound_flight": out_row.get("FLTNO"),
                        "outbound_carrier": out_row.get("CARRIER_CD_ICAO"),
                        "outbound_departure": out_row.get("DEPTIM"),
                        "outbound_arrival": out_row.get("ARRTIM"),
                        "outbound_departure_airport": out_row.get("DEPAPT"),
                        "outbound_arrival_airport": out_row.get("ARRAPT"),
                        "outbound_co2": out_row.get("ESTIMATED_CO2_TOTAL_TONNES"),
                        "outbound_fuel": out_row.get("ESTIMATED_FUEL_BURN_TOTAL_TONNES"),
                        "outbound_date": out_row.get("FLIGHT_DATE"),
                        "connection_airport": connection_airport,
                        "wait_time_mins": wait_mins,
                        "inbound_flight": in_row.get("FLTNO"),
                        "inbound_carrier": in_row.get("CARRIER_CD_ICAO"),
                        "inbound_departure": in_row.get("DEPTIM"),
                        "inbound_arrival": in_row.get("ARRTIM"),
                        "inbound_departure_airport": in_row.get("DEPAPT"),
                        "inbound_arrival_airport": in_row.get("ARRAPT"),
                        "inbound_co2": in_row.get("ESTIMATED_CO2_TOTAL_TONNES"),
                        "inbound_fuel": in_row.get("ESTIMATED_FUEL_BURN_TOTAL_TONNES"),
                        "inbound_date": in_row.get("FLIGHT_DATE"),
                        "total_co2": float(out_row.get("ESTIMATED_CO2_TOTAL_TONNES", 0))
                        + float(in_row.get("ESTIMATED_CO2_TOTAL_TONNES", 0)),
                        "total_fuel": float(out_row.get("ESTIMATED_FUEL_BURN_TOTAL_TONNES", 0))
                        + float(in_row.get("ESTIMATED_FUEL_BURN_TOTAL_TONNES", 0)),
                    }
                )

    return pl.DataFrame(connections) if connections else pl.DataFrame()


def main():
    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 5, 31)
    joined = join_schedule_and_emissions(start_date, end_date)
    print(joined)

if __name__ == "__main__":
    main()