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
            df = pl.scan_csv(path, infer_schema_length=0)
            frames.append(df)
        current_date += timedelta(days=1)

    if not frames:
        return pl.LazyFrame()

    return pl.concat(frames)


def load_emissions() -> pl.LazyFrame:
    """Load emissions CSV with all fields."""
    return pl.scan_csv(EMISSIONS_FILE, infer_schema_length=0)


def join_flights_with_emissions(
    schedule: pl.LazyFrame,
    emissions: pl.LazyFrame,
    max_emissions: float = None,
    max_flight_time: int = None,
) -> pl.DataFrame:
    """
    Join schedule and emissions data on CARRIER and FLTNO.

    Args:
        schedule: Schedule lazy frame
        emissions: Emissions lazy frame
        max_emissions: Maximum CO2 emissions in tonnes to include (optional)
        max_flight_time: Maximum flight time in minutes (optional)

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
            # Emissions data (very important) - cast to float
            pl.col("ESTIMATED_FUEL_BURN_TOTAL_TONNES").cast(pl.Float64),
            pl.col("ESTIMATED_CO2_TOTAL_TONNES").cast(pl.Float64),
        ])
        .drop_nulls()
    )

    # Filter by max emissions if specified
    if max_emissions is not None:
        result = result.filter(pl.col("ESTIMATED_CO2_TOTAL_TONNES") <= max_emissions)

    # Filter by max flight time if specified
    if max_flight_time is not None:
        # Calculate flight time as arrival time - departure time (in minutes)
        result = result.with_columns(
            flight_time_mins=(
                (pl.col("ARRTIM").cast(pl.Int32) - pl.col("DEPTIM").cast(pl.Int32)).fill_null(0)
            )
        )
        # Handle case where arrival is next day
        result = result.with_columns(
            flight_time_mins=pl.when(pl.col("flight_time_mins") < 0)
                .then(pl.col("flight_time_mins") + 24 * 60)
                .otherwise(pl.col("flight_time_mins"))
        )
        result = result.filter(pl.col("flight_time_mins") <= max_flight_time)
        result = result.drop("flight_time_mins")

    result = result.sort("ESTIMATED_CO2_TOTAL_TONNES").collect()

    return result


def stream_joined_flights(
    schedule: pl.LazyFrame,
    emissions: pl.LazyFrame,
    max_emissions: float = None,
    max_flight_time: int = None,
):
    """
    Stream joined flight and emissions data row by row using streaming engine.

    Args:
        schedule: Schedule lazy frame
        emissions: Emissions lazy frame
        max_emissions: Maximum CO2 emissions in tonnes to include (optional)
        max_flight_time: Maximum flight time in minutes (optional)

    Yields:
        Dictionary for each row of joined data
    """
    joined_lazy = (
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
            # Emissions data (very important) - cast to float
            pl.col("ESTIMATED_FUEL_BURN_TOTAL_TONNES").cast(pl.Float64),
            pl.col("ESTIMATED_CO2_TOTAL_TONNES").cast(pl.Float64),
        ])
        .drop_nulls()
    )

    # Filter by max emissions if specified
    if max_emissions is not None:
        joined_lazy = joined_lazy.filter(pl.col("ESTIMATED_CO2_TOTAL_TONNES") <= max_emissions)

    # Filter by max flight time if specified
    if max_flight_time is not None:
        # Calculate flight time as arrival time - departure time (in minutes)
        joined_lazy = joined_lazy.with_columns(
            flight_time_mins=(
                (pl.col("ARRTIM").cast(pl.Int32) - pl.col("DEPTIM").cast(pl.Int32)).fill_null(0)
            )
        )
        # Handle case where arrival is next day
        joined_lazy = joined_lazy.with_columns(
            flight_time_mins=pl.when(pl.col("flight_time_mins") < 0)
                .then(pl.col("flight_time_mins") + 24 * 60)
                .otherwise(pl.col("flight_time_mins"))
        )
        joined_lazy = joined_lazy.filter(pl.col("flight_time_mins") <= max_flight_time)
        joined_lazy = joined_lazy.drop("flight_time_mins")

    # Stream the results using streaming engine - processes incrementally without materializing entire dataset
    for row in joined_lazy.collect(streaming=True).iter_rows(named=True):
        yield row


def get_connecting_flights(
    schedule: pl.LazyFrame,
    emissions: pl.LazyFrame,
    origin: str,
    destination: str,
    min_connection_mins: int = 40,
    max_connection_mins: int = 300,
    limit: int = 10,
    max_emissions: float = 300,
    max_journey_time: int = None,
):
    """
    Find connecting flights from origin to destination via a connecting airport, with emissions data.

    Args:
        schedule: Schedule lazy frame
        emissions: Emissions lazy frame
        origin: Origin airport code (e.g., 'JFK')
        destination: Destination airport code (e.g., 'LAX')
        min_connection_mins: Minimum connection time in minutes (default 40)
        max_connection_mins: Maximum connection time in minutes (default 300 = 5 hours)
        limit: Maximum number of connections to return (default 10)
        max_emissions: Maximum total CO2 emissions in tonnes (default 300)
        max_journey_time: Maximum total journey time in minutes (optional)

    Returns:
        List of connections with flights and emissions data
    """
    # Join schedule with emissions for both legs
    schedule_with_emissions = (
        schedule.join(
            emissions,
            left_on=["CARRIER", "FLTNO"],
            right_on=["CARRIER_CODE", "FLIGHT_NUMBER"],
            how="inner"
        )
        .select([
            "CARRIER", "FLTNO", "FLIGHT_DATE", "DEPAPT", "ARRAPT",
            "DEPTIM", "ARRTIM", "DISTANCE",
            pl.col("ESTIMATED_FUEL_BURN_TOTAL_TONNES").cast(pl.Float64),
            pl.col("ESTIMATED_CO2_TOTAL_TONNES").cast(pl.Float64),
        ])
        .with_columns(pl.col("DEPTIM").cast(pl.Int32), pl.col("ARRTIM").cast(pl.Int32))
    )

    # Get first flights (origin -> anywhere)
    first_flights_lazy = (
        schedule_with_emissions
        .filter(pl.col("DEPAPT") == origin.upper())
    )

    # Get second flights (anywhere -> destination)
    second_flights_lazy = (
        schedule_with_emissions
        .filter(pl.col("ARRAPT") == destination.upper())
    )

    # Collect only what we need
    first_flights = first_flights_lazy.collect()
    second_flights = second_flights_lazy.collect()

    connections = []

    for first in first_flights.to_dicts():
        if len(connections) >= limit:
            break

        # Look for second flights that:
        # 1. Depart from the same airport where first flight arrives
        # 2. Depart within connection window (40 mins to 5 hours after first arrival)
        min_depart = int(first["ARRTIM"]) + min_connection_mins
        max_depart = int(first["ARRTIM"]) + max_connection_mins

        matching_second = second_flights.filter(
            (pl.col("DEPAPT") == first["ARRAPT"]) &
            (pl.col("DEPTIM") >= min_depart) &
            (pl.col("DEPTIM") <= max_depart)
        )

        for second in matching_second.to_dicts():
            # Calculate total emissions and journey time
            total_emissions = (
                first.get("ESTIMATED_CO2_TOTAL_TONNES", 0) +
                second.get("ESTIMATED_CO2_TOTAL_TONNES", 0)
            )

            # Filter out connections with too much emissions
            if total_emissions > max_emissions:
                continue

            # Total time from departure of first flight to arrival of second flight (in minutes)
            journey_time_mins = int(second["ARRTIM"]) - int(first["DEPTIM"])
            # Handle case where arrival time is next day
            if journey_time_mins < 0:
                journey_time_mins += 24 * 60

            # Filter out connections with too much journey time
            if max_journey_time is not None and journey_time_mins > max_journey_time:
                continue

            connection = {
                "first_flight": first,
                "second_flight": second,
                "total_co2_emissions": total_emissions,
                "journey_time_minutes": journey_time_mins
            }
            connections.append(connection)
            if len(connections) >= limit:
                break

    return connections


def stream_connecting_flights(
    schedule: pl.LazyFrame,
    emissions: pl.LazyFrame,
    origin: str,
    destination: str,
    min_connection_mins: int = 40,
    max_connection_mins: int = 300,
    limit: int = 10,
    max_emissions: float = 300,
    max_journey_time: int = None,
):
    """
    Stream connecting flights from origin to destination via a connecting airport, with emissions data.

    Args:
        schedule: Schedule lazy frame
        emissions: Emissions lazy frame
        origin: Origin airport code (e.g., 'JFK')
        destination: Destination airport code (e.g., 'LAX')
        min_connection_mins: Minimum connection time in minutes (default 40)
        max_connection_mins: Maximum connection time in minutes (default 300 = 5 hours)
        limit: Maximum number of connections to yield (default 10)
        max_emissions: Maximum total CO2 emissions in tonnes (default 300)
        max_journey_time: Maximum total journey time in minutes (optional)

    Yields:
        Dictionary with first_flight and second_flight for each connection
    """
    # Join schedule with emissions for both legs
    schedule_with_emissions = (
        schedule.join(
            emissions,
            left_on=["CARRIER", "FLTNO"],
            right_on=["CARRIER_CODE", "FLIGHT_NUMBER"],
            how="inner"
        )
        .select([
            "CARRIER", "FLTNO", "FLIGHT_DATE", "DEPAPT", "ARRAPT",
            "DEPTIM", "ARRTIM", "DISTANCE",
            pl.col("ESTIMATED_FUEL_BURN_TOTAL_TONNES").cast(pl.Float64),
            pl.col("ESTIMATED_CO2_TOTAL_TONNES").cast(pl.Float64),
        ])
        .with_columns(pl.col("DEPTIM").cast(pl.Int32), pl.col("ARRTIM").cast(pl.Int32))
    )

    # Get first flights (origin -> anywhere) - collect just this one
    first_flights_lazy = (
        schedule_with_emissions
        .filter(pl.col("DEPAPT") == origin.upper())
    )

    # Get second flights (anywhere -> destination) - keep as lazy
    second_flights_lazy = (
        schedule_with_emissions
        .filter(pl.col("ARRAPT") == destination.upper())
    )

    # Only collect first flights (usually smaller set)
    first_flights = first_flights_lazy.collect()

    count = 0
    for first in first_flights.to_dicts():
        if count >= limit:
            break

        # Look for second flights that:
        # 1. Depart from the same airport where first flight arrives
        # 2. Depart within connection window (40 mins to 5 hours after first arrival)
        min_depart = int(first["ARRTIM"]) + min_connection_mins
        max_depart = int(first["ARRTIM"]) + max_connection_mins

        # Execute lazy query for each first flight - streaming style
        matching_second = (
            second_flights_lazy
            .filter(pl.col("DEPAPT") == first["ARRAPT"])
            .filter((pl.col("DEPTIM") >= min_depart) & (pl.col("DEPTIM") <= max_depart))
            .collect()
        )

        for second in matching_second.to_dicts():
            # Calculate total emissions and journey time
            total_emissions = (
                first.get("ESTIMATED_CO2_TOTAL_TONNES", 0) +
                second.get("ESTIMATED_CO2_TOTAL_TONNES", 0)
            )

            # Filter out connections with too much emissions
            if total_emissions > max_emissions:
                continue

            # Total time from departure of first flight to arrival of second flight (in minutes)
            journey_time_mins = int(second["ARRTIM"]) - int(first["DEPTIM"])
            # Handle case where arrival time is next day
            if journey_time_mins < 0:
                journey_time_mins += 24 * 60

            # Filter out connections with too much journey time
            if max_journey_time is not None and journey_time_mins > max_journey_time:
                continue

            connection = {
                "first_flight": first,
                "second_flight": second,
                "total_co2_emissions": total_emissions,
                "journey_time_minutes": journey_time_mins
            }
            yield connection
            count += 1
            if count >= limit:
                break


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
