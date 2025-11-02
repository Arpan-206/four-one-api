from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from datetime import datetime
import json
from data import load_schedule, load_emissions, join_flights_with_emissions, stream_joined_flights, get_connecting_flights, stream_connecting_flights


app = FastAPI(title="forty-one", version="0.1.0")


@app.get("/")
def read_root():
    """Root endpoint that returns a welcome message."""
    return {"message": "Welcome to forty-one API!"}


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/flights")
def get_flights(
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
    depart_airport: str = None,
    arrival_airport: str = None,
    max_emissions: float = None,
    max_flight_time: int = None,
):
    """
    Get joined flight and emissions data.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        depart_airport: Optional filter by departure airport (DEPAPT)
        arrival_airport: Optional filter by arrival airport (ARRAPT)
        max_emissions: Optional maximum CO2 emissions in tonnes
        max_flight_time: Optional maximum flight time in minutes

    Returns:
        JSON array with flight and emissions data
    """
    try:
        # Parse dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Load schedule and emissions
        schedule = load_schedule(start, end)
        emissions = load_emissions()

        # Convert max_emissions to float if provided
        max_emissions_val = float(max_emissions) if max_emissions is not None else None

        # Join the data
        df = join_flights_with_emissions(
            schedule, emissions,
            max_emissions=max_emissions_val,
            max_flight_time=max_flight_time
        )

        # Apply optional airport filters
        if depart_airport:
            df = df.filter(df["DEPAPT"] == depart_airport.upper())
        if arrival_airport:
            df = df.filter(df["ARRAPT"] == arrival_airport.upper())

        # Convert to list of dicts and return as JSON
        return {"flights": df.to_dicts(), "count": len(df)}
    except ValueError as e:
        return {"error": f"Invalid date format: {str(e)}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/flights/stream")
def stream_flights(
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
    format: str = "ndjson",
    depart_airport: str = None,
    arrival_airport: str = None,
    max_emissions: float = None,
    max_flight_time: int = None,
):
    """
    Stream joined flight and emissions data.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        format: Output format - 'ndjson' (default) or 'json'
        depart_airport: Optional filter by departure airport (DEPAPT)
        arrival_airport: Optional filter by arrival airport (ARRAPT)
        max_emissions: Optional maximum CO2 emissions in tonnes
        max_flight_time: Optional maximum flight time in minutes

    Returns:
        StreamingResponse with data in requested format
    """

    def generate_ndjson():
        try:
            # Parse dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            # Load schedule and emissions
            schedule = load_schedule(start, end)
            emissions = load_emissions()

            # Convert max_emissions to float if provided
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

            # Stream joined data
            for row in stream_joined_flights(
                schedule, emissions,
                max_emissions=max_emissions_val,
                max_flight_time=max_flight_time
            ):
                # Apply optional airport filters
                if depart_airport and row["DEPAPT"] != depart_airport.upper():
                    continue
                if arrival_airport and row["ARRAPT"] != arrival_airport.upper():
                    continue

                yield json.dumps(row) + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    def generate_json():
        try:
            # Parse dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            # Load schedule and emissions
            schedule = load_schedule(start, end)
            emissions = load_emissions()

            # Convert max_emissions to float if provided
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

            # Stream as JSON array
            yield "["
            first = True
            for row in stream_joined_flights(
                schedule, emissions,
                max_emissions=max_emissions_val,
                max_flight_time=max_flight_time
            ):
                # Apply optional airport filters
                if depart_airport and row["DEPAPT"] != depart_airport.upper():
                    continue
                if arrival_airport and row["ARRAPT"] != arrival_airport.upper():
                    continue

                if not first:
                    yield ","
                first = False
                yield json.dumps(row)
            yield "]"
        except Exception as e:
            yield json.dumps({"error": str(e)})

    if format.lower() == "json":
        return StreamingResponse(generate_json(), media_type="application/json")
    else:
        return StreamingResponse(generate_ndjson(), media_type="application/x-ndjson")


@app.get("/connecting-flights")
def get_connecting(
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
    origin: str = None,
    destination: str = None,
    limit: int = 10,
    max_emissions: float = 300,
    max_journey_time: int = None,
):
    """
    Get connecting flights from origin to destination within 40 mins to 5 hours connection time, with emissions data.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        origin: Origin airport code (e.g., 'JFK')
        destination: Destination airport code (e.g., 'LAX')
        limit: Maximum number of connections to return (default 10)
        max_emissions: Maximum total CO2 emissions in tonnes (default 300)
        max_journey_time: Maximum total journey time in minutes (optional)

    Returns:
        JSON array with connecting flight pairs including emissions
    """
    try:
        if not origin or not destination:
            return {"error": "origin and destination airports are required"}

        # Parse dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Load schedule and emissions
        schedule = load_schedule(start, end)
        emissions = load_emissions()

        # Convert max_emissions to float if provided
        max_emissions_val = float(max_emissions) if max_emissions is not None else None

        # Get connecting flights
        connections = get_connecting_flights(
            schedule, emissions, origin, destination,
            limit=limit, max_emissions=max_emissions_val,
            max_journey_time=max_journey_time
        )

        # Return connections
        return {"connections": connections, "count": len(connections)}
    except ValueError as e:
        return {"error": f"Invalid format: {str(e)}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/connecting-flights/stream")
def stream_connecting(
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
    origin: str = None,
    destination: str = None,
    format: str = "ndjson",
    limit: int = 10,
    max_emissions: float = 300,
    max_journey_time: int = None,
):
    """
    Stream connecting flights from origin to destination within 40 mins to 5 hours connection time, with emissions data.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        origin: Origin airport code (e.g., 'JFK')
        destination: Destination airport code (e.g., 'LAX')
        format: Output format - 'ndjson' (default) or 'json'
        limit: Maximum number of connections to stream (default 10)
        max_emissions: Maximum total CO2 emissions in tonnes (default 300)
        max_journey_time: Maximum total journey time in minutes (optional)

    Returns:
        StreamingResponse with connecting flight pairs including emissions
    """

    def generate_ndjson():
        try:
            if not origin or not destination:
                yield json.dumps({"error": "origin and destination airports are required"}) + "\n"
                return

            # Parse dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            # Load schedule and emissions
            schedule = load_schedule(start, end)
            emissions = load_emissions()

            # Convert max_emissions to float if provided
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

            # Stream connecting flights
            for connection in stream_connecting_flights(
                schedule, emissions, origin, destination,
                limit=limit, max_emissions=max_emissions_val,
                max_journey_time=max_journey_time
            ):
                yield json.dumps(connection) + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    def generate_json():
        try:
            if not origin or not destination:
                yield json.dumps({"error": "origin and destination airports are required"})
                return

            # Parse dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            # Load schedule and emissions
            schedule = load_schedule(start, end)
            emissions = load_emissions()

            # Convert max_emissions to float if provided
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

            # Stream as JSON array
            yield "["
            first = True
            for connection in stream_connecting_flights(
                schedule, emissions, origin, destination,
                limit=limit, max_emissions=max_emissions_val,
                max_journey_time=max_journey_time
            ):
                if not first:
                    yield ","
                first = False
                yield json.dumps(connection)
            yield "]"
        except Exception as e:
            yield json.dumps({"error": str(e)})

    if format.lower() == "json":
        return StreamingResponse(generate_json(), media_type="application/json")
    else:
        return StreamingResponse(generate_ndjson(), media_type="application/x-ndjson")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
