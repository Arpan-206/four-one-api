from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from datetime import datetime
import json
from data import join_schedule_and_emissions, find_connecting_flights


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
    start_airport: str,
    end_airport: str,
    start_date: str = "2025-05-01",
    end_date: str = "2025-05-31",
):
    """
    Get flight data with emissions information (non-streaming).

    Args:
        start_airport: Departure airport code (DEPAPT, e.g., 'LIS') - REQUIRED
        end_airport: Arrival airport code (ARRAPT, e.g., 'CGN') - REQUIRED
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD

    Returns:
        JSON array with flight data
    """
    try:
        # Parse dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Load and join data with mandatory airport filters
        df = join_schedule_and_emissions(
            start, end, start_airport.upper(), end_airport.upper()
        )

        # Convert to list of dicts and return as JSON
        return {"flights": df.to_dicts(), "count": len(df)}
    except ValueError as e:
        return {"error": f"Invalid date format: {str(e)}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/connections")
def get_connecting_flights(
    start_airport: str,
    end_airport: str,
    start_date: str = "2025-05-01",
    end_date: str = "2025-05-31",
    min_wait: int = 40,
    max_wait: int = 240,
):
    """
    Get connecting flights between two airports with specified wait time.

    Args:
        start_airport: Starting airport code (DEPAPT, e.g., 'LIS') - REQUIRED
        end_airport: Final destination airport code (ARRAPT, e.g., 'AMS') - REQUIRED
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        min_wait: Minimum connection wait time in minutes (default 40)
        max_wait: Maximum connection wait time in minutes (default 240 = 4 hours)

    Returns:
        JSON array with connecting flight pairs and emissions data
    """
    try:
        # Parse dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Find connecting flights
        connections_df = find_connecting_flights(
            start_airport.upper(),
            end_airport.upper(),
            start,
            end,
            min_wait,
            max_wait,
        )

        if len(connections_df) == 0:
            return {"connections": [], "count": 0}

        # Convert to dicts
        connections = connections_df.to_dicts()

        return {"connections": connections, "count": len(connections)}
    except ValueError as e:
        return {"error": f"Invalid date format: {str(e)}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/flights/stream")
def stream_flights(
    start_date: str = "2025-05-01",
    end_date: str = "2025-05-31",
    format: str = "ndjson",
    start_airport: str = None,
    end_airport: str = None,
):
    """
    Stream flight data with emissions information.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        format: Output format - 'ndjson' (default) or 'json'
        start_airport: Optional filter by departure airport code (DEPAPT, e.g., 'LHR')
        end_airport: Optional filter by arrival airport code (ARRAPT, e.g., 'BOM')

    Returns:
        StreamingResponse with data in requested format
    """

    def generate_ndjson():
        try:
            # Parse dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            # Load and join data with airport filters
            df = join_schedule_and_emissions(start, end, start_airport, end_airport)

            # Stream each row as NDJSON
            for row in df.to_dicts():
                yield json.dumps(row) + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    def generate_json():
        try:
            # Parse dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            # Load and join data with airport filters
            df = join_schedule_and_emissions(start, end, start_airport, end_airport)

            # Stream as JSON array
            yield "["
            rows = df.to_dicts()
            for i, row in enumerate(rows):
                if i > 0:
                    yield ","
                yield json.dumps(row)
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
