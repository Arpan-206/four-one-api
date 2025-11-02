from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from datetime import datetime
import json
from data import load_schedule, load_emissions, join_flights_with_emissions, stream_joined_flights


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
    start_date: str = "2025-05-01",
    end_date: str = "2025-05-31",
    depart_airport: str = None,
    arrival_airport: str = None,
):
    """
    Get joined flight and emissions data.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        depart_airport: Optional filter by departure airport (DEPAPT)
        arrival_airport: Optional filter by arrival airport (ARRAPT)

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

        # Join the data
        df = join_flights_with_emissions(schedule, emissions)

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
    start_date: str = "2025-05-01",
    end_date: str = "2025-05-31",
    format: str = "ndjson",
    depart_airport: str = None,
    arrival_airport: str = None,
):
    """
    Stream joined flight and emissions data.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        format: Output format - 'ndjson' (default) or 'json'
        depart_airport: Optional filter by departure airport (DEPAPT)
        arrival_airport: Optional filter by arrival airport (ARRAPT)

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

            # Stream joined data
            for row in stream_joined_flights(schedule, emissions):
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

            # Stream as JSON array
            yield "["
            first = True
            for row in stream_joined_flights(schedule, emissions):
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
