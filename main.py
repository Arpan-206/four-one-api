from fastapi import FastAPI, Body
from fastapi.responses import StreamingResponse
from datetime import datetime
import json
import polars as pl
from data import load_schedule, load_emissions, join_flights_with_emissions, stream_joined_flights, get_connecting_flights, stream_connecting_flights
from candidates import get_candidate_cities, get_nearest_cities, get_candidates_with_custom
from filter import get_filtered_candidates, build_attendee_candidates


app = FastAPI(title="forty-one", version="0.1.0", root_path="/api")


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
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        schedule = load_schedule(start, end)
        emissions = load_emissions()
        max_emissions_val = float(max_emissions) if max_emissions is not None else None

        df = join_flights_with_emissions(
            schedule, emissions,
            max_emissions=max_emissions_val,
            max_flight_time=max_flight_time
        )

        if depart_airport:
            df = df.filter(pl.col("DEPAPT") == depart_airport.upper())
        if arrival_airport:
            df = df.filter(pl.col("ARRAPT") == arrival_airport.upper())

        # Collect the lazy frame and return all data
        df_collected = df.collect()
        return {"count": df_collected.height, "flights": df_collected.to_dicts()}
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
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            schedule = load_schedule(start, end)
            emissions = load_emissions()
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

            for row in stream_joined_flights(
                schedule, emissions,
                max_emissions=max_emissions_val,
                max_flight_time=max_flight_time
            ):
                if depart_airport and row["DEPAPT"] != depart_airport.upper():
                    continue
                if arrival_airport and row["ARRAPT"] != arrival_airport.upper():
                    continue

                yield json.dumps(row) + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    def generate_json():
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            schedule = load_schedule(start, end)
            emissions = load_emissions()
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

            yield "["
            first = True
            for row in stream_joined_flights(
                schedule, emissions,
                max_emissions=max_emissions_val,
                max_flight_time=max_flight_time
            ):
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

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        schedule = load_schedule(start, end)
        emissions = load_emissions()
        max_emissions_val = float(max_emissions) if max_emissions is not None else None

        connections = get_connecting_flights(
            schedule, emissions, origin, destination,
            limit=limit, max_emissions=max_emissions_val,
            max_journey_time=max_journey_time
        )

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

            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            schedule = load_schedule(start, end)
            emissions = load_emissions()
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

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

            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            schedule = load_schedule(start, end)
            emissions = load_emissions()
            max_emissions_val = float(max_emissions) if max_emissions is not None else None

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


@app.get("/candidate-cities")
def list_candidate_cities():
    """
    Get all candidate cities for meeting location optimization.

    Returns:
        JSON object with all candidate cities and their information
    """
    try:
        cities = get_candidate_cities()
        return {
            "total_cities": len(cities),
            "cities": cities
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/candidate-cities/nearest")
def nearest_cities(lat: float, lon: float, num_cities: int = 10):
    """
    Get nearest candidate cities to a given latitude/longitude.

    Args:
        lat: Latitude
        lon: Longitude
        num_cities: Number of nearest cities to return (default 10)

    Returns:
        JSON object with nearest cities sorted by distance
    """
    try:
        cities = get_nearest_cities(lat, lon, num_cities)
        return {
            "reference_location": {"lat": lat, "lon": lon},
            "total_cities": len(cities),
            "cities": cities
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/candidate-cities/with-custom")
def get_candidates_with_additions(payload: dict = Body(...)):
    """
    Get all candidate cities, auto-populating missing cities from input list.

    Accepts a list of city names, looks up predefined cities, and geocodes any unknown cities via Nominatim.

    Input format:
    {
        "cities": ["Mumbai", "Tokyo", "New York", "Unknown City"]
    }

    Returns:
        JSON object with all candidate cities (default + auto-populated custom)
    """
    try:
        cities_list = payload.get("cities", [])

        if not cities_list:
            return {"error": "cities list required"}

        _, custom_cities = build_attendee_candidates(cities_list)
        all_cities = get_candidates_with_custom(custom_cities)

        return {
            "total_cities": len(all_cities),
            "custom_cities_added": len(custom_cities),
            "cities": all_cities
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/filter-candidates")
def filter_candidates(payload: dict = Body(...)):
    """
    Filter candidate cities based on convex hull polygon of attendee locations.

    Input format:
    {
        "cities": ["Mumbai", "Tokyo", "Hong Kong", "Singapore", "Sydney"]
    }

    Output format:
    {
        "filtered_candidates": {
            "SYD": {"city": "Sydney", "country": "Australia", "lat": -33.9461, "lon": 151.1772},
            "HND": {"city": "Tokyo", "country": "Japan", "lat": 35.5494, "lon": 139.7798},
            ...
        },
        "polygon_vertices": [[35.5494, 139.7798], [-33.9461, 151.1772], ...],
        "attendee_locations": {
            "BOM": [19.0895, 72.8656],
            "HND": [35.5494, 139.7798],
            ...
        },
        "total_candidates": 43,
        "candidates_in_polygon": 6
    }
    """
    try:
        cities = payload.get("cities", [])

        if not cities:
            return {"error": "cities field required (list of city names)"}

        result = get_filtered_candidates(cities)
        return result

    except Exception as e:
        return {"error": f"Error filtering candidates: {str(e)}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
