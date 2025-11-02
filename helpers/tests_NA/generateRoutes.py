"""import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Iterable, Optional
import heapq

# local API client helper (created earlier)
try:
    from api_client import get_flights_non_streaming, stream_flights_ndjson
except Exception:
    get_flights_non_streaming = None
    stream_flights_ndjson = None

OPENROUTESERVICE_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjcxZjJjMzRhYWU1NDRhNTE4MDdiNGE5MTFjZmE0YmJmIiwiaCI6Im11cm11cjY0In0="
AVIATIONSTACK_API_KEY = "058ef69b1b70fb9bef2fdfb478c715b6"

def get_flight_route(dep_iata, arr_iata):
    url = "https://api.aviationstack.com/v1/flights"
    params = {
        "access_key": AVIATIONSTACK_API_KEY,
        "dep_iata": "LHR",
        "arr_iata": "CDG"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    routes = data.get("data", [])
    if not routes:
        print(f"No direct flights found from {dep_iata} → {arr_iata}.")
        return

    print(f"\nFlights from {dep_iata} → {arr_iata}:\n")
    for r in routes[:5]:  # Show first 5 routes
        airline = r["airline"]["name"]
        flight = r["flight"]["iata"]
        aircraft = r.get("aircraft", {}).get("model", "Unknown")
        print(f"✈️ {airline} flight {flight} ({aircraft})")

# Example: London Heathrow to Paris Charles de Gaulle
get_flight_route("LHR", "CDG")"""