"""
Candidate cities for meeting location optimization.
Maps airport codes to city names and coordinates.
"""

CANDIDATE_CITIES = {
    "JFK": {"city": "New York", "country": "USA", "lat": 40.7127, "lon": -74.0060},
    "LAX": {"city": "Los Angeles", "country": "USA", "lat": 34.0537, "lon": -118.2428},
    "ORD": {"city": "Chicago", "country": "USA", "lat": 41.8756, "lon": -87.6244},
    "DFW": {"city": "Dallas", "country": "USA", "lat": 32.7763, "lon": -96.7969},
    "DEN": {"city": "Denver", "country": "USA", "lat": 39.7392, "lon": -104.9849},
    "SFO": {"city": "San Francisco", "country": "USA", "lat": 37.7793, "lon": -122.4193},
    "SEA": {"city": "Seattle", "country": "USA", "lat": 47.6038, "lon": -122.3301},
    "BOS": {"city": "Boston", "country": "USA", "lat": 42.3656, "lon": -71.0096},
    "MIA": {"city": "Miami", "country": "USA", "lat": 25.7907, "lon": -80.2871},
    "ATL": {"city": "Atlanta", "country": "USA", "lat": 33.7545, "lon": -84.3898},
    "LHR": {"city": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278},
    "CDG": {"city": "Paris", "country": "France", "lat": 48.8589, "lon": 2.3200},
    "FRA": {"city": "Frankfurt", "country": "Germany", "lat": 50.1106, "lon": 8.6821},
    "AMS": {"city": "Amsterdam", "country": "Netherlands", "lat": 52.3731, "lon": 4.8925},
    "DUB": {"city": "Dublin", "country": "Ireland", "lat": 53.4129, "lon": -6.2700},
    "SYD": {"city": "Sydney", "country": "Australia", "lat": -33.9461, "lon": 151.1772},
    "MEL": {"city": "Melbourne", "country": "Australia", "lat": -37.8142, "lon": 144.9632},
    "NRT": {"city": "Tokyo", "country": "Japan", "lat": 35.6769, "lon": 139.7639},
    "HND": {"city": "Tokyo", "country": "Japan", "lat": 35.6769, "lon": 139.7639},
    "ICN": {"city": "Seoul", "country": "South Korea", "lat": 37.5667, "lon": 126.9783},
    "PVG": {"city": "Shanghai", "country": "China", "lat": 31.2304, "lon": 121.3425},
    "HKG": {"city": "Hong Kong", "country": "Hong Kong", "lat": 22.3506, "lon": 114.1849},
    "SIN": {"city": "Singapore", "country": "Singapore", "lat": 1.3521, "lon": 103.8198},
    "BKK": {"city": "Bangkok", "country": "Thailand", "lat": 13.7525, "lon": 100.4935},
    "DEL": {"city": "Delhi", "country": "India", "lat": 28.5635, "lon": 77.1903},
    "BOM": {"city": "Mumbai", "country": "India", "lat": 19.0895, "lon": 72.8656},
    "DXB": {"city": "Dubai", "country": "UAE", "lat": 25.0743, "lon": 55.1885},
    "DOH": {"city": "Doha", "country": "Qatar", "lat": 25.2731, "lon": 51.6126},
    "SVO": {"city": "Moscow", "country": "Russia", "lat": 55.6256, "lon": 37.6064},
    "IST": {"city": "Istanbul", "country": "Turkey", "lat": 41.0064, "lon": 28.9759},
    "MXP": {"city": "Milan", "country": "Italy", "lat": 45.4642, "lon": 9.1896},
    "ZRH": {"city": "Zurich", "country": "Switzerland", "lat": 47.4647, "lon": 8.5492},
    "VIE": {"city": "Vienna", "country": "Austria", "lat": 48.2084, "lon": 16.3725},
    "BCN": {"city": "Barcelona", "country": "Spain", "lat": 41.2974, "lon": 2.0833},
    "MAD": {"city": "Madrid", "country": "Spain", "lat": 40.4730, "lon": -3.6282},
    "CDT": {"city": "Casablanca", "country": "Morocco", "lat": 33.5945, "lon": -7.6200},
    "CAI": {"city": "Cairo", "country": "Egypt", "lat": 30.0443879, "lon": 31.2357257},
    "JNB": {"city": "Johannesburg", "country": "South Africa", "lat": -26.2050, "lon": 28.0497},
    "CPT": {"city": "Cape Town", "country": "South Africa", "lat": -33.9288, "lon": 18.4172},
    "SAO": {"city": "Sao Paulo", "country": "Brazil", "lat": -23.5507, "lon": -46.6334},
    "RIO": {"city": "Rio de Janeiro", "country": "Brazil", "lat": -22.9068, "lon": -43.1729},
    "MEX": {"city": "Mexico City", "country": "Mexico", "lat": 19.3208, "lon": -99.1515},
    "TOR": {"city": "Toronto", "country": "Canada", "lat": 43.6535, "lon": -79.3839},
    "YVR": {"city": "Vancouver", "country": "Canada", "lat": 49.1847, "lon": -123.1786},
    "AAL": {"city": "Aarhus", "country": "Denmark", "lat": 56.0896, "lon": 10.6182},
    "IAH": {"city": "Houston", "country": "USA", "lat": 29.9792, "lon": -95.3369},
}


def get_candidate_cities():
    """
    Get list of all candidate cities.

    Returns:
        Dict mapping airport codes to city information
    """
    return CANDIDATE_CITIES


def get_cities_by_region(region: str):
    """
    Get candidate cities in a specific region.

    Args:
        region: Region name - 'north_america', 'south_america', 'europe', 'asia', 'africa', 'australia'

    Returns:
        Dict of cities in that region
    """
    regions = {
        "north_america": ["JFK", "LAX", "ORD", "DFW", "DEN", "SFO", "SEA", "BOS", "MIA", "ATL", "TOR", "YVR", "MEX"],
        "south_america": ["SAO", "RIO"],
        "europe": ["LHR", "CDG", "FRA", "AMS", "DUB", "SVO", "IST", "MXP", "ZRH", "VIE", "BCN", "MAD"],
        "africa": ["CDT", "CAI", "JNB", "CPT"],
        "asia": ["NRT", "HND", "ICN", "HKG", "SIN", "BKK", "DEL", "BOM", "DXB", "DOH"],
        "australia": ["SYD", "MEL"],
    }

    if region.lower() not in regions:
        return {}

    city_codes = regions[region.lower()]
    return {code: CANDIDATE_CITIES[code] for code in city_codes if code in CANDIDATE_CITIES}


def get_nearest_cities(lat: float, lon: float, num_cities: int = 10) -> dict:
    """
    Get nearest cities to a given location.

    Args:
        lat: Latitude
        lon: Longitude
        num_cities: Number of nearest cities to return (default 10)

    Returns:
        Dict of nearest cities with distances, sorted by distance
    """
    import math

    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinate pairs in kilometers."""
        R = 6371
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(a))

    distances = {
        code: {**info, "distance_km": round(haversine_distance(lat, lon, info["lat"], info["lon"]), 2)}
        for code, info in CANDIDATE_CITIES.items()
    }

    return dict(sorted(distances.items(), key=lambda x: x[1]["distance_km"])[:num_cities])


def add_custom_cities(custom_cities_dict: dict):
    """
    Add or update custom cities to the candidate list.

    Args:
        custom_cities_dict: Dict mapping airport codes to city info
                           e.g., {"BLR": {"city": "Bangalore", "country": "India", "lat": 13.1939, "lon": 77.7068}}

    Returns:
        Updated candidate cities dict with custom cities merged in
    """
    updated_cities = CANDIDATE_CITIES.copy()
    updated_cities.update(custom_cities_dict)
    return updated_cities


def get_candidates_with_custom(custom_cities_dict: dict = None):
    """
    Get candidate cities list, optionally merged with custom cities.

    Args:
        custom_cities_dict: Optional dict of custom cities to add/override

    Returns:
        Dict of all candidate cities (default + custom)
    """
    if custom_cities_dict is None:
        return CANDIDATE_CITIES.copy()
    return add_custom_cities(custom_cities_dict)


def get_airport_code_by_city(city_name: str) -> str:
    """
    Find airport code for a given city name.

    Args:
        city_name: City name (case-insensitive)

    Returns:
        Airport code if found, None otherwise
    """
    city_name_lower = city_name.lower()
    for code, info in CANDIDATE_CITIES.items():
        if info["city"].lower() == city_name_lower:
            return code
    return None


def get_all_candidate_airport_codes() -> list:
    """
    Get list of all candidate airport codes.

    Returns:
        List of airport codes
    """
    return list(CANDIDATE_CITIES.keys())
