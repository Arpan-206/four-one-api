"""Meeting location candidate filtering using convex hull polygon."""

from typing import Dict, List, Tuple
from scipy.spatial import ConvexHull
import numpy as np
from candidates import get_candidate_cities, get_candidates_with_custom
import requests


def get_coordinates_from_nominatim(city_name: str) -> Tuple[float, float] | None:
    """
    Fetch city coordinates from Nominatim (OpenStreetMap) API.

    Args:
        city_name: Name of the city to geocode

    Returns:
        Tuple of (latitude, longitude) or None if not found
    """
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={city_name}&format=json"
        response = requests.get(url, headers={"User-Agent": "forty-one-api/1.0"}, timeout=5)
        if response.status_code == 200 and response.json():
            data = response.json()[0]
            return (float(data["lat"]), float(data["lon"]))
    except Exception as e:
        print(f"Error geocoding {city_name}: {e}")
    return None


def build_attendee_candidates(attendees) -> Tuple[Dict[str, Tuple[float, float]], Dict]:
    """
    Build mapping of attendee cities to coordinates.

    Uses predefined cities when available, falls back to Nominatim for unknown cities.

    Args:
        attendees: List of city names or dict mapping city names to counts

    Returns:
        Tuple of (attendee_locations dict, custom_cities dict)
    """
    candidate_cities = get_candidate_cities()
    city_to_code = {info["city"].lower(): code for code, info in candidate_cities.items()}

    attendee_locations = {}
    custom_cities_to_add = {}

    city_names = attendees if isinstance(attendees, list) else attendees.keys()

    for city_name in city_names:
        city_lower = city_name.lower()

        if city_lower in city_to_code:
            code = city_to_code[city_lower]
            info = candidate_cities[code]
            attendee_locations[code] = (info["lat"], info["lon"])
        else:
            coords = get_coordinates_from_nominatim(city_name)
            if coords:
                pseudo_code = city_name[:3].upper()
                attendee_locations[pseudo_code] = coords
                custom_cities_to_add[pseudo_code] = {
                    "city": city_name,
                    "country": "Unknown",
                    "lat": coords[0],
                    "lon": coords[1]
                }

    return attendee_locations, custom_cities_to_add


def point_in_polygon(point: Tuple[float, float], polygon_vertices: List[Tuple[float, float]]) -> bool:
    """
    Check if point is inside or on polygon boundary using ray casting.

    Args:
        point: (lat, lon) coordinate
        polygon_vertices: List of (lat, lon) vertices

    Returns:
        True if point is inside or on boundary, False otherwise
    """
    x, y = point
    tolerance = 1e-10

    for vx, vy in polygon_vertices:
        if abs(x - vx) < tolerance and abs(y - vy) < tolerance:
            return True

    inside = False
    p1x, p1y = polygon_vertices[0]

    for i in range(1, len(polygon_vertices) + 1):
        p2x, p2y = polygon_vertices[i % len(polygon_vertices)]

        if min(p1y, p2y) <= y <= max(p1y, p2y) and min(p1x, p2x) <= x <= max(p1x, p2x):
            if p1y != p2y:
                slope = (p2y - p1y) / (p2x - p1x) if p2x != p1x else float('inf')
                if slope == float('inf'):
                    if abs(x - p1x) < tolerance:
                        return True
                else:
                    expected_x = p1x + (y - p1y) / slope
                    if abs(x - expected_x) < tolerance:
                        return True

        if y > min(p1y, p2y) and y <= max(p1y, p2y) and x <= max(p1x, p2x):
            if p1y != p2y:
                xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
            if p1x == p2x or x <= xinters:
                inside = not inside

        p1x, p1y = p2x, p2y

    return inside


def calculate_bounding_polygon(locations: Dict[str, Tuple[float, float]]) -> List[Tuple[float, float]]:
    """
    Calculate convex hull polygon from attendee locations.

    Args:
        locations: Dict mapping codes to (lat, lon) tuples

    Returns:
        List of (lat, lon) polygon vertices
    """
    coords = list(locations.values())

    if len(coords) < 3:
        return coords

    points = np.array(coords)

    if len(coords) == 3:
        return coords

    try:
        hull = ConvexHull(points)
        return [tuple(points[i]) for i in hull.vertices]
    except Exception:
        return coords


def filter_candidates_by_polygon(
    candidate_cities: Dict,
    polygon_vertices: List[Tuple[float, float]],
    custom_cities: Dict = None
) -> Dict:
    """
    Filter candidate cities to those within polygon.

    Args:
        candidate_cities: Dict of airport code -> city info
        polygon_vertices: List of (lat, lon) polygon vertices
        custom_cities: Optional custom cities to include

    Returns:
        Filtered dict of candidates within polygon
    """
    all_cities = {**candidate_cities}
    if custom_cities:
        all_cities.update(custom_cities)

    return {
        code: info for code, info in all_cities.items()
        if point_in_polygon((info["lat"], info["lon"]), polygon_vertices)
    }


def get_filtered_candidates(attendees) -> Dict:
    """
    Filter candidate cities by convex hull polygon of attendee locations.

    Args:
        attendees: List of city names or dict mapping city names to counts

    Returns:
        Dict with filtered candidates and polygon/location metadata
    """
    try:
        attendee_locations, custom_cities = build_attendee_candidates(attendees)

        if not attendee_locations:
            return {"error": "Could not locate any attendee cities"}

        polygon_vertices = calculate_bounding_polygon(attendee_locations)
        base_candidates = get_candidates_with_custom(custom_cities)

        filtered_candidates = filter_candidates_by_polygon(
            base_candidates,
            polygon_vertices,
            custom_cities=None
        )

        if not filtered_candidates:
            return {
                "error": "No candidate cities found within polygon",
                "polygon_vertices": polygon_vertices,
                "attendee_locations": attendee_locations
            }

        return {
            "filtered_candidates": filtered_candidates,
            "polygon_vertices": polygon_vertices,
            "attendee_locations": attendee_locations,
            "total_candidates": len(base_candidates),
            "candidates_in_polygon": len(filtered_candidates)
        }

    except Exception as e:
        return {"error": f"Error filtering candidates: {str(e)}"}
