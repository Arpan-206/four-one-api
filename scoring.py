"""
Scoring system for meeting location optimization.

Uses travel time and CO2 emissions with configurable weights.
- Travel time (from haversineMethod)
- CO2 emissions (from flight data)
"""

from datetime import datetime
from typing import Dict, List, Tuple, Optional
from helpers.haversineMethod import haversine, get_best_travel_time, custom_speeds
from helpers.scoringMethod import compute_travel_scores
from data import load_schedule, load_emissions, stream_joined_flights
import json


def score_meeting_location(
    attendee_cities: Dict[str, int],  # {city: count}
    attendee_coords: Dict[str, Tuple[float, float]],  # {city: (lat, lon)}
    candidate_cities: Dict[str, Dict],  # {code: {city, lat, lon, country}}
    start_date: str,
    end_date: str,
    time_limit_hours: float = 24,
    time_weight: float = 0.5,
    emissions_weight: float = 0.5
) -> Dict:
    """
    Score candidate cities based on travel time and CO2 emissions.

    Uses existing flight schedule and emissions data to calculate optimal meeting location.

    Args:
        attendee_cities: Dict mapping city names to attendee counts
        attendee_coords: Dict mapping city names to (lat, lon)
        candidate_cities: Dict of candidate cities with coordinates
        start_date: Start date for flights (YYYY-MM-DD)
        end_date: End date for flights (YYYY-MM-DD)
        time_limit_hours: Maximum travel time allowed (hours)
        time_weight: Weight for travel time importance (0-1, default 0.5)
        emissions_weight: Weight for emissions importance (0-1, default 0.5)

    Returns:
        Dict with best candidate and detailed scores
    """
    details = {}

    # Normalize weights to dict format for compute_travel_scores
    user_weights = {
        'time': time_weight,
        'co2': emissions_weight
    }

    # Load flight data
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        schedule = load_schedule(start_dt, end_dt)
        emissions = load_emissions()
    except Exception as e:
        print(f"[DEBUG] Error loading flight data: {e}")
        return {
            'best_candidate': None,
            'best_candidate_name': None,
            'scores': {},
            'details': {}
        }

    # Get airport codes for attendees
    attendee_airports = _get_airport_codes(list(attendee_cities.keys()))

    # Score each candidate city
    all_route_options = []
    attendee_travel_times = {}  # Map: candidate_code -> {attendee_city -> travel_hours}
    candidate_list = list(candidate_cities.keys())

    for candidate_code in candidate_list:
        candidate_info = candidate_cities[candidate_code]
        candidate_name = candidate_info.get('city', candidate_code)
        candidate_coords = (candidate_info['lat'], candidate_info['lon'])

        # Collect flights from each attendee to this candidate
        route_options = []
        attendee_travel_times[candidate_code] = {}

        for attendee_city, attendee_airport in attendee_airports.items():
            try:
                # Get flights from attendee to candidate
                flights_data = []
                for flight in stream_joined_flights(schedule, emissions):
                    if (flight.get('DEPAPT') == attendee_airport and
                        flight.get('ARRAPT') == candidate_code):
                        flights_data.append(flight)

                if flights_data:
                    # Calculate max travel time from flights using ELPTIM (elapsed time in minutes)
                    travel_times = []
                    for f in flights_data:
                        # Try ELPTIM first (elapsed time in minutes), convert to hours
                        if 'ELPTIM' in f and f.get('ELPTIM'):
                            elptim = f.get('ELPTIM', 0)
                            # Handle both string and numeric ELPTIM
                            if isinstance(elptim, str):
                                try:
                                    elptim = int(elptim)
                                except (ValueError, TypeError):
                                    elptim = 0
                            travel_times.append(elptim / 60.0)
                        # Fallback: calculate from DEPTIM and ARRTIM
                        elif 'DEPTIM' in f and 'ARRTIM' in f:
                            try:
                                dep_time = f.get('DEPTIM', 0)
                                arr_time = f.get('ARRTIM', 0)
                                # Handle both string and numeric times
                                if isinstance(dep_time, str):
                                    dep_time = int(dep_time)
                                if isinstance(arr_time, str):
                                    arr_time = int(arr_time)
                                # Calculate duration, handling day wraparound
                                duration_mins = arr_time - dep_time
                                if duration_mins < 0:
                                    duration_mins += 1440  # Add 24 hours in minutes
                                travel_times.append(duration_mins / 60.0)
                            except (ValueError, TypeError):
                                pass  # Skip this flight if conversion fails

                    max_travel = max(travel_times) if travel_times else 0
                    attendee_travel_times[candidate_code][attendee_city] = max_travel
                    route_options.append({'flights': flights_data})
                else:
                    # No flights found, use haversine estimate
                    travel_time, _ = get_best_travel_time(
                        attendee_coords[attendee_city],
                        candidate_coords,
                        custom_speeds
                    )
                    travel_hours = travel_time / 60
                    attendee_travel_times[candidate_code][attendee_city] = travel_hours
                    route_options.append({
                        'max_travel_hours': travel_hours,
                        'total_co2': 0
                    })
            except Exception as e:
                print(f"[DEBUG] Error getting flights from {attendee_airport} to {candidate_code}: {e}")
                # Fallback to haversine estimate
                travel_time, _ = get_best_travel_time(
                    attendee_coords[attendee_city],
                    candidate_coords,
                    custom_speeds
                )
                travel_hours = travel_time / 60
                attendee_travel_times[candidate_code][attendee_city] = travel_hours
                route_options.append({
                    'max_travel_hours': travel_hours,
                    'total_co2': 0
                })

        all_route_options.append(route_options)

        # Store candidate info for later
        details[candidate_code] = {
            'city_name': candidate_name,
            'country': candidate_info.get('country', ''),
            'coordinates': candidate_coords
        }

    # Use the scoring helper to compute scores
    if all_route_options:
        score_json = compute_travel_scores(all_route_options, user_weights)
        try:
            scores_result = json.loads(score_json)
        except:
            scores_result = {}
    else:
        scores_result = {}

    # Find best candidate (lowest score is best)
    best_candidate = None
    best_score = float('inf')
    scores = {}

    for idx, candidate_code in enumerate(candidate_list):
        # Calculate composite score
        score = 0
        try:
            if 'flights' in all_route_options[idx][0]:
                # Use actual flight data
                flights = all_route_options[idx][0]['flights']

                # Extract travel times from ELPTIM or calculate from DEPTIM/ARRTIM
                travel_times = []
                for f in flights:
                    if 'ELPTIM' in f and f.get('ELPTIM'):
                        elptim = f.get('ELPTIM', 0)
                        if isinstance(elptim, str):
                            try:
                                elptim = int(elptim)
                            except (ValueError, TypeError):
                                elptim = 0
                        travel_times.append(elptim / 60.0)  # Convert to hours
                    elif 'DEPTIM' in f and 'ARRTIM' in f:
                        try:
                            dep_time = f.get('DEPTIM', 0)
                            arr_time = f.get('ARRTIM', 0)
                            if isinstance(dep_time, str):
                                dep_time = int(dep_time)
                            if isinstance(arr_time, str):
                                arr_time = int(arr_time)
                            duration_mins = arr_time - dep_time
                            if duration_mins < 0:
                                duration_mins += 1440
                            travel_times.append(duration_mins / 60.0)
                        except (ValueError, TypeError):
                            pass

                # Extract CO2 emissions
                co2_values = [f.get('ESTIMATED_CO2_TOTAL_TONNES', 0) for f in flights]

                max_travel = max(travel_times) if travel_times else 0
                avg_co2 = sum(co2_values) / len(co2_values) if co2_values else 0

                # Normalize to 0-1 scale
                travel_score = min(max_travel / time_limit_hours, 1.0) if time_limit_hours > 0 else 0
                co2_score = min(avg_co2 / 500.0, 1.0)  # Assume 500 tonnes is worst case
            else:
                # Use haversine estimate
                max_travel = all_route_options[idx][0].get('max_travel_hours', 0)
                travel_score = min(max_travel / time_limit_hours, 1.0) if time_limit_hours > 0 else 0
                co2_score = 0.5  # Neutral when no flight data

            # Weighted composite score
            total_weight = time_weight + emissions_weight
            if total_weight > 0:
                score = (travel_score * time_weight + co2_score * emissions_weight) / total_weight

            scores[candidate_code] = score
            details[candidate_code]['travel_time_score'] = travel_score
            details[candidate_code]['emissions_score'] = co2_score
            details[candidate_code]['composite_score'] = score

            if score < best_score:
                best_score = score
                best_candidate = candidate_code
        except Exception as e:
            print(f"[DEBUG] Error scoring candidate {candidate_code}: {e}")
            scores[candidate_code] = 1.0
            details[candidate_code]['composite_score'] = 1.0

    # Get travel hours for best candidate
    best_attendee_travel_hours = {}
    if best_candidate:
        best_attendee_travel_hours = attendee_travel_times.get(best_candidate, {})

    return {
        'best_candidate': best_candidate,
        'best_candidate_name': details[best_candidate]['city_name'] if best_candidate else None,
        'scores': scores,
        'details': details,
        'attendee_travel_hours': best_attendee_travel_hours
    }


def _get_airport_codes(cities: List[str]) -> Dict[str, str]:
    """Map city names to airport codes."""
    city_to_airport = {
        'Mumbai': 'BOM',
        'Shanghai': 'PVG',
        'Hong Kong': 'HKG',
        'Singapore': 'SIN',
        'Sydney': 'SYD',
        'Tokyo': 'HND',
        'London': 'LHR',
        'New York': 'JFK',
        'LAX': 'LAX',
        'SFO': 'SFO',
        'Delhi': 'DEL',
        'Bangkok': 'BKK',
        'Paris': 'CDG',
        'Amsterdam': 'AMS',
        'Berlin': 'BER',
        'Frankfurt': 'FRA',
        'Zurich': 'ZRH',
        'Toronto': 'YYZ',
        'Vancouver': 'YVR',
        'Calgary': 'YYC',
        'Mexico City': 'MEX',
        'São Paulo': 'GIG',
    }
    return {city: city_to_airport.get(city, city.upper()[:3]) for city in cities}


def _score_travel_time(
    attendee_coords: Dict[str, Tuple[float, float]],
    candidate_coords: Tuple[float, float],
    time_limit_minutes: int
) -> float:
    """
    Score travel time. Lower time = lower score = better.
    Returns value between 0-1.
    """
    if not attendee_coords:
        return 1.0

    times = []
    for city, coords in attendee_coords.items():
        travel_time, _ = get_best_travel_time(coords, candidate_coords, custom_speeds)
        times.append(travel_time)

    if not times:
        return 1.0

    max_time = max(times)
    normalized_score = min(max_time / time_limit_minutes, 1.0)
    return normalized_score


