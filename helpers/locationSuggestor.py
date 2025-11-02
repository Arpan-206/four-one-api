from . import haversineMethod
from .scoringMethod import compute_travel_scores
from datetime import datetime, timedelta
import json

# collect anomalous ARRDAY values seen while parsing
_BAD_ARRDAY_VALUES = set()
from .api_client import filter_candidate_cities, get_flights_non_streaming

custom_speeds = {
    'walking': 5,
    'cycling': 15,
    'driving': 60,
    'flying': 800
}

def chooseLocation(starting_locs, starting_locs_data, start_time, end_time, sliderVal=0.5, timeVal=2400):
    print(f"[DEBUG] chooseLocation called")
    print(f"[DEBUG] starting_locs: {starting_locs}")
    print(f"[DEBUG] starting_locs_data: {starting_locs_data}")

    dict_of_locations = filter_candidate_cities({"cities": starting_locs_data})
    print(f"[DEBUG] dict_of_locations: {dict_of_locations}")

    if not dict_of_locations:
        print("[DEBUG] No locations found, returning None")
        return None

    locations = [(loc['lat'], loc['lon']) for loc in dict_of_locations.values()]
    print(f"[DEBUG] locations: {locations}")

    result_json = analyseLocations(starting_locs, starting_locs_data, locations, dict_of_locations, start_time, end_time, {sliderVal,timeVal})
    print(f"[DEBUG] analyseLocations returned: {result_json}")

    # Parse the JSON result
    try:
        result_data = json.loads(result_json)
        event_location = result_data.get("event_location")
        print(f"[DEBUG] event_location from result: {event_location}")

        if event_location and event_location in dict_of_locations:
            coords = (dict_of_locations[event_location]['lat'], dict_of_locations[event_location]['lon'])
            print(f"[DEBUG] returning coordinates: {coords}")
            return coords
        else:
            print(f"[DEBUG] event_location {event_location} not found in dict_of_locations")
            return None
    except (json.JSONDecodeError, TypeError) as e:
        print(f"[DEBUG] Error parsing result: {e}")
        return None

def analyseLocations(starting_locations, starting_locations_data, target_locations, location_data, start_time, end_time, weighting):
    """
    hardcoded_locations = [(loc['lat'], loc['lon']) for loc in dict_of_locations.values()]

    """
    """
    1. Create potential routes from starting locations to target locations
    2. Score each target location based on criteria such as travel time, cost, and convenience
    3. Return a list of scores corresponding to each target location
    """

    time_limit = int((end_time - start_time).total_seconds() // 60)
    all_route_options = []

    # Need to think about time limit, possible connections,
    for key in location_data.keys():
        latlon = (location_data[key]['lat'], location_data[key]['lon'])
        locvals = [location_data[key], location_data[key]["city"], location_data[key]["country"]]
        #=======================Create Routes=======================
        # First we use the Haversine method to filter out impossible routes
        # Then we call findRoutes for each starting location to get possible routes
        route_options = []

        for i in range(len(starting_locations)):
            start = starting_locations[i]
            haversineResult = haversineMethod.get_best_travel_time(start, latlon, custom_speeds)
            if haversineResult[0] <= time_limit:
                route_options.append(calculateRoutes(starting_locations_data[i], key, start_time, end_time))
                # Here we would generate routes from 'start' to 'location' and append to route_options
            else:
                route_options = [-1]
                print("No possible routes within time limit")
                break

        # Add this location's route options to the overall list
        all_route_options.append(route_options)

    #=======================Score Locations=======================
    #print("Route options before scoring:", all_route_options)
    score = compute_travel_scores(all_route_options, weighting)

    return score

def scoreLocation(location):
    return 5  # Placeholder for actual scoring logic

def calculateRoutes(start, end, start_time, end_time):
    """Lookup flights between start and end in the internal flights DB and return a list of parsed flights.

    start/end can be airport codes (ICAO/IATA string) or other types. This function only performs a
    lookup when both `start` and `end` are strings. If the API client is not available, returns [].
    """

    base = 'http://10.249.36.85'
    try:
        print('Looking up flights from', start, 'to', end)
        raw = get_flights_non_streaming(base, start, end)
    except Exception as e:
        print('Error calling internal API:', e)
        return []
    
    return raw
