import math
from typing import List, Tuple, Dict

# Define custom speeds for each mode in km/h
custom_speeds = {
    'walking': 5,
    'cycling': 15,
    'driving': 60,
    'flying': 800
}

# Haversine formula to calculate distance between two lat/lon points
def haversine(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    R = 6371  # Earth radius in kilometers
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c  # in kilometers

# Calculate best travel time and mode for a user to a candidate location in minutes
def get_best_travel_time(user_loc: Tuple[float, float], candidate_loc: Tuple[float, float], speeds: Dict[str, float]) -> Tuple[float, str]:
    distance = haversine(user_loc, candidate_loc)
    best_time = float('inf')
    best_mode = None
    for mode, speed in speeds.items():
        time = distance / speed * 60  # convert hours to minutes
        if time < best_time:
            best_time = time
            best_mode = mode
    return best_time, best_mode

# Analyze candidate locations
def analyse_locations(user_locations: List[Tuple[float, float]],
                      candidate_locations: List[Tuple[float, float]],
                      time_constraint: float,
                      speeds: Dict[str, float]) -> Dict:
    location_scores = {}

    for candidate in candidate_locations:
        reachable_times = []
        used_modes = []
        for user in user_locations:
            travel_time, mode = get_best_travel_time(user, candidate, speeds)
            if travel_time <= time_constraint:
                reachable_times.append(travel_time)
                used_modes.append(mode)

        if reachable_times:
            score = {
                'reachable_users': len(reachable_times),
                'average_time': sum(reachable_times) / len(reachable_times),
                'modes': used_modes
            }
            location_scores[candidate] = score

    sorted_candidates = sorted(location_scores.items(),
                               key=lambda x: (-x[1]['reachable_users'], x[1]['average_time']))

    return {
        'best_candidates': sorted_candidates,
        'all_scores': location_scores
    }


"""
# Example usage
user_locations = [(49.2827, -123.1207), (51.0447, -114.0719), (43.6510, -79.3470)]  # Vancouver, Calgary, Toronto
candidate_locations = [(50.4452, -104.6189), (53.5461, -113.4938), (45.4215, -75.6995)]  # Regina, Edmonton, Ottawa
time_constraint = 300  # minutes

result = analyse_locations(user_locations, candidate_locations, time_constraint, custom_speeds)

# Display results
print("Best Candidate Locations:")
for loc, score in result['best_candidates']:
    print(f"Location: {loc}, Reachable Users: {score['reachable_users']}, Avg Time: {score['average_time']:.2f}, Modes: {score['modes']}")
"""