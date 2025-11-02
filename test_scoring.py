#!/usr/bin/env python3
"""Test script for scoring function"""

import json
from datetime import datetime
from candidates import CANDIDATE_CITIES, get_airport_code_by_city
from filter import get_filtered_candidates
from scoring import score_meeting_location

# Test input data
test_input = {
    "attendees": {
        "Mumbai": 2,
        "Shanghai": 3,
        "Hong Kong": 1,
        "Singapore": 2,
        "Sydney": 2
    },
    "availability_window": {
        "start": "2024-01-01T09:00:00Z",
        "end": "2025-01-15T17:00:00Z"
    },
    "event_duration": {
        "days": 0,
        "hours": 4
    }
}

# Extract attendees and get candidate cities
attendees = test_input.get("attendees", {})
availability_window = test_input.get("availability_window", {})

# Get coordinates for attendee cities
attendee_coords = {}
for city_name in attendees.keys():
    # Try exact city name match in CANDIDATE_CITIES
    found = False
    for code, info in CANDIDATE_CITIES.items():
        if info.get("city") == city_name:
            attendee_coords[city_name] = (info["lat"], info["lon"])
            found = True
            break

    if not found:
        # Try airport code lookup
        airport_code = get_airport_code_by_city(city_name)
        if airport_code and airport_code in CANDIDATE_CITIES:
            info = CANDIDATE_CITIES[airport_code]
            attendee_coords[city_name] = (info["lat"], info["lon"])

print("Attendee Coordinates:")
print(json.dumps(attendee_coords, indent=2))

# Get candidate cities (using custom with attendee cities included)
try:
    from filter import build_attendee_candidates
    from candidates import get_candidates_with_custom

    _, custom_cities = build_attendee_candidates(list(attendees.keys()))
    candidate_cities = get_candidates_with_custom(custom_cities)
    print(f"\nCandidates (45 existing + custom attendee cities): {len(candidate_cities)}")
except Exception as e:
    print(f"Error getting candidates with custom: {e}")
    candidate_cities = {
        code: {
            "city": info["city"],
            "country": info["country"],
            "lat": info["lat"],
            "lon": info["lon"]
        }
        for code, info in CANDIDATE_CITIES.items()
    }
    print(f"Fallback - Using all candidates: {len(candidate_cities)}")

# Extract dates
start_date_str = availability_window["start"].split("T")[0]
end_date_str = availability_window["end"].split("T")[0]

start = datetime.fromisoformat(availability_window["start"].replace("Z", "+00:00"))
end = datetime.fromisoformat(availability_window["end"].replace("Z", "+00:00"))
time_limit_hours = (end - start).total_seconds() / 3600

print(f"\nDate Range: {start_date_str} to {end_date_str}")
print(f"Time Limit Hours: {time_limit_hours}")

# Run scoring
print("\nRunning scoring...")
result = score_meeting_location(
    attendee_cities=attendees,
    attendee_coords=attendee_coords,
    candidate_cities=candidate_cities,
    start_date=start_date_str,
    end_date=end_date_str,
    time_limit_hours=time_limit_hours,
    time_weight=0.5,
    emissions_weight=0.5
)

print("\n=== SCORING RESULT ===")
print(f"Best Candidate: {result['best_candidate']}")
print(f"Best Candidate Name: {result['best_candidate_name']}")
print(f"\nAttendee Travel Hours:")
print(json.dumps(result['attendee_travel_hours'], indent=2))
print(f"\nScores (sample):")
for code, score in list(result['scores'].items())[:5]:
    print(f"  {code}: {score}")
print(f"\nBest Candidate Details:")
best_details = result['details'][result['best_candidate']]
print(json.dumps(best_details, indent=2))

print("\n=== FULL OUTPUT ===")
output = {
    "event_location": best_details['city_name'],
    "event_location_code": result['best_candidate'],
    "event_dates": {
        "start": availability_window.get("start"),
        "end": availability_window.get("end"),
        "hours": time_limit_hours
    },
    "event_duration": test_input.get("event_duration", {}),
    "attendee_travel_hours": result['attendee_travel_hours'],
    "scoring_details": {
        "travel_time_score": best_details['travel_time_score'],
        "emissions_score": best_details['emissions_score'],
        "composite_score": best_details['composite_score'],
        "coordinates": best_details['coordinates'],
        "weights": {
            "time": 0.5,
            "emissions": 0.5
        }
    }
}
print(json.dumps(output, indent=2))
