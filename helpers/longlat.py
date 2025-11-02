from datetime import datetime
from .locationSuggestor import chooseLocation

def enrich_event_data(sliderVal, timeVal, data, starting_locs):
    """
    Reads dictionary and adds:
      - longitude and latitude for each city
      - duration in hours for the 'availability_window'

    Args:
        sliderVal: Slider value for optimization
        timeVal: Time value for optimization
        data: Input data dictionary with attendees, availability_window, event_duration
        starting_locs: Ordered list of tuples (lat, lon) representing attendee locations

    Returns output dict with event_location and attendee_travel_hours
    """
    print(f"[DEBUG] enrich_event_data called with starting_locs: {starting_locs}")

    city_and_loc = {}
    starting_locs_data = []

    # Extract city names from attendees (maintaining order)
    for city in data["attendees"].keys():
        starting_locs_data.append(city)

    print(f"[DEBUG] starting_locs_data: {starting_locs_data}")

    # calc date/time in hours
    start = datetime.fromisoformat(data["availability_window"]["start"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(data["availability_window"]["end"].replace("Z", "+00:00"))
    duration_hours = (end - start).total_seconds() / 3600

    print(f"[DEBUG] duration_hours: {duration_hours}")

    # update
    data["attendees"] = city_and_loc
    data["availability_window"]["duration in hours"] = duration_hours

    print(f"[DEBUG] Calling chooseLocation with: starting_locs={starting_locs}, starting_locs_data={starting_locs_data}")
    location_coords = chooseLocation(starting_locs, starting_locs_data, start, end, sliderVal, timeVal)
    print(f"[DEBUG] chooseLocation returned: {location_coords}")

    if not location_coords:
        print("[DEBUG] No location found, returning None")
        return None

    # Format output in the expected format
    output = {
        "event_location": None,  # We don't have the city name, just coordinates
        "event_dates": {
            "start": data["availability_window"]["start"],
            "end": data["availability_window"]["end"],
            "hours": duration_hours
        },
        "event_duration": data.get("event_duration", {}),
        "attendee_travel_hours": {city: 0.0 for city in starting_locs},  # Placeholder
        "raw_input": data,
        "filtered_candidates": {}
    }

    print(f"[DEBUG] returning output: {output}")
    return output
    
    
