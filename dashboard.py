import streamlit as st
import pandas as pd
import json
import pydeck as pdk
import time
import random
from datetime import datetime, timedelta
from candidates import CANDIDATE_CITIES, get_airport_code_by_city
from filter import get_coordinates_from_nominatim, get_filtered_candidates


@st.cache_data
def city_to_coords(city_name: str) -> tuple:
    """
    Convert city name to latitude and longitude coordinates.
    Uses candidate cities database for fast lookup, falls back to Nominatim.

    Args:
        city_name: Name or airport code of the city

    Returns:
        Tuple of (latitude, longitude), or None if not found
    """
    # Try airport code first (e.g., "JFK")
    if city_name.upper() in CANDIDATE_CITIES:
        info = CANDIDATE_CITIES[city_name.upper()]
        return (info["lat"], info["lon"])

    # Try city name lookup in candidates
    airport_code = get_airport_code_by_city(city_name)
    if airport_code:
        info = CANDIDATE_CITIES[airport_code]
        return (info["lat"], info["lon"])

    # Fallback to Nominatim for unknown cities
    coords = get_coordinates_from_nominatim(city_name)
    if coords:
        return coords

    return None


def create_arc_layer_data(data: dict, animation_progress: float = 1.0) -> pd.DataFrame:
    """
    Transform JSON data into arc layer format.

    Creates arcs from attendee cities to event location.
    Arc width is proportional to travel hours.

    Args:
        data: The JSON data dict
        animation_progress: Float between 0 and 1 representing animation progress
                           0 = at source cities, 1 = at target city
    """
    attendee_travel_hours = data.get("attendee_travel_hours", {})
    event_location = data.get("event_location", "Unknown")

    if not attendee_travel_hours:
        return None

    # Get event location coordinates
    event_coords = city_to_coords(event_location)
    if not event_coords:
        st.error(f"Could not find coordinates for event location: {event_location}")
        return None

    # Create list of arcs with animated positions
    arcs = []
    for city, travel_hours in attendee_travel_hours.items():
        source_coords = city_to_coords(city)

        if not source_coords:
            st.warning(f"Could not find coordinates for city: {city}")
            continue

        # Interpolate position based on animation progress
        anim_lat = source_coords[0] + (event_coords[0] - source_coords[0]) * animation_progress
        anim_lon = source_coords[1] + (event_coords[1] - source_coords[1]) * animation_progress

        arcs.append({
            "source_city": city,
            "target_city": event_location,
            "source_lat": source_coords[0],
            "source_lon": source_coords[1],
            "target_lat": event_coords[0],
            "target_lon": event_coords[1],
            "anim_lat": anim_lat,
            "anim_lon": anim_lon,
            "travel_hours": travel_hours,
        })

    if not arcs:
        return None

    return pd.DataFrame(arcs)


def convert_input_to_output(input_data: dict) -> dict:
    """
    Convert input format (attendees, availability_window, event_duration) to output format.

    This is a stub - actual implementation will be done by other code.
    For now, returns basic conversion with placeholder event location.

    Args:
        input_data: Input dict with attendees, availability_window, event_duration

    Returns:
        Output dict with event_location and attendee_travel_hours
    """
    attendees = input_data.get("attendees", {})

    # Placeholder: use the first attendee's city as event location
    # TODO: This will be determined by optimization algorithm
    event_location = list(attendees.keys())[0] if attendees else "New York"

    # Placeholder travel hours - TODO: will be calculated based on flights
    attendee_travel_hours = {city: 10.0 for city in attendees.keys()}

    return {
        "event_location": event_location,
        "event_dates": input_data.get("availability_window", {}),
        "event_duration": input_data.get("event_duration", {}),
        "attendee_travel_hours": attendee_travel_hours,
        "raw_input": input_data
    }


def generate_random_output(input_data: dict) -> dict:
    """
    Generate random output by filtering candidates and selecting random event location.

    Args:
        input_data: Input dict with attendees, availability_window, event_duration

    Returns:
        Output dict with randomly selected event_location and random time slot
    """
    attendees = input_data.get("attendees", {})
    availability_window = input_data.get("availability_window", {})
    event_duration = input_data.get("event_duration", {})

    if not attendees:
        st.error("No attendees in input data")
        return None

    # Get filtered candidates based on attendee locations
    try:
        filter_result = get_filtered_candidates(list(attendees.keys()))
        filtered_candidates = filter_result.get("filtered_candidates", {})

        if not filtered_candidates:
            st.warning("No candidates found within polygon. Using all attendees as candidates.")
            candidate_cities = list(attendees.keys())
        else:
            candidate_cities = [info["city"] for info in filtered_candidates.values()]
    except Exception as e:
        st.warning(f"Could not filter candidates: {e}. Using all attendees as candidates.")
        candidate_cities = list(attendees.keys())

    # Randomly select event location from candidates
    event_location = random.choice(candidate_cities)

    # Parse availability window and generate random time slot
    try:
        start_dt = datetime.fromisoformat(availability_window["start"].replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(availability_window["end"].replace("Z", "+00:00"))

        # Event duration in hours
        duration_hours = event_duration.get("hours", 4) + (event_duration.get("days", 0) * 24)

        # Random start time within window, leaving room for event duration
        time_available = (end_dt - start_dt).total_seconds() / 3600 - duration_hours
        if time_available > 0:
            random_hours = random.uniform(0, time_available)
            event_start = start_dt + timedelta(hours=random_hours)
            event_end = event_start + timedelta(hours=duration_hours)
        else:
            event_start = start_dt
            event_end = end_dt

        event_dates = {
            "start": event_start.isoformat(),
            "end": event_end.isoformat(),
            "hours": (event_end - event_start).total_seconds() / 3600
        }
    except Exception as e:
        st.warning(f"Could not parse dates: {e}")
        event_dates = availability_window

    # Placeholder travel hours
    attendee_travel_hours = {city: random.uniform(5, 20) for city in attendees.keys()}

    return {
        "event_location": event_location,
        "event_dates": event_dates,
        "event_duration": event_duration,
        "attendee_travel_hours": attendee_travel_hours,
        "raw_input": input_data,
        "filtered_candidates": filtered_candidates if filtered_candidates else {}
    }


def main():
    st.title("WorkerBees Dashboard")
    st.write("Meeting location optimizer with travel visualization")

    # Initialize session state for map rendering
    if "last_output_data" not in st.session_state:
        st.session_state.last_output_data = None

    # Control sliders
    c1, c2 = st.columns(2)
    with c1:
        time_slider = st.slider("Time Progress", 0, 100, 100) / 100
    with c2:
        co2_level = st.slider("CO2 Level Filter", 0, 100, 100) / 100

    upload_json = st.file_uploader("Upload optimization input JSON", type=["json"])

    col1, col2, col3 = st.columns(3)
    with col1:
        use_random = st.button("🎲 Random", help="Generate random meeting location from filtered candidates", use_container_width=True)
    with col2:
        optimize_btn = st.button("⚙️ Optimize", help="Run optimization algorithm", use_container_width=True)
    with col3:
        export_placeholder = st.empty()

    if upload_json:
        try:
            input_data = json.load(upload_json)

            # Convert input to output format
            if use_random:
                output_data = generate_random_output(input_data)
                if output_data is None:
                    return
                st.success("Generated random meeting location and time slot!")
            else:
                output_data = convert_input_to_output(input_data)

            # Store output data in session state for map updates
            st.session_state.last_output_data = output_data

        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON: {e}")
            return
        except Exception as e:
            st.error(f"Error processing file: {e}")
            return

        # Create download button (outside try block to ensure it renders on first pass)
        if st.session_state.last_output_data:
            output_data = st.session_state.last_output_data
            export_data = {k: v for k, v in output_data.items() if k not in ["raw_input", "filtered_candidates"]}
            export_json = json.dumps(export_data, indent=2)
            with export_placeholder:
                st.download_button(
                    label="📥 Export",
                    data=export_json,
                    file_name="output.json",
                    mime="application/json",
                    use_container_width=True
                )

        # Create arc layer visualization with animation progress from slider
        if st.session_state.last_output_data:
            arc_data = create_arc_layer_data(st.session_state.last_output_data, animation_progress=time_slider)
        else:
            arc_data = None

        if arc_data is not None and len(arc_data) > 0:
            st.subheader("Travel Paths Visualization")

            # Get event location coordinates
            event_location = output_data.get("event_location", "Unknown")
            event_coords = city_to_coords(event_location)

            # Create the arc layer
            arc_layer = pdk.Layer(
                "ArcLayer",
                data=arc_data,
                get_source_position=["source_lon", "source_lat"],
                get_target_position=["target_lon", "target_lat"],
                get_source_color=[255, 100, 0, 160],
                get_target_color=[200, 30, 0, 160],
                auto_highlight=True,
                width_scale=0.0001,
                get_width="travel_hours",
                width_min_pixels=3,
                width_max_pixels=30,
                get_tilt=1,
            )

            # Calculate map center and zoom to fit all coordinates
            attendee_travel_hours = output_data.get("attendee_travel_hours", {})

            # Collect all valid coordinates
            coords_list = []
            if event_coords:
                coords_list.append(event_coords)

            for city in attendee_travel_hours.keys():
                coords = city_to_coords(city)
                if coords:
                    coords_list.append(coords)

            if coords_list:
                all_lats = [coord[0] for coord in coords_list]
                all_lons = [coord[1] for coord in coords_list]

                min_lat, max_lat = min(all_lats), max(all_lats)
                min_lon, max_lon = min(all_lons), max(all_lons)

                center_lat = (min_lat + max_lat) / 2
                center_lon = (min_lon + max_lon) / 2

                # Calculate zoom based on bounding box
                import math
                lat_range = max_lat - min_lat
                lon_range = max_lon - min_lon

                # Add 20% padding
                lat_range *= 1.2
                lon_range *= 1.2

                # Zoom formula: fit the range in the viewport
                if lat_range < 0.01 and lon_range < 0.01:
                    # Very small area - zoom in
                    zoom = 12
                else:
                    # Calculate based on range
                    # At zoom level z, approximately 360 degrees / 2^z degrees per pixel
                    max_range = max(lat_range, lon_range)
                    if max_range > 0:
                        zoom = max(1, min(24, int(9 - math.log2(max_range))))
                    else:
                        zoom = 9
            else:
                # Fallback to world view
                center_lat = 20
                center_lon = 0
                zoom = 2

            view_state = {
                "latitude": center_lat,
                "longitude": center_lon,
                "zoom": int(zoom),
                "pitch": 50,
            }

            try:
                st.pydeck_chart(
                    pdk.Deck(
                        map_style=None,
                        initial_view_state=view_state,
                        layers=[arc_layer],
                    )
                )
            except Exception as e:
                st.error(f"Error rendering arc layer map: {e}")

                # Fallback: show simple map with points
                st.write("**Fallback: Simple map visualization**")
                map_data = pd.DataFrame({
                    "latitude": [event_coords[0]] + [city_to_coords(city)[0] for city in attendee_travel_hours.keys()],
                    "longitude": [event_coords[1]] + [city_to_coords(city)[1] for city in attendee_travel_hours.keys()],
                })
                st.map(map_data)
        else:
            st.warning("No attendee travel data found in JSON.")

        # Display JSON data at the end
        st.divider()
        col_input, col_output = st.columns(2)

        with col_input:
            st.subheader("Input Data")
            st.json(input_data)

        with col_output:
            st.subheader("Output Data")
            st.json({k: v for k, v in output_data.items() if k not in ["raw_input", "filtered_candidates"]})

    else:
        st.markdown("""
        ### Expected Input JSON Format:
        ```json
        {
            "attendees": {
                "Mumbai": 2,
                "Shanghai": 3,
                "Hong Kong": 1,
                "Singapore": 2,
                "Sydney": 2
            },
            "availability_window": {
                "start": "2025-12-10T09:00:00Z",
                "end": "2025-12-15T17:00:00Z"
            },
            "event_duration": {
                "days": 0,
                "hours": 4
            }
        }
        ```
        """)

if __name__ == "__main__":
    main()
