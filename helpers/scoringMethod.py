import pandas as pd
import numpy as np
import json

def compute_travel_scores(input_array, weights):
    """Accept a variety of input shapes and produce the scoring JSON.

    Supported input formats:
    - A dict with key 'flights' -> list[dict]
    - A list where each item may be:
      * -1 (sentinel, skipped)
      * dict with 'flights' key
      * list of flight dicts
      * single flight dict
    - A direct list of flight dicts
    """
    rows = []

    # Normalize different input shapes into a flat list of flight dicts
    if isinstance(input_array, dict) and 'flights' in input_array:
        rows = list(input_array['flights'])
    elif isinstance(input_array, list):
        for item in input_array:
            if item == -1 or item is None:
                continue
            if isinstance(item, dict) and 'flights' in item and isinstance(item['flights'], list):
                rows.extend(item['flights'])
            elif isinstance(item, list):
                rows.extend([f for f in item if isinstance(f, dict)])
            elif isinstance(item, dict):
                # single flight dict
                rows.append(item)
            else:
                # unknown element type: skip
                continue
    else:
        # Unknown top-level shape: try to coerce to list
        try:
            rows = list(input_array)
        except Exception:
            rows = []

    # Build DataFrame
    if rows:
        df = pd.DataFrame(rows)
    else:
        # Return empty result when no data available
        df = pd.DataFrame()

    # Handle empty dataframe
    if df.empty:
        return json.dumps({
            "event_location": None,
            "event_dates": None,
            "event_span": None,
            "total_co2": 0,
            "average_travel_hours": 0,
            "median_travel_hours": 0,
            "max_travel_hours": 0,
            "min_travel_hours": 0,
            "attendee_travel_hours": None,
        }, indent=4)

    # Normalize column names to lower-case for downstream code
    df.columns = [c.lower() for c in df.columns]

    # Map known source column names to the ones expected by scoring routines
    col_map = {}
    if 'estimated_co2_total_tonnes' in df.columns:
        col_map['estimated_co2_total_tonnes'] = 'total_co2'
    if 'estimated_fuel_burn_total_tonnes' in df.columns:
        col_map['estimated_fuel_burn_total_tonnes'] = 'fuel_burn_tonnes'
    if 'distance' in df.columns:
        col_map['distance'] = 'distance'

    if col_map:
        df = df.rename(columns=col_map)

    # Coerce numeric columns
    for col in ['total_co2', 'fuel_burn_tonnes', 'distance']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Ensure scoring routine required columns exist; add sensible defaults if missing
    if 'max_travel_hours' not in df.columns:
        df['max_travel_hours'] = 0.0
    if 'average_travel_hours' not in df.columns:
        df['average_travel_hours'] = 0.0
    if 'median_travel_hours' not in df.columns:
        df['median_travel_hours'] = 0.0
    if 'total_co2' not in df.columns:
        df['total_co2'] = 0.0

    df = normalize_results(df)
    df, winner = calculate_best_option(df, weights)
    return format_winner_as_json(winner)

def normalize_results(results_df):
    """
    Applies min-max normalization to the core scoring columns.
    It ignores all other columns and just carries them along.
    """
    df = results_df.copy()

    # --- Normalize CO2 ---
    if "total_co2" in df.columns and len(df) > 0 and not df["total_co2"].isna().all():
        min_co2 = df["total_co2"].min()
        max_co2 = df["total_co2"].max()
        co2_range = max_co2 - min_co2
        df["co2_norm"] = np.where(co2_range == 0, 0.0, (df["total_co2"] - min_co2) / co2_range)
    else:
        df["co2_norm"] = 0.0

    # --- Normalize Time ---
    if "max_travel_hours" in df.columns and len(df) > 0 and not df["max_travel_hours"].isna().all():
        min_time = df["max_travel_hours"].min()
        max_time = df["max_travel_hours"].max()
        time_range = max_time - min_time
        df["time_norm"] = np.where(time_range == 0, 0.0, (df["max_travel_hours"] - min_time) / time_range)
    else:
        df["time_norm"] = 0.0

    return df

def calculate_best_option(normalized_df, weights):
    """
    Calculates the best option from a normalized DataFrame based on user weights.
    Returns the full, sorted DataFrame and the single best row (as a Series).
    """
    df = normalized_df.copy()

    # Handle empty dataframe
    if df.empty:
        return df, pd.Series()

    # Start with a score of 0
    df['final_score'] = 0.0

    # Add scores based on the weights provided
    if isinstance(weights, dict):
        if 'time' in weights and 'time_norm' in df.columns:
            df['final_score'] += df['time_norm'] * weights['time']
        if 'co2' in weights and 'co2_norm' in df.columns:
            df['final_score'] += df['co2_norm'] * weights['co2']
        if 'cost' in weights and 'cost_norm' in df.columns:
            df['final_score'] += df['cost_norm'] * weights['cost']

    sorted_df = df.sort_values(by='final_score', ascending=True)
    best_option_row = sorted_df.iloc[0]

    return sorted_df, best_option_row


def format_winner_as_json(winner_row):
    """
    Converts the winning pandas Series (row) into the required
    JSON output format.
    """

    if winner_row.empty or len(winner_row) == 0:
        # Return empty template when no data
        output_dict = {
            "event_location": None,
            "event_dates": None,
            "event_span": None,
            "total_co2": 0,
            "average_travel_hours": 0,
            "median_travel_hours": 0,
            "max_travel_hours": 0,
            "min_travel_hours": 0,
            "attendee_travel_hours": None,
        }
    else:
        data = winner_row.to_dict()

        output_dict = {
            "event_location": data.get("candidate_city"),
            "event_dates": data.get("event_dates"),
            "event_span": data.get("event_span"),
            "total_co2": data.get("total_co2"),
            "average_travel_hours": data.get("average_travel_hours"),
            "median_travel_hours": data.get("median_travel_hours"),
            "max_travel_hours": data.get("max_travel_hours"),
            "min_travel_hours": data.get("min_travel_hours"),
            "attendee_travel_hours": data.get("attendee_travel_hours"),
        }

    return json.dumps(output_dict, indent=4)


"""
# Normalize the scoring columns
normalized_df = normalize_results(raw_data_df)

# Set user priorities
user_weights = {
    'time': 0.7,  
    'co2':  0.30
}

# 4. Find the best option
#    'winner' is a pandas Series containing all data for the best row
all_scores, winner = calculate_best_option(normalized_df, user_weights)

# 5. Generate the final JSON output
final_json_output = format_winner_as_json(winner)

print("\n--- THE WINNER ---")
print(final_json_output)
"""
