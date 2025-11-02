from typing import Generator, Iterable, Dict, Any, Optional
import requests
import json


def get_flights_non_streaming(base_url: str,
                               start_airport: str,
                               end_airport: str,
                               start_date: Optional[str] = None,
                               end_date: Optional[str] = None,
                               timeout: int = 30,
                               headers: Optional[Dict[str, str]] = None,
                               endpoint: str = '/flights') -> Iterable[Dict[str, Any]]:
    url = base_url.rstrip('/') + endpoint
    params = {
        'depart_airport': start_airport,
        'arrive_airport': end_airport,
    }
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date

    hdrs = headers or {}
    resp = requests.get(url, params=params, headers=hdrs, timeout=timeout)
    resp.raise_for_status()
    # The Swagger docs say this returns a JSON array
    return resp.json()


def filter_candidate_cities(payload: Dict[str, Any]) -> None:
    base = "http://10.249.36.85"
    url = f"{base}/filter-candidates"

    try:
        resp = requests.post(url, json=payload, timeout=15, headers={"Accept":"application/json"})
        resp.raise_for_status()
    except requests.RequestException as e:
        print("Request failed:", e)
    else:
        data = resp.json()
        #print(data.get("filtered_candidates", {}))
        
        return data.get("filtered_candidates", {})
