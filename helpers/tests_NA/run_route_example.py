from datetime import datetime
from api_client import get_flights_non_streaming

BASE_URL = 'http://10.249.36.85'

#http://10.249.36.85/flights?start_airport=LIS&end_airport=CGN&start_date=2025-05-01&end_date=2025-05-31


def example_non_streaming():
    print('Calling non-streaming flights endpoint for LIS -> CGN (2025-05-01..2025-05-07)')
    flights = get_flights_non_streaming(BASE_URL, 'LIS', 'CGN', start_date='2025-05-01', end_date='2025-05-07')
    try:
        # flights is expected to be a list-like JSON array
        print('Received', len(flights), 'records')
        if flights:
            print('Sample record keys:', list(flights[0].keys()))
            print('First record (raw):')
            print(flights[0])
    except Exception:
        # If the response was not a list, pretty-print the object
        print('Response (non-list):')
        print(flights)

if __name__ == '__main__':
    example_non_streaming()
