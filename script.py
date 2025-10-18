import requests
import os
import csv
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000

def run_stock_job():
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    data = response.json()

    tickers = []

    # Check if 'results' is in the initial response and the status is not an error
    if 'results' in data and data.get('status') != 'ERROR':
        for ticker in data['results']:
            tickers.append(ticker)

    while 'next_url' in data:
        print('requesting next page', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        # Check if 'results' is in the subsequent response and the status is not an error
        if 'results' in data and data.get('status') != 'ERROR':
            for ticker in data['results']:
                tickers.append(ticker)
        else:
            print("Error retrieving next page:", data.get('error', 'Unknown error'))
            break # Exit the loop if there's an error
    else:
        print("Error retrieving initial data:", data.get('error', 'Unknown error'))


    print(len(tickers))

    # Write tickers to CSV
    if tickers:  # Check if tickers list is not empty
        fieldnames = list(tickers[0].keys())
        output_csv = 'tickers.csv'

        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for t in tickers:
                row = {key: t.get(key, '') for key in fieldnames}
                writer.writerow(row)

        print(f'Wrote {len(tickers)} rows to {output_csv}')
    else:
        print("No tickers retrieved to write to CSV.")