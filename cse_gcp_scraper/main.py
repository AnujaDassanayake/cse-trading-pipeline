# main.py

import requests
import datetime
import pytz
import pandas as pd
import os
from google.cloud import storage
from flask import jsonify

def scrape_cse_and_upload(request):
    """
    Google Cloud Function entry point to scrape Colombo Stock Exchange (CSE) trading data
    and upload it as a CSV to Google Cloud Storage.
    """

    try:
        # Generate current date and timestamp
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        today_colombo = utc_now.astimezone(pytz.timezone("Asia/Colombo"))
        today_stem = today_colombo.strftime('%Y%m%d')
        today_str = today_colombo.strftime('%Y-%m-%d')

        # Prepare headers for the CSE API
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Referer': 'https://www.cse.lk/pages/trade-summary/trade-summary.component.html',
            'Origin': 'https://www.cse.lk',
        }

        # Make the POST request to fetch CSE trading data
        r = requests.post('https://www.cse.lk/api/tradeSummary', headers=headers, json={})

        if r.status_code != 200:
            return jsonify({
                "status": "fail",
                "message": f"Failed to fetch data: {r.status_code}",
                "details": r.text
            }), 500

        jsony = r.json()
        listo = jsony.get('reqTradeSummery', [])

        if not listo:
            return jsonify({
                "status": "fail",
                "message": "No data returned from CSE API."
            }), 500

        # Convert to DataFrame and add metadata columns
        df = pd.DataFrame.from_records(listo)
        df['Scrape_time_UTC'] = utc_now
        df['Date_Colombo'] = today_str

        # Save locally in /tmp (required by Cloud Functions)
        local_csv = f"/tmp/{today_stem}.csv"
        df.to_csv(local_csv, index=False)

        # Upload to Google Cloud Storage
        bucket_name = os.environ.get('BUCKET_NAME')
        if not bucket_name:
            return jsonify({
                "status": "fail",
                "message": "BUCKET_NAME environment variable not set."
            }), 500

        destination_blob_name = f"cse_trading_data/{today_stem}.csv"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_csv)

        return jsonify({
            "status": "success",
            "message": f"Uploaded {destination_blob_name} to bucket {bucket_name}.",
            "records_scraped": len(df)
        }), 200

    except Exception as e:
        return jsonify({
            "status": "fail",
            "message": "An error occurred while scraping or uploading.",
            "error": str(e)
        }), 500
