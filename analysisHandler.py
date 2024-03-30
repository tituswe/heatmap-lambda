import base64
from io import BytesIO
import json

import pandas as pd

from modules.analysis import *

def analyze(event, context):
    # Extract data from event
    print("Extracting data...")
    body = json.loads(event["body"])
    base64_encoded_data = body['data'] 
    file_bytes = base64.b64decode(base64_encoded_data)
    file_type = body['fileType'] 
    filters = body['filters']

    if file_type == 'text/csv':
        df = pd.read_csv(BytesIO(file_bytes))
    elif file_type in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
        df = pd.read_excel(BytesIO(file_bytes))
    else:
        return {
            'statusCode': 400,
            'body': 'Unsupported file type'
        }
    
    print("Purging...")
    df = purge(df)
    print("Processing platform...")
    df = process_platform(df)
    print("Processing date_time...")
    df = process_date_time(df)
    print("Processing sku...")
    df = process_sku(df)
    print("Processing postal_code...")
    df = process_postal_code(df)
    print("Done")
    df = df[["key", "platform", "date", "time", "sku", "price", "qty", "name", "lat", "lng"]]

    filtered_df = df.copy()
    filtered_df = filtered_df[filtered_df["platform"].isin(filters["selectedPlatforms"])]
    filtered_df = filtered_df[filtered_df["name"].isin(filters["selectedProducts"])]
    filtered_df = filtered_df[filtered_df["sku"].isin(filters["selectedSku"])]
    filtered_df = filtered_df[filtered_df["price"] >= float(filters["lowPrice"])]
    filtered_df = filtered_df[filtered_df["price"] <= float(filters["highPrice"])]
    filtered_df = filtered_df[filtered_df["qty"] >= int(filters["lowQty"])]
    filtered_df = filtered_df[filtered_df["qty"] <= int(filters["highQty"])]

    # heatmap_df = filtered_df.groupby(["lat", "lng"]).size().reset_index(name="weight").head(1000)
    heatmap_df = filtered_df[["key", "name", "lat", "lng"]].drop_duplicates(subset="key").head(2000)


    data = heatmap_df.to_dict(orient="records")

    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "message": "All good!",
            "data": data
        })
    }

    return response