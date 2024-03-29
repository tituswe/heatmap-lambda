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
    file_type = body['fileType'] 
    file_bytes = base64.b64decode(base64_encoded_data)

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
    df = df[["platform", "date", "time", "sku", "price", "qty", "name", "lat", "lng"]]

    unique_platforms = df["platform"].unique()
    unique_products = df["name"].unique()
    unique_skus = df["sku"].unique()
    min_price = df["price"].min()
    max_price = df["price"].max()
    min_qty = df["qty"].min()
    max_qty = df["qty"].max()


    data = {
        "metaData": {
            "platformOptions": unique_platforms.tolist(),
            "productOptions": unique_products.tolist(),
            "skuOptions": unique_skus.tolist(),
            "minPrice": min_price,
            "maxPrice": max_price,
            "minQty": min_qty,
            "maxQty": max_qty,
        },
        "orderData": df.to_dict(orient="records"),
        "input": event
    }

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