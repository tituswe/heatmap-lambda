from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd


def purge(data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with missing values
    """

    df = data.copy()
    df.columns = ["id", "date_time", "description", "sku_var", "price", "qty", "postal_code", "name", "sku_final", "cust_id"]
    df["key"] = df["id"] + df["cust_id"]
    df = df.dropna()

    return df


def process_platform(data: pd.DataFrame) -> pd.DataFrame:
    """
    Process platform to extract platform name
    """

    df = data.copy()
    df["platform"] = df["id"].apply(lambda x: x.split("-")[0]).dropna()

    return df # id


def process_date_time(data: pd.DataFrame) -> pd.DataFrame:
    """
    Process date_time to extract date and time
    """

    df = data.copy()
    df["date_time"] = pd.to_datetime(df["date_time"])
    df["date"] = df["date_time"].dt.date.astype(str)
    df["time"] = df["date_time"].dt.time.astype(str)

    return df # date, time


def process_sku(data: pd.DataFrame) -> pd.DataFrame:
    """
    Process sku_var to extract sku and variant
    """

    df = data.copy()
    df['sku'] = df['sku_final']

    return df # sku


def process_postal_code(data: pd.DataFrame) -> pd.DataFrame:
    """
    Process postal code to extract lat, lng
    """

    postal_coord_map = pd.read_csv("modules/postal_coord_map.csv")
    postal_coord_map["postal_code"] = postal_coord_map["postal_code"].astype(float)

    df = data.copy()
    df["postal_code"] = df["postal_code"].str.extract("(\d{6})").dropna().astype(float)
    df = df.merge(postal_coord_map, how="left", on="postal_code").rename(columns={"lon": 'lng'}).dropna()

    return df # lat, lng
