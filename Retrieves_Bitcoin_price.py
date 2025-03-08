import requests
import datetime
import os  
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
import pyspark
from delta import *


def get_cryptocurrencies(api_key=None):
    """Retrieves a list of all cryptocurrencies with id, name, and symbol."""
    url = "https://api.coingecko.com/api/v3/coins/list"
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        return [{"id": coin["id"], "name": coin["name"], "symbol": coin["symbol"]} for coin in data]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching cryptocurrency list: {e}")
        return None

def get_bitcoin_id(api_key=None):
    """Retrieves the Bitcoin coin ID."""
    cryptocurrencies = get_cryptocurrencies(api_key)
    if cryptocurrencies:
        for coin in cryptocurrencies:
            if coin["symbol"] == "btc":
                return coin["id"]
    return None

def get_bitcoin_price_usd_by_date(start_date, end_date, api_key=None):
    """Retrieves Bitcoin's price in USD by date."""
    bitcoin_id = get_bitcoin_id(api_key)
    if not bitcoin_id:
        print("Bitcoin ID not found.")
        return None

    url = f"https://api.coingecko.com/api/v3/coins/{bitcoin_id}/market_chart/range"
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    params = {
        "vs_currency": "usd",
        "from": start_timestamp,
        "to": end_timestamp,
    }
    headers = {}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        prices = data.get("prices", [])
        return [{"timestamp": timestamp / 1000, "price": price} for timestamp, price in prices]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Bitcoin price: {e}")
        return None

# Retrieve API KEY from enviroent:
API_KEY = os.environ.get("COINGECKO_API_KEY")  # Get API key from environment variable
if not API_KEY:
    print("Warning: COINGECKO_API_KEY environment variable not set. Some API endpoints may be limited.")

cryptocurrencies = get_cryptocurrencies(API_KEY)
if cryptocurrencies:
    print("Cryptocurrencies:")
    for coin in cryptocurrencies[:5]:  # print first 5 for brevity
        print(f"  ID: {coin['id']}, Name: {coin['name']}, Symbol: {coin['symbol']}")

bitcoin_id = get_bitcoin_id(API_KEY)
if bitcoin_id:
    print(f"\nBitcoin ID: {bitcoin_id}")

start_date = datetime.datetime(2024, 4, 1)
end_date = datetime.datetime(2024, 6, 30)

bitcoin_prices = get_bitcoin_price_usd_by_date(start_date, end_date, API_KEY)


delta_path = "bitcoin_prices_delta"  # Choose your Delta Lake path

if bitcoin_prices:
    # Convert bitcoin_prices to a Spark DataFrame
    builder = pyspark.sql.SparkSession.builder.appName("BitcoinPricesDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = spark.createDataFrame(bitcoin_prices)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Save to Delta Lake
    try:
        df.write.format("delta").mode("overwrite").save(delta_path)
        print(f"Bitcoin prices saved to Delta Lake at {delta_path}")
    except Exception as e:
        print(f"Error saving to Delta Lake: {e}")

    spark.stop()  # Stop the Spark session
else:
    print("No bitcoin prices to save.")
