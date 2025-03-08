import requests
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
import pyspark
from delta import *
import unittest
import tempfile
import shutil

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

class TestBitcoinDataPipeline(unittest.TestCase):

    def setUp(self):
        self.api_key = os.environ.get("COINGECKO_API_KEY")
        self.start_date = datetime.datetime(2024, 4, 1)
        self.end_date = datetime.datetime(2024, 4, 3) #reduced time for testing
        self.delta_path = tempfile.mkdtemp() #create temp delta path
        self.builder = pyspark.sql.SparkSession.builder.appName("TestBitcoinPricesDelta") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = configure_spark_with_delta_pip(self.builder).getOrCreate()

    def tearDown(self):
        self.spark.stop()
        shutil.rmtree(self.delta_path) #remove temp delta path

    def test_get_cryptocurrencies(self):
        cryptocurrencies = get_cryptocurrencies(self.api_key)
        self.assertIsInstance(cryptocurrencies, list)
        if cryptocurrencies:
            self.assertIsInstance(cryptocurrencies[0], dict)
            self.assertIn("id", cryptocurrencies[0])
            self.assertIn("name", cryptocurrencies[0])
            self.assertIn("symbol", cryptocurrencies[0])

    def test_get_bitcoin_id(self):
        bitcoin_id = get_bitcoin_id(self.api_key)
        if get_cryptocurrencies(self.api_key): #only test if get_cryptocurrencies works
            self.assertIsInstance(bitcoin_id, str) or self.assertIsNone(bitcoin_id) #bitcoin_id can be None or a string

    def test_get_bitcoin_price_usd_by_date(self):
        bitcoin_prices = get_bitcoin_price_usd_by_date(self.start_date, self.end_date, self.api_key)
        self.assertIsInstance(bitcoin_prices, list)
        if bitcoin_prices:
            self.assertIsInstance(bitcoin_prices[0], dict)
            self.assertIn("timestamp", bitcoin_prices[0])
            self.assertIn("price", bitcoin_prices[0])

    def test_save_to_delta_lake(self):
        bitcoin_prices = get_bitcoin_price_usd_by_date(self.start_date, self.end_date, self.api_key)
        if bitcoin_prices:
            df = self.spark.createDataFrame(bitcoin_prices)
            df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
            df.write.format("delta").mode("overwrite").save(self.delta_path)

            # Verify data in Delta Lake
            loaded_df = self.spark.read.format("delta").load(self.delta_path)
            self.assertEqual(loaded_df.count(), len(bitcoin_prices)) #verify the correct number of rows.
            self.assertEqual(loaded_df.columns, df.columns) #verify the correct columns.

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False) #run all tests.
