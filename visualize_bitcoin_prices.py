import requests
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col, to_timestamp
import matplotlib.pyplot as plt  # Import matplotlib for plotting

def visualize_bitcoin_prices(delta_path):
    """Visualizes Bitcoin prices and the 5-day moving average."""

    spark = SparkSession.builder.appName("BitcoinVisualization").getOrCreate()

    try:
        df = spark.read.format("delta").load(delta_path)

        window_spec = Window.orderBy(col("timestamp")).rowsBetween(Window.currentRow - 4, Window.currentRow)
        df_with_ma = df.withColumn("moving_average_5d", avg(col("price")).over(window_spec))

        # Convert to pandas DataFrame for plotting
        pandas_df = df_with_ma.toPandas()
        pandas_df['timestamp'] = pandas_df['timestamp'].dt.date #extract date only from timestamp

        # Plotting
        plt.figure(figsize=(12, 6))
        plt.plot(pandas_df['timestamp'], pandas_df['price'], label='Bitcoin Price (USD)')
        plt.plot(pandas_df['timestamp'], pandas_df['moving_average_5d'], label='5-Day Moving Average')

        plt.title('Bitcoin Price and 5-Day Moving Average')
        plt.xlabel('Date')
        plt.ylabel('Price (USD)')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45) #rotate x axis labels for better readability
        plt.tight_layout() #adjust layout to prevent labels from being cut off
        plt.show()

    except Exception as e:
        print(f"Error visualizing data: {e}")

    finally:
        spark.stop()

delta_path = "bitcoin_prices_delta"
try:
   visualize_bitcoin_prices(delta_path) #visualize after saving
except Exception as e:
    print(f"Error tring to visualize the data: {e}")
