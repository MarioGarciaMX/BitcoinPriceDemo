from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col, to_date

def calculate_5day_moving_average(delta_path):
    """Calculates the 5-day moving average of Bitcoin prices from Delta Lake."""

    spark = SparkSession.builder.appName("BitcoinMovingAverage").getOrCreate()

    try:
        # Read data from Delta Lake
        df = spark.read.format("delta").load(delta_path)

        # Create a window specification
        window_spec = Window.orderBy(col("timestamp")).rowsBetween(Window.currentRow - 4, Window.currentRow) #last 5 rows

        # Calculate the 5-day moving average
        df_with_ma = df.withColumn("moving_average_5d", avg(col("price")).over(window_spec))

        # Show the results
        df_with_ma.show()

    except Exception as e:
        print(f"Error calculating moving average: {e}")

    finally:
        spark.stop()

# Example usage:
delta_path = "bitcoin_prices_delta"  # Replace with your Delta Lake path
calculate_5day_moving_average(delta_path)
