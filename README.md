# BitcoinPriceDemo

Objective:
Develop an automated and scalable process to obtain the 5-day moving
average of Bitcoin’s price during the first quarter of 2022.
Brief:
The finance team needs to analyze Bitcoin's behavior to determine if it's feasible
to invest in this cryptocurrency. Your task is to automate this process and be
prepared for real-time adjustments when necessary.


### Fetch the data from the api ###
1. Clearer Function Structure:
   The code is organized into well-defined functions (get_cryptocurrencies, get_bitcoin_id, get_bitcoin_price_usd_by_date), making it more readable and maintainable.
2. Date Handling:
   Uses the datetime module to handle dates and timestamps correctly. The timestamps required by the coingecko API need to be in seconds since epoch. The code now converts the datetime objects to the correct format.
3. Bitcoin ID Retrieval:
   The get_bitcoin_id function now efficiently searches the list of cryptocurrencies to find the Bitcoin ID.
4. Error Handling:
   The code now includes robust error handling using try...except blocks to catch potential requests.exceptions.RequestException errors (e.g., network issues, invalid URLs). It also uses response.raise_for_status() to check for HTTP errors (4xx and 5xx) and raise an exception if they occur. This makes the code more resilient.
   
5. API Key Handling:
    The api_key parameter is added to each function that makes an API request.
    The API key is passed in the x-cg-pro-api-key header of the HTTP request.
    The code now retrieves the API key from the environment variable COINGECKO_API_KEY. This is the most secure way to store API keys.
    A warning message is displayed if the API Key is not found.
   
      5.1 Environment Variables:
      Using os.environ.get("COINGECKO_API_KEY") is the recommended way to handle API keys. Avoid hardcoding them directly into your script.

      5.2 Headers:
      The headers dictionary is created, and if an API key is provided, it is added to the headers of the request.

      5.3 Function Parameter:
      The API key is now a parameter of all the functions that use the API. this keeps the code organized.

      5.4 Security:
      By using environment variables, you prevent your API key from being accidentally exposed in your code or version control.

6. Spark Session Initialization:
    spark = SparkSession.builder.appName("BitcoinPricesDelta").getOrCreate() initializes a Spark session.

      6.1 Spark DataFrame Creation:
      df = spark.createDataFrame(bitcoin_prices) creates a Spark DataFrame from the bitcoin_prices list of dictionaries.
      df = df.withColumn("timestamp", to_timestamp(col("timestamp"))) converts the timestamp column from Unix timestamps to Spark timestamps.

      6.2 Delta Lake Saving:
      delta_path = "bitcoin_prices_delta" sets the path where the Delta Lake table will be saved.
      df.write.format("delta").mode("overwrite").save(delta_path) writes the DataFrame to Delta Lake format.
      format("delta") specifies the Delta Lake format.
      mode("overwrite") overwrites the existing Delta Lake table if it exists. Change it to append if needed.
      save(delta_path) saves the table to the specified path.

      6.3 Error Handling:
      A try...except block handles potential exceptions during the Delta Lake saving process.

      6.4 Spark Session Stop:
      spark.stop() stops the Spark session to release resources.

### window/partition function ###
1. Spark Session Initialization:
    spark = SparkSession.builder.appName("BitcoinMovingAverage").getOrCreate() initializes a Spark session.

2. Read Delta Lake Data:
    df = spark.read.format("delta").load(delta_path) reads the Bitcoin price data from the Delta Lake table.

3. Window Specification:
    window_spec = Window.orderBy(col("timestamp")).rowsBetween(Window.currentRow - 4, Window.currentRow) defines the window specification.
        orderBy(col("timestamp")) orders the rows by the timestamp column.
        rowsBetween(Window.currentRow - 4, Window.currentRow) defines the window frame. It includes the current row and the four preceding rows, resulting in a 5-day window.

4. Moving Average Calculation:
    df_with_ma = df.withColumn("moving_average_5d", avg(col("price")).over(window_spec)) calculates the 5-day moving average.
        avg(col("price")).over(window_spec) calculates the average of the price column within the defined window.
        df.withColumn("moving_average_5d", ...) adds a new column named moving_average_5d to the DataFrame, containing the calculated moving averages.

5. Show Results:
    df_with_ma.show() displays the DataFrame with the moving average column.

### Visualize the Moving Average ###
1. Import matplotlib.pyplot:
    import matplotlib.pyplot as plt is added to import the Matplotlib library for plotting.

2. visualize_bitcoin_prices() Function:
    The visualization logic is encapsulated in this function.
    It reads the data from Delta Lake, calculates the moving average, converts the Spark DataFrame to a pandas DataFrame, and then creates the plot.

3. Pandas Conversion:
    pandas_df = df_with_ma.toPandas() converts the Spark DataFrame to a pandas DataFrame, which is easier to plot with Matplotlib.
    pandas_df['timestamp'] = pandas_df['timestamp'].dt.date extracts the date from the timestamp column.

4. Matplotlib Plotting:
    plt.figure(figsize=(12, 6)) creates a figure with a specified size.
    plt.plot() plots the Bitcoin price and the moving average.
    plt.title(), plt.xlabel(), plt.ylabel(), plt.legend(), plt.grid(), and plt.show() add labels, a legend, grid lines, and display the plot.

5. Error Handling:
    A try...except block handles potential exceptions during the visualization process.

6. X-Axis Rotation:
    plt.xticks(rotation=45) rotates the x axis labels by 45 degrees, which improves readability when dealing with many date labels.

7. Tight Layout:
    plt.tight_layout() adjusts the plot layout to prevent labels from being cut off.
   
8. Visualization call:
    The visualize_bitcoin_prices(delta_path).


### Dependencies ###
    You will need to install pyspark and delta-spark: pip install pyspark delta-spark matplotlib
    You may also need to configure your spark environment to include the delta-spark jar files.  
