# BitcoinPriceDemo

Objective:
Develop an automated and scalable process to obtain the 5-day moving
average of Bitcoin’s price during the first quarter of 2022.
Brief:
The finance team needs to analyze Bitcoin's behavior to determine if it's feasible
to invest in this cryptocurrency. Your task is to automate this process and be
prepared for real-time adjustments when necessary.
Tasks:
1. Explore the Crypto API CoinGecko API Documentation
2. Create a DEMO account in the API to avoid any costs.
3. Retrieve a list of all cryptocurrencies with id, name, and symbol (using the
CoinGecko API).
4. Retrieve the Bitcoin coin id.
5. Get Bitcoin’s price in USD and by date for the first quarter of 2022 (using the
CoinGecko API).
6. Save the data in the database of your choice.
7. Use the data previously stored in the database to calculate the 5-day
moving average using a window/partition function in Python (you may
use either pandas or PySpark).

Extras:
● PySpark: Use PySpark instead of pandas for the window/partition function
to handle larger datasets and distributed processing (Extra point).
● Iceberg: Save the information using Apache Iceberg in a local environment
(Extra point, not mandatory).
● Documentation: Provide clear documentation in a README explaining how
to run your code and the assumptions made (Extra point).
● Testing: Add unit or integration tests to ensure the process works correctly
(Extra point).
● GitHub: Share your code in a public GitHub repository and provide the link
before the interview.
● Visualization: Use a tool of your choice to visualize the results in a graph
(Extra point).
● Scalability Plan: Include a brief plan explaining how you would scale the
solution to handle real-time data for multiple cryptocurrencies (Extra
point).
● Data Analysis: Provide a brief analysis of Bitcoin's price behavior based on
the moving average (Extra point).
