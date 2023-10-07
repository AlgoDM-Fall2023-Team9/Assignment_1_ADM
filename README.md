# Assignment_1_ADM

## About

This project is a Python application for executing Snowflake SQL queries using a Streamlit-based user interface. It provides various predefined queries and allows users to run them by providing the required parameters.

## Getting Started

Follow these steps to get started with the project.

### Prerequisites

Before running the application, make sure you have the following prerequisites installed:

- Python 3.x

- Required Python packages (specified in `requirements.txt`)

- Snowflake account credentials (username, password, account identifier, etc.)

### Installation

1. Clone the repository:

bash

git clone https://github.com/your-username/your-project.git

cd your-project

## Install the required dependencies:

pip install -r requirements.txt

To run the application, use the following command: streamlit run your_app.py This will start a local Streamlit server, and you can access the application in your web browser.

## Supported Queries

The application supports the following Snowflake SQL queries:

Query 1: Find Customers with High Catalog Returns

Description: Find customers and their detailed customer data who have returned items bought from the catalog more than 20 percent the average customer returns for customers in a given state in a given time period. Order output by customer data.

Query 2: Customers with High Online Spending

Description: Find customers who tend to spend more money (net-paid) on-line than in stores.

Query 3: High Return Items

Description: Retrieve the items with the highest number of returns where the number of returns was approximately equivalent across all store, catalog, and web channels (within a tolerance of +/- 10%), within the week ending a given date.

Query 4: List Customers by Income and City

Description: List all customers living in a specified city, with an income between two specified values.

Query 5: Web Return Reasons Analysis

Description: For all web return reasons, calculate the average sales, average refunded cash, and average return fee by different combinations of customer and sales types.

Query 6: Web Sales Rollup

Description: Rollup the web sales for a given year by category and class, and rank the sales among peers within the parent, for each group compute the sum of sales, location with the hierarchy, and rank within the group.

Query 7: Count Customers with Various Purchases

Description: Count how many customers have ordered on the same day items on the web and the catalog and on the same day have bought items in a store.

Query 8: Sales Analysis by Time and Customer Demographics

Description: Analyze how many items are sold between specific times of a day in certain stores to customers with specific demographic characteristics.

Query 9: Monthly Sales Analysis

Description: Within a year, list all months and combinations of item categories, classes, and brands that have had monthly sales larger than 0.1 percent of the total yearly sales.

Query 10: AM vs. PM Purchase Ratio

Description: Calculate the ratio between the number of items sold over the internet in the morning (8 to 9 am) to the number of items sold in the evening (7 to 8 pm) of customers with a specified number of dependents. Consider only websites with a high amount of content.

Please refer to the application's user interface for more details on how to use these queries and provide the required parameters.

The Streamlit application (your_app.py) provides an interactive interface for running the supported Snowflake queries. It allows you to select a query, input the required parameters, and execute the query to view the results.

## Streamlit Application

The heart of this project is the Streamlit application (`main.py`) that provides an interactive interface for running Snowflake queries. Here, we'll provide an overview of the code structure and how users can interact with the application.

### Code Structure

The Streamlit application code (`main.py`) is structured as follows:

- **Importing Libraries**: We begin by importing the necessary Python libraries, including Streamlit, Pandas, and Snowflake Connector.

- **Snowflake Connection**: The Snowflake connection details are set up using the `sqlalchemy` library. You'll need to provide your Snowflake account credentials and database information in this section.

- **Query Functions**: There are separate functions for each supported query (e.g., `Q1`, `Q2`, etc.). These functions take user inputs (parameters) and return the corresponding Snowflake SQL query.

- **Streamlit UI**: The Streamlit user interface is defined using Streamlit widgets such as `st.title`, `st.selectbox`, `st.text_input`, and `st.button`. Users can select a query from a dropdown, enter parameters, and click a button to execute the query.

- **Executing Queries**: Upon clicking the "Run Query" button, the selected query is executed, and the results are displayed in a Pandas DataFrame. The results are then shown using `st.write`.

- **Error Handling**: Basic error handling is included to handle any exceptions that may occur during query execution.

### Interacting with the Application

1. **Select a Query**: Users can choose a specific query from the dropdown menu. Each query has a description explaining its purpose.

2. **Input Parameters**: Depending on the selected query, users need to provide the required input parameters. These parameters can include years, states, date ranges, income thresholds, and other query-specific values.

3. **Run Query**: After entering the parameters, users can click the "Run Query" button to execute the query.

4. **View Results**: The results of the query are displayed below the "Run Query" button in a tabular format. Users can scroll through the results to view the data.

5. **Query Execution**: Behind the scenes, the Streamlit application connects to your Snowflake account, sends the SQL query with user-provided parameters, retrieves the results, and displays them in real-time.

This Streamlit-based interface provides an accessible way to interact with Snowflake data and analyze it using predefined queries.

Feel free to customize the Streamlit code (`main.py`) further to match your project's requirements and enhance the user experience
