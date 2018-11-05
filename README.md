# Data Engineer Coding Exerise
Simple ETL process with Python and SQLite to generate reports with [weather](https://www.kaggle.com/selfishgene/historical-hourly-weather-data) and sales data.
ETL pipeline design to productize the reports. 
One Simple Airflow pipeline: extact weather data from [Weather API](https://openweathermap.org/api) -> Flatten to .csv -> Load to S3 bucket

## Prerequisites
1. [Python 3.6](https://www.python.org/)
2. [Virtualenv](https://virtualenv.pypa.io/en/latest/)
3. [Jupyter Notebook](http://jupyter.org/)

## Instructions for running the code
1. Run `source env/bin/activate` to activate virtual environment

