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
2. Run `jupyter notebook` to open Jupyter Notebook
3. Navigate to **python_ETL.ipynb**
	* SOLite database is used in this exercise 
	* This notebook will take 'morse.csv' and [Historical Hourly Weather Data] 2012-2017 for 30 US & Canadian Cities + 6 Israeli Cities (https://www.kaggle.com/selfishgene/historical-hourly-weather-data) and create **'sales.db'** based on the following data model. San Francisco weather is used to estimate W.C. Morse cafe (37.831106, -122.254110). 
	* ![alt text](https://github.com/xinlutu2/Data_Engineer_Sales_Weather/blob/master/images/data_model.png 'Data Model')
4. Navigate to **python_ETL.ipynb**
	* This notebook will run queries to generate two required reports. 
	* The reports are saved as **report1.csv** and **report2.csv**
5. MySQL Scripts to create same tables and reports are also included in **MySQL_scripts.sql** 


## Productize Reports 
Below is a sample design to productize the reports:
![alt text](https://github.com/xinlutu2/Data_Engineer_Sales_Weather/blob/master/images/design.png 'Data Model')

	* Data Modeling with be similar to the data model shown for local ETL process, but more comprehensive based on production report need. 
	* For historical on-premise database (MySQL/Oracle), data will be partitioned by year. For analytics data mart (RedShift is an example), data will be partitioned by month. Sales data will be partitioned by store and then by year
	# The enriched RedShift monthly data will be used to analytics and machine learning based on needs and data will be backfilled to historical on-premise database once it reach a full year complete data

Airflow is the tool considered for ETL pipeline and there is one simple Airflow data pipeline included under **Airflow_demo** directory, extact weather data from [Weather API](https://openweathermap.org/api) -> Flatten to .csv -> Load to S3 bucket. (Free account for [Weather API](https://openweathermap.org/api) can only fetch dailiy data. Historical data costs more)

## Tradeoffs and Assumptions
The [weather](https://www.kaggle.com/selfishgene/historical-hourly-weather-data) historical data (.csv) from Kaggle are excepted to be complete and accurate for current ETL process. 
AWS is taken as an example in ETL design. Google Cloud Platform (GCP) and Azure can also achieve same purpose.  
The ETL design is for batch process, streaming capability is not included in the design.




