# Objectives
### Overview
The objective of this project is to implement a data pipeline for transforming raw Operational Log (OLTP) data from a television system into structured, analytical data for the purpose of generating meaningful insights through a Customer 360 model. The pipeline will facilitate the Extract, Transform, Load (ETL) process to transition data from its transactional state to an analytical state, enabling efficient querying and analysis.

Tech stack: PySpark, MySQL, Power BI, Python.

# Architecture
![image](https://github.com/doanminhquan/The_ETL_job_transforms_and_analyzes_log_data_from_Television_Platform/assets/89577025/3b2d08de-e2b6-42b0-872f-bbded6c303f4)


# Raw data
- Log data is stored as Parquet files organized by date and stored within the file system. Diretory structure:
```
root
├── 20220601
│   └── log_data_20220601.parquet
├── 20220602
│   └── log_data_20220602.parquet
└── ...
```
### Log data schema
```
root
 |-- eventID: string (nullable = true)
 |-- datetime: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- keyword: string (nullable = true)
 |-- category: string (nullable = true)
 |-- proxy_isp: string (nullable = true)
 |-- platform: string (nullable = true)
 |-- networkType: string (nullable = true)
 |-- action: string (nullable = true)
 |-- userPlansMap: array (nullable = true)
 |    |-- element: string (containsNull = true)
```
![image](https://github.com/doanminhquan/The_ETL_job_transforms_and_analyzes_log_data_from_Television_Platform/assets/89577025/4d3fa563-fab1-45ae-9dbe-6821f7170d71)

# Processing data
Processing raw data to obtain valuable clean data:
- Extract the user_id and keyword columns from the log data for each day, filtering out any null values.
- Union the filtered data for each day to get data by month.
- Use window functions and the row_number function in PySpark to identify the most searched keyword of each month.
- Map the most searched keyword found to its corresponding category using data from 'map_search_category.csv'
- Union two dataframes (June & July) to analyze trend of users.
- Create two new columns: 'trending_type' and 'previous' to determine if a category is trending and how it has changed.
- Import processed data into a data warehouse which is MySQL for further analysis and insights.

# Cleansed data
- Cleansed data schema
```
root
 |-- user_id: string (nullable = true)
 |-- most_search_t6: string (nullable = true)
 |-- category_t6: string (nullable = true)
 |-- most_search_t7: string (nullable = true)
 |-- category_t7: string (nullable = true)
 |-- trending_type: string (nullable = false)
 |-- previous: string (nullable = false)
 ```
![image](https://github.com/doanminhquan/The_ETL_job_transforms_and_analyzes_log_data_from_Television_Platform/assets/89577025/ff90e35b-da09-426e-aeef-ef8b8c994aa3)

- Data in MySQL
![image](https://github.com/doanminhquan/The_ETL_job_transforms_and_analyzes_log_data_from_Television_Platform/assets/89577025/63e1f4ca-0cba-4699-a0b1-05bb53e4344d)

# Visualizing Data with Power BI
![image](https://github.com/doanminhquan/The_ETL_job_transforms_and_analyzes_log_data_from_Television_Platform/assets/89577025/64dd3ff8-3fb4-4269-ad1a-6b49aebb1b11)
![image](https://github.com/doanminhquan/The_ETL_job_transforms_and_analyzes_log_data_from_Television_Platform/assets/89577025/c559a879-05ba-4a84-8f04-6e721ccbc81f)
- Embedded dashboard will be updated later.
