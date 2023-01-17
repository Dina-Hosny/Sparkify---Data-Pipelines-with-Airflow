# Sparkify---Data-Pipelines-with-Airflow
Sparkify - Data Pipelines with Airflow - Udacity Data Engineering Expert Track.

In this project, I created custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

## Project Details:

The project's purpose is to introduce more automation and monitoring to a music streaming startup called 'Sparkify' data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They want to create high-grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. 

They have also noted that the data quality plays a big part when analyses are executed on top of the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

Their data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift.
The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

![example-dag](https://user-images.githubusercontent.com/46838441/212951621-75b3871e-ecf2-4794-a314-0754da98cfe9.png)


In this project, I built four different operators that will stage the data, transform the data, and run checks on data quality.

- Stage Operator:

The staging operator's job is to load any JSON formatted files from S3 to Amazon Redshift.
The operator creates and runs a SQL COPY statement based on the parameters provided.
The operator's parameters specify where in S3 the file is loaded and what is the target table.

The parameters are used to distinguish between JSON files. 
The staging operator also contains a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

- Fact and Dimension Operators:

The fact and dimension operator's job is to take as input a SQL statement and target database on which to run the query. 
Also, there is the target table that will contain the results of the transformation.

Dimension loads are done with the truncate-insert pattern where the target table is emptied before the load. 
So, there is a parameter that allows switching between insert modes when loading dimensions.
Fact tables are so massive that they should only allow append-type functionality.

  -- Fact Table:

    - ```songplays``` Represents the metrics used for the song plays analytics.
      - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.

  -- Dimension Tables:
    - ```users``` The data of the users that registered in the application.
      - user_id, first_name, last_name, gender, level
    - ```songs``` The data about songs in the music database.
      - song_id, title, artist_id, year, duration
    - ```artists``` The data of the artists that are registered in the music database.
      - artist_id, name, location, latitude, longitude
    - ```time``` timestamps of records in song plays broken down into specific units.
      - start_time, hour, day, week, month, year, weekday
    

- Data Quality Operator:

The data quality operator's job is used to run checks on the data itself.
The operator's main functionality is to receive one or more SQL-based test cases along with the expected results and execute the tests.
For each test, the test result and expected result are checked, and if there is no match, the operator raises an exception, and the task retries and fails eventually.

For example, one test could be a SQL statement that checks if a certain column contains NULL values by counting all the rows that have NULL in the column. 
We do not want to have any NULLs, so the expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.


## Project Datasets:

The project has two datasets that reside in S3.

- Song data: s3://udacity-dend/song_data.
- Log data: s3://udacity-dend/log_data


Songs Dataset:

  This dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

Log Dataset:

  This dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations. The files are partitioned by year and month.


## Project files:

The project contains three major components:

- ```dags``` has all the imports and task in place.
- ```operators``` contains all operators of the project.
- ```helper class``` conrains the SQL transformation.

The DAG graph in the Airflow UI:

![screenshot-2019-01-21-at-20 55 39](https://user-images.githubusercontent.com/46838441/212952277-8fda4bc6-8b75-4188-9491-884c5849369e.png)


## Tools and Technologies:

- Apache Airflow.
- AWS Services.
- AWS IAM.
- Amazon Elastic MapReduce (EMR) Clusters.
- Python 3.
- ETL: Extract, Transform, Load Data.
- Data Warehouse Concepts.
- Cloud Computing Concepts.
- Big Data and NoSQL concepts.


## Project Steps:

- 1- Design fact and dimension tables in a way to answers Sparkify's analytics team's given queries.
- 2- Create an IAM User in AWS.
- 3- Create a Redshift Cluster in AWS.
- 4- Connect Airflow and AWS
- 5- Connect Airflow to the AWS Redshift Cluster.
- 6- Configuring the DAG, its default parameters, and configure the task dependencies.
- 7- Building the operators.
- 8- Load the Data.
- 9- Test the results.
