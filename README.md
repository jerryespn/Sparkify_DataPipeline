# Udacity - Data Engineer Nanodegree - Sparkify - DataPipeline Project

## About this project

Sparkify dataset is a database that contains music and artist records created for educational purposes.
The data extracted from this database it's on a json format and its available at this repository at AWS by Udacity at this AWS S3 Bucket: s3a://udacity-dend

For this project two datasets were used: songs and log data.

- Song json files, were used to get the information about songs (of course) and artist.
- Log json files, were used to get the information about covers song, artist and the complementary information for each song. 

This project is intended to create a data pipeline using Apache Airflow, that will control de tasks an ETL in AWS using S3 as repository source data and AWS Redshift as the target database. Airflow also controls the data transformation before loading into Redhshift tables.


## Prerequisites to use this repository

1. Python 3.x
1. Airflow installed and configured, if you use AWS, the better.
1. Jupyter Notebook (Recommended, to install with Anaconda)
1. An AWS Redshift Cluster
1. Windows/Mac/Linux - Compatible
2. Firefox, Chrome, Edge Navigators - Compatible

## Deployment instructions

I seriously recommend to clone this Github Folder to create the same structure but just be sure that you have the right structure, read below:

You will need the next hierarchy of files to execute the project:

```
airflow
│   README.md
│─── dags
│   │-- dag.py
└─── plugins
    │--- helpers
    │--- operators
```

## Executing Airflow DAG

Once you have started your Airflow instance and had opened the project on it. You can Switch the **dag** to **ON** from GUI. As the dag is configured by default, since his last programming task, the task was scheduled to run hourly so you can monitor the dag trough the GUI.

Remember you are moving a lot of data, so you will see that some of the extraction processes are taking too long, but it's normal.
