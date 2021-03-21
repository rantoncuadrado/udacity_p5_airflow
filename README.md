# udacity_p5_airflow
Udacity Data Engineering Nanodegree. Part5 Airflow Final Project.

Sparkify is a music app taht decided to automate their data warehouse ETL pipelines with Apache Airflow.

The source data resides in S3 buckets, and the basic ETL process is as follows:

1. Data in 2 collection of .json files needs to be uploaded to a data warehouse in Amazon Redshift. We'll use COPY function to load .json files into an Amazon Redshift cluster
2. After that it needs to be processed to compose a number of tables (1 fact table + 4 dimension tables), out of the 2 json files (and subsequently stagging tables)

The difference in this new project is that we'll use Apache Airflow for workflow management, running at an interval of our choice and also automatically log all tasks and retry them when they fail.

## Setup and Prepare workspace

These steps are needed to prepare the workspace to run the code

1. __Basic Project template__ is https://github.com/rantoncuadrado/udacity_p5_airflow/tree/main/Basic%20Project%20Template
2. __Launch Redshift Cluster__. This is explained here https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/fad03fb3-ce48-4a69-9887-4baf8751cae3 _lesson 3: implementing Data Wareouses / 15. Exercise 1. Launch Redshift Cluster_

    Sign in to the AWS Management Console and open the Amazon Redshift console at https://console.aws.amazon.com/redshift/.
    On the Amazon Redshift Dashboard, choose Launch cluster.

    On the Cluster details page, enter the following values and then choose Continue:
        Cluster identifier: Enter redshift-cluster.
        Database name: Enter dev.
        Database port: Enter 5439.
        Master user name: Enter awsuser.
        Master user password and Confirm password: Enter a password for the master user account.



4. After you have updated the DAG, you will need to run /opt/airflow/start.sh command to __start the Airflow webserver.__ 
5. __Setup AirFLow Params__: https://classroom.udacity.com/nanodegrees/nd027/parts/45d1c3b1-d87b-4578-a6d0-7e86bb5fea6c/modules/57c3b9d1-4d8b-4afe-bfb4-92cfac622c7f/lessons/4d1d5892-2cab-4456-8b1a-fb2b5fa1488d/concepts/f5c91ffd-60dc-4af3-bc86-44efce885834 

__CONNECTION 1__
```
Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.
```

__CONNECTION 2__
```
Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
Schema: Enter dev. This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password you created when launching your Redshift cluster.
Port: Enter 5439.

```
5. __Create tables in RedShift__ (https://github.com/rantoncuadrado/udacity_p5_airflow/blob/main/Basic%20Project%20Template/create_tables.sql)
I opted for manually create them in the Redshift console

## Run ETL

Once finshed the setup the Raul_project DAG can be launched by togling the ON/OFF button. 

Once Raul_project DAG has run succesfully, you will be able to query the songplays fact table and the songs, users, artists and time dimension tables in Redshift, from the Query Editor.
