# udacity_p5_airflow
Udacity Data Engineering Nanodegree. P5 Airflow


## Prepare workspace

1. Basic Project template is https://github.com/rantoncuadrado/udacity_p5_airflow/tree/main/Basic%20Project%20Template
2. Launch Redshift Cluster is explained here https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/445568fc-578d-4d3e-ab9c-2d186728ab22/lessons/21d59f40-6033-40b5-81a2-4a3211d9f46e/concepts/fad03fb3-ce48-4a69-9887-4baf8751cae3 __lesson 3: implementing Data Wareouses / 15. Exercise 1. Launch Redshift Cluster__
3. After you have updated the DAG, you will need to run /opt/airflow/start.sh command to start the Airflow webserver. 
4. Setup AirFLow Params: https://classroom.udacity.com/nanodegrees/nd027/parts/45d1c3b1-d87b-4578-a6d0-7e86bb5fea6c/modules/57c3b9d1-4d8b-4afe-bfb4-92cfac622c7f/lessons/4d1d5892-2cab-4456-8b1a-fb2b5fa1488d/concepts/f5c91ffd-60dc-4af3-bc86-44efce885834 

__CONNECTION 1__
Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

__CONNECTION 2__
Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
Schema: Enter dev. This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password you created when launching your Redshift cluster.
Port: Enter 5439.

5. Create tables in RedShift (https://github.com/rantoncuadrado/udacity_p5_airflow/blob/main/Basic%20Project%20Template/create_tables.sql)
