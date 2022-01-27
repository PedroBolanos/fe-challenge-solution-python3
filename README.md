# fe-challenge-solution-python3
Data loading application that ingests data from 2 groups of .csv files into PostgreSQL relational database.

## Deployment
Go to the bottom of *pandas-data-loading.py* file and find the **if "__main__"** section.
1.  To load zip file from local path:
    *   Set **zip_file_path** to the local path where the zip file to be ingested is. **Make sure the zip file is in that path.** By default as part of the repository (in the current working directory)
    *   Set **from_s3_uri** to False
    *   Ignore s3 related variables
2.  To load zip file from s3 path:
    *   Set **from_s3_uri** to True
    *   Set **session** variable properties to respective values for **aws_access_key_id** and **aws_secret_access_key** for aws authentication.
    *   Set **s3_bucket_name** to the name of the bucket where the zip file is.
    *   Set **s3_file_path** to the path inside the bucket (excluding from after the bucket name on, on object *uri*).
3.  Export **DB_CONNECTION_STRING** environment variable to a string in the following format: *<db_type>://<user>:<password>@<host>:<port>*. (It was successfully tested for a local PostgreSQL instance).
4.  Export **DB_DATBASE_NAME** environment variable. If no database with that name exists it will be created.
5.  Export **DB_SCHEMA_NAME** environment variable. If no schema with that name exists it will be created.
6.  Make sure all the imported libraries are installed in the environment.
7.  If running locally use command **python3 pandas-data-loading.py** to start its execution.

## Notice
Database and Schema named under db_options will be created if not exist.

## Analytics
All the queries to answer the questions about the data are comment (respective question) separated in *analytics.sql* file.