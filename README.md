# fe-challenge-solution-python3
Data loading application that ingests data from 2 groups of .csv files into PostgreSQL relational database.

## Deployment
Go to the bottom of *pandas-data-loading.py* file and find the **if "__main__"** section.
1. Set **zip_file_path** to the local or s3 path where the zip file to be ingested is. **Make sure the zip file is in that path.**
2. Set **extract_at_path** to the local path where the zip file is going to be extracted to.
3. Set **db_options** properties to the wanted values. (It was successfully tested for a local PostgreSQL instance).
4. Find requirements in *requirement.py* file.

## Notice
Database and Schema named under db_options will be created if not exist.

## Analytics
All the queries to answer the questions about the data are comment (respective question) separated in *analytics.sql* file.