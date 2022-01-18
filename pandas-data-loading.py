import os
from zipfile import ZipFile
import pandas as pd
import pandasql as psql
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from sqlalchemy_utils import database_exists, create_database

def get_files(zip_file_path, input_files_location):
    """Returns a list containing all the files
    """
    # ZIP FILE WAS PREVIOSLY EXTRACTED TO THE EXPECTED LOCATION
    try:
        return os.listdir(input_files_location)
    # ZIP FILE HAS NOT BEEN EXTRACTED
    except:
        with ZipFile(zip_file_path, 'r') as zipObj:
        # Extract all the contents of zip file in current directory
            zipObj.extractall(input_files_location)
        return os.listdir(input_files_location)
    
## READ CSV FILES INTO DATAFRAMES
def read_input_files(input_files_location, file_list, name):
    """ file_list: list contaning all the .csv files
        name: prefix of the files to be read into a dataframe
        returns: pandas dataframe with data in all files with name like 'name%'
    """
    df_list = []
    for file in file_list:
        if name in file and ".csv" in file:
            df_list.append(pd.read_csv("{0}/{1}".format(input_files_location,file)))
            
    return pd.concat(df_list)

def initialize_postgresql(db_engine, db_schema):

    if not database_exists(db_engine.url):
        create_database(db_engine.url)
    
    if not db_engine.dialect.has_schema(db_engine, db_schema):
        db_engine.execute(CreateSchema(db_schema))

## OPEN_EVENTS
def ingest_open_events(inputh_path, file_list, db_engine, db_schema):

    # Load open events files into a pandas dataframe
    open_events_df = read_input_files(inputh_path, file_list, 'open_events')

    # Reset index column to ensure unicity of it.
    open_events_df.reset_index(inplace=True)

    # Format 'date' column
    open_events_df['date'] = pd.to_datetime(open_events_df['date'], infer_datetime_format=True)

    ## CHECK DUPLICATES AND WRITE INTO DATABASE

    # Add 'row_num' column starting at 0
    query = """
    select *, row_number() over (partition by id order by date desc) - 1 as row_num
    from open_events_df"""
    
    open_events_dup_checked_df = psql.sqldf(query)

    ## HISTORICAL DATAFRAME
    ## SELECT * FROM open_events_dup_checked_df WHERE row_num > 0 (Not last row (max(date)) associated to the id)
    open_events_hist_df = open_events_dup_checked_df[open_events_dup_checked_df['row_num'] > 0]

    ## NO DUPLICATES FOUND
    if len(open_events_hist_df.index) == 0:
        ## Write it as it was
        open_events_df.to_sql('open_events', db_engine, db_schema, 'replace', index=False)

    ## DUPLICATES FOUND
    else:
        # Write de-duped dataframe into database
        ## SELECT * FROM open_events_dup_checked_df WHERE row_num = 0 (last row (max(date)) associated to id)
        open_events_deduped_df = open_events_dup_checked_df[open_events_dup_checked_df['row_num'] == 0].drop(columns = 'row_num')
        open_events_deduped_df.to_sql('open_events', db_engine, db_schema, 'replace', index=False)

        # Write historical data into database
        open_events_hist_df.to_sql('open_events_historical', db_engine, db_schema, 'replace', index=False)

## RECEIPT_EVENTS
def ingest_receipt_events(input_path, file_list, db_engine, db_schema):

    rfc_822_regexp = r'((?:(?:\r\n)?[ \t])*(?:(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*)|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*:(?:(?:\r\n)?[ \t])*(?:(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*)(?:,\s*(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*))*)?;\s*))'
    # rfc_822_regexp = r'(\S+@\S+)'

    # Load open events files into a pandas dataframe
    receipt_events_df = read_input_files(input_path, file_list, 'receipt_events')

    # Reset index column to ensure unicity of it.
    receipt_events_df.reset_index(inplace=True)

    # Format 'date' column
    receipt_events_df['date'] = pd.to_datetime(receipt_events_df['date'], infer_datetime_format=True)

    # Format 'trans_amt' column
    receipt_events_df['trans_amt'] = receipt_events_df['trans_amt'].str.extract(r'([0-9]+\.[0-9]+)', expand=True).astype(float)

    # Format 'email_adderess' column
    receipt_events_df['email_address'] = receipt_events_df['email_address'].str.extract(rfc_822_regexp).groupby(level=0).head()[0]
    
    ## CHECK DUPLICATES AND WRITE INTO DATABASE

    # Add 'row_num' column starting at 0
    query = """
    select *, row_number() over (partition by id order by date desc) - 1 as row_num
    from receipt_events_df"""
    
    receipt_events_dup_checked_df  = psql.sqldf(query)

    ## HISTORICAL DATAFRAME
    ## SELECT * FROM receipt_events_dup_checked_df WHERE row_num > 0 (Not last row (max(date)) associated to the id)
    receipt_events_hist_df = receipt_events_dup_checked_df[receipt_events_dup_checked_df['row_num'] > 0]

    ## NO DUPLICATES FOUND
    if len(receipt_events_hist_df.index) == 0:
        ## Write it as it was
        receipt_events_df.to_sql('receipt_events', db_engine, db_schema, 'replace', index=False)

    ## DUPLICATES FOUND
    else:
        # Write de-duped dataframe into database
        ## SELECT * FROM receipt_events_dup_checked_df WHERE row_num = 0 (last row (max(date)) associated to id)
        receipt_events_deduped_df = receipt_events_dup_checked_df[receipt_events_dup_checked_df['row_num'] == 0].drop(columns = 'row_num')
        receipt_events_deduped_df.to_sql('receipt_events', db_engine, db_schema, 'replace', index=False)

        # Write historical data into database
        receipt_events_hist_df.to_sql('receipt_events_historical', db_engine, db_schema, 'replace', index=False)

def main(zip_file, extract_at_path, db_options):

    ## CREATE DATABASE ENGINE WITH 'sqlalchemy'
    engine = create_engine('{0}://{1}:{2}@{3}:{4}/{5}'.format(db_options["db_type"], db_options["db_user"], db_options["db_pass"], db_options["db_host"], db_options["db_port"], db_options["db_database"]))

    ## GET POSTGRESQL DATABASE AND SCHEMA CREATED IF NOT EXIST
    initialize_postgresql(engine, db_options["db_schema"])

    # List of all files in  zip file
    file_list = get_files(zip_file, extract_at_path)

    print("...Ingesting open_events into {0}. Database:{1}. Schema:{2}".format(db_options["db_type"], db_options["db_database"], db_options["db_schema"]))
    ingest_open_events(extract_at_path, file_list, engine, db_options["db_schema"])
    print("Done!!")

    print("...Ingesting receipt_events into {0}. Database:{1}. Schema:{2}".format(db_options["db_type"], db_options["db_database"], db_options["db_schema"]))
    ingest_receipt_events(extract_at_path, file_list, engine, db_options["db_schema"])
    print("Done!!")

if '__main__':
    zip_file_path = '../input-files.zip'
    extract_at_path = '../input-files'
    db_options = {
        "db_type": "postgresql",
        "db_host": "127.0.0.1",
        "db_user": "postgres",
        "db_pass": "**************",
        "db_port": "5432",
        "db_database": "challenge",
        "db_schema": "events"
    }
    main(zip_file_path, extract_at_path, db_options)