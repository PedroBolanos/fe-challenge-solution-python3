import boto3
import io
import os
from zipfile import ZipFile
import pandas as pd
import pandasql as psql
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from sqlalchemy_utils import database_exists, create_database
# from email_validator import validate_email, EmailNotValidError

## RETURNS S3 OBJECT
def get_s3_object(session, bucket_name, s3_file_path):

    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    return bucket.Object(s3_file_path)

## ENSURE THE EXISTANCE OF DATABASE AND SCHEMA. CREATE THEM IF NOT EXIST.
def initialize_postgresql(db_engine, db_schema):

    if not database_exists(db_engine.url):
        create_database(db_engine.url)
    
    if not db_engine.dialect.has_schema(db_engine, db_schema):
        db_engine.execute(CreateSchema(db_schema))

## READ CSV FILES INTO DATAFRAMES
def read_input_files(zip_file, name):
    """ file_list: list contaning all the .csv files
        name: prefix of the files to be read into a dataframe
        returns: pandas dataframe with data in all files with name like 'name%'
    """
    df_list = []
    for file in zip_file.namelist():
        if name in file and ".csv" in file:
            df_list.append(pd.read_csv(zip_file.open(file)))
            
    return pd.concat(df_list)

## OPEN_EVENTS
def ingest_open_events(zip_file, db_engine, db_schema, receipt_events_df):

    # Load open events files into a pandas dataframe
    open_events_df = read_input_files(zip_file, 'open_events')

    # Reset index column to ensure unicity of it.
    open_events_df.reset_index(inplace=True)
    receipt_events_df.reset_index(inplace=True)

    # Format 'date' column
    open_events_df['date'] = pd.to_datetime(open_events_df['date'], infer_datetime_format=True)

    join_query = """
    select  oe.*,
            re.brand_id,
            re.email_domain
    from open_events_df oe
    left join receipt_events_df re on oe.receipt_id = re.id"""
    open_events_df = psql.sqldf(join_query)

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
def ingest_receipt_events(zip_file, db_engine, db_schema):

    rfc_822_regexp = r'((?:(?:\r\n)?[ \t])*(?:(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*)|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*:(?:(?:\r\n)?[ \t])*(?:(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*)(?:,\s*(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*))*)?;\s*))'
    # email_regexp = r'(?!<)(\S+@\S+)(?<!>)'
    domain_regexp = r'@(\S+)(?<!>)'
    name_regexp = r'(?!")(\S+\s\S+)(?<!")'

    # Load open events files into a pandas dataframe
    receipt_events_df = read_input_files(zip_file, 'receipt_events')

    # Reset index column to ensure unicity of it.
    receipt_events_df.reset_index(inplace=True)

    # Format 'date' column
    receipt_events_df['date'] = pd.to_datetime(receipt_events_df['date'], infer_datetime_format=True)

    # Format 'trans_amt' column
    # receipt_events_df['trans_amt'] = receipt_events_df['trans_amt'].astype(currency)
    receipt_events_df['trans_amt'] = receipt_events_df['trans_amt'].str.extract(r'([0-9]+\.[0-9]+)', expand=True).astype(float)

    # Format 'email_adderess' column
    # receipt_events_df['valid_email'] = receipt_events_df['email_address'].apply(lambda x:validate_email(x))
    receipt_events_df['email_address'] = receipt_events_df['email_address'].str.extract(rfc_822_regexp).groupby(level=0).head()[0]

    receipt_events_df['name_on_email'] = receipt_events_df['email_address'].str.extract(name_regexp).groupby(level=0).head()[0]

    receipt_events_df['email_domain'] = receipt_events_df['email_address'].str.extract(domain_regexp).groupby(level=0).head()[0]

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
        return receipt_events_df
    ## DUPLICATES FOUND
    # Write de-duped dataframe into database
    ## SELECT * FROM receipt_events_dup_checked_df WHERE row_num = 0 (last row (max(date)) associated to id)
    receipt_events_deduped_df = receipt_events_dup_checked_df[receipt_events_dup_checked_df['row_num'] == 0].drop(columns = 'row_num')
    receipt_events_deduped_df.to_sql('receipt_events', db_engine, db_schema, 'replace', index=False)

    # Write historical data into database
    receipt_events_hist_df.to_sql('receipt_events_historical', db_engine, db_schema, 'replace', index=False)

    return receipt_events_deduped_df

def main(zip_file_id, db_connection_string, db_database, db_schema):

    ## CREATE DATABASE ENGINE WITH 'sqlalchemy'
    engine = create_engine('{0}/{1}'.format(db_connection_string, db_database))

    ## GET POSTGRESQL DATABASE AND SCHEMA CREATED IF NOT EXIST
    initialize_postgresql(engine, db_schema)

    with ZipFile(zip_file_id, mode='r') as zip_file:

        print("...Ingesting receipt_events into Database:{0} and Schema:{1}".format(db_database, db_schema))
        receipt_events_df = ingest_receipt_events(zip_file, engine, db_schema)
        print("Done!!")

        print("...Ingesting open_events into Database:{0} and Schema:{1}".format(db_database, db_schema))
        ingest_open_events(zip_file, engine, db_schema, receipt_events_df)
        print("Done!!")

if '__main__':
    
    ## TO USE THIS PATH SET from_s3_uri TO FALSE
    zip_file_path = 'input-files.zip'
    
    ## SET TO TRUE TO READ FROM A S3 LOCATION
    from_s3_uri = False

    ## WILL BE USED ONLY IF from_s3_uri IS SET TO TRUE
    session = boto3.session.Session(
        aws_access_key_id="********", 
        aws_secret_access_key="*************"
    )
    
    s3_bucket_name = 'input-data-events'
    
    # PATH INSIDE THE BUCKET
    s3_file_path = 'actual-path/input-files.zip'
    # EXAMPLE: FOR PATH "s3://input-data-events/actual-path/input-files.zip"
    # WILL BE "actual-path/input-files.zip"

    # DATABASE CONNECTION
    db_connection_string = os.environ.get('DB_CONNECTION_STRING')
    db_database = os.environ.get('DB_DATBASE_NAME')
    db_schema = os.environ.get('DB_SCHEMA_NAME')

    #READING ZIP FILE FROM S3 LOCATION
    if from_s3_uri:
        print("Reading zip file from s3://{0}/{1}".format(s3_bucket_name, s3_file_path))
        with io.BytesIO(get_s3_object(session, s3_bucket_name, s3_file_path).get()["Body"].read()) as tf:
            # rewind the file
            tf.seek(0)
            main(tf, db_connection_string, db_database, db_schema)

    #READING LOCAL ZIP FILE
    else:
        print("Reading local zip file at {0}".format(zip_file_path))
        main(zip_file_path, db_connection_string, db_database, db_schema)