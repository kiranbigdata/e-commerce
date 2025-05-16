from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, scoped_session
import pandas as pd
import json
import io
import boto3
import os
import mysql.connector

# Load AWS credentials from config
with open('/Users/kiranteja/PycharmProjects/pythonProject/uploadtos3/venv/config.json') as content:
    config = json.load(content)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

# Load SQL Server and MySQL credentials from environment
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

# MySQL credentials
mysql_user = "root"
mysql_password = "Jaisriram44"
mysql_host = "127.0.0.1"
mysql_port = 3306
mysql_db = "orders"

# (Optional test connection)
conn = mysql.connector.connect(
    host=mysql_host,
    port=int(mysql_port),
    user=mysql_user,
    password=mysql_password,
    database=mysql_db
)


# Extract data from MySQL
def extract():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}"
        )
        Session = scoped_session(sessionmaker(bind=engine))
        s = Session()

        query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema 
              AND table_name IN ('DimProduct','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales')
        """)
        src_tables = s.execute(query, {'schema': mysql_db})
        for tbl in src_tables:
            table_name = tbl[0]
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", engine)
            load(df, table_name)
    except Exception as e:
        print("Data extract error: " + str(e))


# Upload dataframe to S3
def load(df, tbl):
    try:
        print(f'Uploading {len(df)} rows for table {tbl}')
        upload_file_bucket = 'my-etl-proj'
        upload_file_key = f'public/{tbl}/{tbl}.csv'

        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            region_name='us-east-1'
        )

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=upload_file_key, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"✅ Upload successful to {upload_file_key}")
            else:
                print(f"❌ Upload failed with status {status}")
    except Exception as e:
        print("Data load error: " + str(e))


# Run the pipeline
try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
