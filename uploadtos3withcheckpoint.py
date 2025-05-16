import mysql.connector
import pandas as pd
import boto3
import json
import os
import io

# Load AWS creds
with open('config.json') as f:
    config = json.load(f)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

# S3 bucket
bucket = 'my-etl-proj'
region = 'us-east-1'

# Checkpoint file
checkpoint_file = 'checkpoint.json'
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, 'r') as f:
        checkpoint = json.load(f)
else:
    checkpoint = {}

# MySQL connection
mysql_conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="your_new_password",
    database="orders"
)
cursor = mysql_conn.cursor(dictionary=True)

tables = ['DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales']

# Loop tables
for table in tables:
    last_ts = checkpoint.get(table, '1970-01-01 00:00:00')
    cursor.execute(f"SELECT * FROM {table} WHERE last_updated > %s", (last_ts,))
    rows = cursor.fetchall()
    if rows:
        df = pd.DataFrame(rows)
        max_ts = df['last_updated'].max().strftime('%Y-%m-%d %H:%M:%S')

        # Upload to S3
        s3 = boto3.client('s3',
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_access_key,
                          region_name=region)

        key = f'public/{table}/{table}.csv'
        with io.StringIO() as buffer:
            df.to_csv(buffer, index=False)
            s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

        print(f"✅ Uploaded {table} with {len(df)} rows.")
        checkpoint[table] = max_ts
    else:
        print(f"🟡 No new data for {table}")

# Update checkpoint
with open(checkpoint_file, 'w') as f:
    json.dump(checkpoint, f)

cursor.close()
mysql_conn.close()
