import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url = params.url 

    # Parquet file
    parquet_name = 'yellow_tripdata_2023-01.parquet'

    # Download the Parquet
    os.system(f"wget {url} -O {parquet_name}")

    # PostgreSQL connection string
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)

    # Open the Parquet file using PyArrow
    parquet_file = pd.read_parquet(parquet_name)

    def chunker(seq, size):
        for pos in range(0, len(seq), size):
            yield seq.iloc[pos:pos + size]

    # Define the number of rows per chunk for database insertion
    db_chunk_size = 10000

    # Iterate over the DataFrame in chunks
    for i, chunk in enumerate(chunker(parquet_file, db_chunk_size)):
        try:
            t_start = time()
            chunk.to_sql(name=table, con=engine, if_exists='append', index=False, chunksize=db_chunk_size)
            t_end = time()
            print(f"Chunk {i} inserted successfully, and took %.3f second" % (t_end - t_start))
        except Exception as e:
            print(f"Error inserting chunk {i}: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest Parquet data to Postgres")

    # user, password, host, port, database name, table name, url of the parquet
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database name for postgres')
    parser.add_argument('--table', help='target table name')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()
    main(args)



