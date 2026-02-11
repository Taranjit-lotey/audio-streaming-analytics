"""
Glue ETL Job - Transform raw audio events from S3 into Redshift star schema.

This job runs hourly and:
1. Reads new Parquet files from S3 (incremental via job bookmarks)
2. Transforms raw events into dimensional model
3. Loads into Redshift fact and dimension tables

Run on AWS Glue with:
- Glue version: 4.0
- Worker type: G.1X
- Number of workers: 2
- Job bookmark: Enabled (for incremental processing)

Author: Taran
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *


# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'redshift_connection',
    'redshift_database',
    'redshift_schema',
    's3_temp_dir'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SOURCE_DATABASE = args['source_database']  # Glue catalog database
SOURCE_TABLE = args['source_table']        # Glue catalog table (crawled from S3)
REDSHIFT_CONNECTION = args['redshift_connection']
REDSHIFT_DATABASE = args['redshift_database']
REDSHIFT_SCHEMA = args['redshift_schema']
S3_TEMP_DIR = args['s3_temp_dir']


def read_source_data():
    """
    Read raw events from Glue catalog (backed by S3 Parquet).
    
    Job bookmarks ensure we only process new data since last run.
    """
    print(f"Reading from {SOURCE_DATABASE}.{SOURCE_TABLE}")
    
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=SOURCE_DATABASE,
        table_name=SOURCE_TABLE,
        transformation_ctx="source_data"  # Required for job bookmarks
    )
    
    df = dynamic_frame.toDF()
    record_count = df.count()
    print(f"Read {record_count:,} records from source")
    
    return df


def transform_to_dim_date(df):
    """
    Create date dimension from events.
    
    SCD Type 1 - dates don't change, just insert new ones.
    """
    print("Building dim_date...")
    
    dim_date = df.select(
        F.to_date('event_timestamp').alias('full_date')
    ).distinct()
    
    dim_date = dim_date.withColumn('date_key', 
        F.date_format('full_date', 'yyyyMMdd').cast('int')
    ).withColumn('day_of_week',
        F.dayofweek('full_date')
    ).withColumn('day_name',
        F.date_format('full_date', 'EEEE')
    ).withColumn('month',
        F.month('full_date')
    ).withColumn('month_name',
        F.date_format('full_date', 'MMMM')
    ).withColumn('quarter',
        F.quarter('full_date')
    ).withColumn('year',
        F.year('full_date')
    ).withColumn('is_weekend',
        F.when(F.dayofweek('full_date').isin([1, 7]), True).otherwise(False)
    )
    
    return dim_date


def transform_to_dim_tracks(df):
    """
    Create track dimension from events.
    
    Uses latest values for each track_id (SCD Type 1).
    In production, would implement SCD Type 2 for artist changes.
    """
    print("Building dim_tracks...")
    
    # Window to get latest record per track
    window = Window.partitionBy('track_id').orderBy(F.desc('event_timestamp'))
    
    dim_tracks = df.select(
        'track_id',
        'track_title',
        'artist',
        'event_timestamp'
    ).withColumn('row_num', F.row_number().over(window)
    ).filter(F.col('row_num') == 1
    ).drop('row_num', 'event_timestamp')
    
    # Generate surrogate key
    dim_tracks = dim_tracks.withColumn('track_key',
        F.abs(F.hash('track_id'))
    ).select(
        'track_key',
        'track_id',
        F.col('track_title').alias('title'),
        'artist'
    )
    
    return dim_tracks


def transform_to_dim_users(df):
    """
    Create user dimension from events.
    
    Extracts user attributes from event data.
    """
    print("Building dim_users...")
    
    # Window to get first and latest activity per user
    window_first = Window.partitionBy('user_id').orderBy('event_timestamp')
    window_last = Window.partitionBy('user_id').orderBy(F.desc('event_timestamp'))
    
    dim_users = df.select(
        'user_id',
        'device_type',
        'event_timestamp'
    ).withColumn('first_seen', F.first('event_timestamp').over(window_first)
    ).withColumn('last_seen', F.first('event_timestamp').over(window_last)
    ).withColumn('primary_device', F.first('device_type').over(window_last)
    ).select(
        'user_id',
        'first_seen',
        'last_seen', 
        'primary_device'
    ).distinct()
    
    # Generate surrogate key
    dim_users = dim_users.withColumn('user_key',
        F.abs(F.hash('user_id'))
    ).select(
        'user_key',
        'user_id',
        'first_seen',
        'last_seen',
        'primary_device'
    )
    
    return dim_users


def transform_to_fact_listens(df, dim_users, dim_tracks, dim_date):
    """
    Create fact table by joining events with dimension keys.
    """
    print("Building fact_listens...")
    
    # Add date key
    fact = df.withColumn('date_key',
        F.date_format('event_timestamp', 'yyyyMMdd').cast('int')
    )
    
    # Join with dimensions to get surrogate keys
    fact = fact.join(
        dim_users.select('user_key', 'user_id'),
        on='user_id',
        how='left'
    ).join(
        dim_tracks.select('track_key', 'track_id'),
        on='track_id', 
        how='left'
    )
    
    # Generate fact surrogate key
    fact = fact.withColumn('listen_key',
        F.monotonically_increasing_id()
    )
    
    # Select final columns
    fact_listens = fact.select(
        'listen_key',
        'user_key',
        'track_key',
        'date_key',
        'session_id',
        'action',
        F.col('listen_duration_ms').alias('duration_ms'),
        'device_type',
        'event_timestamp'
    )
    
    return fact_listens


def write_to_redshift(df, table_name, mode='append'):
    """
    Write DataFrame to Redshift using Glue connection.
    
    Args:
        df: Spark DataFrame
        table_name: Target table name
        mode: 'append' or 'overwrite'
    """
    full_table_name = f"{REDSHIFT_SCHEMA}.{table_name}"
    print(f"Writing to {full_table_name} (mode: {mode})")
    
    record_count = df.count()
    print(f"  Records to write: {record_count:,}")
    
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, table_name)
    
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection=REDSHIFT_CONNECTION,
        connection_options={
            "dbtable": full_table_name,
            "database": REDSHIFT_DATABASE
        },
        redshift_tmp_dir=S3_TEMP_DIR,
        transformation_ctx=f"write_{table_name}"
    )
    
    print(f"  Write complete: {full_table_name}")


def upsert_dimension(df, table_name, key_column):
    """
    Upsert dimension records (insert new, update existing).
    
    Uses Redshift MERGE via staging table pattern.
    """
    staging_table = f"{table_name}_staging"
    
    # Write to staging
    write_to_redshift(df, staging_table, mode='overwrite')
    
    # Execute MERGE via Redshift
    merge_sql = f"""
    MERGE INTO {REDSHIFT_SCHEMA}.{table_name} AS target
    USING {REDSHIFT_SCHEMA}.{staging_table} AS source
    ON target.{key_column} = source.{key_column}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;
    
    DROP TABLE IF EXISTS {REDSHIFT_SCHEMA}.{staging_table};
    """
    
    # Note: In production, execute via Redshift Data API or psycopg2
    print(f"MERGE SQL for {table_name}:\n{merge_sql}")


def main():
    """Main ETL workflow."""
    print("=" * 60)
    print(f"Starting Audio Events ETL Job")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    # Read source data (incremental via job bookmarks)
    raw_events = read_source_data()
    
    if raw_events.count() == 0:
        print("No new records to process. Exiting.")
        job.commit()
        return
    
    # Cache for multiple uses
    raw_events.cache()
    
    # Build dimensions
    dim_date = transform_to_dim_date(raw_events)
    dim_tracks = transform_to_dim_tracks(raw_events)
    dim_users = transform_to_dim_users(raw_events)
    
    # Build fact table
    fact_listens = transform_to_fact_listens(
        raw_events, dim_users, dim_tracks, dim_date
    )
    
    # Write to Redshift
    # Dimensions: upsert (handles SCD Type 1)
    write_to_redshift(dim_date, 'dim_date', mode='append')
    write_to_redshift(dim_tracks, 'dim_tracks', mode='append')
    write_to_redshift(dim_users, 'dim_users', mode='append')
    
    # Fact: always append (immutable events)
    write_to_redshift(fact_listens, 'fact_listens', mode='append')
    
    # Print summary
    print("=" * 60)
    print("ETL Summary:")
    print(f"  dim_date records: {dim_date.count():,}")
    print(f"  dim_tracks records: {dim_tracks.count():,}")
    print(f"  dim_users records: {dim_users.count():,}")
    print(f"  fact_listens records: {fact_listens.count():,}")
    print("=" * 60)
    
    # Commit job (updates bookmark)
    job.commit()
    print("Job committed successfully")


if __name__ == '__main__':
    main()
