"""
Glue ETL Job - Transform raw audio events from S3 into Redshift star schema.


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
