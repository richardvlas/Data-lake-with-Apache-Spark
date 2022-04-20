import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear, 
                                   dayofweek, monotonically_increasing_id)
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a SparkSession in a way that if you're not sure that there already 
    is one and since creating multiple SparkSessions and SparkContexts can cause
    issues, so it's best practice to use the SparkSession.builder.getOrCreate() 
    method. This returns an existing SparkSession if there's already one in 
    the environment, or creates a new one if necessary.

    Parameters:
    -----------
    None

    Returns:
    --------
    spark : SparkSession object
        Create or retrieved object with given configuration
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read data from song_data dataset, extract columns to create songs table
    and artists table and write tables to parquet files 

    Parameters:
    -----------
    spark : SparkSession object
        Spark session with given configuration

    input_data : str
        Path to s3 bucket containing input data

    output_data : str
        Path to s3 bucket to store prepared data

    Returns:
    --------
    None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        ['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 
                               'artist_latitude', 'artist_longitude'])
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(
        os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Read data from log_data dataset, extract columns for users table, create 
    timestamp and datetime columns to create time table, extract columns from 
    joined song and log datasets to create songplays table and writes the 
    processed tables to parquet files

    Parameters:
    -----------
    spark : SparkSession object
        Spark session with given configuration

    input_data : str
        Path to s3 bucket containing input data

    output_data : str
        Path to s3 bucket to store prepared data

    Returns:
    --------
    None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
        ['userId', 'firstName', 'lastName', 'gender', 'level']
        ).dropDuplicates(['userId']).orderBy('userId')
    
    # write users table to parquet files
    users_table.write.parquet(
        os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(int(x) / 1000.0), TimestampType())

    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', get_timestamp(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour('timestamp'))
    df = df.withColumn('day', dayofmonth('timestamp'))
    df = df.withColumn('week', weekofyear('timestamp'))
    df = df.withColumn('month', month('timestamp'))
    df = df.withColumn('year', year('timestamp'))
    df = df.withColumn('weekday', dayofweek('timestamp'))

    time_table = df.select(col('datetime'),
                           col('hour'),
                           col('day'),
                           col('week'),
                           col('month'),
                           col('year'),
                           col('weekday')
                          ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    
    songplays_table = df.select(
        col('ts'),
        col('userId'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId'),
        col('location'),
        col('userAgent'),
        year(df.datetime).alias('year'),
        month(df.datetime).alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'songplays/songplays.parquet'),
                 'overwrite')                         


def main():

    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"

    # s3 bucket to store prepared data
    output_data = "s3a://output-data-spark"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
