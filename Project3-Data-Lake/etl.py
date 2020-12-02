import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """SparkSession Initiation
    
    This function is to initiate a spark session and return a SparkSession object. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Song Data Process
    Load song_data JSON files from S3 bucket, extract columns for songs table and artists table, write data into parquet files and then load it back to S3.

    Parameters
    ==========
    spark: object, spark session be created.
    input_data: string, JSON file path of song_data.
    output_data: string, file path where the written parquet files.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/{for_example_TRAAAAW128F429D538.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"), "overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"), "overwrite")


def process_log_data(spark, input_data, output_data):
    """Log Data Process
    Load log_data JSON files from S3 bucket, extract columns for users table and time table, read both song_data and log_data and extract columns for songplays table, write data into parquet files and then load it back to S3.

    Parameters
    ==========
    spark: object, spark session be created.
    input_data: string, JSON file path of log_data.
    output_data: string, file path where the written parquet files.
    """
    # get filepath to log data file
    log_data = os.path.json(input_data, "log_data/for_example_2018-11-01-events.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    songplays_table = df.filter(df.page=="NextSong").select("ts", "userId", "level", "sessionId", "location", "userAgent")

    # extract columns for users table    
    artists_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "users.parquet"), "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df["ts"]))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn("datetime", get_datetime(df["ts"]))
    
    # extract columns to create time table
    time_table = df.select("datetime") \
                   .withColumn("start_time", df["datetime"]) \
                   .withColumn("hour", hour("datetime") \
                   .withColumn("day", ("datetime")) \
                   .withColumn("weekOfYear", weekofyear("datetime")) \
                   .withColumn("month", month("datetime")) \
                   .withColumn("year", year("datetime")) \
                   .withColumn("dayOfWeek", dayofweek("datetime")) \
                   .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time.parquet"), "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data, "song_data/A/A/A/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    joined_song_log_table = df.join(song_df, song_df.title==df.song)
    songplays_table = joined_song_log_table.select(
        col("ts").alias("start_time"),
        col("userId").alias("userId"),
        col("level").alias("level"),
        col("song_id").alias("song_id"),
        col("artist_id").alias("artist_id"),
        col("sessionId").alias("sessionId"),
        col("location").alias("location"),
        col("userAgent").alias("userAgent"),
        col("year").alias("year"),
        col("userId").alias("userId"),
        month("datetime").alias("month")
    ).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays.parquet"), "overwrite")


def main():
    """Main Function for Executing Above Functions
    
    Steps:
    1. Create a spark session.
    2. Read song_data and log_data from S3.
    3. Extract and transform data to tables that are later written to parquet files.
    4. Load parquet files back to S3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dendpj3-loaded-data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
