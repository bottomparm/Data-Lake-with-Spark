import configparser
from datetime import datetime
import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Data Lake with Spark") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read json files from S3 using input_data. Process song data. Write them back to S3 using output_data.
    Arguments:
        spark (spark instance) -- a spark instance
        input_data (string) -- a filepath for input data
        output_data (string) -- a filepath for output data
    """
    song_data = "{}/song_data/A/A/A/*.json".format(input_data)
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates().cache()

    # extract columns to create songs table
    songs_table = df.select(
        col("song_id"),
        col("title"),
        col("artist_id"),
        col("year"),
        col("duration")
    ).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet("{}songs/songs_table.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df.select(
        col("artist_id"),
        col("artist_name"),
        col("artist_location"),
        col("artist_latitude"),
        col("artist_longitude")
    ).distinct() 
    
    # write artists table to parquet files
    artists_table.write.parquet("{}artists/artists_table.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    Read json files from S3 using input_data. Process log data. Write them back to S3 using output_data.
    Here with the log_data
    Args:
        - spark: spark session
        - input_data_log: input data logs s3 link
        - output_data: My own s3 bucket (set in public)
    """

    # get filepath to log data file
    log_data = "{}/log_data/*/*/*events.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong").cache()

    # extract columns for users table    
    users_table = df.select(
        col("firstName"),
        col("lastName"),
        col("gender"),
        col("level"),
        col("userId")
    ).distinct()
    
    # write users table to parquet files
    users_table.write.parquet("{}users/users_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts"))) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts"))) 
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))

    time_table = df.select(
        col("start_time"),
        col("hour"),
        col("day"),
        col("week"),
        col("month"),
        col("year"),
        col("weekday")
    ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet("{}time/time_table.parquet".format(output_data))

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner") \
        .distinct() \
        .select( \
            col("start_time"), \
            col("userId"), \
            col("level"), \
            col("sessionId"), \
            col("location"), \
            col("userAgent"), \
            col("song_id"), \
            col("artist_id"), \
            col("year"), \
            col("month")) \
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet("{}song_plays/songplays_table.parquet".format(output_data))


def main():
    """
    Main Function
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-with-spark/sparkify/"
    
    process_song_data(spark, input_data, output_data)  
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
