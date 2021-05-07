import configparser
from datetime import datetime
import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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
    Process song data to read data from S3 by input_data and write them back to S3 with output_data.
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
    print('process_log_data func has run')

    # get filepath to log data file
    # log_data =

    # read log data file
    # df = 
    
    # filter by actions for song plays
    # df = 

    # extract columns for users table    
    # artists_table = 
    
    # write users table to parquet files
    # artists_table

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    # time_table = 
    
    # write time table to parquet files partitioned by year and month
    # time_table

    # read in song data to use for songplays table
    # song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    # songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    # songplays_table


def main():
    print('main func has run')
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    # song_data = "s3a://udacity-dend/song_data"
    # song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    log_data = "s3a://udacity-dend/log_data"
    output_data = "s3a://data-lake-with-spark/sparkify/"

    
    process_song_data(spark, input_data, output_data)  
    # process_log_data(spark, log_data, output_data)


if __name__ == "__main__":
    main()
