__version__ = '0.1'
__author__ = 'Vamsi Chanakya'

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
 
def get_timestamp (timestamp):
     
    """
    - formats timestamp into datetime
    
    Arguments :
     timestamp 
    
    Returns :
     datetime
    
    """
    return datetime.fromtimestamp(timestamp / 1000.0)
    
def create_spark_session():
    
    """
    Get or Create a Spark Session
    
    return: Spark Session
    """
        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    
    """
    - Constructs input data path for song data
    - reads song data file from s3 into datafarme
    - Process Song and Artist Data 
    - filters artist_id null records and removes duplicates 
    - Write songs and artists files to S3 bucket in partitioned parquet format
    
    Arguments :
     param spark: spark session
     param input_data: Input Song Data Path
     param output_data: Output Path 
     
    Returns :
     none
    
    """
    
    # get filepath to song data file
    # song_data =  "s3a://udacity-dend/song_data/A/A/A/*.json"
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs dataframe
    songs_df = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                         .filter("artist_id is not null") \
                         .distinct()
    
    # write songs dataframe to parquet files partitioned by year and artist
    songs_df.write \
            .mode("overwrite") \
            .partitionBy("year","artist_id") \
            .parquet(os.path.join(output_data,'songs'))

    # extract columns to create artists dataframe
    artists_df = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                        .withColumnRenamed("artist_name", "name") \
                        .withColumnRenamed("artist_location", "location") \
                        .withColumnRenamed("artist_latitude", "latitude") \
                        .withColumnRenamed("artist_longitude", "longitude") \
                        .filter("artist_id is not null") \
                        .distinct()
    
    # write artists dataframe to parquet files
    artists_df.write \
              .mode("overwrite") \
              .parquet(os.path.join(output_data,'artists'))

def process_log_data(spark, input_data, output_data):
    
    """
    - Constructs input data path for log data
    - reads log data file from s3 into datafarme
    - Process user, time and sonplays Data  
    - removes duplicates user, time and sonplays Dataframes 
    - Write user, time and sonplays Dataframes to S3 bucket in partitioned parquet format
    
    Arguments :
     param spark: spark session
     param input_data: Input Log Data Path
     param output_data: Output Path 
     
    Returns :
     none
    
    """
    
    # create timestamp column from original timestamp column
    get_datetime = udf(lambda s: get_timestamp(s), t.TimestampType())
    
    # get filepath to log data file
    # log_data = "s3a://udacity-dend/log_data/2018/11/*.json"
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data) \
                       .withColumn('datetime', get_datetime('ts')) \
                       .drop('ts').take(10)
    
    # filter by actions for song plays
    log_df_filtered = log_df.filter(log_df.page == 'NextSong') \
                            .withColumnRenamed("firstName", "first_name") \
                            .withColumnRenamed("lastName", "last_name") \
                            .withColumnRenamed("userId", "user_id") \
                            .withColumnRenamed("sessionId", "session_id") \
                            .withColumnRenamed("userAgent", "user_agent") 

    # extract columns for users dataframe    
    users_df = log_df_filtered.select('user_id', 'first_name', 'last_name', 'gender', 'level').distinct()
    
    # write users dataframe to parquet files
    users_df.write \
            .mode('overwrite') \
            .parquet(os.path.join(output_data,'users'))

    datetime_df = log_df_filtered.select('datetime').distinct()
    
    # extract columns to create time dataframe
    time_df = datetime_df.select(col('datetime').alias('start_time'), 
                                    hour('datetime').alias('hour'), 
                                    dayofmonth('datetime').alias('day'), 
                                    weekofyear('datetime').alias('week'), 
                                    month('datetime').alias('month'), 
                                    year('datetime').alias('year'),
                                    dayofweek('datetime').alias('weekday')) \
                                    .distinct()
    
    # write time dataframe to parquet files partitioned by year and month
    time_df.write \
           .mode("overwrite") \
           .partitionBy("year", "month") \
           .parquet(os.path.join(output_data, 'time'))
                                                                            
    # read in song data to use for songplays table
    songs_df = spark.read.json(input_data + 'song_data/*/*/*/*.json') \
                         .select('song_id', 'title', 'artist_id', 'year', 'duration') \
                         .filter("artist_id is not null") \
                         .distinct()

    # read in artist data to use for songplays table
    artists_df = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                        .withColumnRenamed("artist_name", "name") \
                        .withColumnRenamed("artist_location", "location") \
                        .withColumnRenamed("artist_latitude", "latitude") \
                        .withColumnRenamed("artist_longitude", "longitude") \
                        .filter("artist_id is not null") \
                        .distinct()

    # combining song and artist data to use for songplays dataframe
    song_df2 =  songs_df.join(artists_df, songs_df.artist_id == artists_df.artist_id) \
                        .select("song_id", "title", "duration", songs_df.artist_id, "name") \
                        .distinct()

    # extract columns from joined song and log datasets to create songplays dataframe 
    songplays_df = log_df_filtered.join(song_df2, \
                      (log_df_filtered.song == song_df2.title) & (log_df_filtered.length == song_df2.duration) & \
                      (log_df_filtered.artist == song_df2.name), 'left_outer') \
                      .select('datetime', 'user_id',  'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent') \
                      .withColumn("songplay_id", monotonically_increasing_id())
    
    songplays_df_final = songplays_df.join(time_df, songplays_df.datetime == time_df.start_time, 'left_outer') \
                                     .drop("hour","day","week","weekday")

    # write songplays dataframe to parquet files partitioned by year and month
    songplays_df_final.write \
                      .mode("overwrite") \
                      .partitionBy("year", "month") \
                      .parquet(os.path.join(output_data, 'songplays'))

def main():
    
    spark = create_spark_session()
                                                                                        
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://vamsi-udaicty-datalake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()