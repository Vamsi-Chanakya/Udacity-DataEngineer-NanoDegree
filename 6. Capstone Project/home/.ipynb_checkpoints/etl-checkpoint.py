__version__ = '0.1'
__author__ = 'Vamsi Chanakya'

import datetime as dt
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from pyspark.sql.functions import udf, col, when, isnan, count, upper, first, date_trunc
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType

def create_spark_session():
    
    """
    Get or Create a Spark Session
    
    return: Spark Session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    return spark

#.enableHiveSupport()

def process_files(spark):
    
    # read all lookup files and load dimensions dataframes
    visa_lookup_df = spark.read.csv("lookup/visa.csv", header='true', inferSchema='true')
    mode_lookup_df = spark.read.csv("lookup/mode.csv", header='true', inferSchema='true')
    addr_lookup_df = spark.read.csv("lookup/addr.csv", header='true', inferSchema='true').filter("i94addr is not null").distinct()
    port_lookup_df = spark.read.csv("lookup/port.csv", header='true', inferSchema='true').filter("i94port is not null").distinct()
    country_lookup_df = spark.read.csv("lookup/country.csv", header='true', inferSchema='true').filter("i94country is not null").distinct()

    # Process cities demographics file
    cities_df = spark.read.csv("data/us-cities-demographics.csv", sep=';', header=True)

    # Creating 'cities_pivot_df' dataset
    cities_pivot_df = cities_df.select("city","state code","Race","count") \
                                  .groupby(cities_df.City, "state code") \
                                  .pivot("Race") \
                                  .agg(first("Count"))

    drop_cols_list = ["Number of Veterans","Race","Count"]

    # Drop columns we don't need and drop duplicate rows
    cities_final_df = cities_df.drop(*drop_cols_list).dropDuplicates()

    # Finally saving (committing) joined cities_final_df dataset
    cities_final_df = cities_final_df.join(cities_pivot_df, ["City","state code"])

    # Change `state code` column name to `state_code` and other similar problems to avoid parquet complications
    cities_final_df = cities_final_df.withColumnRenamed("State Code", "State_Code") \
                                     .withColumnRenamed("Median Age", "Median_Age") \
                                     .withColumnRenamed("Male Population", "Male_Population") \
                                     .withColumnRenamed("Female Population", "Female_Population") \
                                     .withColumnRenamed("Total Population", "Total_Population") \
                                     .withColumnRenamed("Foreign-born", "Foreign_born") \
                                     .withColumnRenamed("Average Household Size", "Avg_Household_Size") \
                                     .withColumnRenamed("American Indian and Alaska Native", "Native_Population") \
                                     .withColumnRenamed("Asian", "Asian_Population") \
                                     .withColumnRenamed("Black or African-American", "African_American_Population") \
                                     .withColumnRenamed("Hispanic or Latino", "Hispanic_Population") \
                                     .withColumnRenamed("White", "White_population")

    # write cities_final_df dataset onto parquet file
    cities_final_df.write.mode('overwrite').parquet("output/us_cities_demographics") 

    # read immigration file
    #fname = "../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat"
    #immigration_df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")

    # Read i94 non-immigration dataset
    immigration_df=spark.read.parquet("data/sas_data") \
                        .select("cicid","i94yr","i94mon","i94cit","i94res","i94port","i94mode","i94addr","i94bir","i94visa",
                                "biryear","gender","visatype","arrdate","depdate") \
                        .withColumnRenamed("i94res", "i94country")
    
    immigration_fmt_df = immigration_df.withColumn("cicid", immigration_df.cicid.cast(IntegerType())) \
                                       .withColumn("i94yr", immigration_df.i94yr.cast(IntegerType())) \
                                       .withColumn("i94mon", immigration_df.i94mon.cast(IntegerType())) \
                                       .withColumn("i94mode", immigration_df.i94mode.cast(IntegerType())) \
                                       .withColumn("i94bir", immigration_df.i94bir.cast(IntegerType())) \
                                       .withColumn("i94visa", immigration_df.i94visa.cast(IntegerType())) \
                                       .withColumn("biryear", immigration_df.biryear.cast(IntegerType())) \
                                       .withColumn("i94country", immigration_df.i94country.cast(IntegerType())) \
                                       .withColumn("i94cit", immigration_df.i94cit.cast(IntegerType())) \
                                       .withColumnRenamed("i94res", "i94country")
    
    # Join with Country Lookup - based on i94country code populating country name
    immigration_ctry_df = immigration_fmt_df.join(country_lookup_df, "i94country", 'left_outer') \
                                        .drop("i94country") \
                                        .withColumnRenamed("country", "resident_country")
    
    # Join with visa lookup - based on i94visa populating visa type
    immigration_visa_df = immigration_ctry_df.join(visa_lookup_df, 'i94visa', 'left_outer').drop("i94visa")
    
    # Join with mode lookup - based on i94mode populating immegration mode
    immigration_mod_df = immigration_visa_df.join(mode_lookup_df, 'i94mode', 'left_outer').drop("i94mode")
    
    # join with addr lookup - based on i94addr populating state code
    immigration_addr_df = immigration_mod_df.join(addr_lookup_df, 'i94addr', 'left_outer').drop("i94addr")
    
    # join with port lookup - based on i94port populating port details
    immigration_port_df=immigration_addr_df.join(port_lookup_df, 'i94port', 'left') \
                                       .drop("i94port","addr") \
                                       .withColumnRenamed("country", "port_country")
    
    # Convert SAS arrival date to datetime format
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    immigrant_date_df = immigration_port_df.withColumn("arrival", get_date(immigration_port_df.arrdate)) \
                                       .withColumn("departure", get_date(immigration_port_df.depdate))
    #                                   .drop("arrdate","depdate")

    # arrival date dim table
    arrival_date = immigrant_date_df.select(col('arrdate').alias('arrival_sasdate'),
                                       col('arrival').alias('arrival_date'),
                                       date_format('arrival','M').alias('arrival_month'),
                                       date_format('arrival','E').alias('arrival_dayofweek'), 
                                       date_format('arrival', 'y').alias('arrival_year'), 
                                       date_format('arrival', 'd').alias('arrival_day'),
                                      date_format('arrival','w').alias('arrival_weekofyear')).dropDuplicates()

    # Add seasons to arrival dimension
    arrival_date.createOrReplaceTempView("arrival_date")
    arrival_season = spark.sql('''select cast(arrival_sasdate as int),
                             arrival_date,
                             arrival_month,
                             arrival_dayofweek,
                             arrival_year,
                             arrival_day,
                             arrival_weekofyear,
                             CASE WHEN arrival_month IN (12, 1, 2) THEN 'winter' 
                                    WHEN arrival_month IN (3, 4, 5) THEN 'spring' 
                                    WHEN arrival_month IN (6, 7, 8) THEN 'summer' 
                                    ELSE 'autumn' 
                             END AS date_season from arrival_date''')
    
    # Save i94date dimension to parquet file partitioned by year and month:
    arrival_season.write.mode("overwrite").partitionBy("arrival_year", "arrival_month").parquet('output/arrival_season')
    
    # Save to final immegration fact details to parquet file
    immigrant_date_df.write.mode("overwrite").partitionBy("i94yr", "i94mon").parquet('output/immigrant_fact_table')
    
    # data quality checks
    
    try:
        immigration_dim_count = immigration_df.count()
        immigration_fact_count = spark.read.parquet("output/immigrant_fact_table").count()
        
        if immigration_dim_count == immigration_fact_count:
                 print("immigration load validation succeded")
    except:
        print("immigration fact load validation failed")
        
    try:
        distinct_cities_dim_count = cities_df.select("City").distinct().count()
        distinct_cities_fact_count = spark.read.parquet("output/us_cities_demographics").select("city").distinct().count()
        
        if immigration_dim_count == immigration_fact_count:
                 print("immigration load validation succeded")
    except:
        print("cities demographics fact load validation failed")


def main():
    
    spark = create_spark_session()
    
    process_files(spark)

if __name__ == "__main__":
    main()