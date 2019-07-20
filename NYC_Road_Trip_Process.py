from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import year,weekofyear, concat
from pyspark.sql.types import StringType
import sys
import os


# Required job parameters
drive_file = sys.argv[1]
drive_zone_file = sys.argv[2]
drive_parquet_outfile = sys.argv[3]
top5_zone_outfile = sys.argv[4]
longest5_trip_outfile = sys.argv[5]

start_time = datetime.utcnow()
spark = SparkSession.builder.appName("NYC Road Trip Cab Data").getOrCreate()


def save_parquet(drive_raw):
    drive_raw.createOrReplaceTempView("drive_flat_view")
    sql = """
       select 
         VendorID
         ,cast(tpep_pickup_datetime as timestamp)
         ,cast(tpep_dropoff_datetime as timestamp)
         ,cast(passenger_count as int)
         ,cast(trip_distance as float)
         ,RatecodeID
         ,store_and_fwd_flag
         ,PULocationID
         ,DOLocationID
         ,payment_type
         ,cast(fare_amount as float)
         ,cast(extra as float)
         ,cast(mta_tax as float)
         ,cast(tip_amount as float)
         ,cast(tolls_amount as float)
         ,cast(improvement_surcharge as float)
         ,cast(total_amount as float)
         from  drive_flat_view
    """
    return spark.sql(sql)


def data_process(drive_df, drive_zone_df):
    drive_df = drive_df.select(drive_df["tpep_pickup_datetime"],
                               drive_df["tpep_dropoff_datetime"],
                               drive_df["trip_distance"],
                               drive_df["tip_amount"]
                               )
    drive_df.createOrReplaceTempView("drive_flat")
    drive_zone_df.createOrReplaceTempView("zone_lookup")
    sql = """
       select
         drv.tpep_pickup_datetime,
         drv.tpep_dropoff_datetime,
         drv.trip_distance,
         drv.tip_amount,
         zl.Borough as Borough,
         zl.zone as Zone,
         to_date(date_format(drv.tpep_pickup_datetime,'dd-MM-y'),'dd-MM-yyyy') as pickup_date
         from drive_flat drv left outer join zone_lookup zl
         on drv.DOLocationID = zl.LocationID
         """
    drive_df = spark.sql(sql)
    #drive_df.show(20)
    drive_df.createOrReplaceTempView("drive_final")
    sql = """
       select 
         Borough,
         Zone,
         round(sum(tip_amount),2) as total_tip_amount
         from  drive_final
         group by Borough,zone
         order by sum(tip_amount) desc
         limit 5
         """
    top5_pd_zone = spark.sql(sql)
    sql = """
       select a.dropoff_date,
              a.tpep_dropoff_datetime,
              a.Borough,
              a.Zone,
              a.trip_distance
        from                 
       (
       select
         date_format(tpep_dropoff_datetime, 'dd-MM-y') as dropoff_date,
         date_format(tpep_dropoff_datetime, "y-MM-dd'T'hh:mm:ss.SSS'Z'") as tpep_dropoff_datetime,
         Borough,
         Zone,
         trip_distance,
         pickup_date,
         row_number() over (partition by pickup_date order by trip_distance desc) as trip_order 
       from drive_final
       where year(pickup_date) = 2018 and 
             weekofyear(pickup_date) = 1
       )a
       where a.trip_order <= 5       
       order by pickup_date asc,trip_distance desc
         """
    longest5_trip_day = spark.sql(sql)
    return top5_pd_zone, longest5_trip_day


if __name__ == "__main__":

    print("log - Job Started. %s" % (datetime.now().strftime('%m/%d/%Y %H:%M:%S')))

    try:
        drive_raw_df = spark.read.csv(drive_file, header=True)
        drive_zone_raw_df = spark.read.csv(drive_zone_file, header=True)
        drive_parquet = save_parquet(drive_raw_df)
        # saving it as parquet
        drive_parquet.write.parquet(drive_parquet_outfile, mode="overwrite")

        # reading it and performing processing
        drive_data = spark.read.parquet(drive_parquet_outfile)
        top5_paid_zone, longest5_trip_day = data_process(drive_data, drive_zone_raw_df)
        top5_paid_zone.\
            repartition(1).\
            write.\
            csv(top5_zone_outfile, header=True, mode="overwrite")
        longest5_trip_day.\
            repartition(1).\
            write.\
            csv(longest5_trip_outfile, header=True, mode="overwrite")

    except Exception as e:
        print("Error")
        raise

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    print("Log - Job Drive Ft Completed. %s seconds" % (duration))