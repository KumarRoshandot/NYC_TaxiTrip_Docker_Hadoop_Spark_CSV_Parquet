# NYC_TaxiTrip_Assignment
NYC Taxi Trip Test Assesements

1)  Input for the above assesment was taken from  'https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page',
    Using the **Yellow** cabs trip dataset for January 2018.
    
2)  For the Zone Lookup Data **Taxi Zone Lookup Table** was taken from above URL.

3)  Assesment is for the following 
    - Query the stored data to generate two CSV reports:
    - `top_tipping_zones.csv`: Top 5 Dropoff Zones that pay the highest amount of tips.
    - `longest_trips_per_day.csv`: Top 5 longest trips per day of the first week of January 2018. 
    
4)  The above has been done in Spark(2.4.3) job written in PySpark (SparkSQL)
      -NYC_Road_Trip_Process.py

5)  Since the Spark job output files in hdfs format ( part-00000-<>-<>.csv filename) , this  needs to be renamed to the required filename mentioned in step 3. 

6)  So for the spark job and rename activity i have clubbed it together in a unix bash script.
      -Cab_Data_Process.bash
      -This shell script will run Spark job and also output 2 files mentioned in step 3.
      -This shell script required following parameters to run
                   -> SPARK_MASTER_URL ( Spark cluster Master URL ,YARN)
                   -> INPUTFILE LOCATION 
                   -> OUTPUT LOCATION
                   
7)  Place .py and .bash file in the same location and then run the .bash shell script with required paramteres mentioned in step 6.

8)  The spark job will first Save the trip data to Parquet Files in OutPut Location in sub Directory 'Drive_parquet', and there will be separate subfolders for the 2 output files.                    
      
