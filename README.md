# NYC_TaxiTrip_Assignment
NYC Taxi Trip Test Assesements

FOR THIS TASK I HAVE PROVIDED DOCKERFILE WITH DEPENDANT ENVIORNMENTS TO EXECUTE.
   - JAVA
   - HADOOP
   - PYSPARK(Python + SPARK )
   


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
      - Cab_Data_Process.bash
      - This shell script will run Spark job and also output 2 files mentioned in step 3.
      - This shell script required following parameters to run 
                     - INPUTFILE LOCATION (/tmp)
                     - OUTPUT LOCATION (/tmp/output)
                   
7) I have attached Dockerfile to Built and run it (docker run -it image_test /bin/sh)
        -  It has JAVA ,PYTHON,  HADOOP and SPARK 

8)  Copy .py and .bash  and input and lookup csv file in the same location (/tmp ) and also create a subfolder (/tmp/output )
    Then run the .bash shell script with required paramteres mentioned in step 6.

9)  The spark job will first Save the trip data to Parquet Files in OutPut Location in sub Directory
      'Drive_parquet'(/tmp/output/Drive_parquet), and there will be separate subfolders for the 2 output files.                    
      
