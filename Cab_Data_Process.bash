#!/bin/bash
# Description: NYC Cab Trip process 
SCRIPTSTARTTIME=$(date +%s)
if [ $# -ne 2 ];
then
echo "wrong no. of arguments provided"
exit 1
fi
SPARK_MASTER_URL='local[*]'
SPARK=/usr/spark-2.4.1/bin/spark-submit
input_path=$1
out_path=$2
inpt_drive_file=$input_path'/yellow_tripdata*.csv'
input_zone_lookup_file=$input_path'/taxi+_zone_lookup.csv'

drive_parquet='Drive_parquet'
top5_paid_zone='top5_paid_zone'
longest5_trip_day='long5_trip_day'

drive_parquet_folder=$out_path'/'$drive_parquet
top5_paid_zone_folder=$out_path'/'$top5_paid_zone
longest5_trip_day_folder=$out_path'/'$longest5_trip_day

#PYSPARK JOB TO PROCESS NYC ROAD TRIP DATA
ENG_START_TIME=$(date +%s)
$SPARK --master $SPARK_MASTER_URL NYC_Road_Trip_Process.py $inpt_drive_file $input_zone_lookup_file $drive_parquet_folder $top5_paid_zone_folder $longest5_trip_day_folder
rc=$?
ENG_END_TIME=$(date +%s)

echo "It takes $(($ENG_START_TIME - $ENG_END_TIME)) seconds to complete this task in Spark job..."

if [ $rc -eq 0 ];
then
echo "Spark Job has been finished successfully"
else 
echo "Spark Job failed"
exit $rc
fi


#RENAMING OUFILES
mv $top5_paid_zone_folder/*.csv $top5_paid_zone_folder/top_tipping_zones.csv
if [ $rc -eq 0 ];
then
echo "top tipping zone file moved"
else 
echo "failed while moving tipping zone file"
exit $rc
fi

mv $longest5_trip_day_folder/*.csv $longest5_trip_day_folder/longest_trips_per_day.csv
if [ $rc -eq 0 ];
then
echo "longest5 trip file moved"
else 
echo "failed while moving longest5 trip file"
exit $rc
fi


SCRIPTENDTIME=$(date +%s)
echo "It takes $(($SCRIPTENDTIME - $SCRIPTSTARTTIME)) seconds to complete all tasks..."

exit 0
