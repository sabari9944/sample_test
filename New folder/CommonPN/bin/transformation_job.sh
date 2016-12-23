#!/bin/sh

export file="/var/opt/data/app/predicitve_network/bin/setup.properties"

if [ -f "$file" ]
then
cd /var/opt/data/app/predicitve_network/lib
. ./setup.properties
#================================================
#Spark Submit Command
#================================================


case $1 in
        "com.obs.pn.ticketenrichi.transf.Launcher")
                jar_to_execute="obs_pn_parcmarine2_2.10-num_version.jar"
                ;;
        "com.obs.pn.parcmarine2.transf.Launcher")
                jar_to_execute="obs_pn_ticketenrichi_2.10-num_version.jar"                
                ;;
        *)
                echo "No launcher with this name"
                ;;
esac



 
/usr/hdp/current/spark-client/bin/spark-submit --class "$1" \
--jars  ${ext_jars} \
--master ${master} \
--num-executors ${num_executors} \
--driver-memory ${driver_memory} \
--executor-memory ${executor_memory} \
--executor-cores ${executor_cores} \
--queue ${queue} \
/var/opt/data/app/predicitve_network/lib/$jar_to_execute 
 else
  echo "$file not found."
fi


