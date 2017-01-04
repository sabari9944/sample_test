#!/bin/sh

#### Function definition ####

#Function to get the value of a property
#$ENV defined in .bash_profile_pn
function get_prop {
cat ../config/application_$ENV.conf | grep $1 | cut -d= -f2
}

#Function to get the version installed
function get_version {
cat ../version.txt | grep "version_number" | cut -d= -f2 | sed 's/[ ]//g'
}

# get the hdfs path from application_env.conf
function get_ext_jar {
cat ../config/application_$ENV.conf | grep ext_jars | cut -d= -f2 | sed 's/["]//g' | sed "s/'//g"
}

function get_lib_path {
cat ../config/application_$ENV.conf | grep jar | cut -d= -f2 | sed 's/["]//g' | sed "s/'//g"
}


#### Init variable####
num_version=$(get_version)
class=$1
date=$2
path_property=$3
ext_jars=$(get_ext_jar)
master=$(get_prop 'master')
num_executors=$(get_prop 'num_executors')
executor_memory=$(get_prop 'executor_memory')
executor_cores=$(get_prop 'executor_cores')
lib_path=$(get_prop 'lib_path')
queue=$(get_prop 'queue')

case "$class" in
        "com.obs.pn.ticketenrichi.transf.Launcher")
                jar_to_execute="pn-ticket-enrichi-"$num_version".jar"
                echo $jar_to_execute  
                ;;
        "com.obs.pn.parcmarine2.transf.Launcher")
                jar_to_execute="pn-parc-marine-"$num_version".jar"                
                ;;
        *)
                echo "No launcher with this name"
                ;;
esac



/usr/hdp/current/spark-client/bin/spark-submit --class $class \
--jars  $ext_jars$num_version".jar" \
--master $master \
--num-executors $num_executors \
--executor-memory $executor_memory \
--executor-cores $executor_cores \
--queue $queue \
--conf spark.sql.tungsten.enabled=false \
$lib_path/$jar_to_execute $date $path_property