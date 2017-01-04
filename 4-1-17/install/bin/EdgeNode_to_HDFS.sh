#!/bin/sh
#====================================================================================#
# PROJECT                       : OBS                                                #
#------------------------------------------------------------------------------------#
# File name                     : Edge_node_to_hdfs.ksh                              #
# Description                   : This script extracts files from Egde node          #
# Parametre                     : 1) ODATE
#                               : 2) Lookup file
#------------------------------------------------------------------------------------#
# Modifications    :                                                                 #
#                                                                                    #
# Date                   Auteur        Description                                   #
# 15/11/2016            Capgemini       Creation                                     #
# 22/11/2016            Capgemini       Config file changed for OBS UAT              #
# 07/12/2016            Capgemini       Design pattern change 17 to 12 file          #
# 21/12/2016            Capgemini       Added Log file                               #
#====================================================================================#
 

# get the hdfs path from application_env.conf
function get_prop {
cat ../config/application_uat.conf |  grep $1 | cut -d= -f2
}

# get the hdfs path from application_env.conf
function get_hdfs_path {
cat ../config/application_uat.conf |  grep -A 5 $1 | grep dir | cut -d= -f2| sed 's/["]//g'
}

# get the filename from application_env.conf
function get_hdfs_filename {
cat ../config/application_uat.conf |  grep -A 5 $1 | grep file | cut -d= -f2| sed 's/["]//g' | sed 's/\///g'
}

#set colors for logs
red=`tput setaf 1`
green=`tput setaf 2`
yelloew=`tput setaf 3`
reset=`tput sgr0` 

# date && time
export ODATE=$1
export TS=$(date +%s)

# start of execution
date_debut=`date`  

# manage archive and log
archive=$(get_prop 'archive')
archive_dir=$archive/$ODATE
log_path=$(get_prop 'log_path')
log=${log_path}/Edge_node_to_hdfs${ODATE}_${TS}.log 

list_all_file=$(get_prop 'mandatory_files')" "$(get_prop 'optional_files')
optional_files=$(get_prop 'optional_files')
abinitio_source=$(get_prop 'abinitio_source')
conf=$(get_prop 'conf')

mkdir -p $archive/$ODATE

        for i in $list_all_file
        do
                file=$i


                       for j in $optional_files
                       do
                       
                           if [ $file == $j ];
                           then 
                              if [ -f $abinitio_source/${i}.csv ]; then
                                if  [ -s $abinitio_source/${i}.csv ]; then
                                echo "${green} SUCCESS: $i file size is good ${reset}" | tee -a $log
                                cd $conf
# copy the file from src to HDFS based on ODATE
                                hdfs_source=$(get_hdfs_path $file) 
 
                                hadoop fs -put -f $abinitio_source/${file}.csv $hdfs_source  | tee -a $log
                                hadoop fs -ls $hdfs_source/${file}.csv | tee -a $log
                                status=$?
                                        if [ ${status} -eq 0 ]; then
                                                echo " ${green} SUCCESS: ${green} SUCCESSfully copied the file to HDFS region $hdfs_source/${file}.csv ${reset} " | tee -a $log                                             
# copy the file from src to archive
                                                cp $abinitio_source/${file}.csv $archive_dir
                                                                                                                                                                                                echo " ${green} SUCCESS: copy the file from local:$abinitio_source  to archive directory: $archive_dir ${reset}" | tee -a $log
                                                cd $archive_dir
                                                gzip $archive_dir/${file}.csv
                                                status=$?
                                                        if [ $status -eq 0 ]; then
                                                                                                                                                                                                                                echo " ${green} SUCCESS: Zip the file in $archive_dir ${reset}" | tee -a $log
# Remove the file from src Path
                                                                rm $abinitio_source/${file}.csv
                                                                echo " ${green} SUCCESS: ${green} SUCCESSfully deleted the file from $abinitio_source ${reset}" | tee -a $log
                                                        else
                                                                echo "${red} ERROR:  not able to archive the $file ${reset}" | tee -a $log
                                                                                                                                                                                                                                                                exit 1;
                                                        fi
                                        else
                                                echo "${red} ERROR: failed to copy ${reset}" | tee -a $log
                                                exit 2;
                                        fi
                                        else
                                  echo "${red} ERROR: $i file SIZE is ZERO ${reset}" | tee -a $log
                                  exit 3;
                                
                                
                                fi
                              fi
                              continue 2
                           fi
                       done
 
                                     
                      if [ -f $abinitio_source/${i}_${ODATE}.csv ]; then
                          if  [ -s $abinitio_source/${i}_${ODATE}.csv ]; then
                          echo " $i file size is good "            | tee -a $log  
                                cd $conf
                                hdfs_source=$(get_hdfs_path $file)                                                                                                                                                     
# creating directory in HDFS based on ODATE
                                hadoop fs -mkdir -p $hdfs_source/$ODATE | tee -a $log
# copy the file from src to HDFS based on ODATE
                                hadoop fs -put $abinitio_source/${file}_${ODATE}.csv $hdfs_source/$ODATE | tee -a $log
                                hadoop fs -ls $hdfs_source/$ODATE/${file}_${ODATE}.csv | tee -a $log
                                status=$?
                                        if [ ${status} -eq 0 ]; then
                                                echo " ${green} SUCCESS: ${green} SUCCESSfully copied the file to HDFS region $hdfs_source/$Destination_file_pattern/$ODATE/${file}_${ODATE}.csv  " | tee -a $log
                                                echo " copy the file from local:$abinitio_source  to archive directory: $archive_dir ${reset}" | tee -a $log
# copy the file from src to archive
                                                cp $abinitio_source/${file}_${ODATE}.csv $archive_dir
                                                                                                                                                                                                echo "${green} SUCCESS: ${green} SUCCESSfully copied the file from $abinitio_source ${reset}" | tee -a $log
                                                cd $archive_dir
                                                gzip $archive_dir/${file}_${ODATE}.csv
                                                status=$?
                                                        if [ $status -eq 0 ]; then
# Remove the file from src Path
                                                                rm $abinitio_source/${file}_${ODATE}.csv
                                                                echo "${green} SUCCESS: ${green} SUCCESSfully deleted the file from $abinitio_source ${reset}" | tee -a $log
                                                        else
                                                                echo " ${yelloew} WARNING: not able to archive the $file ${reset}" | tee -a $log
                                                        fi
                                                        continue
                                        else
                                            echo "${red} ERROR:  failed to copy ${reset}" | tee -a $log
                                            exit 2;
                                        fi
                                        else       
                                            echo "${red} ERROR: $i file SIZE is ZERO  ${reset}" | tee -a $log
                                            exit 3;                                                                                                   
                          fi
                        else
                          echo "${red} ERROR: $i file does not exist in $abinitio_source ${reset}" | tee -a $log   
                          exit 4;
                        fi 
                                         
                         
        done

cd $conf        
Marine_hive=$(get_hdfs_path "hive_pn_parc_marine2")  
 
# Hive directory
hadoop fs -mkdir -p $Marine_hive/$ODATE | tee -a $log
 
# Delete 7 days older file
find $archive/* -mtime +7 -exec rm -rf {} \; | tee -a $log


date_fin=`date`
 
echo "LOG:  For more details LOG: $log  "
echo "${green} SUCCESS:  ${green} SUCCESSfully Copied the file to HDFS path  ${reset}" | tee -a $log
echo "start date :" $date_debut | tee -a $log
echo "end date :" $date_fin | tee -a $log 
  
exit 0; 