#!/bin/sh
#====================================================================================#
# PROJECT                       : OBS                                                #
#------------------------------------------------------------------------------------#
# File name                     : Edge_node_to_hdfs.ksh                              #
# Description                   : This script extracts files from Egde node          #
# Parametre                     : 1) ODATE
#------------------------------------------------------------------------------------#
# Modifications    :                                                                 #
#                                                                                    #
# Date                   Auteur        Description                                   #
# 15/11/2016            Capgemini       Creation                                     #
# 22/11/2016            Capgemini       config file changed for OBS UAT              #
#====================================================================================#

# date && time
date
time
export ODATE=$1

# ---- Lookup LIST ----
export file_list=obs_file_list.lkp

# ---- env value ----
export conf=/home/TechnicalCore/dataset/bin

# ---- env value ----
cd $conf
# ------- config ------

        if  [ -s $conf/config.sh ]; then
                echo " config file is present running the config  "
        else
                echo " config file is not present  "
                exit 1;
        fi
. ./config.sh

# ----- create Archive directory -----

mkdir -p $archive/$ODATE
export archive_dir=$archive/$ODATE

# lookup file

        if  [ -s $lookup_path/$file_list ]; then
                echo " Lookup file size is good "
        else
                echo " Lookup file size is zero $lookup_path/$file_list  "
                exit 1;
        fi

# copy the file from local to hdfs
        for i in $(cat $lookup_path/$file_list)
        do
          export file=$i
                if  [ -s $abinitio_source/${i}_${ODATE}.csv ]
                then
                        echo " $i file size is good "
                        export file=$i
                        export Destination_file_pattern=$(echo "$file"| tr '[:lower:]' '[:upper:]')
                        echo " $Destination_file_pattern"

# creating directory in HDFS based on ODATE

                                hadoop fs -mkdir $hdfs_source/$Destination_file_pattern/$ODATE

# copy the file from src to HDFS based on ODATE

                                hadoop fs -put $abinitio_source/${file}_${ODATE}.csv $hdfs_source/$Destination_file_pattern/$ODATE
                                hadoop fs -ls $hdfs_source/$Destination_file_pattern/$ODATE/${file}_${ODATE}.csv
                                status=$?
                                        if [ ${status} -eq 0 ]; then
                                                echo " successfully copied the file to HDFS region $hdfs_source  "
                                                echo " copy the file from local to archive directory"
# copy the file from src to archive 
                                                cp $abinitio_source/${file}_${ODATE}.csv $archive_dir
                                                cd $archive_dir
                                                gzip $archive_dir/${file}_${ODATE}.csv
                                                status=$?
                                                        if [ $status -eq 0 ]; then
# Remove the file from src Path
                                                                rm $abinitio_source/${file}_${ODATE}.csv
                                                                echo " successfully deleted the file from $abinitio_source "
                                                        else
                                                                echo " not able to archive the $file"
                                                        fi
                                        else
                                                echo " failed to copy "
                                                exit 2;
                                        fi
                else
                        echo " $file is not present in the path $abinitio_source"
                        exit 3;
                fi
        done

# Delete 7 days older file
find $archive/* -mtime +7 -exec rm -rf {} \;


echo " successfully Copied the file to HDFS path  "

exit 0;


