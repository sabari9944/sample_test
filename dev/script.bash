#!/bin/sh
#====================================================================================#
# PROJECT                       : OBS                                                #
#------------------------------------------------------------------------------------#
# File name                     : Edge_node_to_hdfs.ksh                              #
# Description                   : This script extracts files from Egde node          #
# Parametre                     : 1) Odate
#------------------------------------------------------------------------------------#
# Modifications    :                                                                 #
#                                                                                    #
# Date                   Auteur        Description                                   #
# 15/11/2016            Capgemini       Creation                                     #
#====================================================================================#

# date && time
date 
time
export ODATE = $1

# ---- env value ----
export devl=/devl/code/obs
cd $devl
./config.sh

# --- Local ---
export odate=date +%Y%m%d
export lookup_path=/obs/data/lookup
export file_list=obs_file_list.lkp
export abinitio_source=/obs/data/raw
export archive=/obs/data/archive
export archive_dir=mkdir $archive/$odate

# ----- Hdfs ---- 
export hdfs_source=/obs/data/src

# lookup file
	if  [ -s $lookup_path/$file_list ] then 
		echo " Lookup file size is good "
	else
		echo " Lookup file size is zero $lookup_path/$file_list  "
		exit 1;
	fi
	
# copy the file from local to hdfs
	for i in $lookup_path/$file_list
	do
		if  [ -s $abinitio_source/${i}_${ODATE}.csv ] 
		then 
			echo " $i file size is good "
			export file=$i
			export Destination_file_pattern = $(echo "$file"| tr '[:lower:]' '[:upper:]')
			echo " $Destination_file_pattern"
				hadoop fs -mkdir $hdfs_source/$Destination_file_pattern/$odate
				hadoop fs -put $abinitio_source/${file}_${ODATE}.csv $hdfs_source/$Destination_file_pattern/$odate
				hadoop fs -ls $hdfs_source/$Destination_file_pattern/$odate/${file}${ODATE}.csv
				status=$?
					if [${status} -eq 0 ]; then
						echo " successfully copied the file to HDFS region $hdfs_source  "
						echo " copy the file from local to archive directory"
						cp $abinitio_source/${file}_${ODATE}.csv $archive_dir
						cd $archive_dir
						gzip $archive_dir/${file}${ODATE}.csv
						status=$?
							if [$status -eq 0 ]; then
								rm $abinitio_source/${file}.csv
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
find /obs/data/archive/* -mtime +7 -exec rm -rf {} \;
#timestamp for file name

exit 0;





