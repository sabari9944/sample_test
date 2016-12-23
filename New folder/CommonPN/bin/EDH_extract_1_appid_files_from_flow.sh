#!/bin/ksh

#====================================================================================#
# PROJECT                       : OBS-PREDICTIVE NETWORK                             #
#------------------------------------------------------------------------------------#
# File name                     : EDH_extract_1_appid_files_from_flow.sh             #
# Description                   : This script extracts files from FLOW               #
#------------------------------------------------------------------------------------#
# Modifications    :                                                                 #
#                                                                                    #
# Date                   Auteur           Description                                #
# 30/11/2016            BROSSE Thomas     Creation                                   #
#====================================================================================#


lDate=`date "+%Y%m%d_%H%M%S"`
lDay=`date "+%Y%m%d"`
logFile=EDH_extract_1_appid_files_from_flow"_"$lDay.log
appId=$1

mkdir -p $REP_PPREP_LOG/$appId

writeLog()
{
        LDate=`date "+%Y%m%d %H:%M"`
        echo $LDate ":" $1 >> $REP_PPREP_LOG/$appId/$logFile
}

#==============================================================================================================================================================================================#
# $HFLOW - Flow client installation folder
# $FLOW_PPREP_SRC_R - Flow Environnement (FLOWPROD/FLOWUAT/FLOWETUD )
# $FLOW_PPREP_SRC_APPID  - Flow Identifier discribed below)
# $REP_PPREP_DATA  - Destination folder
#==============================================================================================================================================================================================#

#==============================================================================================================================================================================================#
# BUI_EDH_MA - Flow identifier for MARINE Files (BR_EDS_yyyymmdd.csv, BR_GAR_yyyymmdd.csv, BR_HPR_yyyymmdd.csv, BR_IPR2_yyyymmdd.csv, BR_ISR_yyyymmdd.csv , BR_TIE_yyyymmdd.csv  )
# BUI_EDH_OC - Flow identifier for OCEANE Files (lkp_activations_yyyymmdd.csv )
# BUI_EDH_WA - Flow identifier for WASAC Files (incidentologie_wasac_yyyymmdd.csv ) 
# BUI_EDH_AC - Flow identifier for ACORT Files (acort_yyyymmdd.csv ) 
# BUI_EDH_SO - Flow identifier for SOIPAD Files (soipad_indic_oceane_yyyymmdd.csv ) 
#==============================================================================================================================================================================================#

    writeLog "Starting transfert of file $1 with target $REP_PPREP_DATA"
    
    # Copy the files from FLOW and put all in an intermediate folder
    $HFLOW/agentlec  $FLOW_PPREP_SRC_R   $1  "cd $REP_PPREP_DATA;cp" -s0  >> $REP_PPREP_LOG/$app/$logFile
    
    #./agentlec >> $REP_PPREP_LOG/$appId/$logFile
    
    # Test excution
    if [ $? -eq 0 ]
    then
         writeLog "Successfully transferred the PPREP files to the dir, $REP_PPREP_DATA/"
    else
         writeLog "ERROR: There is problem in transferring the files from FLOW to $REP_PPREP_DATA/ .. Please investigate.."
         exit 1
    fi
