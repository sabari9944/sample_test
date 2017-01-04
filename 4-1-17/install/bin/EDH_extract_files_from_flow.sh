#!/bin/ksh

#====================================================================================#
# PROJECT                       : OBS-PREDICTIVE NETWORK                             #
#------------------------------------------------------------------------------------#
# File name                     : EDH_extract_files_from_flow.sh                     #
# Description                   : This script extracts all files from FLOW           #
#------------------------------------------------------------------------------------#
# Modifications    :                                                                 #
#                                                                                    #
# Date                   Auteur           Description                                #
# 30/11/2016            BROSSE Thomas     Creation                                   #
#====================================================================================#

#==============================================================================================================================================================================================#
# BUI_EDH_MA - Flow identifier for MARINE Files (BR_EDS_yyyymmdd.csv, BR_GAR_yyyymmdd.csv, BR_HPR_yyyymmdd.csv, BR_IPR2_yyyymmdd.csv, BR_ISR_yyyymmdd.csv , BR_TIE_yyyymmdd.csv  )
# BUI_EDH_OC - Flow identifier for OCEANE Files (lkp_activations_yyyymmdd.csv )
# BUI_EDH_WA - Flow identifier for WASAC Files (incidentologie_wasac_yyyymmdd.csv ) 
# BUI_EDH_AC - Flow identifier for ACORT Files (acort_yyyymmdd.csv ) 
# BUI_EDH_SO - Flow identifier for SOIPAD Files (soipad_indic_oceane_yyyymmdd.csv ) 
#==============================================================================================================================================================================================#


APPID="BUI_EDH_MA BUI_EDH_OC BUI_EDH_WA BUI_EDH_AC BUI_EDH_SO"
for app in $APPID; do
    ./EDH_extract_1_appid_files_from_flow.sh $app
done