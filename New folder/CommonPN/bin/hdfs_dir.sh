#!/bin/sh

# To create HDFS dirctory ONE time run
hadoop fs -mkdir /obs
hadoop fs -mkdir /obs/data
hadoop fs -mkdir /obs/data/src
hadoop fs -mkdir /obs/data/src/BR_EDS
hadoop fs -mkdir /obs/data/src/ALLRAPREFERENCE
hadoop fs -mkdir /obs/data/src/CATEGORIESINCIDENTS
hadoop fs -mkdir /obs/data/src/COMMUNES_INSEE_UTILE
hadoop fs -mkdir /obs/data/src/CORRESPONDANCE-CODE-INSEE-CODE-POSTAL_UTILE
hadoop fs -mkdir /obs/data/src/NRGTRREFERENTIEL
hadoop fs -mkdir /obs/data/src/BR_GAR
hadoop fs -mkdir /obs/data/src/BR_HPR
hadoop fs -mkdir /obs/data/src/BR_IPR2
hadoop fs -mkdir /obs/data/src/BR_ISR
hadoop fs -mkdir /obs/data/src/BR_TIE
hadoop fs -mkdir /obs/data/src/INCIDENTOLOGIE_FOSAV
hadoop fs -mkdir /obs/data/src/ACORT
hadoop fs -mkdir /obs/data/src/INCIDENTOLOGIE_WASAC
hadoop fs -mkdir /obs/data/src/INCIDENTOLOGIE_WASAC_IAI
hadoop fs -mkdir /obs/data/src/SOIPAD_INDIC_OCEANE
hadoop fs -mkdir /obs/data/src/LKP_REPETITIFS
hadoop fs -mkdir /obs/data/src/LKP_ACTIVATIONS
hadoop fs -mkdir /obs/data/intermediate
hadoop fs -mkdir /obs/data/hive
hadoop fs -mkdir /obs/data/hive/PN_TICKETS_ENRICHIS
hadoop fs -mkdir /obs/data/hive/PN_PARC_MARINE2

echo " successfully created the HDFS directory "

exit 0;

