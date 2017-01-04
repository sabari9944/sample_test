#!/bin/sh

# To create HDFS dirctory ONE time run



hadoop fs -mkdir -p /integration/data_lake/BR_EDS
hadoop fs -mkdir -p /integration/data_lake/ALLRAPREFERENCE
hadoop fs -mkdir -p /integration/data_lake/CATEGORIESINCIDENTS
hadoop fs -mkdir -p /integration/data_lake/COMMUNES_INSEE_UTILE
hadoop fs -mkdir -p /integration/data_lake/CORRESPONDANCE-CODE-INSEE-CODE-POSTAL_UTILE
hadoop fs -mkdir -p /integration/data_lake/NRGTRREFERENTIEL
hadoop fs -mkdir -p /integration/data_lake/BR_GAR
hadoop fs -mkdir -p /integration/data_lake/BR_HPR
hadoop fs -mkdir -p /integration/data_lake/BR_IPR2
hadoop fs -mkdir -p /integration/data_lake/BR_ISR
hadoop fs -mkdir -p /integration/data_lake/BR_TIE
hadoop fs -mkdir -p /integration/data_lake/INCIDENTOLOGIE_FOSAV
hadoop fs -mkdir -p /integration/data_lake/ACORT
hadoop fs -mkdir -p /integration/data_lake/INCIDENTOLOGIE_WASAC
hadoop fs -mkdir -p /integration/data_lake/INCIDENTOLOGIE_WASAC_IAI
hadoop fs -mkdir -p /integration/data_lake/SOIPAD_INDIC_OCEANE
hadoop fs -mkdir -p /integration/data_lake/LKP_REPETITIFS
hadoop fs -mkdir -p /integration/data_lake/LKP_ACTIVATIONS
hadoop fs -mkdir -p /integration/technical_core/workspace
hadoop fs -mkdir -p /integration/technical_core/publication/PN_TICKETS_ENRICHIS
hadoop fs -mkdir -p /integration/technical_core/publication/PN_PARC_MARINE2

hadoop fs -chmod 755 /integration/

hadoop fs -mkdir -p /integration/technical_core/dataset/bin
hadoop fs -mkdir -p /integration/technical_core/dataset/sql

hadoop fs -chmod 770 /integration/technical_core/dataset/bin
hadoop fs -chmod 770 /integration/technical_core/dataset/sql


echo " successfully created the HDFS directory "
