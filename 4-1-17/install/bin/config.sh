#!/bin/sh


# -------- Local --------
#lookup
#export lookup_path=/obs/data/lookup

#abinitio source file
#export abinitio_source=/obs/data/raw

# Archive
#export archive=/obs/data/archive

#---------- Config Path -------
#export conf=/devl/code/obs


#---------- UAT Config Path -------
export conf=/home/TechnicalCore/dataset/bin
export log_path=/var/opt/data/log

#---------- UAT Lookup Path -------
export lookup_path=/home/TechnicalCore/dataset/bin

#---------- UAT Edge node Path -------
export abinitio_source=/var/opt/data/app


#---------- UAT Archive Path -------
export archive=/var/opt/data/app


#---------- Business owners request a change file -------

export A=AllRapReference
export B=NRGTRreferentiel
export C=communes_INSEE_utile
export D=correspondance-code-insee-code-postal_utile
export E=CategoriesIncidents
export F=lkp_repetitifs


# -------- HDFS -------
# hdfs source
export hdfs_source=/exploration/data_lake

export BR_EDS_src=${hdfs_source}/BR_EDS
export ALLRAPREFERENCE_src=${hdfs_source}/ALLRAPREFERENCE
export CATEGORIESINCIDENTS_src=${hdfs_source}/CATEGORIESINCIDENTS
export COMMUNES_INSEE_UTILE_src=${hdfs_source}/COMMUNES_INSEE_UTILE
#export CORRESPONDANCE_CODE_INSEE_CODE_POSTAL_UTILE_src=${hdfs_source}/CORRESPONDANCE_CODE_INSEE_CODE_POSTAL_UTILE
export CORRESPONDANCE_CODE_INSEE_CODE_POSTAL_UTILE_src=${hdfs_source}/CORRESPONDANCE-CODE-INSEE-CODE-POSTAL_UTILE
export NRGTRREFERENTIEL_src=${hdfs_source}/NRGTRREFERENTIEL
export BR_GAR_src=${hdfs_source}/BR_GAR
export BR_HPR_src=${hdfs_source}/BR_HPR
export BR_IPR2_src=${hdfs_source}/BR_IPR2
export BR_ISR_src=${hdfs_source}/BR_ISR
export BR_TIE_src=${hdfs_source}/BR_TIE
export INCIDENTOLOGIE_FOSAV_src=${hdfs_source}/INCIDENTOLOGIE_FOSAV
export ACORT_src=${hdfs_source}/ACORT
export INCIDENTOLOGIE_WASAC_src=${hdfs_source}/INCIDENTOLOGIE_WASAC
export INCIDENTOLOGIE_WASAC_IAI_src=${hdfs_source}/INCIDENTOLOGIE_WASAC_IAI
export SOIPAD_INDIC_OCEANE_src=${hdfs_source}/SOIPAD_INDIC_OCEANE
export LKP_REPETITIFS_src=${hdfs_source}/LKP_REPETITIFS
export LKP_ACTIVATIONS_src=${hdfs_source}/LKP_ACTIVATIONS

#Intermediate file location
#hadoop fs -mkdir /obs/data/intermediate

# hive table
export Marine_hive=/exploration/technical_core/publication/PN_PARC_MARINE2
export enrichis_hive=/exploration/technical_core/publication/PN_TICKETS_ENRICHIS

#exit 0;

