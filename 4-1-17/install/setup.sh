#!/bin/bash

#### Description: Installation script of Predictive Network
#### Written by: Song-Eun Alexandre DOUMBIA
#### Reviewed by: Thierry BEBIN and Thomas BROSSE
#### v1 : 19/12/2016 (steps definition)
#### v2 : 20/12/2016 (steps 1 to 5 implementation)
#### v3 : 21/12/2016 (code refactoring, parameters insertion, help, log and error management)
#### v3 : 22/12/2016 (step 6 implementation, comments)
#### v4 : 22/12/2016 (code refactoring , execution order changed, tested on dev cluster)

### VARIABLES DEFINITON
USER="TechnicalCore"  
DEBUG=0  #0 pas de debug a l ecran
DISPLAYHELP=0
PROJECT_INSTALL_FILE="install"
HOME_TECHNICALCORE=$HOME
LOG_DIR=/var/opt/data/log
BEGIN_TIME=date

CLUSTER_IP="frparvm51143345.corp.capgemini.com"
CLUSTER_PORT="8020"
DEV=exploration

SSH_LOGIN="TechnicalCore"
SSH_SERVER="obitedhs-vcs001"
SSH_PORT=22

INVERSEDEB=`tput smso`
INVERSEFIN=`tput rmso`

### VARIABLES DEFINITON -END 

### READING PARAMETRES
while [ $# -gt 0 ]
do
  case $1 in
    -p ) 
	    PROJECT_INSTALL_FILE=$2
	    shift 1
	    ;;
    -user|-u ) 
	    USER=$2
	    shift 1
	    ;;
    -home ) 
	    HOME_TECHNICALCORE=$2
	    shift 1
	    ;;
    -ssh_login ) 
	    SSH_LOGIN=$2
	    shift 1
	    ;;
    -ssh_server ) 
	    SSH_SERVER=$2
	    shift 1
	    ;;
    -ssh_port ) 
	    SSH_PORT=$2
	    shift 1
	    ;;
    -d | -debug )
		DEBUG=1
		;;
    -h | -help )
		DISPLAYHELP=1
		;;
	-cluster_ip ) 
	    CLUSTER_IP=$2
	    shift 1
	    ;;
	-cluster_port ) 
	    CLUSTER_PORT=$2
	    shift 1
	    ;;
	-dev ) 
	    DEV=$2
	    shift 1
	    ;;
  esac
  shift 1
done

if [ $DISPLAYHELP -eq 1 ]
then
  echo "syntax : $0 [-p FILE] [-user USER] [-home DIR] [-d|-debug] [-h|-help]"
  echo "                 [-dev DEV] [-cluster_ip CLUSTER_IP] [-cluster_port CLUSTER_PORT]"
  echo "                 [-ssh_server SSH_SERVER_NAME] [-ssh_login SSH_LOGIN] [-ssh_port SSH_PORT]"
  echo "FILE  		: Filename of delivery to install(default value :$PROJECT_INSTALL_FILE)"
  echo "USER   		: User to start script : $0 (default value :$USER)"
  echo "DIR    		: Home directory (default value :$HOME_TECHNICALCORE)"
  echo "DEV	        : Environment or directory above TechnicalCore (default value :$DEV)"
  echo "LOG_DIR		: Log trace location (default value :$LOG_DIR)"
  echo "CLUSTER_IP	: Hadoop cluster ip adress (default value :$CLUSTER_IP)"
  echo "CLUSTER_PORT	: Hadoop cluster Hive port (default value :$CLUSTER_PORT)"
  echo "SSH_SERVER_NAME : hostname of ssh server hosting hive (default value ${SSH_SERVER})"
  echo "SSH_LOGIN       : login for ssh server (default value ${SSH_LOGIN})"
  echo "SSH_PORT        : ssh port on ssh server (default value ${SSH_PORT})"
  #exit
  return
fi

### READING PARAMETRES - END

### LOG FILE CREATION
# If for any reason, user can't have acces to the default directory, use /tmp
# The log file will end by the day of running
if [ ! -d ${LOG_DIR} ] ; then
 LOG_DIR=/tmp
fi
BASENAME=$(basename $0)
LOG_FILE=${LOG_DIR}/${BASENAME}_`date +"%Y_%m_%d.log"`
>> ${LOG_FILE}
### LOG FILE CREATION - END

### FUNCTIONS DECLARATION
# trace_log : Saves logs and prompt them on screen in debug mode (debug= ?)
# trace_info: Prompt infos on screen to keep the install on track
function trace_log {
	if [ $DEBUG -eq 1 ] ; then 
		echo `date +"%Y/%m/%d %H:%M:%S"` $* | tee -a ${LOG_FILE}
	else 
		echo `date +"%Y/%m/%d %H:%M:%S"` $* >>  ${LOG_FILE}
	fi
}
function trace_info {
	echo `date +"%Y/%m/%d %H:%M:%S"` $* | tee -a ${LOG_FILE}
}

function continuer {
  message=$*
  while ( true )
  do
    echo -e "`date +"%Y/%m/%d %H:%M:%S"` ${INVERSEDEB}${message} (y/n)${INVERSEFIN} \c"
    read continue
    case "$continue" in
      o|O|y|Y)
	       return 0
	       ;;	
      n|N)
	   return 1
	   ;;	
      *)
	 ;;	
    esac		
  done
}

function enter_to_continue {
  message=$*
  echo -e "`date +"%Y/%m/%d %H:%M:%S"` ${INVERSEDEB}${message}${INVERSEFIN} \c"
  read
}

### FUNCTIONS DECLARATION - END

#
# =============================== STEP 0 ===============================
#

#STEP 0 : Extraction of .tar project archive on client side
#User should move the .tar file to /home/TechnicalCore/ and extract it using one of the following commands :
#--> tar -xvf obs_pn-2.10.g0.r0.c1.tar (for a tar file) or
#--> unzip obs_pn-2.10.g0.r0.c1.zip (for a zip file)
#It contains several directories (bin,lib,sql,src and install)

###VERIFY USER ACCES
check_user()
{
if [ "$(whoami)" != "$USER" ] ; then 
  trace_info "ERROR #0.1:  Install aborted. Please run as $USER not as $(whoami)!"
  exit 10
else 
  trace_info "INFO : ============================================="
  trace_info "INFO : Launching program ... (logging in ${LOG_FILE})"
fi
}
check_user
###VERIFY USER ACCES - END
trace_log "INFO : Lancement du script : ${BASENAME}"
trace_log "INFO : LOG                 : $LOG_FILE"

trace_log "INFO : USER                : $USER"
trace_log "INFO : Install file		  : $PROJECT_INSTALL_FILE"


#
# =============================== STEP 1 ===============================
#

#STEP 1 : Configuration of the user profile
#On the first install we copy the 2 profile configuration files available in the install folder
#Otherwise it is already configured, we can go to the installation steps
trace_log "INFO : STEP 1 : Configuration of the user profile"
continuer "STEP 1: Is it the first Install ?"
CR=$?
if [ ${CR} -eq 0 ] ; then
  trace_log "INFO : LAUCHING FIRST INSTALL"
  PROJECT_INSTALL=${HOME_TECHNICALCORE}/${PROJECT_INSTALL_FILE}
  file='.bash_profile'
  filepn='.bash_profile_pn'

  #copy for the first time the config files on TechnicalCore Home directory
  trace_info "INFO : Moving to $PROJECT_INSTALL ..."
  cd $PROJECT_INSTALL 1>> ${LOG_FILE} 2>&1
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #1.2 ($CR): cd $PROJECT_INSTALL"
    exit 2 	
  fi
	
  trace_info "INFO : Copying $file to $HOME ..."
  cp $file $HOME 1>> ${LOG_FILE} 2>&1
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #1.3 ($CR): cp $file $HOME"
    exit 2
  fi
  
  trace_info  "INFO : Copying $filepn to $HOME ..."
  cp $filepn $HOME  1>> ${LOG_FILE} 2>&1
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #1.4 ($CR): cp $filepn $HOME"
    exit 2
  fi	
		
  #If files are correctly copied (to check), go and source the bash_profile
  #It automatically ask to import the predictive network configuration
	
  trace_info  "INFO : Moving to $HOME ..."
  cd $HOME
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #1.5 ($CR): cd $HOME"
    exit 2
  fi
  
  chmod +x $file $filepn
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #1.6 ($CR): chmod +x $file $filepn"
    exit 2
  fi
  source $file 2>>${LOG_FILE} #1>>${LOG_FILE} 2>&1
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #1.7 ($CR): source $file"
    exit 2
  fi

  trace_info  "INFO : Moving to $PROJECT_INSTALL ..."
  cd $PROJECT_INSTALL
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #1.8 ($CR): cd $PROJECT_INSTALL"
    exit 2
  fi
else 
	trace_log "INFO : Ok, you may have already configured $file ..."
fi

#
# =============================== STEP 2 ===============================
#

#STEP 2: Backup files
#Move all current (bin,lib,sql,src and install) files to backup_date directory
#And clear the dataset directory
trace_log "INFO : STEP 2 : Backup files (tar)"
if [ ! -d ${HOME_TECHNICALCORE}/backup ] ; then
  mkdir -p ${HOME_TECHNICALCORE}/backup
  trace_info "INFO #2.1: ${HOME_TECHNICALCORE}/backup was created"
fi
if [ ! -d ${HOME_TECHNICALCORE}/dataset ] ; then
  trace_info "ERROR #2.2: Missing directory ${HOME_TECHNICALCORE}/dataset"
  exit 2
fi
TAR_FILE=`date +"%Y_%m_%d_%Hh%Mm%S.tar"` 
tar -cvf ${HOME_TECHNICALCORE}/backup/${TAR_FILE} ${HOME_TECHNICALCORE}/dataset 1>/dev/null 2>&1
CR=$?
if [ ! ${CR} -eq 0 ] ; then
  trace_info "ERROR #2.3 ($CR): tar -cvf ${HOME_TECHNICALCORE}/backup/${TAR_FILE} ${HOME_TECHNICALCORE}/dataset"
  exit 2 
else
  trace_info "INFO : STEP 2 : Backup files generated:${HOME_TECHNICALCORE}/backup/${TAR_FILE}"
fi


#
# =============================== STEP 3 ===============================
#


#STEP 3: Update dataset
# Copy new extracted files to the (cleaned) dataset directory
# from ${HOME_TECHNICALCORE}/${PROJECT_INSTALL_FILE} dans ${HOME_TECHNICALCORE}/dataset
trace_log "INFO : STEP 3 : Updating dataset"
if [ ! -d ${HOME_TECHNICALCORE}/${PROJECT_INSTALL_FILE} ] ; then
  trace_info "ERROR #3.1: Missing source directory:${HOME_TECHNICALCORE}/${PROJECT_INSTALL_FILE}"
  exit 2
fi
if [ ! -d ${HOME_TECHNICALCORE}/dataset ] ; then
  trace_info "ERROR #3.2: Missing target directory: ${HOME_TECHNICALCORE}/dataset"
  exit 2
fi
# -r recursive -p : to keep file permissions
cp -r -p  ${HOME_TECHNICALCORE}/${PROJECT_INSTALL_FILE}/* ${HOME_TECHNICALCORE}/dataset/
CR=$?
if [ ! ${CR} -eq 0 ] ; then
  trace_info "ERROR #3.3($CR): cp -r -p  ${HOME_TECHNICALCORE}/${PROJECT_INSTALL_FILE}/* ${HOME_TECHNICALCORE}/dataset/"
  exit 2
else
  trace_info "INFO : STEP 3 : Dataset is available on: ${HOME_TECHNICALCORE}/dataset/"
fi

trace_log "INFO : STEP 3++ : Adapting rights for bin folder"
chmod 750 ${HOME_TECHNICALCORE}/dataset/bin/*
CR=$?
if [ ! ${CR} -eq 0 ] ; then
  trace_info "ERROR #3++.1($CR): chmod 750 ${HOME_TECHNICALCORE}/dataset/bin/* "
  exit 2
else
  trace_info "INFO : STEP 3++ : Dataset is available on: ${HOME_TECHNICALCORE}/dataset/"
fi

#
# =============================== STEP 4 ===============================
#

#STEP 4: HDFS hierarchy creation
#We will need to create HDFS structure first, hive tables secondly, load the data into HDFS and run all the transformations
#First check if structure already exist (++maybe go further on checking matches and add new directories) to avoid running the step 4.
#First approach: Do a count check on directories
#hadoop fs -ls /obs/data/src | wc -l
#hadoop fs -ls /obs/data/intermediate | wc -l
#hadoop fs -ls /obs/data/hive | wc -l
trace_log "INFO : STEP 4 : HDFS hierarchy creation"
continuer "STEP 4: Do you want to create HDFS structure ?"
CR=$?
if [ ${CR} -eq 0 ] ; then
  trace_info "INFO : HDFS structure creating..."
  SCRIPT_SHELL=${HOME_TECHNICALCORE}/dataset/bin/hdfs_dir.sh 

  if [ ! -x ${SCRIPT_SHELL} ] ; then
    trace_info "ERROR #4.1 Non executable ${SCRIPT_SHELL}"
    exit 2
  fi
  source ${SCRIPT_SHELL} 2>>${LOG_FILE} #1>>${LOG_FILE} 2>&1
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #4.2 ($CR): source ${SCRIPT_SHELL}"
	return
  else
    trace_info "INFO : HDFS structure done"
  fi
else
  trace_log "INFO : Ok, You have maybe already created the directories !"
fi


#
# =============================== STEP 5 ===============================
#

#STEP 5: Copy configuration files on hdfs
#Configuration file 1: Application.conf
#source : /home/TechnicalCore/dataset/bin/
#destination: /integration/TechnicalCore/use_case/bin/
#Ensure that Application.cong file exists in HDFS before continue

#Configuration file 2: All hql scripts
#source : /home/TechnicalCore/dataset/sql
#destination: /integration/TechnicalCore/use_case/sql
#Ensure that Application.cong file exists in HDFS before continue

CONF_FILE=application_uat.conf
BIN_PATH=${HOME_TECHNICALCORE}/dataset/config
CONF_FILE_PATH=${BIN_PATH}/${CONF_FILE}
SQL_PATH=${HOME_TECHNICALCORE}/dataset/sql/*

HDFS_BIN_PATH=/${DEV}/technical_core/dataset/bin/
HDFS_SQL_PATH=/${DEV}/technical_core/dataset/sql
#HDFS_BIN_PATH=/user/sdoumbia/home/TechnicalCore/dataset/bin
#HDFS_SQL_PATH=/user/sdoumbia/home/TechnicalCore/dataset/sql
trace_log "INFO  : STEP 5 : configuration files loading into HDFS"

continuer "STEP 5: Do you want to put configuration files into HDFS ?"
CR=$?
if [ ${CR} -eq 0 ] ; then
  trace_info "INFO : loading configuration files into hdfs ..."
  ##Moving configuration file 1 into hdfs
  if [ ! -f ${CONF_FILE_PATH} ] ; then
    trace_info "ERROR #5.1 The file ${CONF_FILE} doens't exist"
    exit 2
  fi
  hdfs dfs -put -f ${CONF_FILE_PATH} ${HDFS_BIN_PATH}
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #5.2 ($CR): hdfs dfs -put ${CONF_FILE_PATH} ${HDFS_BIN_PATH}"
    exit 2
  else
    trace_info "INFO : Configuration file : ${CONF_FILE} succesfully loaded into hdfs into ${CONF_FILE_PATH}"
  fi

  ##Moving configuration file 2 into hdfs
  if [ ! -f ${SQL_PATH} ] ; then
    trace_info "ERROR #5.3 The file ${SCRIPT_SHELL} doens't exist"
    exit 2
  fi
  hdfs dfs -put -f ${SQL_PATH} ${HDFS_SQL_PATH} 
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #5.4 ($CR): hdfs dfs -put ${SQL_PATH} ${HDFS_SQL_PATH}"
    exit 2
  else
    trace_info "INFO : configuration files : ${SQL_PATH} succesfully loaded into hdfs"
  fi
  
else
  ##Checking if configuration file 1 exist into hdfs
  hdfs dfs -ls ${HDFS_BIN_PATH}/${CONF_FILE} >>${LOG_FILE} 
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #5.5 ($CR): There is no ${CONF_FILE} on HDFS !"
    #exit 2
	return
  else
    trace_info "INFO : Configuration file 1 is avaible on HDFS : ${HDFS_BIN_PATH}/${CONF_FILE} "
  fi
  
  ##Checking if configuration files 2 exist into hdfs
  hdfs dfs -ls ${HDFS_SQL_PATH} >>${LOG_FILE}
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #5.6 ($CR): There are no hql files in ${HDFS_SQL_PATH} on HDFS !"
    #exit 2
	return
  else
    trace_info "INFO : Configuration files 2 are avaible on HDFS : ${HDFS_SQL_PATH}/* "
  fi
  
fi

#
# =============================== STEP 6 ===============================
#

#STEP 6: Hive output tables creation
#Hql files are available on hdfs
#ssh connect and Launch the hive scripts for creating the tables
#Jump back to the intital server : obitedhs-vet001
trace_log "INFO : STEP 6 : Hive output tables creation"

continuer "STEP 6: Do you want to create Hive tables ?"
CR=$?
if [ ${CR} -eq 0 ] ; then
  trace_info "INFO : Creating PARC_MARINE2 and TICKET_ENRICHI tables ..."
  HDFS_PATH="hdfs://${CLUSTER_IP}:${CLUSTER_PORT}"
  trace_log "Accessing ${HDFS_PATH}${HDFS_SQL_PATH} ..."
  ssh -p ${SSH_PORT} ${SSH_LOGIN}@${SSH_SERVER}<<EOF 1>>${LOG_FILE} 2>&1
date
hostname
hive --hiveconf tez.queue.name=TechnicalCore -f ${HDFS_PATH}${HDFS_SQL_PATH}/create_pn.hql
hive --hiveconf tez.queue.name=TechnicalCore --hivevar marine_location='/exploration/technical_core/publication/PN_PARC_MARINE2' -f ${HDFS_PATH}${HDFS_SQL_PATH}/parcMarine2.hql
hive --hiveconf tez.queue.name=TechnicalCore --hivevar ticket_location='/exploration/technical_core/publication/PN_TICKETS_ENRICHIS' -f ${HDFS_PATH}${HDFS_SQL_PATH}/ticketEnrich.hql
exit
EOF
  
  CR=$?
  if [ ! ${CR} -eq 0 ] ; then
    trace_info "ERROR #6.1 ($CR): ssh -p ${SSH_PORT} ${SSH_LOGIN}@${SSH_SERVER}"
    #exit 2
	return
  else
    trace_info "INFO : PARC_MARINE2 and TICKET_ENRICHI tables created"
  fi
else
  trace_log "INFO : Ok, You've maybe already created the tables !"
fi

### END OF INSTALLATION PART

trace_log "INFO  :loading files into hdfs"
trace_log "INFO : applying scala transformations"
trace_log "INFO : saving output"
trace_log "INFO : $PROJECT_INSTALL_FILE version executed succesfully"

trace_info "INFO : Installation finished "
enter_to_continue "press ENTER to continue"
