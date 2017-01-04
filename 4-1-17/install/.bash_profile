# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
        . ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/.local/bin:$HOME/bin

export PATH

###############################################################################
##                                FLOW                                       ##
###############################################################################

#Flow executable
export HFLOW='/opt/FLW/flowcli/bin/FLOW/CLIENTUAT'

#Log folder for flow script reader
export REP_PPREP_LOG='/var/opt/data/log'

#Define destination folder
export REP_PPREP_DATA='/var/opt/data/app'

################################################################################
##                             END    FLOW                                    ##
################################################################################

### FUNCTIONS DECLARATION
# trace_log : Saves logs and prompt them on screen in debug mode (debug= ?)
# trace_info: Prompt infos on screen to keep the install on track
LOG_DIR=/var/opt/data/log
BASENAME=$(basename $0)
LOG_FILE=${LOG_DIR}/${BASENAME}_`date +"%Y_%m_%d.log"`
function trace_info {
	echo `date +"%Y/%m/%d %H:%M:%S"` $* | tee -a ${LOG_FILE}
}
trace_info "INFO: bash_profile was sourced"

#when login, this script will ask for predictive network env variables import
#the config can be found on bash_profile_pn
#if the user accepts , the bash_profile_pn is sourced
#otherwise the user skip this step

x=0
while [ $x = 0 ]
do
	trace_info "Do you want to import predictive network configuration ? (y/n)"
	read continue
	case "$continue" in
	    y|Y)
		filepn='.bash_profile_pn'
		source $filepn
		x=1
		;;
		n|N)
		trace_info "INFO: Ok, you don't need $filepn config file ..."
		x=1
		;;
		*)
		trace_info "WARNING: Enter a correct input please !"
		;;
	esac
done