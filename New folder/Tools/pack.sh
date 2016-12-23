#!/bin/sh

num_version=`grep version ../build.sbt | cut -d\" -f2`
date_generation=$1
build_number=$2

project_home='./..'
deploy_home='./OBS_PN_'$num_version'-'$build_number

# nettoyage avant recreation
rm -rf $deploy_home

# creation repertoire livraisont
mkdir -p $deploy_home
mkdir -p $deploy_home/lib
mkdir -p $deploy_home/bin
mkdir -p $deploy_home/sql
mkdir -p $deploy_home/src
mkdir -p $deploy_home/src/ParcMarine2
mkdir -p $deploy_home/src/TicketEnrichi
mkdir -p $deploy_home/src/CommonPN

#creation of version.txt
cat > $deploy_home/version.txt << EOF
version_number = $num_version
build date = $date_generation
build_number = $build_number
EOF

# copy des jar 
cp $project_home/ParcMarine2/target/scala-2.10/obs_pn_parcmarine2*.jar $deploy_home/lib
cp $project_home/TicketEnrichi/target/scala-2.10/obs_pn_ticketenrichi*.jar $deploy_home/lib
cp $project_home/CommonPN/target/scala-2.10/obs_pn_common*.jar $deploy_home/lib

# copy des dependencies
for i in $(cat jar_dependencies.txt)
do 
	cp $project_home/TicketEnrichi/target/pack/lib/$i $deploy_home/lib
done


#copy des shell
cp $project_home/CommonPN/bin/* $deploy_home/bin

#copy des sql
cp $project_home/CommonPN/sql/* $deploy_home/sql

#copy des sources
cp -R $project_home/ParcMarine2/src/* $deploy_home/src/ParcMarine2/
cp -R $project_home/TicketEnrichi/src/* $deploy_home/src/TicketEnrichi/
cp -R $project_home/CommonPN/src/* $deploy_home/src/CommonPN/

# remove SVN directories
find $deploy_home -name ".svn" -type d -exec rm -r "{}" \;

#Compression finale
#tar -cvf OBS_PN_$num_version.tar $deploy_home
zip -r OBS_PN_$num_version-$build_number.zip ./OBS_PN_$num_version-$build_number
