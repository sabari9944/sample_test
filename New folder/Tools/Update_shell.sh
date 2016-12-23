#!/bin/sh

# Get version number from build.sbt
num_version=`grep version ../build.sbt | cut -d\" -f2`

echo $num_version

# Update shell to execute the version
sed -i 's/num_version/'$num_version'/g' ../CommonPN/bin/transformation_job.sh