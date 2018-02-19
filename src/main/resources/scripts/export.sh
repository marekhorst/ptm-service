#!/bin/bash

# This script is responsible for exporting topic modeling outcome from local postgres database to remote database.
# Only limited set of tables is taken into account, results are filtered by experiment identifier.
# Implicit assumptions:
# * target database is accessible to 'ptm' user, password (whenever required) is defined in ~/.pgpass file
# * target database model needs to be created before running this script
# * target database model has to be aligned with source database model (only with respect to the tables taking part in copying process)
#
# The following input parameters are required:
# * source database name (the data will be read from)
# * target database name (the data will be copied to)
# * destination host name where target database is deployed
# * experiment identifier limiting the set of data to be exported

set -eu

sourceDB=$1
targetDB=$2
hostName=$3
experimentId=$4

if [ -z $sourceDB ]; then 
    echo "exitting, source database name is unset!"
    exit 1;
fi

if [ -z $targetDB ]; then 
    echo "exitting, target database name is unset!"
    exit 1;
fi

if [ -z $hostName ]; then 
    echo "exitting, target host name is unset!"
    exit 1;
fi

if [ -z $experimentId ]; then 
    echo "exitting, experiment identifier is unset!"
    exit 1;
fi

# removing already existing entries for given experiment identifier
commands='
delete from pubtopic where experimentid = '\'$experimentId\'';
delete from experiment where experimentid = '\'$experimentId\'';
delete from topicdetails where experimentid = '\'$experimentId\'';
delete from topicdescription where experimentid = '\'$experimentId\'';
delete from topicanalysis where experimentid = '\'$experimentId\'';
delete from expdiagnostics where experimentid = '\'$experimentId\'';
'
psql -U ptm -d $targetDB -h $hostName -c "$commands"

# exporting data to remote database
table_names_csv='pubtopic,experiment,topicdescription,topicdetails,topicanalysis,expdiagnostics'

OIFS=$IFS
IFS=','
table_names=($table_names_csv)

for table_name in ${table_names[@]}; do 
    psql -c " copy (select * from $table_name where experimentId = '$experimentId') to stdin " $sourceDB | psql -U ptm -d $targetDB -h $hostName -c " copy $table_name from stdout "
done

IFS=$OIFS

