#!/bin/bash
HUBBINDIR=`dirname $0`

function abspath(){
    pushd $1 >/dev/null 2>&1
    pwd
    popd $1 >/dev/null 2>&1
}

HUBBINDIR=`abspath $HUBBINDIR`

PORT=${1-8811}
echo $HUBBINDIR/run_java.sh stop ${HUBBINDIR}/.. ranpanf.thrift.HBaseHub 
$HUBBINDIR/run_java.sh stop ${HUBBINDIR}/.. ranpanf.thrift.HBaseHub 
