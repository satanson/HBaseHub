#!/bin/bash
HUBBINDIR=`dirname $0`

function abspath(){
    pushd $1 >/dev/null 2>&1
    pwd
    popd $1 >/dev/null 2>&1
}

HUBBINDIR=`abspath $HUBBINDIR`

INETADDR=${1-localhost:8811}
HOST=${INETADDR%%:*}
PORT=${INETADDR##*:}
echo $HUBBINDIR/run_java.sh start ${HUBBINDIR}/.. ranpanf.thrift.HBaseHubClient  $PORT $HOST
$HUBBINDIR/run_java.sh start ${HUBBINDIR}/.. ranpanf.thrift.HBaseHubClient $PORT $HOST 
