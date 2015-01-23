#!/bin/bash
if [ -n "$JAVA_HOME" ];then
   JAVA=$JAVA_HOME/bin/java
else
   JAVA=java
fi 

function abspath(){
    pushd $1 >/dev/null 2>&1
    pwd
    popd $1 >/dev/null 2>&1
}
cmd=$1;shift
basedir=$1;shift
[ -d $basedir ] || { echo "$1 not exists!!!";exit 0;}
basedir=$(abspath $basedir)
CLASSPATH="$(abspath ${basedir}/target/classes/):$(abspath ${basedir})"

for jar in `ls $basedir/lib/*.jar`;do
    CLASSPATH=$jar:$CLASSPATH
done

case `uname` in
    CYGWIN*)cygwin=true
        ;;
    *) cygwin=false
esac

if $cygwin;then
    CLASSPATH=`cygpath -wp "$CLASSPATH"`
fi

#echo java -cp $CLASSPATH $*
pids="pids"
class=$1
case $cmd in
    daemon)
        nohup java -cp $CLASSPATH $* >/dev/null 2>&1 </dev/null &
        if [ $? -eq 0 ];then
            echo $! >${class}.pid
        else
            echo "Can't start $class";
        fi
        ;;
    start)
        java -cp $CLASSPATH $* 
        if [ $? -eq 0 ];then
            echo $! >>$pids
        fi
        ;;
    stop)
        pidfile=${class}.pid
        if [ ! -f $pidfile ];then
            echo $pidfile not exists!
            exit 1
        fi

        for pid in $(cat $pidfile);do
            if kill -0 $pid >/dev/null 2>&1;then
                echo kill -9 $pid
                kill -9 $pid
            else
                echo process $pid not exists!
            fi
        done
        ;;
    *)
        echo run_java {basedir} start {package.class} {arg1} {arg2}...
        echo run_java {baeedir} stop  {package.class}
        ;;
esac
