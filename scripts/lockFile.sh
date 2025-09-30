#!/bin/sh
lock_file=${1:?}
maxTime=3600 # seconds, don't poll longer
elapsedTime=0
mkdir -p `dirname $lock_file`

while [ $elapsedTime -lt $maxTime ]; do
    echo "--------------------- lockFile `date -u` -------------------------"
    (set -C; : > $lock_file) 2> /dev/null
    if [ $? != "0" ] ; then
        delay=$[60+RANDOM/256] # wait 60-187 seconds
        elapsedTime=$[elapsedTime+delay]
        echo "$lock_file exists; another instance is running, waiting $delay seconds"
        sleep $delay
    else
        echo "$lock_file does not exist; ok to run"
        exit 0
    fi
done

exit 1


