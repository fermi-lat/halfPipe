#!/bin/sh
lock_file=$1
mkdir -p `dirname $lock_file`
echo "--------------------- lockFile `date -u` -------------------------"
(set -C; : > $lock_file) 2> /dev/null
if [ $? != "0" ] ; then
    echo "$lock_file exists; another instance is running"
    exit 1
fi
echo "$lock_file does not exist; ok to run"
exit 0

