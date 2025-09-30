#!/bin/bash
echo "prolog script is called..."

prolog_file_path=$1
while true; do
    if test -f "$prolog_file_path"; then
        echo "File $prolog_file_path exists!!!!"
        sleep 5
    else
        echo "File $prolog_file_path does not exist."
        break
    fi
done
exit 0
