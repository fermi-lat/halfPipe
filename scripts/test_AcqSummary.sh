#!/bin/bash 
export mypython='shisoc --add-env=flightops python'
taskBase='/sdf/home/o/omodei/L1P/halfPipe/v7r0p0_devel' 
HALFPIPE_DOWNLINKID='230401001'
l0key='1081720'
HALFPIPE_OUTPUTBASE='/sdf/group/fermi/a/isoc/flightOps/volumes/vol5/offline/halfPipe/v7r0p0_devel/outputBase'


${mypython} ${taskBase}/scripts/AcqSummary.py -p glastops -d $HALFPIPE_DOWNLINKID -k $l0key \
	    -i $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID --load --retire --evttimes -f $HALFPIPE_OUTPUTBASE/force --moot || exit 1
