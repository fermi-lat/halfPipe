#!/bin/bash -vx
#
# DeliverEvents.sh <stagedir> <force> <lsfdir>
#
# This script examines $stagedir for event-index files written 
# by getLSEChunk and performs the following actions:
#
# - determine what runs are present
# - for each run, determine what APIDs / processors are present
# - for each apid, determine what datagrams are present
# - based on data completeness and forcing policy, create a merged
#   index of events, write the corresponding merged .evt file,
#   and deliver it to the analysis processing
#
# If $lsfdir is not specified then $stagedir is used by default
#
# Allowable values of $force are as follows:
#   "" or "yes"      -- always merge & deliver regardless of data completeness
#   "no"             -- never merge & deliver without complete data
#   "<directory>"    -- merge & deliver if a file named $groundId-$runstart
#                       exists in <directory>
#   "<# of minutes>" -- merge & deliver if all .idx files for $groundid-$runstart
#                       are more than N minutes old
#

# use scratch as tmp if available
if [ -d ${LSCRATCH} ] ; then
    export TMPDIR=${LSCRATCH}
fi

# get the command-line arguments
stagedir=$1
force=$2
lsfdir=$3

# deconflict operations with other parts of the chain
lock_file="${stagedir}/.decode"
echo "--------------------- DeliverEvents `date -u` -------------------------"
(set -C; : > $lock_file) 2> /dev/null
if [ $? != "0" ] ; then
    echo "$lock_file exists; another instance is running"
    exit 1
fi
trap 'rm -f $lock_file' EXIT

# determine the forcing strategy
if [ -z $force ] ; then
    # always deliver incomplete runs
    echo "forcing all incomplete runs"
    force="yes"
elif [ "x$force" == "xyes" ] ; then
    # always deliver incomplete runs
    echo "forcing all incomplete runs"
elif [ "x$force" == "xno" ] ; then
    # never deliver incomplete runs
    echo "requiring complete data for all runs"
elif [ -d $force ] ; then
    # deliver incomplete runs referenced in the specified directory
    forcedir=$force
    force="dir"
    echo "forcing incomplete runs referenced in $forcedir"
else
    # deliver incomplete runs older than the specified number of minutes
    forcetime=`date -u -d "-$force minutes" -Iseconds` || exit 1
    forcetime=`echo $forcetime | awk -F + '{print $1}' | tr 'T' ' '`
    echo "forcing incomplete runs prior to $forcetime UTC"
fi
if [ -z "$lsfdir" ] ; then
    echo "using input directory $stagedir for output"
    lsfdir="."
fi

# fail unless the input directory exists
if [ ! -d $stagedir ] ; then
    echo "input directory $stagedir does not exist, exiting"
    exit 1
fi

# drop into the input directory
pushd $stagedir 2>&1 >/dev/null

# get the list of groundId's present in the directory
echo "processing .idx files in $stagedir"
groundids=`find . -name '*.idx' -maxdepth 1 -print | awk -F- '{print $1}' | sort -u`
for gid in $groundids ; do

    # get the run-start time(s) for this gid
    gid=`basename $gid`
    runstarts=`ls -1 ${gid}-*.idx | awk -F- '{print $2}' | sort -u`
    for runstart in $runstarts; do
        # get the list of apids for this gid
	apids=`ls -1 ${gid}-${runstart}-*.idx | awk -F- '{print $3}' | sort -u`
	numapids=`ls -1 ${gid}-${runstart}-*.idx | awk -F- '{print $3}' | sort -u | wc -l`
	numcomplete=0
	echo "run $gid started at $runstart with $numapids APIDs"
	for apid in $apids ; do 
	    echo "examining APID $apid for $gid-$runstart"

	    # get the first and last datagram, and datagram count for this apid
	    firstdgm=`grep -h ^DGM $gid-$runstart-$apid-*.idx | sort -u | head -1`
	    lastdgm=`grep -h ^DGM $gid-$runstart-$apid-*.idx | sort -u | tail -1`
	    numdgms=`grep -h ^DGM $gid-$runstart-$apid-*.idx | sort -u | wc -l`
	    
	    # get the opening and closing actions for this apid
	    oaction=`echo $firstdgm | awk '{print $10}'`
	    caction=`echo $lastdgm | awk '{print $12}'`

	    # get the first and last sequence counts and the number of datagrams
	    firstseq=`echo $firstdgm | awk '{print $6}'`
	    lastseq=`echo $lastdgm | awk '{print $6}'`

	    # if we've got the first and last datagrams, see if we have all of them
	    if [[ "x$oaction" == "xstart" && "x$caction" == "xstop" ]] ; then
		echo "found start/stop datagrams for APID $apid of $gid"
		if ((numdgms == lastseq + 1)) ; then
		    ((numcomplete++))
		    echo "all $numdgms APID $apid datagrams for $gid are present, numcomplete=$numcomplete"
		else
		    echo "only $numdgms of $[lastseq+1] APID $apid datagrams for $gid are present"
		fi
	    elif [ "x$oaction" == "xstart" ] ; then
		echo "missing stop datagram for APID $apid of $gid"
	    elif [ "x$caction" == "xstop" ] ; then
		echo "missing start datagram for APID $apid of $gid"
	    else
		echo "missing start/stop datagrams for APID $apid of $gid"
	    fi

	done

	# implement the data-force logic
	if [ $numcomplete -eq $numapids ] ; then
	    echo "found complete data for $gid-$runstart"
	elif [ "x$force" == "xno" ] ; then
	    echo "Requiring complete data for delivery of $gid-$runstart"
	elif [ "x$force" == "xyes" ] ; then
	    echo "Unconditionally forcing delivery of incomplete run $gid-$runstart"
	    numcomplete=$numapids
	elif [ "x$force" == "xdir" ] ; then
	    if [ -f $forcedir/$gid-$runstart ] ; then
		echo "$force/$gid-$runstart exists, forcing delivery of incomplete run"
		numcomplete=$numapids
	    fi
	else
	    numidx=`find . -name "${gid}-${runstart}-*.idx" -print | wc -l`
	    numold=`find . -name "${gid}-${runstart}-*.idx" -mmin +${force} -print | wc -l`
	    if [ $numold -eq $numidx ] ; then
		echo "all ${gid}-${runstart} files expired, forcing delivery of incomplete run"
		numcomplete=$numapids
	    fi
	fi

        # create the merged index and deliver the .evt file
	if [ $numcomplete -eq $numapids ] ; then
	    # create an output subdirectory for this run
	    mkdir -p $gid 2>&1 >/dev/null
	    mergeidx=$gid/$gid-$runstart.idx

	    # write out the merged index
	    grep -h ^EVT $gid-$runstart-*.idx | sort -u -b -k 3g \
		| awk '{print $1 " " $2 " " $3 " " $8 " " $9 " " $11 " " $12 " " $14 " " $15}' \
		>$gid/$gid-$runstart.idx

	    # write and deliver the merged .evt file
	    writeMerge.exe $mergeidx "$gid/r%010d-e%020d.evt" 0 || exit 1
	    echo "delivering merged file $gid-$runstart.evt to $lsfdir"
	    mv $gid/r*.evt $lsfdir/$gid-$runstart.evt || exit 1

	    # clean up the stage directory
	    rm -rf $gid
	    rm $gid-$runstart-*
	else
	    echo "incomplete data for $gid, skipping"
	fi

    done

done
	
# return from the input directory
popd 2>&1 >/dev/null


	    
	    

		
		
	


