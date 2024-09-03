#!/bin/bash
#
# script to deliver events for and launch Online analysis
#

# make sure the lockfile gets removed
lock_file="${HALFPIPE_OUTPUTBASE}/lock/launchOnline"
trap 'rm -f $lock_file' EXIT

# Set up the environment for FlightOps code.
flavor=`cat ${taskBase}/config/flavor`
platform=`/afs/slac/g/glast/isoc/flightOps/isoc-platform`
echo "using ISOC platform $platform flavor $flavor with halfPipe v6r2p0"
eval `/afs/slac/g/glast/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

# use scratch as tmp if available
if [ -d /scratch ] ; then
    export TMPDIR=/scratch
fi

# set up the source and destination directories
HP_IDXSRC="${HALFPIPE_OUTPUTBASE}/${HALFPIPE_DOWNLINKID}"
HP_IDXDST="${HALFPIPE_OUTPUTBASE}/stage"
HP_EVTDST="${HALFPIPE_OUTPUTBASE}/evt"
HP_FORCEDIR="${HALFPIPE_OUTPUTBASE}/force"

# link the index files to the staging directory
find $HP_IDXSRC -name '????????-????????-????-?????.idx' -exec ln -sf {} $HP_IDXDST \;

# get the SCID of the decoded data
idxf=`ls -1 $HP_IDXSRC/????????-????????-????-?????.idx | head -1`
scid=`grep ^DGM $idxf | head -1 | awk '{print $4}'`

# get the list of .evt files currently in the output directory
find $HP_EVTDST -name '*.evt' -print | sort > evt0.txt

# short-circuit the Online dispatch
if [ -f $taskBase/config/haltOnline ] ; then
    echo "skipping $HALFPIPE_ONLINETASK dispatch due to global lock"
    exit 0
fi
if [ -f ${HP_IDXSRC}/haltOnline ] ; then
    echo "skipping $HALFPIPE_ONLINETASK dispatch due to local lock"
    exit 0
fi

# invoke the Online event-delivery script
${taskBase}/scripts/DeliverEvents.sh $HP_IDXDST $HP_FORCEDIR $HP_EVTDST || exit 1

# get the list of .evt files currently in the output directory
# and determine the list of acquisitions to be launched as Online analyses
find $HP_EVTDST -name '*.evt' -print | sort > evt1.txt
comm -1 -3 evt0.txt evt1.txt > launch.txt
if [ -s launch.txt ] ; then
    echo "launching newly-completed runs:"
    cat launch.txt
else
    echo "no runs to launch"
fi

# launch an Online analysis job for each new .evt file
export PATH=$PATH:/usr/local/bin:/bin:/usr/bin
farm_root=`dirname $HALFPIPE_OUTPUTBASE`/Online
for f in `cat launch.txt`; do
    rstx=`basename $f .evt | awk -F- '{print $2}'`
    echo "trying to map $rstx..."
    rm -f aq_${rstx}.txt
    ${taskBase}/scripts/AcqToAlgAndQueue.py --scid ${scid} --started 0x${rstx} -o aq_${rstx}.txt || exit 1
    if [ ! -f aq_${rstx}.txt ] ; then
	echo "no key/alg/queue mapping for $rstx, removing $f"
	rm -f $f
	continue
    fi
    echo "found key/alg/queue mapping for $rstx == `cat aq_${rstx}.txt`"
    runid=`cat aq_${rstx}.txt | awk '{print $1}'`
    mkey=`cat aq_${rstx}.txt | awk '{print $2}'`
    alg=`cat aq_${rstx}.txt | awk '{print $3}'`
    queue=`cat aq_${rstx}.txt | awk '{print $4}'`
    [[ $HALFPIPE_STARTONLINE -eq 0 ]] || \
	/afs/slac.stanford.edu/g/glast/ground/bin/pipeline -m ${HALFPIPE_PLFLAVOR} createStream \
	-D "EVENT_FILE=$f" -D "ANALYSIS_QUEUE=$queue" -D "ANALYSIS_ALGORITHM=$alg" -D "RUN_ID=$runid" \
	-D "FLAVOR=`echo $flavor | awk -F_ '{print $2}'`" -D "FARM_ROOT=$farm_root" $HALFPIPE_ONLINETASK
done
