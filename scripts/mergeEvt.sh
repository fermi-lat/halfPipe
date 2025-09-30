#!/bin/bash -vx
#
# script to merge multiple evt files as directed by an index
#

#export mypython='shisoc --add-env=flightops python'

# use scratch as tmp if available
echo ${LSCRATCH}
if [ -d ${LSCRATCH} ]; then
    export TMPDIR=${LSCRATCH}
fi
echo ${TMPDIR}
df -hP
id
ls -ld ${HALFPIPE_OUTPUTBASE}

# HALFPIPE_RUNSTART is empty.  Thus no valid lock_file path can be constructed.
# On rh6, HALFPIPE_OUTPUTBASE should be the hex code of the run id.
# Here, in the lock dir is a file halfpipe-230401001 (i.e. the dl id).
# What the heck!

echo "HALFPIPE_OUTPUTBASE=${HALFPIPE_OUTPUTBASE}"
echo "HALFPIPE_RUNSTART=${HALFPIPE_RUNSTART}"
echo "HALFPIPE_RUNSTART_DEX=${HALFPIPE_RUNSTART_DEX}"

############################################################### DIRTY HACKS
# set RUNSTART manually for the time being (or we always get it from here)
#[ -z "${HALFPIPE_RUNSTART}" ] && export HALFPIPE_RUNSTART=$(printf "%x" ${PIPELINE_STREAM})
export HALFPIPE_RUNSTART=$(printf "%x" ${PIPELINE_STREAM})
echo "PIPELINE_STREAM=${PIPELINE_STREAM}"
echo "HALFPIPE_RUNSTART=${HALFPIPE_RUNSTART}"

# just make everything writeable
#ls -ld ${HALFPIPE_OUTPUTBASE}
#chmod -R a+w ${HALFPIPE_OUTPUTBASE}
#############################################################################
$prolog_script
return_code=$?
# Check the return code
if [ $return_code -eq 1 ]; then
  exit 1
fi
# make sure the lockfile gets removed
lock_file="${HALFPIPE_OUTPUTBASE}/lock/${PIPELINE_STREAM}"
# exit 1
trap 'rm -f $lock_file' EXIT

# Set up the environment for FlightOps code.
#flavor=`cat ${taskBase}/config/flavor`
flavor=${fosFlavor}
platform=`/afs/slac/g/glast/isoc/flightOps/isoc-platform`
echo "using ISOC platform $platform flavor $flavor with halfPipe v6r2p0"
eval `/afs/slac/g/glast/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

# to deal with compressed data, we need access to FMX
export FMX_C_FDB="$HALFPIPE_FMXROOT"

# set up the environment variables
export HP_OUTPUTDIR="$HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID"

# drop into the input directory
pushd $HP_OUTPUTDIR 2>&1 >/dev/null
echo "PWD=${PWD}"
# make a subdirectory for this acquisition
if [ -d ${PIPELINE_STREAM} ] ; then
    echo "removing existing directory ${PIPELINE_STREAM}"
    rm -rvf ${PIPELINE_STREAM}
fi
mkdir ${PIPELINE_STREAM}
echo "created working directory ${PIPELINE_STREAM}"

# make a single datagram-index file for this rst
# include all datagrams previously decoded
echo "creating datagram index"
time grep -h ^DGM ${HALFPIPE_OUTPUTBASE}/*/${PIPELINE_STREAM}/dgm-${HALFPIPE_RUNSTART}.idx | \
    sort > ${PIPELINE_STREAM}/dgm-previous-${HALFPIPE_RUNSTART}.tmp

time grep -h ^DGM *-${HALFPIPE_RUNSTART}-*.idx | \
    sort > ${PIPELINE_STREAM}/dgm-current-${HALFPIPE_RUNSTART}.tmp

time sort -u -b -k 2n,2 -k 5n,5 -k 6n,6 ${PIPELINE_STREAM}/dgm-*-${HALFPIPE_RUNSTART}.tmp > ${PIPELINE_STREAM}/dgm-${HALFPIPE_RUNSTART}.idx
rm -vf ${PIPELINE_STREAM}/dgm-*-${HALFPIPE_RUNSTART}.tmp

# make a single event-index file for this rst
# include all events previously decoded
echo "creating event index"
time grep -h ^EVT ${HALFPIPE_OUTPUTBASE}/*/${PIPELINE_STREAM}/evt-${HALFPIPE_RUNSTART}.idx | \
    sort -u -b -k 3g,3 -k 8n,8 > ${PIPELINE_STREAM}/evt-previous-${HALFPIPE_RUNSTART}.tmp

time grep -h ^EVT *-${HALFPIPE_RUNSTART}-*.idx | \
    sort -u -b -k 3g,3 -k 8n,8 > ${PIPELINE_STREAM}/evt-current-${HALFPIPE_RUNSTART}.tmp

time sort -u -b -k 3g,3 -k 8n,8 ${PIPELINE_STREAM}/evt-*-${HALFPIPE_RUNSTART}.tmp > ${PIPELINE_STREAM}/evt-${HALFPIPE_RUNSTART}.idx
rm -vf ${PIPELINE_STREAM}/evt-*-${HALFPIPE_RUNSTART}.tmp

# run the merging application for each acquisition
echo "merging indices"
time python $taskBase/scripts/MergeDatagrams.py \
    -d ${PIPELINE_STREAM}/dgm-${HALFPIPE_RUNSTART}.idx \
    -e ${PIPELINE_STREAM}/evt-${HALFPIPE_RUNSTART}.idx \
    -o ${PIPELINE_STREAM} \
    -l ${HALFPIPE_DOWNLINKID} \
    -b ${HALFPIPE_OUTPUTBASE} --merge || exit 1

# put the moot key/alias into the environment so it gets stored in the .evt files
moot_file=moot_keys_${HALFPIPE_DOWNLINKID}.txt
export LSEWRITER_MOOTKEY=`grep ${PIPELINE_STREAM} $moot_file | awk '{print $2}'`
export LSEWRITER_MOOTALIAS=`grep ${PIPELINE_STREAM} $moot_file | awk '{print $3}'`
echo "exported LSEWRITER_MOOTKEY=$LSEWRITER_MOOTKEY"
echo "exported LSEWRITER_MOOTALIAS=$LSEWRITER_MOOTALIAS"

# bail out on staging the output files if specified
if [ -f ${taskBase}/config/haltStage ] ; then
    echo "Skipping evt-file staging due to global lock"
    exit 0
fi
if [ -f haltStage ] ; then
    echo "Skipping evt-file staging due to local lock"
    exit 0
fi

# get the run-directory name and create it
destdir=`ls -1 ${PIPELINE_STREAM}/r??????????-e*.idx | head -1 | awk -F- '{print $1}'`
# No data for the run? Just quit.
[ -z "$destdir" ] && exit 0
destdir=`basename $destdir`
if [ -d $destdir ] ; then
    echo "removing existing directory $destdir"
    rm -rvf $destdir
fi
stagedir=`cat ${taskBase}/config/stagedir`

if [[ $flavor =~ "TEST" ]]; then
    echo "mergeEvt::Found 'TEST' in $flavor"
    stagedir=${stagedir}Test
fi

tokendir=${stagedir}/chunktokens/${destdir}
destdir=${stagedir}/${HALFPIPE_DOWNLINKID}/$destdir
rm -rvf $destdir
mkdir -pv $destdir || exit 1
echo "created evt-file staging directory $destdir"

# pick up any chunk-scaling environment variables
if [ -s $taskBase/config/ChunkScaling ] ; then
    echo "Overriding chunk-scaling parameters:"
    cat $taskBase/config/ChunkScaling
    . $taskBase/config/ChunkScaling
fi

# make a local output directory for the merged .evt files
scratchdir="$LSCRATCH/$$"
mkdir -pv $scratchdir || exit 1
trap 'rm -vf $lock_file; rm -rvf $scratchdir' EXIT
echo "created evt-file output scratch directory $scratchdir"

# set the output filespec and write the merged index files
outFile="${scratchdir}/r%010d-e%020d.evt"
for midx in `ls -1 ${PIPELINE_STREAM}/r??????????-e*.idx` ; do

    # optionally override translated LATC master key
    if [ -x $taskBase/config/overrideLATCmerge.sh ] ; then
	runid=`basename $destdir`
	overrideLATC=`$taskBase/config/overrideLATCmerge.sh $runid`
    fi

    # optionally override max number of events-per-chunk
    maxEvents=$HALFPIPE_MAXEVENTS
    if [ -s $taskBase/config/maxEvents ] ; then	maxEvents=`cat $taskBase/config/maxEvents`
	echo "overriding maxEvents from $HALFPIPE_MAXEVENTS to $maxEvents"
    fi

    # now run the merging application
    echo "writing merged event files for $midx to $scratchdir"
    time writeMerge.exe $midx $outFile $HALFPIPE_DOWNLINKID $maxEvents $overrideLATC || exit 1

done

# copy the merged files to the staging directory
echo "copying merged files from $scratchdir to $destdir"
for f in $scratchdir/*.evt ; do
    echo "copying $f to $destdir"
    time cp -pv $f $destdir || exit 1
done

# make a set of chunk-token files for the level-1 pipeline
echo "making chunk-tokens"
mkdir -pv $tokendir || exit 1
for f in $destdir/r*.evt; do
    touch $tokendir/`basename $f .evt` || exit 1
done

