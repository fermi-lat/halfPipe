#!/bin/bash -vx
#
# script to extract event data from raw packets
#

# use scratch as tmp if available
echo ${LSCRATCH}
if [ -d ${LSCRATCH} ]; then
    export TMPDIR=${LSCRATCH}
fi
echo ${TMPDIR}
#df -hP
#id
#ls -ld ${HALFPIPE_OUTPUTBASE}

# create an overall lockfile for this stream
lockFile=${HALFPIPE_OUTPUTBASE}/lock/halfpipe-${HALFPIPE_DOWNLINKID}
: > ${lockFile}
ls -l ${lockFile}

# Set up the environment for FlightOps code.
#flavor=`cat ${taskBase}/config/flavor`
flavor=${fosFlavor}
echo 'Running MakeEvt.sh with flavor $flavor'

platform=`/afs/slac/g/glast/isoc/flightOps/isoc-platform`
echo "using ISOC platform $platform flavor $flavor with halfPipe"
eval `/afs/slac/g/glast/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

# to deal with compressed data, we need access to FMX
export FMX_C_FDB="$HALFPIPE_FMXROOT"

# set up the per-chunk environment variables
export HP_OUTPUTDIR="$HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID"
export HP_RETDEFCHUNKFILE="$HP_OUTPUTDIR/RetDef-$HALFPIPE_DOWNLINKID-$HALFPIPE_CHUNKID.xml"
export HP_EVTCHUNKFILE="$HP_OUTPUTDIR/$HALFPIPE_DOWNLINKID-$HALFPIPE_CHUNKID.evt"
export HP_IDXCHUNKFILE="$HP_OUTPUTDIR/$HALFPIPE_DOWNLINKID-$HALFPIPE_CHUNKID.idx"

# optionally override translated LATC master key
if [ -x $taskBase/config/overrideLATCdecode.sh ] ; then
    hwkey=`$taskBase/config/overrideLATCdecode.sh`
    overrideLATC=""
    if [ ! -z $hwkey ] ; then
	overrideLATC="-k $hwkey"
    fi
fi

# stat the raw-archive directory a few times to try and make sure it's mounted
#rawarch=`python -c 'from ISOC import SiteDep; print SiteDep.get( "RawArchive", "archdir" )' 2>/dev/null`
#echo "stat'ing rawarchive dir $rawarch"
#stat $rawarch
#stat $rawarch
#stat $rawarch
#stat $rawarch

# now run the decoding application, making sure that errors in either
# getLSEChunk.exe or logChunkExceptions.py are reported
#/sdf/group/fermi/a/isoc/flightOps/rhel5_gcc41/ISOC_P ROD/lib/FLIGHTOPS_P ROD/bin/getLSEChunk.exe
#/sdf/group/fermi/a/isoc/flightOps/rhel5_gcc41/ISOC_T EST/lib/FLIGHTOPS_T EST/bin/getLSEChunk.exe

set -o pipefail
strace=strace
${strace} getLSEChunk.exe -r ${HP_RETDEFCHUNKFILE} -o ${HP_OUTPUTDIR} ${overrideLATC} 2>&1 | \
    ${taskBase}/scripts/logChunkExceptions.py ${HALFPIPE_DOWNLINKID} ${HALFPIPE_CHUNKID}
