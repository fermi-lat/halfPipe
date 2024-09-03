#!/bin/bash
#
# script to clean up temporary .evt and .idx files
#

# make sure the lockfile gets removed
lock_file="${HALFPIPE_OUTPUTBASE}/lock/cleanup"
trap 'rm -f $lock_file' EXIT

# remove this stream's overall lockfile
rm -f ${HALFPIPE_OUTPUTBASE}/lock/halfpipe-${HALFPIPE_DOWNLINKID}

# Set up the environment for FlightOps code.
flavor=`cat ${taskBase}/config/flavor`
platform=`/afs/slac/g/glast/isoc/flightOps/isoc-platform`
echo "using ISOC platform $platform flavor $flavor with halfPipe v6r2p0"
eval `/afs/slac/g/glast/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

# use scratch as tmp if available
if [ -d /scratch ] ; then
    export TMPDIR=/scratch
fi

# bail out if not configured to run
if [ -f ${taskBase}/config/haltCleanup ] ; then
    echo "skipping cleanup"
    exit 0
fi

# clean up evt/idx files for completed runs
python ${taskBase}/scripts/RunCleanup.py --nodryrun --lockdir ${HALFPIPE_OUTPUTBASE}/lock

# clean up broken links from stage directory
for f in ${HALFPIPE_OUTPUTBASE}/stage/*.idx ; do
    stat -L $f >/dev/null || rm $f
done
