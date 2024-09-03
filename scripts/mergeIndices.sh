#!/bin/bash
#
# script to merge chunk indices into acquisitions
#

# Set up the environment for FlightOps code.
flavor=`cat ${taskBase}/config/flavor`
platform=`/afs/slac/g/glast/isoc/flightOps/isoc-platform`
echo "using ISOC platform $platform flavor $flavor with halfPipe v6r2p0"
eval `/afs/slac/g/glast/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

# point to a compatible MySQL client lib 
export LD_LIBRARY_PATH=/afs/slac/g/glast/ground/GLAST_EXT/rh9_gcc32/MYSQL/4.1.22/lib/mysql:$LD_LIBRARY_PATH

# use scratch as tmp if available
if [ -d /scratch ] ; then
    export TMPDIR=/scratch
fi

# to deal with compressed data, we need access to FMX
export FMX_C_FDB=/afs/slac.stanford.edu/g/glast/fmx

# set up the per-chunk environment variables
export HP_OUTPUTDIR="$HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID"
export HP_ORPHANFILE="$HALFPIPE_OUTPUTBASE/orphan-events.idx"

# drop into the input directory
pushd $HP_OUTPUTDIR 2>&1 >/dev/null

# get the level0 file key from the retrieval-definition files
echo "getting l0key value"
cat >l0key.xsl <<EOF
<?xml version="1.0" encoding="US-ASCII"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/">
    <xsl:value-of select="retdef/run/@l0key"/>
  </xsl:template>
</xsl:stylesheet>
EOF
#rdfile=`ls -1 $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID/RetDef-*.xml | head -1` # fails if too many files
rdfile=`find $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID -name 'RetDef-*.xml' -print | head -1`
l0key=`xsltproc l0key.xsl $rdfile | tail -1`
rm l0key.xsl

# get the list of run-starts present in the directory and block the
# cleanup-run processing for L1Proc
runstarts=`find . -name '????????-????????-????-?????.idx' -maxdepth 1 -print | awk -F- '{print $2}' | sort -u`
tokendir=`cat ${taskBase}/config/stagedir`/chunktokens
for rst in $runstarts ; do
    rst_upper=$(echo $rst | tr '[a-f]' '[A-F]')
    runid=r0$(echo "ibase=16; $rst_upper" | bc)
    echo "creating chunktoken directory ${tokendir}/${runid}"
    mkdir -p ${tokendir}/${runid}
    touch ${tokendir}/${runid}/haltCleanup-${HALFPIPE_DOWNLINKID}
done
echo "created chunk-token lockfiles:"
ls -l ${tokendir}/r*/haltCleanup-${HALFPIPE_DOWNLINKID}

# run the posting application, make sure to use the right MOOT database...
if [ x"$flavor" != "xISOC_PROD" ] ; then
    export MOOT_ARCHIVE=/afs/slac/g/glast/moot/srcArchive-test/
fi
echo "posting summary information with MOOT_ARCHIVE = $MOOT_ARCHIVE"
time python ${taskBase}/scripts/AcqSummary.py -p glastops -d $HALFPIPE_DOWNLINKID -k $l0key \
    -i $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID --load --retire --evttimes -f $HALFPIPE_OUTPUTBASE/force --moot || exit 1

# spin off the merging substreams
iStream=0
for rst in $runstarts ; do
    rst_upper=$(echo $rst | tr '[a-f]' '[A-F]')
    rst_decimal=$(echo "ibase=16; $rst_upper" | bc)
    pipelineCreateStream doMerge ${rst_decimal} "HALFPIPE_RUNSTART=\"${rst}\""
done

