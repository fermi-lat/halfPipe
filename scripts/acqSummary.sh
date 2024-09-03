#!/bin/sh
#
# script to post downlink-summary information to the database
#

# Set up the environment for FlightOps code.
flavor=`cat ${taskBase}/config/flavor`
platform=`/afs/slac/g/glast/isoc/flightOps/isoc-platform`
echo "using ISOC platform $platform flavor $flavor with halfPipe v6r2p0"
eval `/afs/slac/g/glast/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

# use scratch as tmp if available
if [ -d /scratch ] ; then
    export TMPDIR=/scratch
fi

# make a set of chunk-token files for the level-1 pipeline
for d in `find $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID -type d -name 'r??????????'`; do
    tokdir=$HALFPIPE_OUTPUTBASE/chunktokens/`basename $d`
    mkdir -p $tokdir
    for f in $d/r*.evt; do
	touch $tokdir/`basename $f .evt`
    done
done

# get the level0 file key from the retrieval-definition files
cat >l0key.xsl <<EOF
<?xml version="1.0" encoding="US-ASCII"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/">
    <xsl:value-of select="retdef/run/@l0key"/>
  </xsl:template>
</xsl:stylesheet>
EOF
rdfile=`ls -1 $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID/RetDef-*.xml | head -1`
l0key=`xsltproc l0key.xsl $rdfile | tail -1`

# run the posting application
AcqSummary.py -p glastops -d $HALFPIPE_DOWNLINKID -k $l0key \
    -i $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID --load --retire -f $HALFPIPE_OUTPUTBASE/force
