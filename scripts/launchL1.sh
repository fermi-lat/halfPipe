#!/bin/bash -vx

# Set up the environment for FlightOps code.
#flavor=`cat ${taskBase}/config/flavor`
flavor=${fosFlavor}
platform=`/afs/slac/g/glast/isoc/flightOps/isoc-platform`
echo "using ISOC platform $platform flavor $flavor with halfPipe v7r0p0"
eval `/afs/slac/g/glast/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

if [ -s ${taskBase}/config/sitedep.ini ] ; then
    echo "==============================================================="
    echo "Modifying SiteDep settings with ${taskBase}/config/sitedep.ini:"
    cat ${taskBase}/config/sitedep.ini
    export ISOC_SITEDEP=${taskBase}/config/sitedep.ini
    echo "==============================================================="
fi

# use scratch as tmp if available
if [ -d ${LSCRATCH} ] ; then
    export TMPDIR=${LSCRATCH}
fi

# drop into the output directory
pushd ${HALFPIPE_OUTPUTBASE}/${HALFPIPE_DOWNLINKID}

# get the level0 archive directory from the retrieval-definition files
echo "getting L0 archive directory"
cat >l0arch.xsl <<EOF
<?xml version="1.0" encoding="US-ASCII"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/">
    <xsl:value-of select="retdef/arch"/>
  </xsl:template>
</xsl:stylesheet>
EOF
#rdfile=`ls -1 $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID/RetDef-*.xml | head -1` # fails if too many files
rdfile=`find $HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID -name 'RetDef-*.xml' -print | head -1`
l0arch=`xsltproc l0arch.xsl $rdfile | tail -1`
echo $PWD
ls -l l0arch.xsl
rm l0arch.xsl
echo "Level-0 archive at $l0arch"

# dump the magic-7 data for the span covered by the first and last events decoded
tevt0=`head -n 1 -q */evt-*.idx | sort | head -n 1 | awk '{print $6}' | python -c 'import datetime, sys; print datetime.datetime.utcfromtimestamp( float( sys.stdin.read() ) - 6000.0 )'`
tevt1=`tail -n 1 -q */evt-*.idx | sort | tail -n 1 | awk '{print $6}' | python -c 'import datetime, sys; print datetime.datetime.utcfromtimestamp( float( sys.stdin.read() ) + 6000.0 )'`
scid=`head -n 1 -q */evt-*.idx | sort | head -n 1 | awk '{print $7}'`
echo "retrieving magic-7 data from scid $scid for $tevt0 --> $tevt1"
time ${taskBase}/scripts/DiagRet.py --arch $l0arch --scid $scid -b "$tevt0" -e "$tevt1" --lsm | grep -E 'ATT|ORB' > magic7_${HALFPIPE_DOWNLINKID}.txt
rc=$?
if [ $rc -ne 0 ] ; then
    echo "Error retrieving magic-7 data"
    exit $rc
fi

# gather summary results from the individual acquisitions
cat */r??????????-delivered.txt > delivered_events_${HALFPIPE_DOWNLINKID}.txt
cat */r??????????-retired.txt > retired_runs_${HALFPIPE_DOWNLINKID}.txt
cat */r??????????-evtgaps.txt > event_gaps_${HALFPIPE_DOWNLINKID}.txt

# optionally override the delivered data type
LPAtype=`cat $taskBase/config/LPAtype`
LCItype=`cat $taskBase/config/LCItype`
if [ -n "$LPAtype" ] ; then
    echo "Overriding LPA datatype to $LPAtype"
    sed -i -e "s/LPA/$LPAtype/g" delivered_events_${HALFPIPE_DOWNLINKID}.txt
fi
if [ -n "$LCItype" ] ; then
    echo "Overriding LCI datatype to $LCItype"
    sed -i -e "s/LCI/$LCItype/g" delivered_events_${HALFPIPE_DOWNLINKID}.txt
fi

# short-circuit the L1 dispatch
if [ -f $taskBase/config/haltStage ] ; then
    echo "skipping file-staging and $HALFPIPE_L1TASK dispatch due to global lock"
    exit 0
fi
if [ -f haltStage ] ; then
    echo "skipping file-staging and $HALFPIPE_L1TASK dispatch due to local lock"
    exit 0
fi

# copy the results to the pipeline-staging area
echo "staging data for L1Proc"
stagedir=`cat ${taskBase}/config/stagedir`
if [[ $flavor =~ "TEST" ]]; then
    echo "Found 'TEST' in $flavor"
    stagedir=${stagedir}Test
fi
#cp -p *.txt ${stagedir}/${HALFPIPE_DOWNLINKID} || exit 1
cp --preserve=timestamps,ownership *.txt ${stagedir}/${HALFPIPE_DOWNLINKID} || exit 1

# clean up any large files
rm -f magic7_${HALFPIPE_DOWNLINKID}.txt

# remove the chunktokens locks
tokendir=${stagedir}/chunktokens
echo "removing chunk-tokens lockfiles:"
ls -l ${tokendir}/r*/haltCleanup-${HALFPIPE_DOWNLINKID}
rm -f ${tokendir}/r*/haltCleanup-${HALFPIPE_DOWNLINKID}

# short-circuit the L1 dispatch
if [ -f $taskBase/config/haltL1 ] ; then
    echo "skipping $HALFPIPE_L1TASK dispatch due to global lock"
    exit 0
fi
if [ -f haltL1 ] ; then
    echo "skipping $HALFPIPE_L1TASK dispatch due to local lock"
    exit 0
fi

# make sure enough Java-stuff is on the path
export PATH=/usr/local/bin:bin:/usr/bin:$PATH
#echo "Submitting task: ${HALFPIPE_L1TASK}"
#submit_file=$HALFPIPE_OUTPUTBASE/$HALFPIPE_DOWNLINKID/createStream_$HALFPIPE_L1TASK.sh
#echo 'echo Noting to submit!' > $submit_file
#[[ $HALFPIPE_STARTL1 -eq 0 ]] ||
#    echo 'echo Submit!' > $submit_file
#chmod a+x $submit_file
#echo /sdf/group/fermi/sw/pipeline-II/dev/pipeline -m ${HALFPIPE_PLFLAVOR} createStream \
    #	 -S $HALFPIPE_DOWNLINKID \
    #	 -D "DOWNLINK_ID=${HALFPIPE_DOWNLINKID}" \
    #	 -D "DOWNLINK_RAWDIR=${stagedir}/${HALFPIPE_DOWNLINKID}" \
    #	 $HALFPIPE_L1TASK > $submit_file
# We can try this with the new bind mounts (see start_rhel6.sh)
# dispatch the L1 pipeline for this downlink.
[[ $HALFPIPE_STARTL1 -eq 0 ]] || \
    exec /sdf/group/fermi/sw/pipeline-II/dev/pipeline -m ${pipelineFlavor} createStream \
	 -S $HALFPIPE_DOWNLINKID \
         -D "DOWNLINK_ID=${HALFPIPE_DOWNLINKID}" \
         -D "DOWNLINK_RAWDIR=${stagedir}/${HALFPIPE_DOWNLINKID}" \
	 $HALFPIPE_L1TASK
         #TonyHelloS3DF
         #$HALFPIPE_L1TASK

#TonyHelloS3DF
