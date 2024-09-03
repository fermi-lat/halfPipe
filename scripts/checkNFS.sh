#!/bin/sh

# Set up the environment for FlightOps code.
#flavor=`cat ${taskBase}/config/flavor`
#platform=`/sdf/group/fermi/a/isoc/flightOps/isoc-platform`
#eval `/sdf/group/fermi/a/isoc/flightOps/${platform}/${flavor}/bin/isoc isoc_env --add-env=flightops`

# stat the raw-archive directory a few times to try and make sure it's mounted
#rawarch=`python -c 'from ISOC import SiteDep; print SiteDep.get( "RawArchive", "archdir" )' 2>/dev/null`
#echo "stat'ing rawarchive dir $rawarch"
#exec stat -t $rawarch
echo "check NFS disabled"
exit 1
