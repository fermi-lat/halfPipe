#!/bin/sh

CONTAINERDIR=/sdf/group/fermi/sw/containers
CONTAINER_IMAGE=$CONTAINERDIR/fermi-rhel6.sif
#apptainer shell --cleanenv -B /sdf \
#apptainer shell --cleanenv --env "DISPLAY=${DISPLAY}" -B /sdf \
#-B $CONTAINERDIR/rhel6/afs/slac.stanford.edu/package/perl:/afs/slac.stanford.edu/package/perl \
#only at runtime, to load mysql: -B /sdf/data/fermi/a/isoc/s3df/nottww:/opt/TWWfsw \

#apptainer shell -B /sdf:/sdf \
#		-B /sdf/data/fermi/a:/afs/slac/g/glast \
#		-B /sdf/data/fermi/a:/afs/slac.stanford.edu/g/glast \
#		-B /sdf/group/fermi/n:/nfs/farm/g/glast \
#                $CONTAINER_IMAGE

#apptainer exec -B /sdf:/sdf \
#		-B /sdf/data/fermi/a:/afs/slac/g/glast \
#		-B /sdf/data/fermi/a:/afs/slac.stanford.edu/g/glast \
#		-B /sdf/group/fermi/n:/nfs/farm/g/glast \
#		-B /sdf/group/fermi/sw/package:/afs/slac/package \
#		-B /sdf/group/fermi/sw/package:/afs/slac.stanford.edu/package \
#		-B $CONTAINERDIR/rhel6/opt/TWWfsw:/opt/TWWfsw \
#		-B $CONTAINERDIR/rhel6/usr/local:/usr/local \
#		$CONTAINER_IMAGE ls /
apptainer exec -B /sdf:/sdf -B /lscratch:/lscratch $CONTAINER_IMAGE echo "LSCRATCH=$LSCRATCH"; ls /lscratch
