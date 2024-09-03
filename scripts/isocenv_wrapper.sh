#!/bin/bash -vx

# #!/sdf/data/fermi/a/isoc/s3df/bin/start_isoc.sh ${pipelineFlavor} 5

#prog=$(basename $0)

#isocBuild=rhel5_gcc41
# for rh6 mysql.so.18 is in ${ISOC_INSTALLROOT}/lib/FLIGHTOPS_/lib !

#export ORACLE_HOME=/sdf/group/fermi/sw/package/oracle/d/linux/11.1.0/
#export ISOC_INSTALLROOT=/sdf/group/fermi/a/isoc/flightOps/${isocBuild}/${fosFlavor}
#export FLIGHTOPSROOT=${ISOC_INSTALLROOT}/lib/FLIGHTOPS_${pipelineFlavor} # libeventRet.so

#export LD_LIBRARY_PATH=${FLIGHTOPSROOT}/lib:${ISOC_INSTALLROOT}/lib:${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}
#export            PATH=${FLIGHTOPSROOT}/bin:${ISOC_INSTALLROOT}/bin:${ORACLE_HOME}/bin:${PATH}

#for i in ORACLE_HOME ISOC_INSTALLROOT FLIGHTOPSROOT LD_LIBRARY_PATH PATH; do
#    eval echo "${prog}: $i=\${$i}"
#done

#set | sed -e 's/^/'${prog}': /'

#echo "${prog}: executing $*"

exec $*
