#!/usr/bin/env python
import os
import sys

run=False
#run=True

taskBase='/sdf/group/fermi/ground/PipelineConfig/halfPipe/v7r0p0'
fmxRoot  = '/sdf/group/fermi/ground/fmx' #/sdf/group/fermi/a/fmx'
pipelineFlavor = 'DEV'
#pipelineFlavor = 'TEST'

#downlinkID = 230401008
#outputBase = '/sdf/data/fermi/ground/ISOC/flight/Downlinks'

#downlinkID = 241009001
downlinkID = 241029014
downlinkID = 250102001
#downlinkID = 250129010
#downlinkID = 250131001
#downlinkID = 250205006
#downlinkID = 250212013
downlinkID =250226012
outputBase = '/sdf/data/fermi/ground/ISOC/test/Downlinks'
#outputBase = '/sdf/home/o/omodei/testHP'

# solo per i dls di Stephen
#downlinkID = 240414002
#outputBase = '/sdf/group/fermi/a/isoc/flightOps/volumes/vol5/offline/halfPipe/v7r0p0_devel/u42/ISOC-test/Downlinks'

fosFlavor  = 'PROD' if pipelineFlavor == 'PROD' else 'TEST'
fosFlavor  = 'ISOC_' + fosFlavor

#pipeline='/sdf/group/fermi/kube/pipeline-II/dev/pipeline'
#pipeline='/sdf/home/g/glast/a/pipeline-II/prod/pipeline'
pipeline='/sdf/group/fermi/sw/pipeline-II/dev/pipeline'
#pipeline='/sdf/home/g/glast/a/pipeline-II/dev/pipeline'

chunks = { 230401001: 10, 230401002: 10, 230401003: 14,
           230401005: 10, 230401006: 10, 230401007:  8,
           230401008: 10, 230401009: 10, 230401010:  8,
           230401011: 12, 230401012:  8, 230401013: 14,
           230401014: 9, 230401015:  6, 230401016:  6,
           230401017: 4, 230401018:  6,
           240328012:14, 240328013:  6,
           240414002: 6,
           241009001: 8,
           241029014: 2,
           250102001:10,
           250129010:14,
           250131001:8,
           250205006:14,
           250212013:6,
           250226012:8}


try:
    numChunks = chunks[downlinkID]
except KeyError:
    print ("Provide number of chunks for downlink", downlinkID)
    sys.exit(1)

options={
    'pipelineFlavor':pipelineFlavor,
    'fosFlavor':fosFlavor,
    'downlinkID':downlinkID,
    'numChunks':numChunks,
    'outputBase':outputBase,
    'level1Name':'L1Proc',
    'maxEvents':125000,
    'taskBase':taskBase,
    'fmxRoot':fmxRoot,
    'onlineName':'intOnlineAnalysis',
    'startL1':0,
    'startOnline':0
}

# Cleanup existing:
for extension in ['.evt','.idx','.txt']:
    cmd='rm -rf %s/%s/*%s' % (options['outputBase'],options['downlinkID'],extension)
    print (cmd)
    if run: os.system(cmd)

# remove locks:
cmd='rm -rf %s/lock/halfpipe-%s' % (options['outputBase'],options['downlinkID'])
print (cmd)
if run: os.system(cmd)

# Copy from Downlinks on rhel6:
cmd='scp -rp omodei@rhel6-64:/nfs/farm/g/glast/u42/ISOC-flight/Downlinks/%s %s/.' % (options['downlinkID'],options['outputBase'])
#print (cmd)

print('/sdf/group/fermi/sw/pipeline-II/dev/pipeline -m DEV load ../xml/HalfPipe-test.xml')

cmd='%s createStream  HalfPipe-s3df' % pipeline
for k,v in options.items():
    cmd+=' --define %s=%s' % (k,v)
    pass

cmd='%s -m %s createStream -S %s -D "' % (pipeline,pipelineFlavor,downlinkID)
for k,v in options.items():
    cmd+='%s=%s,' % (k,v)
    pass
cmd=cmd[:-1]
cmd+='" HalfPipe-s3df'
#"downlinkID=241009001,onlineName=intOnlineAnalysis,fmxRoot=/sdf/group/fermi/ground/fmx,startL1=1,numChunks=8,level1Name=L1Proc,startOnline=1,taskBase=/sdf/group/fermi/ground/PipelineConfig/halfPipe/v7r0p0,pipelineFlavor=DEV,fosFlavor=ISOC_TEST,outputBase=/sdf/data/fermi/ground/ISOC/test/Downlinks,maxEvents=125000" HalfPipe-s3df' % (pipeline,pipelineFlavor,downlinkID)


print (cmd)
if run: os.system(cmd)
