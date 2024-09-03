#!/usr/bin/env python
import os
import sys

run=False
run=True

mode = 'DEV'

downlinkID = 230401008
outputBase = '/sdf/group/fermi/a/isoc/flightOps/volumes/vol5/offline/halfPipe/v7r0p0_devel/u42/ISOC-flight/Downlinks'

# solo per i dls di Stephen
#downlinkID = 240414002
#outputBase = '/sdf/group/fermi/a/isoc/flightOps/volumes/vol5/offline/halfPipe/v7r0p0_devel/u42/ISOC-test/Downlinks'

pipelineFlavor  = 'PROD' if mode == 'PROD' else 'TEST'
fosFlavor  = 'ISOC_' + pipelineFlavor

#pipeline='/sdf/group/fermi/kube/pipeline-II/dev/pipeline'
#pipeline='/sdf/home/g/glast/a/pipeline-II/prod/pipeline'
pipeline='/sdf/group/fermi/sw/pipeline-II/dev/pipeline'
#pipeline='/sdf/home/g/glast/a/pipeline-II/dev/pipeline'

chunks = { 230401001: 10, 230401002: 10, 230401003: 14,               230401005: 10, 230401006: 10, 230401007:  8, 230401008: 10, 230401009: 10, 230401010:  8,
           230401011: 12, 230401012:  8, 230401013: 14, 230401014: 9, 230401015:  6, 230401016:  6, 230401017:  4, 230401018:  6,
                          240328012: 14, 240328013:  6,
                          240414002:  6 }

try:
    numChunks = chunks[downlinkID]
except KeyError:
    print ("Provide number of chunks for downlink", downlinkID)
    sys.exit(1)

options={
    'mode':mode,
    'pipelineFlavor':pipelineFlavor,
    'fosFlavor':fosFlavor,
    'downlinkID':downlinkID,
    'numChunks':numChunks,
    'outputBase':outputBase,
    'level1Name':'L1Proc',
    'maxEvents':125000,
    'taskBase':'/sdf/group/fermi/a/isoc/flightOps/volumes/vol5/offline/halfPipe/v7r0p0_devel',
    'fmxRoot':'/sdf/group/fermi/a/fmx',
    'onlineName':'intOnlineAnalysis',
    'startL1':1,
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
print (cmd)


cmd='%s createStream  HalfPipe-s3df' % pipeline
for k,v in options.items():
    cmd+=' --define %s=%s' % (k,v)
    pass
print (cmd)
if run: os.system(cmd)
