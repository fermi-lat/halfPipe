#!/bin/env python
#                               Copyright 2010
#                                     by
#                        The Board of Trustees of the
#                     Leland Stanford Junior University.
#                            All rights reserved.
#

"""Scan the output of getLSEChunk.exe for exceptions and post a
message to the central event log for each one (event type
halfPipe.chunkException).
"""

__facility__ = "GLAST ISOC"
__abstract__ = __doc__
__author__   = "Stephen Tether <tether@slac.stanford.edu> SLAC - GLAST ISOC"
__date__     = "2010/07/27"
__updated__  = "$Date: 2010/07/29 21:46:56 $"
__version__  = "$Revision: 1.1 $"
__release__  = "$HeadURL: file:///nfs/slac/g/glast/online/svnroot/quarks/trunk/boilerplate/module.py $"
__credits__  = "SLAC"

import quarks.legal.copyright

## @namespace logChunkExceptions
#  @brief Scan the standard input text for exception messages and
#  post a message to the central event log for each one (event type
#  halfPipe.dfiException). Echo all lines to stdout.
#
#  The standard input is expected to be the standard output/error of
#  getLSEChunk.exe.  The two command line arguments should be the
#  downlink ID and the chunk ID. There are three sources of
#  exceptions.  PktFile and PktRetriever exceptions mark problems in
#  getting the raw data from the archive. DFI exceptions mark problems
#  in parsing datagrams into events.
#
#  We keep track of the run involved (there should be just one) by
#  looking for lines beginning with "getLSEChunk: processing" since
#  they contain the run number in hex.


import re, sys

from ISOC import Log

def main(downlinkid, chunkid):
    infile = sys.stdin
    redflag = re.compile(r"\b(?:DFI|PktFile|PktRetriever)\s+exception\b")
    runline = re.compile(r"getLSEChunk: processing\s+[0-9A-Fa-f]+-([0-9A-Fa-f]+)-")
    run = "?"
    for line in infile:
        print line,
        match = runline.match(line)
        if match:
            run = int(match.group(1), 16)
        match = redflag.search(line)
        if match:
            logError(line.strip(), downlinkid, chunkid, str(run))
    return 0

def logError(text, downlinkid, chunkid, run):
    tag = "downlink=%s;chunk=%s;run=%s" % (downlinkid, chunkid, run)
    Log.error("halfPipe.chunkException", text, tgt=tag)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >>sys.stderr, "Usage: logChunkExceptions.py DOWNLINKID CHUNKID"
        sys.exit(1)
    sys.exit( main(sys.argv[1], sys.argv[2]) )
