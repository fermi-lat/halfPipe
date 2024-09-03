#!/usr/bin/env python

import datetime, logging, os, pprint, subprocess, sys
from collections import defaultdict

from ISOC import Log

_log = logging.getLogger()

MERGE_APIDS = ( 956, 957, 958 )

class NoDatagramsFound( RuntimeError ):
    """!Raised when no datagrams are found in an index file"""

class TooManyOrphans( RuntimeError ):
    """!Raised when fewer events are merged than orphaned."""

class DatagramSegment( object ):
    """A contiguous series of decoded datagrams."""
    def __init__( self, idx = None, session = None, dgms = None ):
        src = None
        if idx:
            src = idx
            # get the list of datagrams from the index file and capture the first and last
            ifd = subprocess.Popen( 'grep ^DGM %s' % idx, shell=True, stdout=subprocess.PIPE ).stdout
            self.dgmlist = [ DgmIdx( dgmstr ) for dgmstr in ifd ]
            if len( self.dgmlist ) == 0:
                raise NoDatagramsFound( 'No datagrams found in idx file %s' % idx )
        elif dgms:
            src = 'input list'
            self.dgmlist = dgms
        else:
            raise NoDatagramsFound( 'No index file or datagram list' )
                
        _log.info( 'DatagramSegment::__init__: found %d datagrams in %s' % ( len(self.dgmlist), src ) )
        self.dgm0 = self.dgmlist[0]
        self.dgm1 = self.dgmlist[-1]
        self.nevts  = sum( [ x.nevts for x in self.dgmlist ] )
        self.scid = self.dgm0.scid
        self.startedat = self.dgm0.startedat
        self.apid = self.dgm0.apid
        self.dgmseq0 = self.dgm0.datagrams
        self.dgmseq1 = self.dgm1.datagrams

        if session:
            session.save_or_update( self.dgm0 )
            session.save_or_update( self.dgm1 )
            session.save_or_update( self )

    def __len__( self ):
        return self.ndgms

    def __eq__( self, rhs ):
        if not rhs: return False
        return self.dgm0 == rhs.dgm0 and self.dgm1 == rhs.dgm1

    def __lt__( self, rhs ):
        if not rhs: return False
        if self.apid == rhs.apid:
            return self.dgm0 < rhs.dgm0
        else:
            return self.apid < rhs.apid

    def expunge( self, session ):
        if session:
            session.expunge( self.dgm0 )
            session.expunge( self.dgm1 )
            session.expunge( self )

    def report( self ):
        _log.info( 'Segment for apid %d with %d datagrams and %d events (%d.%d:%s, %d.%d:%s)' % \
                   ( self.apid, self.ndgms, self.nevts, self.dgmseq0, self.evtseq0,
                     self.oaction, self.dgmseq1, self.evtseq1, self.caction ) )

    @property
    def key( self ):
        return ( self.startedAt, self.evtseq0 )

    @property
    def hwkey( self ):
        return self.dgm0.hwkey

    @property
    def swkey( self ):
        return self.dgm0.swkey

    @property
    def ndgms( self ):
        return self.dgm1.datagrams - self.dgm0.datagrams + 1

    @property
    def startedAt( self ):
        return self.dgm0.startedat

    @property
    def groundId( self ):
        return self.dgm0.groundid
    
    @property
    def oaction( self ):
        return self.dgm0.oaction

    @property
    def evtseq0( self ):
        return self.dgm0.evtseq0

    @property
    def dgmutc0( self ):
        return self.dgm0.utc

    @property
    def evtutc0( self ):
        return self.dgm0.evtutc0

    @property
    def evtseq1( self ):
        return self.dgm1.evtseq1

    @property
    def caction( self ):
        return self.dgm1.caction

    @property
    def dgmutc1( self ):
        return self.dgm1.utc

    @property
    def evtutc1( self ):
        return self.dgm1.evtutc1


class DgmIdx( object ):
    """!object to represent a decoded datagram"""
    def __init__( self, instr ):
        fields = instr.split()
        if fields[0].startswith( 'DGM' ):
            self.startedat   = int(   fields[ 1] )
            self.utc         = datetime.datetime.utcfromtimestamp( float( fields[ 2] ) )
            self.scid        = int(   fields[ 3] )
            self.apid        = int(   fields[ 4] )
            self.datagrams   = int(   fields[ 5] )
            self.groundid    = int(   fields[ 6] )
            self.modechanges = int(   fields[ 7] )
            self.modename    =        fields[ 8]
            self.oaction     =        fields[ 9]
            self.oreason     =        fields[10]
            self.caction     =        fields[11]
            self.creason     =        fields[12]
            self.platform    =        fields[13]
            self.origin      =        fields[14]
            self.crate       =        fields[15]
            self.evtutc0     = datetime.datetime.utcfromtimestamp( float( fields[16] ) )
            self.evtseq0     = long(  fields[17] )
            self.evtutc1     = datetime.datetime.utcfromtimestamp( float( fields[18] ) )
            self.evtseq1     = long(  fields[19] )
            self.nevts       = int(   fields[20] )
            if len(fields) > 21: self.hwkey       = long(  fields[21] )
            if len(fields) > 22: self.swkey       = long(  fields[22] )

            # store the original representations of the timestamps
            self.utc_stamp    = fields[2]
            self.evtutc0_stamp = fields[16]
            self.evtutc1_stamp = fields[18]

    def __eq__( self, rhs ):
        if not rhs: return False
        return self.scid == rhs.scid and \
               self.apid == rhs.apid and \
               self.startedat == rhs.startedat and \
               self.datagrams == rhs.datagrams

    def __str__( self ):
        buf = "DGM: %10d %19s %3d %4d %7d %8d %3d %16s %16s %16s %16s %16s %16s %16s %16s %19s %20d %19s %20d %10d %10d %10d" % \
              (self.startedat, self.utc_stamp, self.scid, self.apid, self.datagrams, self.groundid,
               self.modechanges, self.modename, self.oaction, self.oreason, self.caction, self.creason,
               self.platform, self.origin, self.crate, self.evtutc0_stamp, self.evtseq0,
               self.evtutc1_stamp, self.evtseq1, self.nevts, self.hwkey, self.swkey )
        return buf

    @property
    def key( self ):
        return ( self.startedAt, self.evtseq0 )

class EvtIdx( object ):
    __slots__ = 'startedAt', 'sequence', 'apid', 'datagrams', 'oaction', 'caction', 'fileofst', 'evtfile'
    def __init__( self, evtfile, evtstr ):
        # store the filename
        self.evtfile   = evtfile

        # extract the fields from the index string
        fields = evtstr.split()

        # populate the data members
        if len( fields ) == 9:
            self.startedAt = int( fields[1] )
            self.sequence  = long( fields[2] )
            self.apid      = int( fields[3] )
            self.datagrams = int( fields[4] )
            self.oaction   = fields[5]
            self.caction   = fields[6]
            self.fileofst  = long( fields[7] )
            self.evtfile   = fields[8]
        else:
            self.startedAt = int( fields[1] )
            self.sequence  = long( fields[2] )
            self.apid      = int( fields[7] )
            self.datagrams = int( fields[8] )
            self.oaction   = fields[10]
            self.caction   = fields[11]
            self.fileofst  = long( fields[13] )
            if len( fields ) > 14:
                self.evtfile = fields[14]

    @property
    def key( self ):
        return ( self.startedAt, self.sequence )

    def __str__( self ):
        return 'EVT: %10d %20d %4d %8d %s %s %20d %s' % ( self.startedAt, self.sequence, self.apid, self.datagrams,
                                                          self.oaction, self.caction, self.fileofst, self.evtfile )

    def __eq__( self, other ):
        return self.startedAt == other.startedAt and self.sequence == other.sequence

    def __ne__( self, other ):
        return not other == self

if __name__ == '__main__':

    import cPickle, glob, itertools, os, pprint, sys

    from quarks.cmdline.xoptparse import OptionParser

    def gen_EvtIdx( idxfile ):
        for estr in open( idxfile ):
            yield ( estr, EvtIdx( None, estr ) )
        return

    def merge( opts ):

        # create DgmIdx objects for each line in the input file
        dgmlists = defaultdict( list )
        seglists = defaultdict( list )
        for dgmstr in open( opts.dgmidx ):
            didx = DgmIdx( dgmstr )
            _log.info("%s", dgmstr[:60]);
            if len( dgmlists[ didx.apid ] ) > 0:
                if didx.datagrams == dgmlists[didx.apid][-1].datagrams+1:
                    dgmlists[ didx.apid ].append( didx )
                else:
                    _log.info( 'new segment %d,%d' % ( didx.apid, didx.datagrams ) )
                    seglists[didx.apid].append( DatagramSegment( dgms = dgmlists[didx.apid] ) )
                    dgmlists[didx.apid] = [didx, ]
            else:
                _log.info( 'new apid %d' % didx.apid )
                dgmlists[didx.apid].append( didx )

        for apid, dgms in dgmlists.iteritems():
            seglists[apid].append( DatagramSegment( dgms = dgms ) )

        # capture the acquisition start-time
        startedAt = seglists.values()[0][0].startedAt

        # report the segments found and check for completeness
        acq_complete = True
        for apid, seglist in seglists.iteritems():
            for seg in seglist:
                seg.report()
            apid_complete = len(seglist) == 1 and seglist[0].oaction=='start' and seglist[0].caction in ('stop', 'abort', 'pause')
            acq_complete = acq_complete and apid_complete
            apid_state = 'COMPLETE' if apid_complete else 'INCOMPLETE'
            _log.info( 'r%010d has %s data for apid %d' % ( startedAt, apid_state, apid ) )
        if acq_complete:
            _log.info( 'retiring r%010d as COMPLETE' % startedAt )
            ofd_ret = open( os.path.join( opts.outdir, 'r%010d-retired.txt' % startedAt ), 'w' )
            print >> ofd_ret, 'r%010d COMPLETE' % startedAt
            ofd_ret.close()
        elif opts.forcedir:
            forcefiles = glob.glob( os.path.join( opts.forcedir, '????????-????????' ) )
            forcestarts = [ eval( '0x'+ os.path.split( x )[1].split('-')[1] ) for x in forcefiles ]
            if startedAt in forcestarts:
                _log.info( 'retiring r%010d as INCOMPLETE' % startedAt )
                ofd_ret = open( os.path.join( opts.outdir, '%r010d-retired.txt' % startedAt ), 'w' )
                print >> ofd_ret, 'r%010d INCOMPLETE' % startedAt
                ofd_ret.close()

        # create list of event spans for output.  LCI is easy, LPA must be merged
        newspans = []
        ofd_orph  = None
        if not set( seglists.keys() ).issubset( set( MERGE_APIDS ) ):
            acqtype = 'LCI'
            _log.info( 'generating list of output spans for LCI data' )
            newspans = [ (x.evtseq0, x.evtseq1) for x in seglists.values()[0] ]
        else:
            if len( seglists.keys() ) != 2:
                _log.warning( 'Found apid segments for %s, cannot merge!' % str(seglists.keys()) )
                return 0
            acqtype = 'LPA'
            _log.info( 'generating list of output spans for LPA data' )
            iter_a = itertools.chain( seglists.values()[0] )
            iter_b = itertools.chain( seglists.values()[1] )
            a = iter_a.next()
            b = iter_b.next()
            bfoundstop = False
            while True:
                # find any overlapping ranges
                if a.oaction == 'start' and b.oaction == 'start':
                    # found the start of the acquisition, check to see if we have
                    # the stop as well...
                    if a.caction == 'stop' and b.caction == 'stop':
                        _log.info( 'merging complete acquisition' )
                        bfoundstop = True
                        newspans.append( (min( a.evtseq0, b.evtseq0 ), max( a.evtseq1, b.evtseq1 )) )
                    else:
                        _log.info( 'merging from start' )
                        newspans.append( (min( a.evtseq0, b.evtseq0 ), min( a.evtseq1, b.evtseq1 )) )

                elif a.caction == 'stop' and b.caction == 'stop':
                    # found the end of the acquisition
                    _log.info( 'merging to stop' )
                    bfoundstop = True
                    newspans.append( (max( a.evtseq0, b.evtseq0 ), max( a.evtseq1, b.evtseq1 )) )

                else:
                    # in the middle of the acquisition
                    _log.info( 'merging at continue' )

                    # do the ranges overlap?
                    if a.evtseq1 >= b.evtseq0 and b.evtseq1 >= a.evtseq0:
                        newspans.append( (max( a.evtseq0, b.evtseq0 ), min( a.evtseq1, b.evtseq1 )) )

                # get the next segment
                try:
                    if a.evtseq1 <= b.evtseq1:
                        apid = a.dgm0.apid
                        a = iter_a.next()
                    else:
                        apid = b.dgm0.apid
                        b = iter_b.next()
                except StopIteration:
                    _log.info( 'no more segments for apid %d' % apid )
                    break

                # end of the mergeable-span loop
                pass

        # write out a list of event gaps for FT2 livetime corrections
        _log.info( 'list of mergeable event spans: %s' % pprint.pformat( newspans, indent=5 ) )
        if len( newspans ) > 1:
            ofd_evtgap = open( os.path.join( opts.outdir, 'r%010d-evtgaps.txt' % startedAt ), 'w' )
            gapstart = newspans[0][1]
            for span in newspans[1:]:
                gapend = span[0]
                _log.info( 'event gap r%010d %d %d' % ( startedAt, gapstart, gapend ) )
                print >> ofd_evtgap, 'r%010d %d %d' % ( startedAt, gapstart, gapend )
                gapstart = span[1]
            ofd_evtgap.close()

        # get the list of spans already merged
        donespans = []
        if opts.basedir:
            for spanfile in glob.glob( os.path.join( opts.basedir, '*/*/r%010d-spans.txt' % startedAt ) ):
                # skip any file from a previous execution in our current location
                if os.path.split( os.path.split( os.path.abspath( spanfile ) )[0] )[0] == os.path.abspath( os.path.join( opts.basedir, opts.downlink ) ):
                    _log.info( 'skipping existing merged-span file %s' % spanfile )
                    continue
                donespans.extend( cPickle.load( open( spanfile ) ) )
                _log.info( 'getting previously-merged spans from %s' % spanfile )
            donespans.sort()
            if donespans: _log.info( 'list of previously-delivered spans: %s' % pprint.pformat( donespans, indent=5) )

        # now write out the index of merged, undelivered events
        _log.info( 'found %d event spans for output' % len(newspans) )
        mergespans = []
        nevt = nmerged = norphaned = nskipped = 0
        ofd_merge = None
        iter_eidx = itertools.chain( open( opts.evtidx ) )
        iter_done = itertools.chain( donespans )
        iter_span = itertools.chain( newspans )
        e0 = e1 = -1
        d0 = d1 = -1
        merge0 = merge1 = 0
        while True:
            # get the next event in the index
            try:
                estr = iter_eidx.next()
            except StopIteration:
                _log.info( 'exhausted event index' )
                if ofd_merge:
                    _log.info( 'closing %s at %d with %d skipped %d merged in %s %d orphaned from %d total' % \
                               (ofd_merge.name, eidx.sequence, nskipped, nmerged, (merge0, merge1), norphaned, nevt) )
                    mergespans.append( ( merge0, merge1 ) )
                    ofd_merge.close()
                    ofd_merge = None
                break
            eidx = EvtIdx( None, estr )
            nevt += 1
            if nevt % 100000 == 0: _log.info( 'processed %d events' % nevt )

            # get the next mergeable span if any
            try:
                span = None
                while iter_span and e1 < eidx.sequence:
                    span = iter_span.next()
                    e0, e1 = span
                if span: _log.info( 'found mergeable span %s at %d' % ( span, eidx.sequence ) )
            except StopIteration:
                _log.info( 'exhausted mergeable spans at %d' % eidx.sequence )
                iter_span = None

            # get the next already-delivered span if any
            try:
                done = None
                while iter_done and d1 < eidx.sequence:
                    done = iter_done.next()
                    d0, d1 = done
                if done: _log.info( 'found delivered span %s at %d' % ( done, eidx.sequence ) )
            except StopIteration:
                _log.info( 'exhaused delivered spans at %d' % eidx.sequence )
                iter_done = None

            # skip already-delivered events
            if d0 <= eidx.sequence <= d1:
                nskipped += 1
                continue

            # skip orphan events (those not delivered prior to the current mergeable span)
            if eidx.sequence < e0 or eidx.sequence > e1:
                norphaned += 1
                nskipped += 1
                continue

            # if we've accumulated any skipped events since the current output file was
            # opened, we need to close it so we don't put disjoint event spans in the same file
            if ofd_merge and nskipped > 0:
                _log.info('closing %s at %d with %d skipped %d merged in %s %d orphaned from %d total' % \
                          ( ofd_merge.name, eidx.sequence, nskipped, nmerged, (merge0, merge1), norphaned, nevt ) )
                ofd_merge.close()
                ofd_merge = None
                mergespans.append( (merge0, merge1) )

            # This event must go to the output file, so open it if necessary
            if not ofd_merge:
                merge0 = eidx.sequence
                nmerged = nskipped = 0
                ofd_merge = open( os.path.join( opts.outdir, 'r%010d-e%020d.idx' % (startedAt, eidx.sequence) ), 'w' )
                _log.info('opening %s' % ofd_merge.name )

            # now write out the event
            merge1 = eidx.sequence
            nmerged += 1
            if eidx.sequence == e0 or eidx.sequence == e1:
                _log.info( 'span-edge event at %010d.%020d' % ( startedAt, eidx.sequence ) )
            print >> ofd_merge, str( eidx )

            # bottom of the output loop
            pass

        # save the merged event spans
        cPickle.dump( mergespans, open( os.path.join( opts.outdir, 'r%010d-spans.txt' % startedAt ), 'w' ) )

        # write the delivery-summary file
        ofd_summ = open( os.path.join( opts.outdir, 'r%010d-delivered.txt' % startedAt ), 'w' )
        print >> ofd_summ, 'r%010d %d %d %s' % ( startedAt, nmerged, norphaned, acqtype )
        ofd_summ.close()

        # if the number of orphans exceeds the number of events successfully merged,
        # something is most likely wrong with the downlink, so emit a warning into the
        # central log.
        if norphaned >= nmerged:
            msg = 'more events orphaned than merged (%d >= %d) for r%010d (%08x) in %s' % \
                  ( norphaned, nmerged, startedAt, startedAt, opts.downlink )
            _log.warning( msg )
            Log.warn( 'halfPipe.mergeEvt.orphans', msg, tgt=opts.downlink )
        else:
            _log.info( 'merged %d events with %d orphans for r%010d in %s' % ( nmerged, norphaned, startedAt, opts.downlink ) )

            
    def main():

        # set basic logging configuration
        logging.basicConfig( format='%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s: %(message)s',
                             datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout )
        logging.getLogger().setLevel( logging.INFO )

        # parse command-line args
        parser = OptionParser()
        parser.add_option( '-d', '--dgmidx', 
                           help='file of decoded-datagram index records' )
        parser.add_option( '-e', '--evtidx', 
                           help='file of decoded-event index records' )
        parser.add_option( '-b', '--basedir',
                           help='base directory for downlink processing' )
        parser.add_option( '-o', '--outdir', default='.',
                           help='output directory (%default)' )
        parser.add_option( '-f', '--forcedir',
                           help='directory with list of forced (known-incomplete) runs' )
        parser.add_option( '--merge', dest='action', action='store_const', const=merge,
                           help='merge event index according to datagram index' )
        parser.add_option( '-l', '--downlink', 
                           help='Downlink ID' )
        opts, args = parser.parse_args()

        # invoke the requested action
        if opts.action:
            opts.action( opts )
        else:
            _log.warning( 'no action specified, nothing to do' )

    main()


                             
