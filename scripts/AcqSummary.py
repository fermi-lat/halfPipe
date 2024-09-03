#!/usr/bin/env python

# /afs/slac/g/glast/isoc/flightOps/rhel5_gcc41/${fosFlavor}/bin/shisoc --add-env=flightops python
# /afs/slac/g/glast/isoc/flightOps/rhel6_gcc44/${fosFlavor}/bin/shisoc --add-env=flightops python

#
#                               Copyright 2007
#                                     by
#                        The Board of Trustees of the
#                     Leland Stanford Junior University.
#                            All rights reserved.
#

"""Record information about a downlink in a database."""

__facility__ = "GLAST ISOC"
__abstract__ = __doc__
__author__   = "Bryson Lee <blee@slac.stanford.edu> SLAC - GLAST ISOC"
__date__     = "2007/08/23"
__updated__  = "2008/07/25 15:38:46"
__version__  = "1.3"
__release__  = "v6r2p0"
__credits__  = "SLAC"

import quarks.legal.copyright

## @namespace DownlinkInfo
#  @brief Record information about a downlink in a database.
#

import datetime, errno, glob, logging, operator, os, subprocess, sys

_log = logging.getLogger()

from py_MOOT import MootUpdate, vectorOfUnsigned

from ISOC.ProductUtils import ProductSpan

class DoesNotBelong( RuntimeError ):
    """!Raised when an event from a different acquisition is added to a datagram"""

class NoDatagramsFound( RuntimeError ):
    """!Raised when no datagrams are found in an index file"""

class Orphans( object ):
    """!Collection of orphan events organized by acquisition and datagram"""
    def __init__( self, ifn, iacq = None ):
        """!@param[in] ifn Orphan-event filename.
        @param[in] iacq List of acqusition id's (run-start-times).
        """
        self.filename = ifn
        self.events = [ EvtIdx( None, x ) for x in open( self.filename ) ]
        self.events.sort( key=lambda x: x.key )

class Downlink( object ):
    """Collection of acquisitions."""
    def __init__( self, id, l0key, indir, outbase ):
        """!@param[in] id Downlink ID.
        @param[in] l0key Level-0 transfer package file key.
        @param[in] indir The input directory to be scanned for decoded event files.
        @param[in] outbase Base directory in which merged output subdirectories should be created.
        """
        self.id      = id
        self.l0key   = l0key
        self.indir   = os.path.abspath( indir )
        self.outbase = os.path.abspath( outbase )
        self.segments = []

    def updateDatabase( self, session ):
        """!@brief summarize downlink contents in the database
        @param[in] session A SqlAlchemy unit-of-work session
        """
        # get the list of index-file in the input directory
        idxfiles = glob.glob( os.path.join( self.indir, '????????-????????-????-?????.idx' ) )
        idxfiles.sort()

        # capture the segments present in this downlink
        self.addseg( idxfiles, session )
        if len( self.segments ) == 0:
            _log.error( "Downlink::updateDatabase: no data segments found for downlink %d in %s" % ( self.id, self.indir ) )
            return

        # store the downlink-based information
        session.save_or_update( self )
        session.flush()

        # update the acquisition-summary table to reflect data received
        # in this downlink
        for rst in self.runstarts():
            # create or retrieve the acquisition object
            acq = session.get( Acquisition, ( self.segments[0].scid, rst ) )
            if acq is None:
                acq = Acquisition( self.segments[0].scid, rst )
                session.save( acq )
                session.flush()

            # update the summary information
            session.refresh( acq )
            acq.update()

            # associate this acquisition with this downlink
            self.acquisitions.append( DownlinkAssociation( acq, session ) )

        # update the database
        session.flush()

    def addseg( self, idxlist, session = None ):
        # load datagram segments from the index files
        for idx in idxlist:
            try:
                seg = DatagramSegment( idx, session )
            except NoDatagramsFound, e:
                _log.exception( 'Acquisition::addseg: %s' %  str(e) )
                continue
            if seg not in self.segments:
                self.segments.append( seg )
        self.segments.sort( key=lambda x: x.key )
        if session: session.save_or_update( self )

    def runstarts( self ):
        rst = list( set( [ x.startedat for x in self.segments ] ) )
        rst.sort()
        return tuple( rst )

    @property
    def nacquisitions( self ):
        return len( self.acquisitions )

    def report( self ):
        print 'Downlink %09d has %d acquisitions' % ( self.id, self.nacquisitions )
        for a in self.acquisitions: a.report()

    def retire_acqs( self, ofd ):
        """!@brief report completed acquisitions to the 'retired' file
        @param[in] ofd File descriptor to which retirement info should be written
        """
        for a in self.acquisitions: a.retire( ofd )

    def report_event_times( self, ofd ):
        """!@brief generate 'event times' information for L1Proc
        @param[in] ofd File object to which event_time infor should be written
        """
        for a in self.acquisitions: a.report_event_times( ofd )

    def map_moot( self, db, prefix, ofd ):
        """!@brief map MOOT key/alias information from the defaults table to each acquisition.
        @param[in] db A DbConfig object
        @param[in] prefix the table prefix for the LPA-defaults table
        @param[in] ofd output file handle for textual summary
        """
        for a in self.acquisitions: a.map_moot( db, prefix, ofd )

    def update_moot( self, mupd ):
        """!@brief update MOOT acquisition-summary table
        @param[in] mupd A MootUpdate instance
        """
        for a in self.acquisitions: a.acquisition.update_moot( mupd )

class Acquisition( object ):
    """Collection of datagram-segments."""
    def __init__( self, scid, startedat ):
        """!@param[in] scid Spacecraft ID
        @param[in] startedat acquisition start-time
        """
        self.scid = scid
        self.startedat = startedat
        self.moot_key   = 0xffffffff
        self.moot_alias = 'UNKNOWN'

    def update( self ):
        # we may not have any information
        if len( self.segments ) == 0:
            self.type = 'LPA'
            rst = datetime.datetime(2001,1,1) + datetime.timedelta( seconds = self.startedat )
            self.dgmutc0 = rst
            self.dgmutc1 = rst
            self.evtutc0 = rst
            self.evtutc1 = rst
            self.ndgms   = 0
            self.nevts   = 0
            self.hwkey   = 0xFFFFFFFF
            self.swkey   = 0xFFFFFFFF
            self.status  = 'InProgress'
            return
        
        # capture summary information
        if self.segments[0].apid == 965:
            self.type = 'LCI'
        else:
            self.type = 'LPA'
        self.dgmutc0 = min( x.dgmutc0 for x in self.segments )
        self.dgmutc1 = max( x.dgmutc1 for x in self.segments )
        self.evtutc0 = min( x.evtutc0 for x in self.segments )
        self.evtutc1 = max( x.evtutc1 for x in self.segments )
        self.ndgms   = sum( x.ndgms for x in self.segments )
        self.nevts   = sum( x.nevts for x in self.segments )
        self.hwkey   = self.segments[0].hwkey
        self.swkey   = self.segments[0].swkey

        # partition segments by apid
        apids = list( set( [ x.apid for x in self.segments ] ) )
        self.status = 'InProgress'
        bcomplete = True
        for apid in apids:
            segs = [ x for x in self.segments if x.apid == apid ]
            segs.sort( key = operator.attrgetter( 'dgmseq0' ) )
            bstart = segs[0].oaction == 'start'
            bstop  = segs[-1].caction in ('stop', 'abort', 'pause')
            bcnt   = sum( [x.ndgms for x in segs ] ) == ( segs[-1].dgmseq1 - segs[0].dgmseq0 + 1 )
            bcomplete = bcomplete and ( bstart and bstop and bcnt )
        if bcomplete:
            self.status = 'Complete'

    @property
    def startedAt( self ):
        return self.segments[0].startedAt

    @property
    def groundId( self ):
        return self.segments[0].groundId

    @property
    def nsegments( self ):
        return len( self.segments )

    def map_moot( self, db, prefix, ofd ):
        """!@brief map MOOT key/alias information from the defaults table to each acquisition.
        @param[in] db A DbConfig object
        @param[in] prefix the table prefix for the LPA-defaults table
        @param[in] ofd output file handle for textual summary
        """
        moot_source = 'missing'
        # if LPA, get the applicable default setting
        if self.type == 'LPA':
            dtbl = SA.Table( '%s_acqdefaults' % prefix, db.metadata, autoload=True )
            dsel = dtbl.select( dtbl.c.tcompleted < self.dgmutc0,
                              order_by=[ SA.desc( dtbl.c.tcompleted ), ] )
            row = dsel.execute().fetchone()
            if row:
                self.moot_key   = row.moot_key
                self.moot_alias = row.moot_alias
                moot_source = 'default'

        # get the acquisition-specific setting if available
        atbl = SA.Table( '%s_acquisition' % prefix, db.metadata, autoload=True )
        asel = atbl.select( SA.and_( atbl.c.trequested < self.dgmutc0,
                                     atbl.c.id        == self.groundId,
                                     atbl.c.startedat == None,
                                     SA.or_( atbl.c.type == 'LPA', atbl.c.type == 'LCI' ),
                                     ),
                            order_by=[SA.desc( atbl.c.trequested ), ] )
        row = asel.execute().fetchone()
        if row:
            self.moot_key   = row.moot_key   if row.moot_key   else self.moot_key
            self.moot_alias = row.moot_alias if row.moot_alias else self.moot_alias
            moot_source = 'specific' if row.moot_key else moot_source

        # write to the summary file
        _log.info( 'Acquisition::map_moot: mapped r%010d to %s MOOT info %s(0x%08x)' % \
                   ( self.startedat, moot_source, self.moot_alias, self.moot_key ) )
        print >>ofd, 'r%010d %d %s' % ( self.startedat, self.moot_key, self.moot_alias )

    def update_moot( self, mupd ):
        """!@brief update MOOT acquisition-summary table
        @param[in] mupd A MootUpdate instance:
        """
        dt0 = self.evtutc0 - datetime.datetime( 1970, 1, 1 )
        dt1 = self.evtutc1 - datetime.datetime( 1970, 1, 1 )
        try:
            mupd.updateAcqSummary( self.startedat,
                                   self.scid,
                                   self.moot_key,
                                   self.type,
                                   '',
                                   float(dt0.days*86400) + float(dt0.seconds) + float(dt0.microseconds)/1000000.0,
                                   float(dt1.days*86400) + float(dt1.seconds) + float(dt1.microseconds)/1000000.0,
                                   self.nevts,
                                   self.hwkey,
                                   self.swkey,
                                   '',
                                   vectorOfUnsigned() )
            _log.info( 'Acquisition::update_moot: updated MOOT acq-summary table for r%010d' % self.startedat )
        except Exception, e:
            _log.error( 'Acquisition::update_moot: %s %s' % ( type(e), str(e) ) )
        try:
            if self.status != 'InProgress':
                mupd.markAcqComplete( self.startedat, self.scid )
                _log.info( 'Acquisition::update_moot: marked r%010d with status %s as Complete in MOOT' % ( self.startedat, self.status ) )
        except Exception, e:
            _log.error( 'Acquisition::update_moot: %s %s' % ( type(e), str(e) ) )

    def report( self ):
        print '%s %010d/0x%08x started at %s with keys 0x%08x/0x%08x has %d segments, %d datagrams, %d events and is %s' %\
              ( self.type, self.startedat, self.startedat,
                ( datetime.datetime( 2001, 1, 1 ) + datetime.timedelta( seconds=self.startedat ) ),
                self.hwkey, self.swkey, self.nsegments,
                self.ndgms, self.nevts, self.status )
        [ x.report() for x in self.segments ]

    def report_event_times( self, ofd ):
        """!@brief generate 'event times' information for L1Proc
        @param[in] ofd File object to which event_time infor should be written
        """
        print >> ofd, 'r%010d %f %f' % \
              ( self.startedat, ProductSpan.timegm( str( self.evtutc0 ) ), ProductSpan.timegm( str( self.evtutc1 ) ) )

    def retire( self, ofd ):
        """!@brief generate 'retired' information
        @param[in] ofd File object to which retirement info should be written
        """
        if self.status == 'Complete':
            print >>ofd, 'r%010d %s' % ( self.startedat, self.status.upper() )

    def spans( self ):
        """!Identify event spans that are eligible for merging."""
        # find the contiguous event-sequence spans from each EPU
        evtsegments = []
        for apid, segments in [ ( x, [s for s in self.segments if s.apid == x] ) for x in set( y.apid for y in self.segments ) ]:
            evtsegments.append( [] )
            evtseq0 == segments[0].evtseq0
            evtseq1 == segments[0].evtseq1
            iseg = 1
            while iseg < len( segments ):
                if segments[iseg-1].dgmseq1+1 == segments[iseg].dgmseg0:
                    # no gap in the datagram sequence, so extend the event sequence
                    evtseq1 = segments[iseg].evtseq1
                else:
                    # datagram-sequence gap, so restart the event sequence
                    evtsegments[-1].append( ( evtseq0, evtseq1, apid ) )
                    evtseq0 = segments[iseg].evtseq0
                    evtseq1 = segments[iseg].evtseq1
                iseg += 1

                
class DatagramSegment( object ):
    """A contiguous series of decoded datagrams."""
    def __init__( self, idx, session = None ):
        # get the list of datagrams from the index file and capture the first and last
        ifd = subprocess.Popen( 'grep ^DGM %s' % idx, shell=True, stdout=subprocess.PIPE ).stdout
        datagrams = [ DgmIdx( dgmstr ) for dgmstr in ifd ]
        if len( datagrams ) == 0:
            raise NoDatagramsFound( 'No datagrams found in idx file %s' % idx )
        _log.info( 'DatagramSegment::__init__: found %d datagrams in %s' % ( len(datagrams), idx ) )
        self.dgm0 = datagrams[0]
        self.dgm1 = datagrams[-1]
        self.nevts  = sum( [ x.nevts for x in datagrams ] )
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

    def expunge( self, session ):
        if session:
            session.expunge( self.dgm0 )
            session.expunge( self.dgm1 )
            session.expunge( self )

    def report( self ):
        print 'Segment for apid %d with %d datagrams and %d events (%d:%s, %d:%s)' %\
              ( self.apid, self.ndgms, self.nevts, self.dgmseq0, self.oaction, self.dgmseq1, self.caction )

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

    def __eq__( self, rhs ):
        if not rhs: return False
        return self.scid == rhs.scid and \
               self.apid == rhs.apid and \
               self.startedat == rhs.startedat and \
               self.datagrams == rhs.datagrams

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

class DownlinkAssociation(object):
    def __init__( self, acq, session = None ):
        self.acquisition = acq
        if session: session.save_or_update( self )

    def report( self ):
        self.acquisition.report()

    def retire( self, ofd ):
        self.acquisition.retire( ofd )

    def report_event_times( self, ofd ):
        self.acquisition.report_event_times( ofd )

    def map_moot( self, db, prefix, ofd ):
        self.acquisition.map_moot( db, prefix, ofd )

if __name__ == '__main__':

    import getpass
    import sqlalchemy as SA

    from quarks.cmdline.xoptparse import OptionParser
    from quarks.database.dbconfig import DbConfig

    from ISOC import SiteDep
    from ISOC.ProductUtils import ProductSpan

    def main():
        # set basic logging configuration
        logging.basicConfig( format='%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s: %(message)s',
                             datefmt='%Y-%m-%d %H:%M:%S' )
        logging.getLogger().setLevel( logging.INFO )

        # get the command-line arguments
        usage = "usage: %prog [options] "
        parser = OptionParser( usage )
        parser.add_option( '-d', '--downlink', type='hexordec',
                           help='Downlink ID' )
        parser.add_option( '-i', '--indir', default='.',
                           help='directory containing event-index files (%default)' )
        parser.add_option( '-k', '--l0key', type='int',
                           help='Level-0 transfer package file key' )
        parser.add_option( '-o', '--outbase',
                           help='directory in which output should be created, defaults to input directory' )
        parser.add_option( '-p', '--prefix', default=getpass.getuser(),
                           help='Tracking-table-name prefix (%default)' )
        parser.add_option( '-r', '--retire', action='store_true', default=False,
                           help='write out retired-run information' )
        parser.add_option( '--evttimes', action='store_true', default=False,
                           help='write out acquisition event-time-spans' )
        parser.add_option( '--dbi', default=SiteDep.get( 'DEFAULT', 'dbi' ),
                           help='database connection name (%default)' )
        parser.add_option( '-f', '--forcedir',
                           help='Directory of forced run-start-times' )
        parser.add_option( '-s', '--scid', default=SiteDep.get( 'DEFAULT', 'scid' ),
                           help='Spacecraft ID (%default)' )
        parser.add_option( '-b', '--beg', default='-1 day',
                           help='Begining of report interval (%default)' )
        parser.add_option( '-e', '--end', default='now',
                           help='End of report interval (%default)' )
        parser.add_option( '--load', dest='action', action='store_const', const='load',
                           help='Load downlink information to database' )
        parser.add_option( '--rebuilddb', dest='action', action='store_const', const='rebuild',
                           help='Rebuild database tables' )
        parser.add_option( '--report', dest='action', action='store_const', const='report',
                           help='Report information from the database' )
        parser.add_option( '--delete', dest='action', action='store_const', const='delete',
                           help='Delete the information from a downlink from the database' )
        parser.add_option( '--started', dest='timefield', action='store_const', const='started',
                           help='report based on started times' )
        parser.add_option( '--datagram', dest='timefield', action='store_const', const='datagram',
                           help='report based on datagram times' )
        parser.add_option( '--event', dest='timefield', action='store_const', const='event',
                           help='report based on event times' )
        parser.add_option( '--received', dest='timefield', action='store_const', const='received',
                           help='report based on received times' )
        parser.add_option( '--moot', action='store_true', default=False,
                           help='cross-load acquisition-summary information to the MOOT database (%default)' )
        opts, args = parser.parse_args()
        if not opts.action:
            parser.error( 'no action specified' )
        if opts.action == 'load':
            if opts.downlink is None:
                parser.error( 'no downlink ID specified' )
            if opts.l0key is None:
                parser.error( 'no transfer-package key specified' )
            if opts.indir is None:
                parser.error( 'no input directory specified' )
        if opts.action == 'delete':
            if opts.downlink is None and opts.l0key is None:
                parser.error( 'no downlink ID or incoming-file FK specified' )
        if opts.outbase is None:
            opts.outbase = opts.indir
        if opts.timefield is None:
            opts.timefield = 'datagram'

        # connect to the database
        db = DbConfig.fromConfigParser( SiteDep, opts.dbi )

        # define tracking tables
        dltbl = SA.Table( '%s_downlink' % opts.prefix,     db.metadata,
                          SA.Column( 'id', SA.Integer, primary_key=True ),
                          SA.Column( 'l0key', SA.Integer ),
                          SA.Column( 'indir', SA.String(256) ),
                          SA.Column( 'outbase', SA.String(256) ),
                          )
        aqtbl = SA.Table( '%s_acqsummary' % opts.prefix, db.metadata,
                          SA.Column( 'scid', SA.Integer, primary_key=True ),
                          SA.Column( 'startedat', SA.Integer, primary_key=True ),
                          SA.Column( 'type', SA.String(16), SA.CheckConstraint( "type in ( 'LPA', 'LCI' )" ) ),
                          SA.Column( 'analysis', SA.String(64) ),
                          SA.Column( 'status', SA.String(16), SA.CheckConstraint( "status in ( 'InProgress', 'Complete', 'Incomplete' )" ) ),
                          SA.Column( 'ndgms', SA.Integer ),
                          SA.Column( 'nevts', SA.Integer ),
                          SA.Column( 'dgmutc0', SA.TIMESTAMP ),
                          SA.Column( 'dgmutc1', SA.TIMESTAMP ),
                          SA.Column( 'evtutc0', SA.TIMESTAMP ),
                          SA.Column( 'evtutc1', SA.TIMESTAMP ),
                          SA.Column( 'hwkey', SA.Integer ),
                          SA.Column( 'swkey', SA.Integer ),
                          SA.Column( 'moot_key', SA.Integer ),
                          SA.Column( 'moot_alias', SA.String(64) ),
                          )
        sgtbl = SA.Table( '%s_downlink_seg' % opts.prefix, db.metadata,
                          SA.Column( 'scid', SA.Integer, primary_key=True ),
                          SA.Column( 'startedat', SA.Integer, primary_key=True ),
                          SA.Column( 'apid', SA.Integer, primary_key=True ),
                          SA.Column( 'nevts', SA.Integer ),
                          SA.Column( 'dgmseq0', SA.Integer, primary_key=True ),
                          SA.Column( 'dgmseq1', SA.Integer, nullable=False ),
                          SA.Column( 'downlink_id', SA.Integer, SA.ForeignKey( '%s_downlink.id' % opts.prefix, ondelete="CASCADE" ) ),
                          SA.ForeignKeyConstraint( [ 'scid', 'startedat', 'apid', 'dgmseq0' ],
                                                   [ '%s_downlink_dgm.scid' % opts.prefix, '%s_downlink_dgm.startedat' % opts.prefix,
                                                     '%s_downlink_dgm.apid' % opts.prefix, '%s_downlink_dgm.datagrams' % opts.prefix ] ),
                          SA.ForeignKeyConstraint( [ 'scid', 'startedat', 'apid', 'dgmseq1' ],
                                                   [ '%s_downlink_dgm.scid' % opts.prefix, '%s_downlink_dgm.startedat' % opts.prefix,
                                                     '%s_downlink_dgm.apid' % opts.prefix, '%s_downlink_dgm.datagrams' % opts.prefix ] ),
                          )
        dgtbl = SA.Table( '%s_downlink_dgm' % opts.prefix, db.metadata,
                          SA.Column( 'downlink_id', SA.Integer, SA.ForeignKey( '%s_downlink.id' % opts.prefix, ondelete="CASCADE" ) ),
                          SA.Column( 'startedat', SA.Integer, primary_key=True ),
                          SA.Column( 'utc', SA.TIMESTAMP ),
                          SA.Column( 'scid', SA.Integer, primary_key=True ),
                          SA.Column( 'apid', SA.Integer, primary_key=True ),
                          SA.Column( 'datagrams', SA.Integer, primary_key=True ),
                          SA.Column( 'groundid', SA.Integer ),
                          SA.Column( 'modechanges', SA.Integer ),
                          SA.Column( 'modename', SA.String(16) ),
                          SA.Column( 'oaction', SA.String(16) ),
                          SA.Column( 'oreason', SA.String(16) ),
                          SA.Column( 'caction', SA.String(16) ),
                          SA.Column( 'creason', SA.String(16) ),
                          SA.Column( 'platform', SA.String(16) ),
                          SA.Column( 'origin', SA.String(16) ),
                          SA.Column( 'evtutc0', SA.TIMESTAMP ),
                          SA.Column( 'evtseq0', SA.Integer ),
                          SA.Column( 'evtutc1', SA.TIMESTAMP ),
                          SA.Column( 'evtseq1', SA.Integer ),
                          SA.Column( 'nevts', SA.Integer ),
                          SA.Column( 'hwkey', SA.Integer ),
                          SA.Column( 'swkey', SA.Integer ),
                          )
        dlaqtbl = SA.Table( '%s_downlink_acqsummary' % opts.prefix, db.metadata,
                            SA.Column( 'downlink_id', SA.Integer, SA.ForeignKey( '%s_downlink.id' % opts.prefix ), primary_key=True  ),
                            SA.Column( 'scid', SA.Integer, primary_key=True ),
                            SA.Column( 'startedat', SA.Integer, primary_key=True ),
                            SA.ForeignKeyConstraint( ['scid', 'startedat'], [ '%s_acqsummary.scid' % opts.prefix, '%s_acqsummary.startedat' % opts.prefix ] ),
                            )

        # map tables to objects
        dgmap = SA.mapper( DgmIdx, dgtbl )
        aqmap = SA.mapper( Acquisition, aqtbl,
                           properties={ 'segments' : SA.relation( DatagramSegment, lazy=False,
                                                                  primaryjoin=SA.and_( sgtbl.c.scid == aqtbl.c.scid,
                                                                                       sgtbl.c.startedat == aqtbl.c.startedat ),
                                                                  foreign_keys=[ sgtbl.c.scid, sgtbl.c.startedat ] ),
                                        } )
        dlaqmap = SA.mapper( DownlinkAssociation, dlaqtbl,
                             properties={ 'acquisition' : SA.relation( Acquisition, lazy=False ) } )
        dlmap = SA.mapper( Downlink, dltbl,
                           properties={ 'acquisitions' : SA.relation( DownlinkAssociation, lazy=False, private=True ),
                                        'segments'     : SA.relation( DatagramSegment, lazy=False, private=True )
                                        } )
        sgmap = SA.mapper( DatagramSegment, sgtbl,
                           properties={ 'dgm0' : SA.relation( DgmIdx, lazy=False, private=True,
                                                             primaryjoin=SA.and_( sgtbl.c.scid == dgtbl.c.scid,
                                                                                  sgtbl.c.startedat == dgtbl.c.startedat,
                                                                                  sgtbl.c.apid == dgtbl.c.apid,
                                                                                  sgtbl.c.dgmseq0 == dgtbl.c.datagrams ) ),
                                        'dgm1' : SA.relation( DgmIdx, lazy=False, private=True,
                                                             primaryjoin=SA.and_( sgtbl.c.scid == dgtbl.c.scid,
                                                                                  sgtbl.c.startedat == dgtbl.c.startedat,
                                                                                  sgtbl.c.apid == dgtbl.c.apid,
                                                                                  sgtbl.c.dgmseq1 == dgtbl.c.datagrams ) ),
                                        } )

        # perform the requested action
        if opts.action == 'rebuild':
            _log.info( 'Rebuilding database tables for prefix "%s" in "%s"' % ( opts.prefix, opts.dbi ) )
            rebuild( opts, db )
        elif opts.action == 'load':
            _log.info( 'Loading info from %s to prefix "%s" in "%s"' % ( opts.indir, opts.prefix, opts.dbi ) )
            load( opts, db )
        elif opts.action == 'report':
            _log.info( 'Reporting from prefix "%s" in "%s"' % ( opts.prefix, opts.dbi ) )
            report( opts, db )
        elif opts.action == 'delete':
            delete( opts, db )

    def rebuild( opts, db ):
        # rebuild the database tables if specified
        db.metadata.drop_all()
        db.metadata.create_all()

    def report( opts, db ):
        sess = SA.create_session( bind_to=db.engine )

        # construct various queries based on user specifications
        if opts.downlink:
            q = sess.query( Downlink ).filter( Downlink.c.id == opts.downlink )
        else:
            tstamp0, tstamp1 = ProductSpan.getspan( opts.beg, opts.end )
            t0 = datetime.datetime.utcfromtimestamp( tstamp0 )
            t1 = datetime.datetime.utcfromtimestamp( tstamp1 )
            if opts.timefield == 'received':
                _log.warn( 'Receive-time queries not yet implemented' )
                return
            elif opts.timefield == 'started':
                e0 = t0 - datetime.datetime( 2001, 1, 1 )
                e1 = t1 - datetime.datetime( 2001, 1, 1 )
                dt0 = 86400 * e0.days + e0.seconds
                dt1 = 86400 * e1.days + e1.seconds
                q = sess.query( Acquisition ).filter(
                    Acquisition.c.startedat.between( dt0, dt1 )
                    )
            elif opts.timefield == 'datagram':
                q = sess.query( Acquisition ).filter(
                    SA.and_( Acquisition.c.dgmutc0 >= t0,
                             Acquisition.c.dgmutc1 <= t1 )
                    )
            elif opts.timefield == 'event':
                q = sess.query( Acquisition ).filter(
                    SA.and_( Acquisition.c.evtutc0 >= t0,
                             Acquisition.c.evtutc1 <= t1 )
                    )

        # report the results
        for item in q:
            item.report()

    def delete( opts, db, sess = None ):
        # clear any pre-existing content related to this downlink
        if sess is None: sess = SA.create_session( bind_to=db.engine )
        if opts.downlink:
            olddl = sess.query( Downlink ).get_by( id = opts.downlink )
        else:
            olddl = sess.query( Downlink ).get_by( l0key = opts.l0key )
        if olddl:
            _log.info( 'clearing previous data for downlink %09d with l0key %d' % \
                       (olddl.id, olddl.l0key) )
            sess.delete( olddl )
            sess.flush()
            for acq in olddl.acquisitions:
                sess.refresh( acq.acquisition )
                acq.acquisition.update()
                _log.info( 'updated acquisition r%010d summary data:' % acq.acquisition.startedat )
                acq.acquisition.report()
            sess.flush()
        else:
            _log.info( 'no previous data found' )

    def load( opts, db ):
        # create the tables if necessary
        db.metadata.create_all()

        # create a unit-of-work session
        sess = SA.create_session( bind_to=db.engine )

        # clear any pre-existing content related to this downlink
        delete( opts, db, sess )

        # create the downlink object
        dlink = Downlink( opts.downlink, opts.l0key, opts.indir, opts.outbase )
        dlink.updateDatabase( sess )
        dlink.report()

        # map and report the MOOT key/alias info
        mootfd = open( os.path.join( opts.outbase, 'moot_keys_%09d.txt' % opts.downlink ), 'w' )
        dlink.map_moot( db, opts.prefix, mootfd )
        mootfd.close()

        # create the "retired_runs file" if specified
        if opts.retire:
            retiredfd = open( os.path.join( opts.outbase, 'retired_runs_%09d.txt' % opts.downlink ), 'w' )
            dlink.retire_acqs( retiredfd )

        # create the "event_times file" if specified
        if opts.evttimes:
            evttimefd = open( os.path.join( opts.outbase, 'event_times_%09d.txt' % opts.downlink ), 'w' )
            dlink.report_event_times( evttimefd )

        # persist the information to the database
        sess.flush()

        # look for in-progress runs that have been forced
        if opts.forcedir:
            forcefiles = glob.glob( os.path.join( opts.forcedir, '????????-????????' ) )
            forcestarts = [ eval( '0x'+ os.path.split( x )[1].split('-')[1] ) for x in forcefiles ]
            forcedacqs = sess.query( Acquisition ).select(
                SA.and_( Acquisition.c.startedat.in_( *forcestarts ),
                         Acquisition.c.status == 'InProgress' ) )
            for a in forcedacqs:
                a.status = 'Incomplete'
                if opts.retire: print >>retiredfd, 'r%010d %s' % (a.startedat, a.status.upper())
            sess.flush()

        # cross-load information to MOOT if requested
        if opts.moot:
            _log.info( 'AcqSummary::load: cross-loading to MOOT' )
            try:
                mupd = MootUpdate()
                dlink.update_moot( mupd )
            except Exception, e:
                _log.error( 'AcqSummary::load: error updating MOOT %s "%s"' % (type(e), str(e)) )

    # invoke the main routine
    main()
