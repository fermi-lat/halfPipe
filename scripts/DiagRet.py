#!/usr/bin/env python
#
# /afs/slac/g/glast/isoc/flightOps/rhel5_gcc41/${fosFlavor}/bin/shisoc --add-env=flightops python
# /nfs/slac/g/glast/ground/cvs/CHS/halfPipe/scripts/DiagRet.py,v 1.1 2008/06/27 23:44:12 blee Exp

import csv
import sys
import os
import getopt
import datetime
import traceback
import binascii
import struct
import pprint

import sqlalchemy as sql

from quarks.database.dbconfig import DbConfig

from ISOC import SiteDep, utility
from ISOC.RawArchive.RawArchive import PktFile, PktClient, PktRetriever
from ISOC.ProductUtils import ProductSpan
from ISOC.TlmUtils.DecomHldbInterface import DecomHlDb
from ISOC.TlmUtils.Decom import DecomList
from ISOC.Planning.Commands import CommandDb
from ISOC.TlmUtils.SpecialDecom import FswMsgDecom, CmdRespDecom, FswModuleStatus, GemLowRateCounters,\
     AcdLowRateCounters, SpacecraftMsgDecom, FswTaskStatus, CmdXmitDecom, AlertPacketDecom, NavDataDecom
from ISOC.TlmUtils.Sequence import LraDiagAssembler

def usage():
    print """
NAME
    DiagRet.py - retrieve and display diagnostic messages

SYNOPSIS
    DiagRet.py [OPTIONS]

    This application retrieves, decodes, and prints out command-response and/or
    FSW-message packet data from the diagnostic telemetry stream.

OPTIONS
    -a, --archdir
        Specifies the root directory of the L0 packet archive. Defaults to the
        value of the 'archdir' key in the 'RawArchive' section of the sitedep file.

    -b, --beg
        Specifies the beginning of the time interval to process. Can be an absolute
        time specification of the form 'YYYY-MM-DD hh:mm:ss[.uuuuuu]', a relative
        specification of the form '[+-]N [seconds|minutes|hours|days]' where N is an
        integer, or the keyword 'now'.  Note that it is an error to specify two
        relative endpoints.

    -c, --cmd
        Print out command-response packet contents (apid 720)

    -e, --end
        Specifies the end of the time interval to process.  See -b.

    -f, --file
        Specifies the name of a file from which CCSDS packets will be read.

    -h, --help
        Displays this information.

    -m, --msg
        Print out FSW-message packet contents (apid 725)

    -r, --run
        Specifies the LICOS run id for which to retrieve data.  The begin/end times
        of the retrieval span will be determined from the Elogbook database.  The times
        are padded by 30 seconds on either end.

    -s, --scid
        Only meaningful with -b/-e or -r archive retrievals.  Specifies the spacecraft ID to
        be retrieved.  Defaults to the value of the 'scid' key in the DEFAULT section
        of the SiteDep file.

    --dbrel
        Specifies the release of the T&C database information that should be used
        for extraction and conversion of the L0 data.  See the Trending web application
        for the latest database release number. Defaults to the value of the 'dbrel'
        key in the DEFAULT section of the sitedep file.

    --tlmdb
        Specifies the handle of the database from which T&C definitions should be
        retrieved.  Defaults to the value of the 'dbi' key
        in the 'DEFAULT' section of the sitedep file.  See the ISOC/utility.py
        documentation for more information on database connection specifications.

    --noa12
        Only meaningful in conjunction with -f | --file.  Indicates that the file to
        be read contains 'naked' CCSDS packets, without ITOS Anno12aos headers prepended.

    --glrs
        Print out event-rates & fractions derived from the GEM statistics register
        telemetry.

    --alrs
        Print out hit-rates from the ACD low-rate science counters

    --lsm
        Print out LSM contents (magic-7 data)

    --lim
        Print out LIM (mode change) information

    --modules
        Print out LCM module report (CMX_asBuilt_print)

    --tasks
        Print out LCM task report

    --xmit
        Print out LICOS command-transmission packets

    --alert
        Print out LAT alerts

    --fields
        With --cmd/--xmit, extracts and prints user-data field values

    --nav
        Print out derived navigation parameters

    --lra
        Print out LRA register data
    """

Gnavwriter = None
def navCB( datadict ):
    global Gnavwriter
    if Gnavwriter is None:
        k = datadict.keys()
        k.remove( 'time' )
        k.remove( 'met' )
        k.sort()
        k1 = ['time', 'met'] + k
        print k1
        Gnavwriter = csv.DictWriter( sys.stdout, k1 )
        Gnavwriter.writerow( dict(zip( k1, k1 ) ) )
    Gnavwriter.writerow( datadict )

def alertCB( datadict ):
    print '%(time)s %(event)-32s %(msg)s' % datadict
    
def msgCB( datadict ):
    print '%(time)s %(node)-4s %(task)-8s %(function)-32s %(facility)-4s %(name)-8s %(text)s' % datadict

def xmitCB( datadict ):
    print '%(time)s XMIT Command %(mnem)-16s apid=0x%(apid)03X(%(apid)04d) fcode=0x%(fcode)04X(%(fcode)05d) payload=%(payload)s' % datadict
    if datadict['mnem'].startswith( 'LFILUPLD' ): return
    cmdFields( datadict['time'], 'XMIT', datadict )

def cmdCB( datadict ):
    datadict['ststr'] = 'SUCC'
    if (datadict['status'] & 1) != 0:
        datadict['ststr'] = 'FAIL'
    print '%(deqtime)s %(node)-4s Command %(mnem)-16s apid=0x%(apid)03X(%(apid)04d) fcode=0x%(fcode)04X(%(fcode)05d) status=0x%(status)08x(%(ststr)s) payload=%(payload)s' % datadict
    cmdFields( datadict['deqtime'], datadict['node'], datadict )

Gcdb = None
def cmdFields( time, node, datadict ):
    global Gcdb
    if Gcdb is not None:
        fmt = Gcdb.formatter( datadict['mnem'] )
        fmt.decode( binascii.unhexlify( '0000000000000000' + datadict['payload'] + '0000' ) )
        cmd = Gcdb[datadict['mnem']]
        for bf in fmt.get_payload_list():
            val = fmt.get_payload( bf )
            outstr = '%s %-4s Field   %-16s %-16s = 0x%x (%d' % \
                     ( time, node, datadict['mnem'], bf, val, val )
            fld = cmd[bf]
            if fld.enum is not None:
                outstr += '=%s' % fld.enum[val]
            outstr += ')'
            print outstr

def modCB( datadict ):
    print '%(time)s %(node)-4s Module %(package)-8s %(constit)-16s %(version)-10s fileid=0x%(fileid)08X' % datadict

def taskCB( datadict ):
    print '%(time)s %(node)-4s Task %(name)-16s tid=0x%(tid)08X pri=%(priority)03d status=0x%(status)08X err=%(error)08X stackCur=%(stackCur)09d stackHi=%(stackHigh)09d' % datadict

def glrsCB( datadict ):
    print '%(time)s %(node)-4s Statistics deadz=%(LSPDEADRATE)12.5f c/s (%(LSPDEADFRAC)7.5f) sent=%(LSPSENTRATE)12.5f w/s (%(LSPSENTFRAC)7.5f) busy=%(LSPBUSYRATE)12.5f w/s (%(LSPBUSYFRAC)7.5f) prescale=%(LSPPRESCRATE)12.5f w/s (%(LSPPRESCFRAC)7.5f) live=%(LSPLIVEFRAC)7.5f' % datadict

def alrsCB( datadict ):
    mnems = [ k for k in datadict.keys() if k.startswith( 'LSP' ) ]
    mnems.sort()
    for m in mnems:
        v = datadict[m]
        print '%s %-16s %-17.6f cond/sec' % ( v[0], m, v[1] ) 
#     for i in range( 0 , len( datadict[ 'tiles' ] ) ):
#         print '%s %-17.6f TILE%03d %-14.9f %05d %-17.6f' % (
#             datadict['time'], ProductSpan.timegm(str(datadict['time'])), datadict['tiles'][i],
#             datadict['dtimes'][i], datadict['counts'][i], datadict['rates'][i]
#             )

def lsmCB( datadict ):
    if datadict['type'] == 'ANCIL':
        print '%(time)s ORB %(tsecs)d %(tfracs)d %(x)f %(y)f %(z)f %(vx)f %(vy)f %(vz)f %(mode)d %(LAT_SAA)d' % datadict
        print '%(time)s ANC SSR=%(ssr)03d MODE=%(modename)s' % datadict
        print '%(time)s ANC ARR_ENA=%(ARR_ENA)d GBM_SAA=%(GBM_SAA)d KU_ON=%(KU_ON)d'% datadict
        print '%(time)s ANC    S_ON=%(S_ON)d GPS_OUT=%(GPS_OUT)d  IN_SUN=%(IN_SUN)d LAT_SAA=%(LAT_SAA)d' % datadict
    elif datadict['type'] == 'ATT':
        print '%(time)s ATT %(tsecs)d %(tfracs)d %(qx)-.17g %(qy)-.17g %(qz)-.17g %(qw)-.17g %(wx)f %(wy)f %(wz)f' % datadict
    elif datadict['type'] == 'TONE':
        print '%(time)s TT  %(tsecs)d 0x%(flags)04X' % datadict

class LimModeDecom( PktClient ):
    def __init__( self, decom ):
        super( LimModeDecom, self ).__init__( 'LIMMODE' )
        self.__decom = decom

    def handlePkt( self, pktinfo ):
        if pktinfo.getAPID() not in self.__decom.getAPIDS():
            return True
        self.__decom.handlePkt( pktinfo )
        datadict = {}
        for item in self.__decom.getItems():
            datadict[ item.getName() ] = str( item )
        datadict['time'] = ProductSpan.utcfromtimestamp( datadict['H0783SECONDS'], datadict['H0783SUBSECS'] )
        self.update( datadict )
        return True

    def update( self, datadict ):
        print '%(time)s mode = %(LIMTOPMODE)-16s action = %(LIMTACTION)-16s status = %(LIMTSTATUS)-16s lpa = %(LIMTLPASTATE)-16s lci = %(LIMTLCISTATE)-16s' % datadict

def limCB( datadict ):
    pass

gFMTCHR = { 1:'B', 2:'H', 4:'I', }
def lraCB( datadict ):
    print '%(time)s LRA data for %(LRANREG)3d registers %(LRAWIDTH)3d wide' % datadict
    fmtspec = '>' + gFMTCHR[datadict['LRAWIDTH']] * datadict['LRANREG']
    for k in [ x for x in datadict.keys() if x.startswith('LRA') ]:
        print '%s %-16s = %d' % ( datadict['time'], k, datadict[k] )
    for regval in struct.unpack( fmtspec, datadict['LSPLRAREGDATA'][16:] ):
        print '%s LRA register = 0x%x' % ( datadict['time'], regval )

class diagClient( PktClient ):
    def __init__( self, tag, decoms ):
        super( diagClient, self ).__init__( tag )
        self.__decoms = decoms

    def handlePkt( self, pktinfo ):
        for decom in self.__decoms:
            decom.handlePkt( pktinfo )
	return True

def DiagRet():
    # set some reasonable defaults
    archdir = SiteDep.get( 'RawArchive', 'archdir' )
    scid    = SiteDep.getint( 'RawArchive', 'scid' )
    t0str   = '-10 minutes'
    t1str   = 'now'
    ifn     = None
    anno12  = 1
    apidlist= []
    verbose = 1
    ofn     = None
    run     = None
    merge   = True
    clients = []
    tnc_dbi = SiteDep.get( 'DEFAULT', 'dbi' )
    dbrel   = SiteDep.get( 'DEFAULT', 'dbrel' )
    blim    = False
    bfields = False
    bneeddb = False
    bnav    = False
    blra    = False

    # get the command-line arguments
    if len( sys.argv ) == 1:
        usage()
        return
    else:
        try:
            opts, args = getopt.getopt( sys.argv[1:], 'a:b:ce:f:hmr:s:', \
                                        ['archdir=',  'beg=', 'cmd', 'end=', 'file=',
                                         'help', 'glrs', 'msg', 'run=', 'scid=', 'modules',
                                         'noa12', 'dbrel=', 'tlmdb=', 'alrs', 'lsm',
                                         'tasks', 'lim', 'xmit', 'fields', 'alert', 'nav', 'lra',
                                       ] )
        except getopt.GetoptError,e:
            print e
            print 'try "DiagRet.py --help" for usage information.'
            return
        
    for o, a in opts:
        if o in ( '-a', '--archdir' ):
            archdir = a
        if o in ( '-b', '--beg' ):
            t0str = a
        if o in ( '-e', '--end' ):
            t1str = a
        if o in ( '-h', '--help' ):
            usage()
            return
        if o in ( '-s', '--scid' ):
            scid = int( a )
        if o in ( '-f', '--file' ):
            ifn = a
        if o in ( '--noa12', ):
            anno12 = 0
        if o in ( '-r', '--run' ):
            run = int( a )
        if o in ( '-c', '--cmd' ):
            apidlist.append( 720 )
        if o in ( '-m', '--msg' ):
            apidlist.append( 725 )
        if o in ( '--modules' ):
            apidlist.append( 721 )
        if o in ( '--dbrel' ):
            dbrel = a
        if o in ( '--tlmdb' ):
            tnc_dbi = a
        if o in ( '--glrs', ):
            apidlist.append( 551 )
            apidlist.append( 591 )
        if o in ( '--alrs', ):
            apidlist.append( 707 )
        if o in ( '--lsm', ):
            apidlist.append( 1020 )
        if o in ( '--tasks', ):
            apidlist.append( 722 )
        if o in ( '--lim', ):
            apidlist.append( 783 )
            blim = True
            bneeddb = True
        if o in ( '--xmit', ):
            apidlist.append( 2047 )
        if o in ( '--fields', ):
            bfields = True
        if o in ( '--alert', ):
            apidlist.extend( range(832, 928) )
        if o in ( '--nav', ):
            apidlist.append( 13 )
            bnav = True
            bneeddb = True
        if o in ( '--lra' ):
            apidlist.append( 753 )
            blra = True
            bneeddb = True

    # must specify at least one apid if not reading a file
    if len( apidlist ) == 0:
        print 'DiagRet: no diagnostics specified.'
        print 'try "DiagRet.py --help" for usage information'
        return

    # if a run has been specified, retrieve the begin/end times
    # from the ELogbook database and set the scid to match the
    # pipeline-dispatch configuration
    if run is not None:
        verbose  = 0
        t0str, t1str = utility.getRunSpan( run, SiteDep.get( 'MakeRetDef', 'runs_dbi' ), 30 )
        if t0str is None:
            print 'DiagRet: run %09d not found' % run
            return
        print 'DiagRet: retrieving data for run %09d %s --> %s' % ( run, t0str, t1str)

    # set up a connection to the t&C database, and the query string
    # for looking up command mnemonics
    db = DbConfig.fromConfigParser( SiteDep, tnc_dbi )
    cmd_tbl = sql.Table( 'v3hkcmdfields', db.metadata, autoload=True )
    cmdselector = cmd_tbl.select()
    global Gcdb
    if bfields:
        Gcdb = CommandDb( db.engine )
        Gcdb.populate( dbrelease=dbrel )
    else:
        Gcdb = None

    # set up the special deommutation clients
    cmdDecom = CmdRespDecom( cmdCB, dbsel=cmdselector )
    fswDecom = FswMsgDecom( msgCB )
    modDecom = FswModuleStatus( modCB )
    glrsDecom = GemLowRateCounters( glrsCB )
    alrsDecom = AcdLowRateCounters( alrsCB )
    lsmDecom = SpacecraftMsgDecom( lsmCB )
    taskDecom = FswTaskStatus( taskCB )
    xmitDecom = CmdXmitDecom( xmitCB, dbsel=cmdselector )
    alertDecom = AlertPacketDecom( alertCB )

    # only populate a database if we need it
    if bneeddb:
        print 'DiagRet: populating t&c database from %s' % dbrel
        tlmdb = DecomHlDb( db )
        tlmdb.populate( source=scid, release=dbrel )

    # use the regular bitfield-decom machinery to crack LIM packets if requested
    limDecom = None
    if blim:
        lim_mnems = tlmdb.getMnemsByApid( (783, ), reexclude=r'PAD' )
        lim_decom = DecomList( lim_mnems, tlmdb=tlmdb, apidlist=(783,) )
        limDecom = LimModeDecom( lim_decom )

    # nav params need a db to get the input variables
    navDecom = None
    if bnav:
        navDecom = NavDataDecom( navCB, tlmdb = tlmdb )

    # LRA needs a DB to get the regData header fields
    lraDecom = None
    if blra:
        lraDecom = LraDiagAssembler( lraCB, tlmdb = tlmdb )

    # retrieve from the archive or read the file
    diagclients = [ cmdDecom, fswDecom, modDecom, glrsDecom, alrsDecom,
                    lsmDecom, taskDecom, xmitDecom, alertDecom ]
    if limDecom is not None:
        diagclients.append( limDecom )
    if navDecom is not None:
        diagclients.append( navDecom )
    if lraDecom is not None:
        diagclients.append( lraDecom )
    diagdump = diagClient( 'DIAG',  diagclients )
    try:
        if ifn is None:
            t0, t1 = ProductSpan.getspan( t0str, t1str )
            pktret = PktRetriever( t0, t1, apidlist, archdir, scid )
            pktret.addClient( diagdump )
            pktret.retrievePkts()
        else:
            pktfile = PktFile( ifn, diagdump, anno12 )
            pktfile.dumpPkts()
    except Exception, e:
        print str(e)

    # all done
    return

if __name__ == '__main__':
    try:
        DiagRet()
    except SystemExit:
        raise
    except:
        traceback.print_exc( file=sys.stdout )
        sys.exit( 1 )
