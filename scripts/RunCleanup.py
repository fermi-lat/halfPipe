#!/usr/bin/env python

import cStringIO, datetime, getpass, glob, os, pprint, smtplib, socket, sys

import sqlalchemy as sql

from quarks.database.dbconfig import DbConfig
from quarks.cmdline.xoptparse import OptionParser
from quarks.timemoney import dt 

from ISOC import SiteDep
from ISOC.ProductUtils import ProductSpan

SQLTEXT = """\
select d.id, a.startedat, d.outbase
from glastops_downlink d, glastops_downlink_acqsummary da, glastops_acqsummary a
where d.id=da.downlink_id and da.scid=a.scid and da.startedat=a.startedat
and da.scid=:scid and a.dgmutc0 between :t1 and :t2 and a.status <> 'InProgress'
order by d.id
"""

p = OptionParser()
p.add_option( '--dbi', default=SiteDep.get( 'RawArchive', 'dbi' ),
              help='Database instance name (%default)' )
p.add_option( '-b', '--beg', default='2008-06-11 00:00:00',
              help='query begin time (%default)' )
p.add_option( '-e', '--end', default='now',
              help='query end time (%default)' )
p.add_option( '--scid', type='int', default=SiteDep.getint( 'RawArchive', 'scid' ),
              help='Spacecraft ID (%default)' )
p.add_option( '--nodryrun', dest='dryrun', action='store_false', default=True,
              help='Really delete the files' )
p.add_option( '--lockdir', help='directory where lockfiles are stored' )
opts, args = p.parse_args()

t1, t2 = ProductSpan.getspan( opts.beg, opts.end )
t1 = float(int(t1+0.5))
t2 = float(int(t2+0.5))

db = DbConfig.fromConfigParser( SiteDep, opts.dbi )

rows = db.engine.text( SQLTEXT ).execute( { 'scid' : opts.scid,
                                            't1'   : dt.utcfromtimestamp( t1 ),
                                            't2'   : dt.utcfromtimestamp( t2 ),
                                            } ).fetchall()

if rows:
    for r in rows:
        # skip downlinks that are still in progress
        if os.path.exists( os.path.join( opts.lockdir, 'halfpipe-%09d' % r.id ) ):
            print 'Skipping cleanup for %010d (0x%08x) in %09d due to stream lock' % ( r.startedat, r.startedat, r.id )
            continue
        
        # generate the list of files to be cleaned up
        cleanfiles = []
        cleanfiles += glob.glob( os.path.join( r.outbase, '*-%08x-*.evt' % r.startedat ) )
        cleanfiles += glob.glob( os.path.join( r.outbase, '*-%08x-*.idx' % r.startedat ) )
        cleanfiles += glob.glob( os.path.join( r.outbase, '*', '*-%08x.idx' % r.startedat ) )
        cleanfiles += glob.glob( os.path.join( r.outbase, '*', 'r%010d-e*.idx' % r.startedat ) )

        # is there anything to do?
        if cleanfiles:
            print 'Cleaning up %010d (0x%08x) from %09d in %s:' % ( r.startedat, r.startedat, r.id, r.outbase )
            pass
        else:
            continue

        # might anyone else be using these files?
        if opts.lockdir:
            # check for another stream processing this runid
            if os.path.exists( os.path.join( opts.lockdir, '%08x' % r.startedat ) ):
                print 'Skipping cleanup for %010d (0x%08x) in %09d due to merge lock' % ( r.startedat, r.startedat, r.id )
                continue
            # check for launchOnline in progress
            if os.path.exists( os.path.join( opts.lockdir, 'launchOnline' ) ):
                print 'Skipping cleanup for %010d (0x%08x) in %09d due to online lock' % ( r.startedat, r.startedat, r.id )
                continue

        # OK to clean up
        for f in cleanfiles:
            try:
                if not opts.dryrun: os.unlink( f )
                print '  unlink succeeded for %s' % f
            except:
                print '  unlink FAILED    for %s' % f
else:
    print '\nNothing to clean up\n'
