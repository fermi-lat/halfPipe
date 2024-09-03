#!/usr/bin/env python
#
#                               Copyright 2007
#                                     by
#                        The Board of Trustees of the
#                     Leland Stanford Junior University.
#                            All rights reserved.
#

__facility__ = "ISOC"
__abstract__ = "Get algorithm name then queue for pipeline online analysis from acquisition run-start-time"
__author__   = "Bryson Lee <blee@slac.stanford.edu>"
__date__     = "2008/03/05 15:00:00"
__updated__  = "2008/03/12 14:54:45"
__version__  = "1.2"
__release__  = "v6r0p0"
__credits__  = "SLAC"


# import quarks.legal.copyright
import sys

import sqlalchemy as SA

from quarks.cmdline.xoptparse import OptionParser
from quarks.database.dbconfig import DbConfig

from LICOS_Scripts.analysis.pipeline.KeyToAlgAndQueue import KeyToAlgAndQueue

from ISOC import SiteDep

def main():
    # command line
    parser = OptionParser()
    parser.add_option( '-d', '--dbi', default=SiteDep.get( 'DEFAULT', 'dbi' ),
                       help='Database connection handle (%default)' )
    parser.add_option( '-s', '--scid', type='hexordec', default=SiteDep.get( 'RawArchive', 'scid' ),
                       help='spacecraft id (%default)' )
    parser.add_option( '-t', '--started', type='hexordec',
                       help='start-time of the acquisition' )
    parser.add_option( '-o', '--outfile',
                       help='name of output file for result' )
    opts, args = parser.parse_args()

    # connect to the database and grab the acq-summary table
    db = DbConfig.fromConfigParser( SiteDep, opts.dbi )
    acqtbl = SA.Table( 'glastops_acqsummary', db.metadata, autoload=True )

    # get the forwarded MOOT key for this acquisition
    acq = acqtbl.select( SA.and_( acqtbl.c.startedat == opts.started, acqtbl.c.scid == opts.scid  ) ).execute().fetchone()

    # only operate on LCI acquisitions for now
    if acq.type == 'LCI':
        # get the algorithm and batch-queue for this acq
        ktaaq = KeyToAlgAndQueue( acq.moot_key )

        # write out the result
        if opts.outfile:
            ofd = open( opts.outfile, 'w' )
        else:
            ofd = sys.stdout
        print >> ofd, 'r%010d %d %s %s' % ( opts.started, acq.moot_key, ktaaq.getAlgorithm(), ktaaq.getQueue() )

    return 0

if __name__ == '__main__':
    import sys, traceback
    try:
        rc = main()
        sys.exit( rc )
    except SystemExit:
        raise
    except:
        traceback.print_exc( file=sys.stderr )
        sys.exit(1)



                       
