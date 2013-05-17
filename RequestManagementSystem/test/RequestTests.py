########################################################################
# $HeadURL$
# File: RequestTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 10:23:40
########################################################################

""" :mod: RequestTests
    =======================

    .. module: RequestTests
    :synopsis: test cases for Request class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Request class
"""

__RCSID__ = "$Id$"

# #
# @file RequestTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 10:23:52
# @brief Definition of RequestTests class.

# # imports
import unittest
import datetime
# # from DIRAC
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # SUT
from DIRAC.RequestManagementSystem.Client.Request import Request

########################################################################
class RequestTests( unittest.TestCase ):
  """
  .. class:: RequestTests

  """

  def setUp( self ):
    """ set up """
    self.fromDict = { "RequestName" : "test", "JobID" : 12345 }

  def tearDown( self ):
    """ tear down """
    del self.fromDict

  def test01CtorSerilization( self ):
    """ test c'tor and serialization """
    # # empty c'tor
    req = Request()
    self.assertEqual( isinstance( req, Request ), True )
    self.assertEqual( req.JobID, 0 )
    self.assertEqual( req.Status, "Waiting" )

    req = Request( self.fromDict )
    self.assertEqual( isinstance( req, Request ), True )
    self.assertEqual( req.RequestName, "test" )
    self.assertEqual( req.JobID, 12345 )
    self.assertEqual( req.Status, "Waiting" )

    toXML = req.toXML()
    self.assertEqual( toXML["OK"], True )

    req = Request.fromXML( toXML["Value"]
                            )
    self.assertEqual( req["OK"], True )
    self.assertEqual( isinstance( req["Value"], Request ), True )
    req = req["Value"]

    self.assertEqual( req.RequestName, "test" )
    self.assertEqual( req.JobID, 12345 )
    self.assertEqual( req.Status, "Waiting" )

    toSQL = req.toSQL()
    self.assertEqual( toSQL["OK"], True )
    toSQL = toSQL["Value"]
    self.assertEqual( toSQL.startswith( "INSERT" ), True )

    req.RequestID = 1

    toSQL = req.toSQL()
    self.assertEqual( toSQL["OK"], True )
    toSQL = toSQL["Value"]
    self.assertEqual( toSQL.startswith( "UPDATE" ), True )

  def test02Props( self ):
    """ test props """
    # # valid values
    req = Request()

    req.RequestID = 1
    self.assertEqual( req.RequestID, 1 )

    req.RequestName = "test"
    self.assertEqual( req.RequestName, "test" )

    req.JobID = 1
    self.assertEqual( req.JobID, 1 )
    req.JobID = "1"
    self.assertEqual( req.JobID, 1 )

    req.CreationTime = "1970-01-01 00:00:00"
    self.assertEqual( req.CreationTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )
    req.CreationTime = datetime.datetime( 1970, 1, 1, 0, 0, 0 )
    self.assertEqual( req.CreationTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )

    req.SubmitTime = "1970-01-01 00:00:00"
    self.assertEqual( req.SubmitTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )
    req.SubmitTime = datetime.datetime( 1970, 1, 1, 0, 0, 0 )
    self.assertEqual( req.SubmitTime, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )

    req.LastUpdate = "1970-01-01 00:00:00"
    self.assertEqual( req.LastUpdate, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )
    req.LastUpdate = datetime.datetime( 1970, 1, 1, 0, 0, 0 )
    self.assertEqual( req.LastUpdate, datetime.datetime( 1970, 1, 1, 0, 0, 0 ) )

  def test04Operations( self ):
    """ test operations arithemtic and state machine """
    req = Request()
    self.assertEqual( len( req ), 0 )

    transfer = Operation()
    transfer.Type = "ReplicateAndRegister"
    transfer.addFile( File( { "LFN" : "/a/b/c", "Status" : "Waiting" } ) )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], None )

    req.addOperation( transfer )
    self.assertEqual( len( req ), 1 )
    self.assertEqual( transfer.Order, req.Order )
    self.assertEqual( transfer.Status, "Waiting" )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], transfer )

    removal = Operation( { "Type" : "RemoveFile" } )
    removal.addFile( File( { "LFN" : "/a/b/c", "Status" : "Waiting" } ) )

    req.insertBefore( removal, transfer )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], removal )

    self.assertEqual( len( req ), 2 )
    self.assertEqual( [ op.Status for op in req ], ["Waiting", "Queued"] )
    self.assertEqual( req.subStatusList() , ["Waiting", "Queued"] )


    self.assertEqual( removal.Order, 0 )
    self.assertEqual( removal.Order, req.Order )

    self.assertEqual( transfer.Order, 1 )

    self.assertEqual( removal.Status, "Waiting" )
    self.assertEqual( transfer.Status, "Queued" )

    for subFile in removal:
      subFile.Status = "Done"
    removal.Status = "Done"

    self.assertEqual( removal.Status, "Done" )

    self.assertEqual( transfer.Status, "Waiting" )
    self.assertEqual( transfer.Order, req.Order )

    # # len, looping
    self.assertEqual( len( req ), 2 )
    self.assertEqual( [ op.Status for op in req ], ["Done", "Waiting"] )
    self.assertEqual( req.subStatusList() , ["Done", "Waiting"] )

    digest = req.toJSON()
    self.assertEqual( digest["OK"], True )

    getWaiting = req.getWaiting()
    self.assertEqual( getWaiting["OK"], True )
    self.assertEqual( getWaiting["Value"], transfer )

  def test05FTS( self ):
    """ FTS state machine """
    req = Request()
    req.RequestName = "FTSTest"

    ftsTransfer = Operation()
    ftsTransfer.Type = "ReplicateAndRegister"
    ftsTransfer.TargetSE = "CERN-USER"

    ftsFile = File()
    ftsFile.LFN = "/a/b/c"
    ftsFile.Checksum = "123456"
    ftsFile.ChecksumType = "Adler32"

    ftsTransfer.addFile( ftsFile )
    req.addOperation( ftsTransfer )

    self.assertEqual( req.Status, "Waiting", "1. wrong request status: %s" % req.Status )

    ftsFile.Status = "Scheduled"

    self.assertEqual( ftsTransfer.Status, "Scheduled", "2. wrong status for ftsTransfer: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Scheduled", "2. wrong status for request: %s" % req.Status )

    insertBefore = Operation()
    insertBefore.Type = "RegisterReplica"
    insertBefore.TargetSE = "CERN-USER"
    insertFile = File()
    insertFile.LFN = "/a/b/c"
    insertFile.PFN = "http://foo/bar"
    insertBefore.addFile( insertFile )

    req.insertBefore( insertBefore, ftsTransfer )

    self.assertEqual( insertBefore.Status, "Waiting", "3. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Scheduled", "3. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Waiting", "3. wrong status for request: %s" % req.Status )

    insertFile.Status = "Done"

    self.assertEqual( insertBefore.Status, "Done", "4. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Scheduled", "4. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Scheduled", "4. wrong status for request: %s" % req.Status )

    ftsFile.Status = "Waiting"

    self.assertEqual( insertBefore.Status, "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Waiting", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Waiting", "5. wrong status for request: %s" % req.Status )

    ftsFile.Status = "Done"

    self.assertEqual( insertBefore.Status, "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status )
    self.assertEqual( ftsTransfer.Status, "Done", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status )
    self.assertEqual( req.Status, "Done", "5. wrong status for request: %s" % req.Status )



# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( RequestTests )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
