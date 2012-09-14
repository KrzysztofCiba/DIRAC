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

##
# @file RequestTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 10:23:52
# @brief Definition of RequestTests class.

## imports 
import unittest
## from DIRAC
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest
import datetime
## SUT
from DIRAC.RequestManagementSystem.Client.Request import Request

########################################################################
class RequestTests(unittest.TestCase):
  """
  .. class:: RequestTests
  
  """

  def setUp( self ):
    """ set up """  
    self.fromDict = { "RequestName" : "test", "JobID" : 12345 }


  def tearDown( self ):
    """ tear down """
    del self.fromDict

  def test_ctor( self ):
    """ test c'tor and serialisation """
    ## empty c'tor
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
    
    req = Request.fromXML( toXML["Value"] )
    self.assertEqual( req["OK"], True )
    self.assertEqual( isinstance( req["Value"], Request ), True )
    req = req["Value"]
    self.assertEqual( req.RequestName, "test" )
    self.assertEqual( req.JobID, 12345 )
    self.assertEqual( req.Status, "Waiting" )

    toSQL = req.toSQL()
    self.assertEqual( toSQL.startswith("INSERT"), True )
    req.RequestID = 1 
    toSQL = req.toSQL()
    self.assertEqual( toSQL.startswith("UPDATE"), True )

  def test_props( self ):
    """ test props """
    ## valid values
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
    self.assertEqual( req.CreationTime, datetime.datetime( 1970, 1, 1, 0, 0, 0) )
    req.CreationTime = datetime.datetime( 1970, 1, 1, 0, 0, 0)
    self.assertEqual( req.CreationTime, datetime.datetime( 1970, 1, 1, 0, 0, 0) )

    req.SubmissionTime = "1970-01-01 00:00:00"
    self.assertEqual( req.SubmissionTime, datetime.datetime( 1970, 1, 1, 0, 0, 0) )
    req.SubmissionTime = datetime.datetime( 1970, 1, 1, 0, 0, 0)
    self.assertEqual( req.SubmissionTime, datetime.datetime( 1970, 1, 1, 0, 0, 0) )

    req.LastUpdate = "1970-01-01 00:00:00"
    self.assertEqual( req.LastUpdate, datetime.datetime( 1970, 1, 1, 0, 0, 0) )
    req.LastUpdate = datetime.datetime( 1970, 1, 1, 0, 0, 0)
    self.assertEqual( req.LastUpdate, datetime.datetime( 1970, 1, 1, 0, 0, 0) )

  def test_subreq( self ):
    """ test subrequest's arithemtic and state machine """
    req = Request()
    self.assertEqual( len(req), 0 )
    
    transfer = SubRequest()
    transfer.RequestType = "transfer"
    transfer.Operation = "replicateAndRegister"

    req.addSubRequest( transfer )
    self.assertEqual( len(req), 1 )
    self.assertEqual( transfer.ExecutionOrder, req.currentExecutionOrder()["Value"] )
    self.assertEqual( transfer.Status, "Waiting" )

    removal = SubRequest( { "RequestType": "removal", "Operation" : "removeFile" } )
    req.insertBefore( removal, transfer )

    self.assertEqual( removal.ExecutionOrder, 0 )
    self.assertEqual( removal.ExecutionOrder, req.currentExecutionOrder()["Value"] )

    self.assertEqual( transfer.ExecutionOrder, 1 )

    self.assertEqual( removal.Status, "Waiting" )
    self.assertEqual( transfer.Status, "Queued" )

    removal.Status = "Done"
    self.assertEqual( removal.Status, "Done" )

    self.assertEqual( transfer.Status, "Waiting" )
    self.assertEqual( transfer.ExecutionOrder, req.currentExecutionOrder()["Value"] )



  
## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase(RequestTests)     
  unittest.TextTestRunner(verbosity=3).run(suite)


