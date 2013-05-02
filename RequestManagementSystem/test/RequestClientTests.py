########################################################################
# $HeadURL $
# File: RequestClientTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/01/11 10:37:11
########################################################################
""" :mod: RequestClientTests 
    ========================
 
    .. module: RequestClientTests
    :synopsis: 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com
"""

__RCSID__ = "$Id $"

##
# @file RequestClientTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/01/11 10:37:26
# @brief Definition of RequestClientTests class.

## imports 
import unittest
import time
## SUT
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
## from DIRAC 
from DIRAC import gLogger, gConfig
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class RequestClientTests(unittest.TestCase):
  """
  .. class:: RequestClientTests

  test case for RequestClient
  """
  
  def setUp( self ):
    """ test case set up """
    self.request = Request( { "RequestName" : "requestClientTest", "JobID" : 2  } )
    self.operation = Operation( { "Type" : "removeFile", "TargetSE" : "CERN-USER" } )
    self.file = File( { "LFN" : "/a/b/c" } )
    self.operation.addFile( self.file )
    self.request.addOperation(  self.operation )

    gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
    gConfig.setOptionValue( '/DIRAC/Setups/Test/RequestManagement', 'Test' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/Host', 'localhost' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/DBName', 'ReqDB' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/User', 'Dirac' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/URLs/RequestManager', 
                            'dips://volhcb13.cern.ch:9143/RequestManagement/RequestManager' )

  def tearDown( self ):
    """ test case tear down """
    del self.request
    del self.operation
    del self.file

  def test01Ctor( self ):
    """ ctor test """
    requestClient = RequestClient()
    self.assertEqual( isinstance( requestClient, RequestClient ), True  )

  def test01PutRequest( self ):
    """ put request """
    requestClient = RequestClient()

    ## put request
    start = time.clock()
    ret = requestClient.putRequest( self.request )
    print time.clock() - start
    self.assertEqual( ret["OK"], True )

  def test02PeekRequest( self ):
    """ peek request """
    requestClient = RequestClient()
    
    # # get requests names
    start = time.clock()
    ret = requestClient.getRequestNamesForJobs( [ self.request.JobID] )
    print time.clock() - start
    self.assertEqual( ret["OK"], True )
    self.assertEqual( ret["Value"][self.request.JobID], self.request.RequestName )

    ## read request for jobs 
    ret = requestClient.readRequestsForJobs( [ self.request.JobID ] )
    self.assertEqual( ret["OK"], True )
    self.assertEqual( ret["Value"][self.request.JobID]["OK"], True )
    
    ## get digest
    ret = requestClient.getDigest( self.request.RequestName )
    self.assertEqual( ret["OK"], True )
    self.assertEqual( type(ret["Value"]), str )
    
    ## get db summary 
    ret = requestClient.getDBSummary()
    self.assertEqual( ret["OK"], True )
    self.assertEqual( ret["Value"], { 'Operation': { 'removeFile': { 'Waiting': 1L } },
                                      'Request': { 'Waiting': 1L },
                                      'File': { 'Waiting': 1L } } )
    
  def test03GetRequest( self ):
    """ get request """
    requestClient = RequestClient()
    ## get request
    start = time.clock()
    ret = requestClient.getRequest()
    print time.clock() - start 
    self.assertEqual( ret["OK"], True )
    self.assertEqual( isinstance( ret["Value"], Request), True )

  def test04DeleteRequest( self ):
    """ delete request """
    requestClient = RequestClient()
    ## delete request
    start = time.clock()
    ret = requestClient.deleteRequest( self.request.RequestName )
    print time.clock() - start
    self.assertEqual( ret["OK"], True )

    ## should be empty now
    ret = requestClient.getDBSummary()
    self.assertEqual( ret["OK"], True )
    self.assertEqual( ret["Value"], { 'Operation': {} ,
                                      'Request': {},
                                      'File': {}  } )              
    
## test execution
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( RequestClientTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner(verbosity=3).run( gSuite )
