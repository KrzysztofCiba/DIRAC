########################################################################
# $HeadURL $
# File: RequestDBTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/19 20:22:16
########################################################################

""" :mod: RequestDBTests 
    =======================
 
    .. module: RequestDBTests
    :synopsis: unittest for RequestDB 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for RequestDB 
"""

__RCSID__ = "$Id $"

##
# @file RequestDBTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/19 20:22:29
# @brief Definition of RequestDBTests class.

## imports 
import unittest
## from DIRAC
from DIRAC import gConfig, gLogger
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
## SUT
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB

########################################################################
class RequestDBTests(unittest.TestCase):
  """
  .. class:: RequestDBTests
  unittest for RequestDB
  """

  def setUp( self ):
    """ test case setup """
    self.request = Request( { "RequestName" : "test1" } )
    self.operation1 = Operation( { "Type" : "replicateAndRegister", "TargetSE" : "CERN-USER" } )
    self.file = File( { "LFN" : "/a/b/c" } )
    self.request.addOperation( self.operation1 )
    self.operation1.addFile( self.file  )
    self.operation2 = Operation()
    self.operation2.Type = "removeFile"
    self.operation2.addFile( File( { "LFN" : "/c/d/e" } ) )
    self.request.addOperation( self.operation2 )

    ### set some defaults
    gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
    gConfig.setOptionValue( '/DIRAC/Setups/Test/RequestManagement', 'Test' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/Host', 'localhost' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/DBName', 'ReqDB' )
    gConfig.setOptionValue( '/Systems/RequestManagement/Test/Databases/ReqDB/User', 'Dirac' )

  def tearDown( self ):
    """ test case tear down """
    del self.file
    del self.operation1
    del self.operation2 
    del self.request

  def testTableDesc( self ):
    """ table description """
    tableDict = RequestDB.getTableMeta()
    self.assertEqual( "Request" in tableDict, True )
    self.assertEqual( "Operation" in tableDict, True )
    self.assertEqual( "File" in tableDict, True )
    self.assertEqual( tableDict["Request"], Request.tableDesc() )
    self.assertEqual( tableDict["Operation"], Operation.tableDesc() )
    self.assertEqual( tableDict["File"], File.tableDesc() )

  def testRequestRW( self ):
    """ db r/w requests """
    db = RequestDB()
    db._checkTables( True )

    ## insert 
    ret = db.putRequest( self.request )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )

    ## select
    ret = db.getRequest()
    self.assertEqual( ret["OK"], True )

    request = ret["Value"]
    self.assertEqual( isinstance( request, Request), True )

    ## update 
    ret = db.putRequest( request )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )

    r = db.getDBSummary()
    print r
    ## delete 
    ret = db.deleteRequest( self.request.RequestName )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )
    
  def testDBSummary( self ):
    """ test getDBSummary """
    db = RequestDB()
    r = db.getDBSummary()




## test suite execution 
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( RequestDBTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner(verbosity=3).run( gSuite )
