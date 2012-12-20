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
    """ test setup """
    self.request = Request( { "RequestName" : "testRequest" } )
    self.operation = Operation( { "Type" : "replicateAndRegister" } )
    self.file = File( { "LFN" : "/a/b/c" } )
    
    self.request.addOperation( self.operation )
    self.operation.addFile( self.file  )


  def tearDown( self ):
    """ test tear down """
    del self.file
    del self.operation
    del self.request

  def testTableDesc( self ):
    
    tableDict = RequestDB.getTableMeta()
    print tableDict
    pass

## test suite execution 
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( RequestDBTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner(verbosity=3).run( gSuite )
