########################################################################
# $HeadURL $
# File: RequestManagerHandlerTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/19 10:18:23
########################################################################

""" :mod: RequestManagerHandlerTests 
    =======================
 
    .. module: RequestManagerHandlerTests
    :synopsis: unittest for RequestManagerHandler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for RequestManagerHandler
"""

__RCSID__ = "$Id $"

##
# @file RequestManagerHandlerTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/19 10:18:34
# @brief Definition of RequestManagerHandlerTests class.

## imports 
import unittest
## from DIRAC
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient

########################################################################
class RequestManagerHandlerTests(unittest.TestCase):
  """
  .. class:: RequestManagerHandlerTests
  
  """

  def setUp( self ):
    """ test setup

    :param self: self reference
    """
    self.request = Request()
    self.request.RequestName = "test"
    self.operation = Operation()
    self.operation.Type = "replicateAndRegister"
    self.operation.TargetSE = "CERN-USER"
    self.file = File()
    self.file.LFN = "/lhcb/user/c/cibak/testFile"
    self.request.addOperation( self.operation )
    self.operation.addFile( self.file )
    ## xml representation of a whole request
    self.xmlStr = self.request.toXML()
    ## request client
    self.requestClient = RequestClient()

    
  def tearDown( self ):
    """ test case tear down """
    del self.request
    del self.operation
    del self.file
    del self.xmlStr

  def testSetRequest( self ):
    """ test set request """
    put = self.requestClient.putRequest( self.request )
    print put

    pass

## test execution
if __name__ == "__main__":
  gLoader = unittest.TestLoader()
  gSuite = gLoader.loadTestsFromTestCase(RequestManagerHandlerTests)     
  unittest.TextTestRunner(verbosity=3).run(gSuite)

