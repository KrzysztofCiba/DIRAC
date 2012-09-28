########################################################################
# $HeadURL $
# File: RequestValidatorTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/25 13:49:20
########################################################################

""" :mod: RequestValidatorTests 
    =======================
 
    .. module: RequestValidatorTests
    :synopsis: test cases for RequestValidator
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for RequestValidator
"""

__RCSID__ = "$Id $"

##
# @file RequestValidatorTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/25 13:49:31
# @brief Definition of RequestValidatorTests class.

## imports 
import unittest
## from DIRAC
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.SubRequest import SubRequest
from DIRAC.RequestManagementSystem.Client.SubReqFile import SubReqFile
## SUT
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator


########################################################################
class RequestValidatorTests(unittest.TestCase):
  """
  .. class:: RequestValidatorTests
  
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    self.request = Request()
    self.subReq = SubRequest()
    self.file = SubReqFile()

  def testValidator( self ):
    """ validator test """
    validator = RequestValidator()
    
    ## RequestName not set 
    ret = validator.validate( self.request )
    self.assertEqual( ret, {'Message': 'RequestName not set', 
                            'OK': False} )
    self.request.RequestName = "test_request"

    ## no subRequests 
    ret = validator.validate( self.request )
    self.assertEqual( ret, {'Message': "SubRequests are not present in request 'test_request'", 
                            'OK': False} )        
    self.request.addSubRequest( self.subReq )

    ## no RequestType
    ret = validator.validate( self.request )
    self.assertEqual( ret, {'Message': "SubRequest #0 hasn't got a proper RequestType set", 
                            'OK': False} )
    self.subReq.RequestType = "transfer"

    ## no Operation
    ret = validator.validate( self.request )
    self.assertEqual( ret, {'Message': "SubRequest #0 hasn't got a proper Operation set", 
                            'OK': False} )
    self.subReq.Operation = "replicateAndRegister"

    ## files not present 
    ret = validator.validate( self.request )
    self.assertEqual( ret, {'Message': "SubRequest #0 of type 'transfer' hasn't got files to process", 
                            'OK': False} )
    self.subReq += self.file 

    ret = validator.validate( self.request )
    self.assertEqual( ret,  {'Message': 'SubRequest #0 of type transfer and operation replicateAndRegister is missing TargetSE attribute.', 
                             'OK': False} )

    self.subReq.TargetSE = "CERN-USER"
    ret = validator.validate( self.request )
    self.assertEqual( ret,  {'Message': 'SubRequest #0 of type transfer and operation replicateAndRegister is missing LFN attribute for file.', 
                             'OK': False} )

    self.file.LFN = "/a/b/c"
    ret = validator.validate( self.request )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )

    
## test suite execution 
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( RequestValidatorTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

