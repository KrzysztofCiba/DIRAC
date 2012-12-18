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
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
## SUT
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator


########################################################################
class RequestValidatorTests(unittest.TestCase):
  """
  .. class:: RequestValidatorTests
  
  """

  def setUp( self ):
    """ test setup """
    self.request = Request()
    self.operation = Operation()
    self.file = File()

  def tearDown( self ):
    """ test tear down """
    del self.request
    del self.operation
    del self.file

  def testValidator( self ):
    """ validator test """
    
    ## create validator
    validator = RequestValidator()
    self.assertEqual( isinstance( validator, RequestValidator ), True )

    ## RequestName not set 
    ret = validator.validate( self.request )
    self.assertEqual( ret, { 'Message' : 'RequestName not set', 
                             'OK' : False } )
    self.request.RequestName = "test_request"

    ## no operations 
    ret = validator.validate( self.request )
    self.assertEqual( ret, { 'Message' : "Operations not present in request 'test_request'", 
                             'OK': False} )        
    self.request.addOperation( self.operation )

    ## type not set
    ret = validator.validate( self.request )
    self.assertEqual( ret, { 'Message' : "Operation #0 in request 'test_request' hasn't got Type set", 
                             'OK' : False } )
    self.operation.Type = "replicateAndRegister"

    ## files not present 
    ret = validator.validate( self.request )
    self.assertEqual( ret, { 'Message' : "Operation #0 of type 'replicateAndRegister' hasn't got files to process.", 
                             'OK' : False } )
    self.operation.addFile( self.file ) 

    ## targetSE not set
    ret = validator.validate( self.request )
    self.assertEqual( ret,  { 'Message' : "Operation #0 of type 'replicateAndRegister' is missing TargetSE attribute.", 
                              'OK': False } )
    self.operation.TargetSE = "CERN-USER"

    ## missing LFN
    ret = validator.validate( self.request )
    self.assertEqual( ret,  
                      { "Message" : "Operation #0 of type 'replicateAndRegister' is missing LFN attribute for file.", 
                        "OK": False} )
    self.file.LFN = "/a/b/c"

    ## all OK
    ret = validator.validate( self.request )
    self.assertEqual( ret, {'OK': True, 'Value': ''} )

    
## test suite execution 
if __name__ == "__main__":
  gTestLoader = unittest.TestLoader()
  gSuite = gTestLoader.loadTestsFromTestCase( RequestValidatorTests )
  gSuite = unittest.TestSuite( [ gSuite ] )
  unittest.TextTestRunner(verbosity=3).run( gSuite )

