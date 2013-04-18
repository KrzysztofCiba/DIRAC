########################################################################
# $HeadURL $
# File: ForwardDISETTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/18 09:23:05
########################################################################

""" :mod: ForwardDISETTests 
    =======================
 
    .. module: ForwardDISETTests
    :synopsis: unittest for ForwardDISET handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for ForwardDISET handler
"""

__RCSID__ = "$Id $"

##
# @file ForwardDISETTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/18 09:23:18
# @brief Definition of ForwardDISETTests class.

# # imports 
import unittest
import mock
# # from DIRAC 
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
# # SUT
from DIRAC.RequestManagementSystem.Agent.RequestOperations.ForwardDISET import ForwardDISET



########################################################################
class ForwardDISETTests(unittest.TestCase ):
  """
  .. class:: ForwardDISETTests
  
  """
  def setUp( self ):
    """ test set up """
    
    self.hiArgs = ( ( 'WorkloadManagement/JobStateUpdate', 
                       { 'skipCACheck': False, 
                         'delegatedDN': '/DC=es/DC=irisgrid/O=ecm-ub/CN=Ricardo-Graciani-Diaz', 
                         'keepAliveLapse': 150, 
                         'timeout': 120, 
                         'delegatedGroup': 'lhcb_mc' } ), 
                      'fooBar', ( 47110968, { '2013-04-13 11:04:21.108434': 
                                              { 'Status': 'Failed', 
                                                'ApplicationStatus': '', 
                                                'MinorStatus': 'Received Kill signal', 
                                                'Source': 'JobWrapper' }, 
                                              '2013-04-13 11:04:21.556816': 
                                              { 'Status': 'Failed', 
                                                'ApplicationStatus': '', 
                                                'MinorStatus': '', 
                                                'Source': 'JobWrapper'} } ) ) 
    self.req = Request( { "RequestName": "testRequest" } ) 
    self.op = Operation( { "Type": "ForwardDISET", 
                           "Arguments": DEncode.encode( self.hiArgs ) } )
    self.req += self.op
    
  def tearDown( self ):
    """ tear down """
    del self.hiArgs
    del self.op
    del self.req

  def testCase( self ):
    """ ctor and functionality """
    forwardDISET = None
    try:
      forwardDISET = ForwardDISET()
    except Exception, error:
      pass
    self.assertEqual( isinstance( forwardDISET, ForwardDISET ), True, "construction error" )

    forwardDISET.setOperation( self.op )
    self.assertEqual( isinstance( forwardDISET.operation, Operation ), True, "setOperation error" )

    call = forwardDISET()
    self.assertEqual( call["OK"], False, "call failed" )


if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  forwardDISETTests = testLoader.loadTestsFromTestCase( ForwardDISETTests )
  suite = unittest.TestSuite( [ forwardDISETTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )


