########################################################################
# $HeadURL $
# File: RequestTaskTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/27 15:59:24
########################################################################
""" :mod: RequestTaskTests
    =======================

    .. module: RequestTaskTests
    :synopsis: test cases for RequestTask class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for RequestTask class
"""
__RCSID__ = "$Id $"
# #
# @file RequestTaskTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/27 15:59:40
# @brief Definition of RequestTaskTests class.
# # imports
import unittest
# # SUT
from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask
# # from DIRAC
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation

########################################################################
class RequestTaskTests( unittest.TestCase ):
  """
  .. class:: RequestTaskTests

  """

  def setUp( self ):
    """ test case set up """
    self.handlerDict = { "ForwardDISET" : "DIRAC/RequestManagementSystem/private/ForwardDISET" }
    self.req = Request()
    self.req.RequestName = "foobarbaz"
    self.op = Operation( { "Type": "ForwardDISET", "Arguments" : "foobar" } )
    self.req.addOperation( self.op )

    self.task = None  # RequestTask( self.req.toXML()["Value"], self.handlerDict )

  def tearDown( self ):
    """ test case tear down """
    del self.req
    del self.op
    del self.task

  def testAPI( self ):
    """ test API """
    self.task = RequestTask( self.req.toXML()["Value"], self.handlerDict )
    ret = self.task()
    print ret

# # tests execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  requestTaskTests = testLoader.loadTestsFromTestCase( RequestTaskTests )
  suite = unittest.TestSuite( [ requestTaskTests ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
